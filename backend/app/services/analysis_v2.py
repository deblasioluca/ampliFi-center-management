"""V2 Analysis service — CEMA migration decision tree.

Builds CenterContext from cc_with_hierarchy upload data + balance + FTE data,
runs the V2 pipeline, assigns PC/CC IDs, and stores proposals.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.domain.decision_tree.context import CenterContext
from app.domain.decision_tree.engine import (
    PipelineEngine,
)
from app.domain.decision_tree.registry import get_registry
from app.domain.decision_tree.v2_id_assignment import assign_v2_ids
from app.models.core import (
    AnalysisConfig,
    AnalysisRun,
    Balance,
    CenterProposal,
    LegacyCostCenter,
    RoutineOutput,
    Wave,
    WaveEntity,
)

log = logging.getLogger(__name__)

# Default V2 pipeline config
V2_DEFAULT_CONFIG: dict = {
    "version": 2,
    "pipeline": [
        {"routine": "v2.retire_flag", "enabled": True, "params": {"retire_pattern": "_RETIRE"}},
        {"routine": "v2.balance_migrate", "enabled": True, "params": {}},
        {
            "routine": "v2.pc_approach",
            "enabled": True,
            "params": {
                "approach_rules": [],
                "default_approach": "1:1",
            },
        },
        {"routine": "v2.combine_migration", "enabled": True, "params": {}},
    ],
    "id_assignment": {
        "pc_prefix": "P",
        "cc_prefix": "C",
        "pc_start": 137,
        "cc_start": 1,
        "id_width": 5,
    },
}


def _build_v2_context(
    cc: LegacyCostCenter,
    db: Session,
) -> CenterContext:
    """Build a CenterContext with V2 fields from cc_with_hierarchy data."""
    attrs = cc.attrs or {}

    # Extract CEMA hierarchy levels from attrs (stored by cc_with_hierarchy loader)
    ext_levels: dict[str, str] = {}
    ext_descs: dict[str, str] = {}
    cema_levels: dict[str, str] = {}
    cema_descs: dict[str, str] = {}

    for i in range(14):
        key = f"ext_l{i}"
        if key in attrs:
            ext_levels[key] = str(attrs[key] or "")
        desc_key = f"ext_l{i}_desc"
        if desc_key in attrs:
            ext_descs[desc_key] = str(attrs[desc_key] or "")

    for i in range(12):
        key = f"cema_l{i}"
        if key in attrs:
            cema_levels[key] = str(attrs[key] or "")
        desc_key = f"cema_l{i}_desc"
        if desc_key in attrs:
            cema_descs[desc_key] = str(attrs[desc_key] or "")

    ext_hierarchy = str(attrs.get("external_hierarchy", ""))
    cema_hierarchy = str(attrs.get("cema_hierarchy", ""))

    # Balance aggregates by category
    balance_by_category: dict[str, float] = {}
    bal_q = (
        select(
            Balance.account_class,
            func.sum(Balance.tc_amt).label("amt"),
        )
        .where(Balance.coarea == cc.coarea, Balance.cctr == cc.cctr)
        .group_by(Balance.account_class)
    )
    for row in db.execute(bal_q).all():
        cls = (row.account_class or "").upper().strip()
        amt = float(row.amt or 0)
        if cls:
            balance_by_category[cls] = amt

    # Aggregate amounts
    total = sum(abs(v) for v in balance_by_category.values())
    bs_amt = sum(
        abs(balance_by_category.get(c, 0.0))
        for c in ("ASSET", "LIABILITY", "EQUITY", "BS", "BALANCE_SHEET")
    )
    rev_amt = abs(balance_by_category.get("REV", 0.0)) + abs(
        balance_by_category.get("REVENUE", 0.0)
    )
    opex_amt = sum(
        abs(balance_by_category.get(c, 0.0))
        for c in ("DIRECT_COST", "OPEX", "EXPENSE", "EXP", "HARD_ALLOC", "ALLOC_COST")
    )

    # FTE info
    fte_count = 0.0
    fte_flag = attrs.get("tbl_ftes__cc.fte_count") or attrs.get("fte_count")
    if fte_flag:
        import contextlib

        with contextlib.suppress(ValueError, TypeError):
            fte_count = float(fte_flag)
    has_ftes = fte_count > 0

    # Owner
    responsible = cc.responsible or ""
    has_owner = bool(responsible.strip())

    return CenterContext(
        center_id=cc.id,
        coarea=cc.coarea or "",
        cctr=cc.cctr or "",
        ccode=cc.ccode or "",
        txtsh=cc.txtsh or "",
        txtmi=cc.txtmi or "",
        responsible=responsible,
        currency=cc.currency or "",
        cctrcgy=cc.cctrcgy or "",
        pctr=cc.pctr or "",
        is_active=cc.is_active if cc.is_active is not None else True,
        bs_amt=bs_amt,
        rev_amt=rev_amt,
        opex_amt=opex_amt,
        total_balance=total,
        has_owner=has_owner,
        ext_levels=ext_levels,
        ext_descs=ext_descs,
        cema_levels=cema_levels,
        cema_descs=cema_descs,
        ext_hierarchy=ext_hierarchy,
        cema_hierarchy=cema_hierarchy,
        balance_by_category=balance_by_category,
        has_ftes=has_ftes,
        fte_count=fte_count,
        attrs=attrs,
    )


def run_v2_analysis(
    wave_id: int | None,
    config_id: int | None,
    db: Session,
    user_id: int | None = None,
    *,
    mode: str = "simulation",
    label: str | None = None,
    excluded_scopes: list[str] | None = None,
) -> dict:
    """Run V2 CEMA migration analysis on a wave or globally.

    mode: 'simulation' uses temp CT/PT IDs, 'activated' uses real P/C IDs.
    wave_id=None runs on all centers (global simulation).
    excluded_scopes: list of wave IDs whose centers to exclude (for global mode).
    Returns summary with proposal counts, ID ranges, etc.
    """
    wave = None
    if wave_id:
        wave = db.get(Wave, wave_id)
        if not wave:
            raise ValueError(f"Wave {wave_id} not found")

    # Load or create config
    config_data = dict(V2_DEFAULT_CONFIG)
    if config_id:
        ac = db.get(AnalysisConfig, config_id)
        if not ac:
            raise ValueError(f"Config {config_id} not found")
        if ac.config:
            config_data = ac.config
    else:
        # Create default V2 config
        existing = db.execute(
            select(AnalysisConfig)
            .where(AnalysisConfig.code == "cema_migration_v2")
            .order_by(AnalysisConfig.version.desc())
            .limit(1)
        ).scalar_one_or_none()
        if existing:
            config_id = existing.id
            config_data = existing.config or config_data
        else:
            ac = AnalysisConfig(
                code="cema_migration_v2",
                version=1,
                name="V2 CEMA Migration (default)",
                config=config_data,
                created_by=user_id,
            )
            db.add(ac)
            db.flush()
            config_id = ac.id

    pipeline_config = {"pipeline": config_data.get("pipeline", V2_DEFAULT_CONFIG["pipeline"])}
    id_config = config_data.get("id_assignment", V2_DEFAULT_CONFIG["id_assignment"])

    # Create analysis run
    run = AnalysisRun(
        wave_id=wave_id,
        config_id=config_id,
        status="running",
        started_at=datetime.now(UTC),
        engine_version="v2.cema_migration",
        triggered_by=user_id,
        mode=mode,
        label=label
        or (
            f"V2 {'Simulation' if mode == 'simulation' else 'Activated'}"
            f" {datetime.now(UTC).strftime('%Y-%m-%d %H:%M')}"
        ),
        excluded_scopes=excluded_scopes,
    )
    db.add(run)
    db.flush()

    # Get centers: from wave entities, or all legacy cost centers for global mode
    if wave_id:
        wave_entity_ids = (
            db.execute(select(WaveEntity.entity_id).where(WaveEntity.wave_id == wave_id))
            .scalars()
            .all()
        )
    else:
        wave_entity_ids = []

    base_q = select(LegacyCostCenter).where(LegacyCostCenter.scope == "cleanup")

    # Exclude centers from already-completed waves
    if excluded_scopes:
        from app.models.core import Entity

        excl_entity_ids = (
            db.execute(
                select(WaveEntity.entity_id).where(
                    WaveEntity.wave_id.in_([int(w) for w in excluded_scopes])
                )
            )
            .scalars()
            .all()
        )
        if excl_entity_ids:
            excl_ccodes = (
                db.execute(select(Entity.ccode).where(Entity.id.in_(excl_entity_ids)))
                .scalars()
                .all()
            )
            if excl_ccodes:
                base_q = base_q.where(LegacyCostCenter.ccode.not_in(excl_ccodes))

    if wave_id is not None:
        # Wave-scoped: only include centers from wave entities (empty wave = 0 centers)
        from app.models.core import Entity

        if wave_entity_ids:
            entity_ccodes = (
                db.execute(select(Entity.ccode).where(Entity.id.in_(wave_entity_ids)))
                .scalars()
                .all()
            )
            centers = (
                db.execute(base_q.where(LegacyCostCenter.ccode.in_(entity_ccodes))).scalars().all()
            )
        else:
            centers = []
    else:
        # Global mode: all centers
        centers = db.execute(base_q).scalars().all()

    total = len(centers)
    run.total_centers = total
    db.flush()

    # Run pipeline on each center
    registry = get_registry()
    engine = PipelineEngine(registry)
    migrate_count = 0
    retire_count = 0

    for i, cc in enumerate(centers):
        ctx = _build_v2_context(cc, db)
        results = engine.execute(pipeline_config, ctx)

        # Extract final outcome from combine_migration
        migrate = "N"
        approach = "1:1"
        pc_name = ""
        cc_name = ""
        group_key = ""
        cleansing = "RETIRE"
        target_obj = "NONE"
        rule_path: list[str] = []

        for r in results:
            if r.code == "v2.combine_migration":
                migrate = r.payload.get("migrate", "N")
                approach = r.payload.get("approach", "1:1")
                pc_name = r.payload.get("pc_name", "")
                cc_name = r.payload.get("cc_name", "")
                group_key = r.payload.get("group_key", "")
                cleansing = r.payload.get("cleansing_outcome", "RETIRE")
                target_obj = r.payload.get("target_object", "NONE")
                rule_path = r.payload.get("rule_path", [])

        if not rule_path:
            rule_path = [f"{r.code}:{r.verdict}" for r in results]

        if migrate == "Y":
            migrate_count += 1
        else:
            retire_count += 1

        # Create proposal
        proposal = CenterProposal(
            run_id=run.id,
            legacy_cc_id=cc.id,
            entity_code=cc.ccode,
            cleansing_outcome=cleansing,
            target_object=target_obj,
            rule_path=rule_path,
            confidence=Decimal("1.0"),
            attrs={
                "migrate": migrate,
                "approach": approach,
                "pc_name": pc_name,
                "cc_name": cc_name,
                "group_key": group_key,
                "ext_levels": dict(ctx.ext_levels),
                "ext_descs": dict(ctx.ext_descs),
                "cema_levels": dict(ctx.cema_levels),
                "cema_descs": dict(ctx.cema_descs),
                "ext_hierarchy": ctx.ext_hierarchy,
                "cema_hierarchy": ctx.cema_hierarchy,
                "engine_version": "v2",
            },
        )
        db.add(proposal)

        # Store routine outputs
        for r in results:
            db.add(
                RoutineOutput(
                    run_id=run.id,
                    routine_code=r.code,
                    legacy_cc_id=cc.id,
                    verdict=r.verdict,
                    payload=r.payload,
                )
            )

        if (i + 1) % 500 == 0:
            db.flush()
            log.info("v2.progress: %d/%d", i + 1, total)

    db.flush()

    # Assign IDs — temp (CT/PT) for simulation, real (P/C) for activated
    if mode == "simulation":
        id_result = assign_v2_ids(
            run_id=run.id,
            db=db,
            pc_prefix="PT",
            cc_prefix="CT",
            pc_start=1,
            cc_start=1,
            id_width=5,
        )
    else:
        id_result = assign_v2_ids(
            run_id=run.id,
            db=db,
            pc_prefix=id_config.get("pc_prefix", "P"),
            cc_prefix=id_config.get("cc_prefix", "C"),
            pc_start=id_config.get("pc_start", 137),
            cc_start=id_config.get("cc_start", 1),
            id_width=id_config.get("id_width", 5),
        )

    # Finalize run + wave status atomically
    run.status = "completed"
    run.completed_centers = total
    run.finished_at = datetime.now(UTC)
    run.kpis = {
        "total_centers": total,
        "migrate_yes": migrate_count,
        "migrate_no": retire_count,
        "id_assignment": id_result,
    }
    if wave and wave.status in ("draft", "analysing"):
        wave.status = "analysing"
    db.commit()

    return {
        "run_id": run.id,
        "wave_id": wave_id,
        "mode": mode,
        "label": run.label,
        "total_centers": total,
        "migrate_yes": migrate_count,
        "migrate_no": retire_count,
        "id_assignment": id_result,
    }
