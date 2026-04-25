"""Analysis execution service — runs cleansing + mapping trees on wave centers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.domain.decision_tree.engine import (
    CenterFeatures,
    evaluate_center,
)
from app.models.core import (
    AnalysisConfig,
    AnalysisRun,
    Balance,
    CenterProposal,
    Entity,
    HierarchyLeaf,
    LegacyCostCenter,
    RoutineOutput,
    Wave,
    WaveEntity,
)

log = logging.getLogger(__name__)


def _build_features(
    cc: LegacyCostCenter, db: Session, inactivity_months: int = 24
) -> CenterFeatures:
    """Build feature vector for a single cost center from DB data."""
    now = datetime.now(UTC)

    # Balance aggregates (all-time for totals)
    bal_q = select(
        func.sum(Balance.tc_amt).label("total"),
        func.max(Balance.fiscal_year * 100 + Balance.period).label("last_period"),
    ).where(Balance.coarea == cc.coarea, Balance.cctr == cc.cctr)
    bal_row = db.execute(bal_q).one_or_none()

    total_balance = float(bal_row.total or 0) if bal_row else 0.0

    # Windowed posting count (only within inactivity window)
    cutoff_year = now.year - (inactivity_months // 12)
    cutoff_month = now.month - (inactivity_months % 12)
    if cutoff_month <= 0:
        cutoff_year -= 1
        cutoff_month += 12
    cutoff_period = cutoff_year * 100 + cutoff_month

    window_q = select(
        func.coalesce(func.sum(Balance.posting_count), 0).label("windowed_postings"),
    ).where(
        Balance.coarea == cc.coarea,
        Balance.cctr == cc.cctr,
        (Balance.fiscal_year * 100 + Balance.period) >= cutoff_period,
    )
    windowed_postings = int(db.execute(window_q).scalar() or 0)

    # Calculate months since last posting
    months_since = None
    if bal_row and bal_row.last_period:
        last_yr = int(bal_row.last_period) // 100
        last_mo = int(bal_row.last_period) % 100
        months_since = (now.year - last_yr) * 12 + (now.month - last_mo)

    # Account class split for revenue / opex / B/S
    acct_q = (
        select(
            Balance.account_class,
            func.sum(Balance.tc_amt).label("amt"),
        )
        .where(Balance.coarea == cc.coarea, Balance.cctr == cc.cctr)
        .group_by(Balance.account_class)
    )
    acct_rows = db.execute(acct_q).all()

    rev_amt = 0.0
    opex_amt = 0.0
    bs_amt = 0.0
    for row in acct_rows:
        amt = float(row.amt or 0)
        cls = (row.account_class or "").upper()
        if cls in ("REVENUE", "REV"):
            rev_amt += amt
        elif cls in ("OPEX", "EXPENSE", "EXP"):
            opex_amt += amt
        elif cls in ("BS", "BALANCE_SHEET", "ASSET", "LIABILITY"):
            bs_amt += amt

    # Hierarchy membership count
    hier_count = (
        db.execute(
            select(func.count(HierarchyLeaf.id)).where(HierarchyLeaf.value == cc.cctr)
        ).scalar()
        or 0
    )

    has_owner = bool(cc.responsible and cc.responsible.strip())

    return CenterFeatures(
        coarea=cc.coarea,
        cctr=cc.cctr,
        ccode=cc.ccode or "",
        txtsh=cc.txtsh or "",
        txtmi=cc.txtmi or "",
        responsible=cc.responsible or "",
        currency=cc.currency or "",
        cctrcgy=cc.cctrcgy or "",
        pctr=cc.pctr or "",
        is_active=cc.is_active,
        months_since_last_posting=months_since,
        posting_count_window=windowed_postings,
        bs_amt=bs_amt,
        rev_amt=rev_amt,
        opex_amt=opex_amt,
        total_balance=total_balance,
        hierarchy_membership_count=hier_count,
        has_owner=has_owner,
        attrs=cc.attrs or {},
    )


def execute_analysis(wave_id: int, config_id: int, user_id: int, db: Session) -> AnalysisRun:
    """Execute decision tree analysis on all cost centers in a wave's scope."""
    wave = db.get(Wave, wave_id)
    if not wave:
        raise ValueError(f"Wave {wave_id} not found")

    config = db.get(AnalysisConfig, config_id)
    if not config:
        raise ValueError(f"Config {config_id} not found")

    # Create the analysis run
    run = AnalysisRun(
        wave_id=wave_id,
        config_id=config_id,
        status="running",
        started_at=datetime.now(UTC),
        triggered_by=user_id,
        data_snapshot=f"snapshot_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}",
    )
    db.add(run)
    db.flush()

    # Get cost centers in scope
    entity_ids = select(WaveEntity.entity_id).where(WaveEntity.wave_id == wave_id)
    entity_ccodes = select(Entity.ccode).where(Entity.id.in_(entity_ids))

    if wave.is_full_scope:
        # Full scope: all cost centers, optionally excluding prior waves
        cc_query = select(LegacyCostCenter)
        if wave.exclude_prior:
            prior_entity_ids = (
                select(WaveEntity.entity_id)
                .join(Wave)
                .where(Wave.status != "cancelled", Wave.id != wave_id)
            )
            prior_ccodes = select(Entity.ccode).where(Entity.id.in_(prior_entity_ids))
            cc_query = cc_query.where(LegacyCostCenter.ccode.notin_(prior_ccodes))
    else:
        cc_query = select(LegacyCostCenter).where(LegacyCostCenter.ccode.in_(entity_ccodes))

    cost_centers = db.execute(cc_query).scalars().all()
    params = config.config.get("params", {}) if config.config else {}

    # Track KPI counters
    kpis = {
        "total_centers": len(cost_centers),
        "keep": 0,
        "retire": 0,
        "merge_map": 0,
        "redesign": 0,
        "target_cc": 0,
        "target_pc_only": 0,
        "target_cc_and_pc": 0,
        "target_wbs_real": 0,
        "target_wbs_stat": 0,
        "target_none": 0,
    }

    for cc in cost_centers:
        inactivity_months = params.get("inactivity_threshold_months", 24)
        features = _build_features(cc, db, inactivity_months=inactivity_months)
        result = evaluate_center(features, params)

        # Store routine output
        output = RoutineOutput(
            run_id=run.id,
            routine_code="dt.cleansing+mapping",
            legacy_cc_id=cc.id,
            verdict=result.cleansing.value,
            confidence=Decimal(str(result.confidence)),
            payload={
                "rule_path": result.rule_path,
                "target_object": result.target_object.value if result.target_object else None,
                "merge_into": result.merge_into,
            },
        )
        db.add(output)

        # Store proposal
        proposal = CenterProposal(
            run_id=run.id,
            legacy_cc_id=cc.id,
            cleansing_outcome=result.cleansing.value,
            target_object=result.target_object.value if result.target_object else None,
            merge_into_cctr=result.merge_into,
            rule_path={"steps": result.rule_path},
            confidence=Decimal(str(result.confidence)),
        )
        db.add(proposal)

        # Update KPIs
        outcome_key = result.cleansing.value.lower()
        if outcome_key in kpis:
            kpis[outcome_key] += 1

        if result.target_object:
            target_key = f"target_{result.target_object.value.lower()}"
            if target_key in kpis:
                kpis[target_key] += 1

    run.status = "completed"
    run.finished_at = datetime.now(UTC)
    run.kpis = kpis

    # Update wave status
    if wave.status == "draft":
        wave.status = "analysing"

    db.commit()
    db.refresh(run)
    return run


def get_or_create_default_config(db: Session) -> AnalysisConfig:
    """Get or create the default STD-CLEANSING-V2 analysis config."""
    existing = db.execute(
        select(AnalysisConfig).where(AnalysisConfig.code == "STD-CLEANSING-V2")
    ).scalar_one_or_none()
    if existing:
        return existing

    config = AnalysisConfig(
        code="STD-CLEANSING-V2",
        version=1,
        name="Standard Cleansing Pipeline v2",
        description="Default cleansing + mapping decision tree pipeline",
        status="active",
        config={
            "pipeline": [
                {
                    "routine": "rule.posting_activity",
                    "enabled": True,
                    "params": {"inactivity_threshold_months": 24, "posting_threshold": 0},
                },
                {"routine": "rule.ownership", "enabled": True, "params": {}},
                {
                    "routine": "rule.redundancy",
                    "enabled": True,
                    "params": {"similarity_threshold": 0.92},
                },
                {
                    "routine": "rule.hierarchy_compliance",
                    "enabled": True,
                    "params": {"strict_hierarchy_mode": False},
                },
                {"routine": "rule.cross_system_dependency", "enabled": True, "params": {}},
            ],
            "params": {
                "inactivity_threshold_months": 24,
                "posting_threshold": 0,
                "strict_hierarchy_compliance": False,
            },
        },
    )
    db.add(config)
    db.commit()
    db.refresh(config)
    return config
