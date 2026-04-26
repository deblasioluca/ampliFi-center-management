"""Analysis execution service — runs cleansing + mapping trees on wave centers.

Supports two modes:
  1. Legacy: hardcoded evaluate_center() for backward compatibility
  2. Pipeline: configurable routine pipeline via PipelineEngine (§04.5)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.domain.decision_tree.context import CenterContext
from app.domain.decision_tree.engine import (
    CenterFeatures,
    evaluate_center,
    evaluate_center_with_pipeline,
)
from app.domain.decision_tree.registry import get_registry
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


def _build_context(cc: LegacyCostCenter, db: Session, inactivity_months: int = 24) -> CenterContext:
    """Build CenterContext for the pipeline engine."""
    features = _build_features(cc, db, inactivity_months)
    attrs = features.attrs.copy() if features.attrs else {}
    return CenterContext(
        center_id=cc.id,
        coarea=features.coarea,
        cctr=features.cctr,
        ccode=features.ccode,
        txtsh=features.txtsh,
        txtmi=features.txtmi,
        responsible=features.responsible,
        currency=features.currency,
        cctrcgy=features.cctrcgy,
        pctr=features.pctr,
        is_active=features.is_active,
        months_since_last_posting=features.months_since_last_posting,
        posting_count_window=features.posting_count_window,
        bs_amt=features.bs_amt,
        rev_amt=features.rev_amt,
        opex_amt=features.opex_amt,
        total_balance=features.total_balance,
        hierarchy_membership_count=features.hierarchy_membership_count,
        has_owner=features.has_owner,
        is_feeder=attrs.get("is_feeder", False),
        is_allocation_vehicle=features.is_allocation_vehicle,
        is_project_related=features.is_project_related,
        in_bw_extractors=attrs.get("in_bw_extractors", False),
        in_grc=attrs.get("in_grc", False),
        in_intercompany=attrs.get("in_intercompany", False),
        has_direct_revenue=attrs.get("has_direct_revenue", False),
        has_operational_costs=attrs.get("has_operational_costs", False),
        collects_project_costs=attrs.get("collects_project_costs", False),
        used_for_revenue_allocation=attrs.get("used_for_revenue_allocation", False),
        used_for_cost_allocation=attrs.get("used_for_cost_allocation", False),
        used_for_info_only=attrs.get("used_for_info_only", False),
        duplicate_cluster_id=features.duplicate_cluster_id,
        duplicate_cluster_size=features.duplicate_cluster_size,
        attrs=attrs,
    )


def _scope_query(wave: Wave, db: Session):
    """Build the cost center query based on wave scope."""
    entity_ids = select(WaveEntity.entity_id).where(WaveEntity.wave_id == wave.id)
    entity_ccodes = select(Entity.ccode).where(Entity.id.in_(entity_ids))

    if wave.is_full_scope:
        cc_query = select(LegacyCostCenter)
        if wave.exclude_prior:
            prior_entity_ids = (
                select(WaveEntity.entity_id)
                .join(Wave)
                .where(Wave.status != "cancelled", Wave.id != wave.id)
            )
            prior_ccodes = select(Entity.ccode).where(Entity.id.in_(prior_entity_ids))
            cc_query = cc_query.where(LegacyCostCenter.ccode.notin_(prior_ccodes))
    else:
        cc_query = select(LegacyCostCenter).where(LegacyCostCenter.ccode.in_(entity_ccodes))

    return cc_query


def execute_analysis(wave_id: int, config_id: int, user_id: int, db: Session) -> AnalysisRun:
    """Execute decision tree analysis on all cost centers in a wave's scope."""
    wave = db.get(Wave, wave_id)
    if not wave:
        raise ValueError(f"Wave {wave_id} not found")

    config = db.get(AnalysisConfig, config_id)
    if not config:
        raise ValueError(f"Config {config_id} not found")

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

    cc_query = _scope_query(wave, db)
    cost_centers = db.execute(cc_query).scalars().all()
    params = config.config.get("params", {}) if config.config else {}
    pipeline_config = config.config if config.config else {}

    use_pipeline = bool(pipeline_config.get("pipeline"))

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

    registry = get_registry()
    inactivity_months = params.get("inactivity_threshold_months", 24)

    for cc in cost_centers:
        if use_pipeline:
            ctx = _build_context(cc, db, inactivity_months)
            result = evaluate_center_with_pipeline(ctx, pipeline_config, registry)
        else:
            features = _build_features(cc, db, inactivity_months)
            result = evaluate_center(features, params)

        # Store per-routine outputs
        for rr in result.routine_results:
            output = RoutineOutput(
                run_id=run.id,
                routine_code=rr.code,
                legacy_cc_id=cc.id,
                verdict=rr.verdict,
                confidence=Decimal(str(rr.score)) if rr.score is not None else Decimal("1.0"),
                payload={
                    "reason": rr.reason,
                    "comment": rr.comment,
                    "short_circuit": rr.short_circuit,
                    **(rr.payload or {}),
                },
            )
            db.add(output)

        if not result.routine_results:
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

        proposal = CenterProposal(
            run_id=run.id,
            legacy_cc_id=cc.id,
            entity_code=cc.ccode or "",
            cleansing_outcome=result.cleansing.value,
            target_object=result.target_object.value if result.target_object else None,
            merge_into_cctr=result.merge_into,
            rule_path={"steps": result.rule_path},
            confidence=Decimal(str(result.confidence)),
        )
        db.add(proposal)

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
                    "params": {"posting_inactivity_threshold": 24, "posting_minimal_threshold": 0},
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
                {"routine": "rule.has_direct_revenue", "enabled": True, "params": {}},
                {"routine": "rule.has_operational_costs", "enabled": True, "params": {}},
                {"routine": "rule.collects_project_costs", "enabled": True, "params": {}},
                {"routine": "rule.revenue_allocation_vehicle", "enabled": True, "params": {}},
                {"routine": "rule.cost_allocation_vehicle", "enabled": True, "params": {}},
                {"routine": "rule.info_only", "enabled": True, "params": {}},
                {"routine": "aggregate.combine_outcomes", "enabled": True, "params": {}},
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
