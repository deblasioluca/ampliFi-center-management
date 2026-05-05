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
        is_allocation_vehicle=attrs.get("is_allocation_vehicle", False),
        is_project_related=attrs.get("is_project_related", False),
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


def execute_analysis(
    wave_id: int | None,
    config_id: int,
    user_id: int,
    db: Session,
    *,
    mode: str = "simulation",
    label: str | None = None,
    excluded_scopes: list[str] | None = None,
) -> AnalysisRun:
    """Execute decision tree analysis.

    If *wave_id* is provided, only cost centers in the wave's scope are
    analysed. If *wave_id* is ``None``, **all** cost centers are analysed
    (global / full-scope analysis).

    mode: 'simulation' for non-destructive preview, 'activated' for final.
    label: optional human-readable label for this run.
    excluded_scopes: list of wave IDs whose centers to exclude (global mode).
    """
    wave = None
    if wave_id is not None:
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
        mode=mode,
        label=label
        or (
            f"V1 {'Simulation' if mode == 'simulation' else 'Activated'}"
            f" {datetime.now(UTC).strftime('%Y-%m-%d %H:%M')}"
        ),
        excluded_scopes=excluded_scopes,
    )
    db.add(run)
    db.flush()

    cc_query = _scope_query(wave, db) if wave is not None else select(LegacyCostCenter)

    # Exclude centers from completed waves (global mode)
    if excluded_scopes and wave is None:
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
                cc_query = cc_query.where(LegacyCostCenter.ccode.not_in(excl_ccodes))

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
        features = _build_features(cc, db, inactivity_months)
        if use_pipeline:
            ctx = _build_context(cc, db, inactivity_months)
            result = evaluate_center_with_pipeline(ctx, pipeline_config, registry)
        else:
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

        # ML predictions (heuristic fallback if no trained model)
        ml_outcome_probs = {}
        ml_target_probs = {}
        ml_shap = []
        try:
            from app.domain.ml.classifiers import OutcomeClassifier, TargetObjectClassifier

            feature_dict = {
                "bs_amt": features.bs_amt,
                "rev_amt": features.rev_amt,
                "opex_amt": features.opex_amt,
                "other_amt": 0.0,
                "posting_count_window": features.posting_count_window or 0,
                "months_active_in_window": 0,
                "months_since_last_posting": features.months_since_last_posting or 0,
                "period_count_with_postings": 0,
                "balance_volatility": 0.0,
                "has_owner": int(features.has_owner),
                "hierarchy_membership_count": features.hierarchy_membership_count,
            }
            oc = OutcomeClassifier()
            tc = TargetObjectClassifier()
            ml_outcome_probs = oc.predict_proba(feature_dict)
            ml_target_probs = tc.predict_proba(feature_dict)
            ml_shap = oc.explain(feature_dict, top_k=5)
        except Exception as ml_err:
            log.warning("ML prediction failed for %s: %s", cc.cctr, ml_err)

        # Compute ML confidence as max probability
        ml_confidence = max(ml_outcome_probs.values()) if ml_outcome_probs else None

        proposal = CenterProposal(
            run_id=run.id,
            legacy_cc_id=cc.id,
            entity_code=cc.ccode or "",
            cleansing_outcome=result.cleansing.value,
            target_object=result.target_object.value if result.target_object else None,
            merge_into_cctr=result.merge_into,
            rule_path={"steps": result.rule_path},
            confidence=Decimal(str(ml_confidence))
            if ml_confidence
            else Decimal(str(result.confidence)),
            ml_scores={
                "outcome_probs": ml_outcome_probs,
                "target_probs": ml_target_probs,
                "shap": ml_shap,
            }
            if ml_outcome_probs
            else None,
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

    if wave is not None and wave.status == "draft":
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
                {"routine": "rule.collects_project_costs", "enabled": True, "params": {}},
                {"routine": "rule.has_operational_costs", "enabled": True, "params": {}},
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
