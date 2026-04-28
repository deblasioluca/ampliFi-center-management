"""Statistics endpoints (section 20.8)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import distinct, func, select, text
from sqlalchemy.orm import Session

from app.api.deps import require_role
from app.infra.db.session import get_db
from app.models.core import (
    AnalysisRun,
    AppUser,
    Balance,
    CenterProposal,
    Entity,
    HousekeepingCycle,
    HousekeepingItem,
    LegacyCostCenter,
    LegacyProfitCenter,
    TargetCostCenter,
    TargetProfitCenter,
    Wave,
    WaveEntity,
)

router = APIRouter()


@router.get("/global")
def global_stats(db: Session = Depends(get_db)) -> dict:
    entities_total = db.execute(select(func.count(Entity.id))).scalar() or 0
    legacy_cc_total = (
        db.execute(
            select(func.count(LegacyCostCenter.id)).where(LegacyCostCenter.is_active.is_(True))
        ).scalar()
        or 0
    )
    legacy_pc_total = (
        db.execute(
            select(func.count(LegacyProfitCenter.id)).where(LegacyProfitCenter.is_active.is_(True))
        ).scalar()
        or 0
    )
    target_cc = db.execute(select(func.count(TargetCostCenter.id))).scalar() or 0
    target_pc = db.execute(select(func.count(TargetProfitCenter.id))).scalar() or 0
    waves_total = db.execute(select(func.count(Wave.id))).scalar() or 0
    balances_total = db.execute(select(func.count(Balance.id))).scalar() or 0
    return {
        "universe": {
            "entities_total": entities_total,
            "legacy_cc_total": legacy_cc_total,
            "legacy_pc_total": legacy_pc_total,
            "target_cc_total": target_cc,
            "target_pc_total": target_pc,
            "waves_total": waves_total,
            "balances_total": balances_total,
        },
    }


@router.get("/wave/{wave_id}")
def wave_stats(wave_id: int, db: Session = Depends(get_db)) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    return {
        "wave_id": wave.id,
        "code": wave.code,
        "status": wave.status,
    }


@router.get("/coverage")
def scope_coverage(db: Session = Depends(get_db)) -> dict:
    """How much of the CC/entity universe is covered by waves."""
    total_entities = db.execute(select(func.count(Entity.id))).scalar() or 0
    total_cc = (
        db.execute(
            select(func.count(LegacyCostCenter.id)).where(LegacyCostCenter.is_active.is_(True))
        ).scalar()
        or 0
    )

    # Entities covered: distinct entity_ids across all wave_entity rows
    covered_entities = db.execute(select(func.count(distinct(WaveEntity.entity_id)))).scalar() or 0

    # CCs covered: distinct legacy_cc_ids that appear in any completed run's proposals
    covered_cc = (
        db.execute(
            select(func.count(distinct(CenterProposal.legacy_cc_id)))
            .join(AnalysisRun, CenterProposal.run_id == AnalysisRun.id)
            .where(AnalysisRun.status == "completed")
        ).scalar()
        or 0
    )

    # Per-wave breakdown
    waves = db.execute(select(Wave).order_by(Wave.created_at.desc())).scalars().all()
    wave_details = []
    for w in waves:
        w_entity_count = (
            db.execute(select(func.count(WaveEntity.id)).where(WaveEntity.wave_id == w.id)).scalar()
            or 0
        )
        w_cc_count = (
            db.execute(
                select(func.count(distinct(CenterProposal.legacy_cc_id)))
                .join(AnalysisRun, CenterProposal.run_id == AnalysisRun.id)
                .where(
                    AnalysisRun.wave_id == w.id,
                    AnalysisRun.status == "completed",
                )
            ).scalar()
            or 0
        )
        wave_details.append(
            {
                "id": w.id,
                "code": w.code,
                "status": w.status,
                "entities_covered": w_entity_count,
                "cc_covered": w_cc_count,
                "is_full_scope": w.is_full_scope,
            }
        )

    return {
        "total_entities": total_entities,
        "total_cc": total_cc,
        "covered_entities": covered_entities,
        "covered_cc": covered_cc,
        "entity_pct": round(covered_entities / total_entities * 100, 1) if total_entities else 0,
        "cc_pct": round(covered_cc / total_cc * 100, 1) if total_cc else 0,
        "waves": wave_details,
    }


@router.get("/housekeeping-summary")
def housekeeping_summary(db: Session = Depends(get_db)) -> dict:
    """Aggregated housekeeping data for analytics charts."""
    # Flag distribution across all cycles
    flag_counts = dict(
        db.execute(
            select(HousekeepingItem.flag, func.count(HousekeepingItem.id)).group_by(
                HousekeepingItem.flag
            )
        ).all()
    )

    # Owner response funnel
    total_items = db.execute(select(func.count(HousekeepingItem.id))).scalar() or 0
    notified = (
        db.execute(
            select(func.count(HousekeepingItem.id)).where(HousekeepingItem.notified_at.isnot(None))
        ).scalar()
        or 0
    )
    responded = (
        db.execute(
            select(func.count(HousekeepingItem.id)).where(HousekeepingItem.decision.isnot(None))
        ).scalar()
        or 0
    )
    decision_counts = dict(
        db.execute(
            select(HousekeepingItem.decision, func.count(HousekeepingItem.id))
            .where(HousekeepingItem.decision.isnot(None))
            .group_by(HousekeepingItem.decision)
        ).all()
    )

    # Closure trend per cycle
    cycles = (
        db.execute(select(HousekeepingCycle).order_by(HousekeepingCycle.period)).scalars().all()
    )
    trend = []
    for c in cycles:
        items_in_cycle = (
            db.execute(
                select(func.count(HousekeepingItem.id)).where(HousekeepingItem.cycle_id == c.id)
            ).scalar()
            or 0
        )
        closed_in_cycle = (
            db.execute(
                select(func.count(HousekeepingItem.id)).where(
                    HousekeepingItem.cycle_id == c.id,
                    HousekeepingItem.decision == "CLOSE",
                )
            ).scalar()
            or 0
        )
        trend.append(
            {
                "period": c.period,
                "status": c.status,
                "total_items": items_in_cycle,
                "closed": closed_in_cycle,
            }
        )

    return {
        "flag_distribution": flag_counts,
        "funnel": {
            "total": total_items,
            "notified": notified,
            "responded": responded,
            "decisions": decision_counts,
        },
        "trend": trend,
    }


@router.post("/balance-aggregation/refresh")
def refresh_balance_mv(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Refresh the materialized view for balance aggregation."""
    db.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY cleanup.mv_balance_per_center"))
    db.commit()
    return {"status": "refreshed"}


@router.get("/balance-aggregation")
def balance_aggregation(
    db: Session = Depends(get_db),
    coarea: str | None = None,
    ccode: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict:
    """Query pre-aggregated balance stats per cost center."""
    where_clauses: list[str] = []
    params: dict = {"lim": min(limit, 5000), "off": offset}
    if coarea:
        where_clauses.append("coarea = :coarea")
        params["coarea"] = coarea
    if ccode:
        where_clauses.append("ccode = :ccode")
        params["ccode"] = ccode

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    count_sql = f"SELECT COUNT(*) FROM cleanup.mv_balance_per_center {where_sql}"  # noqa: S608
    total = db.execute(text(count_sql), params).scalar() or 0

    data_sql = f"SELECT coarea, cctr, ccode, row_count, total_postings, total_tc_amt, total_gc_amt, min_period, max_period, last_posting_period FROM cleanup.mv_balance_per_center {where_sql} ORDER BY total_postings DESC LIMIT :lim OFFSET :off"  # noqa: S608, E501
    rows = db.execute(text(data_sql), params).mappings().all()
    return {
        "total": total,
        "items": [dict(r) for r in rows],
    }
