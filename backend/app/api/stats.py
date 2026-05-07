"""Statistics endpoints (section 20.8)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import distinct, func, or_, select, text
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
    """How much of the CC/entity universe is covered by waves.

    Coverage is split between **scoped waves** (real units of cleansing
    work — each one targets a specific subset of the entity universe)
    and **global analysis** (the full-scope wave row plus any analysis
    runs that aren't tied to any wave). Mixing the two distorts the
    completeness picture: a single full-scope wave with all 608
    entities makes "100%" coverage even when no actual cleansing work
    has happened.

    The response keeps the legacy top-level ``covered_entities`` /
    ``covered_cc`` / ``entity_pct`` / ``cc_pct`` fields for backwards
    compatibility, but they now reflect the **scoped** subset only —
    the meaningful progress metric for wave-based work. New ``scoped``
    and ``global`` blocks expose both views explicitly.
    """
    total_entities = db.execute(select(func.count(Entity.id))).scalar() or 0
    total_cc = (
        db.execute(
            select(func.count(LegacyCostCenter.id)).where(LegacyCostCenter.is_active.is_(True))
        ).scalar()
        or 0
    )

    # Identify wave rows by full-scope flag — that's the authoritative
    # split between "global reference" rows (full scope) and real
    # scoped waves.
    scoped_wave_ids = (
        db.execute(select(Wave.id).where(Wave.is_full_scope.is_(False))).scalars().all()
    )
    global_wave_ids = (
        db.execute(select(Wave.id).where(Wave.is_full_scope.is_(True))).scalars().all()
    )

    # Entities covered by scoped waves (the real progress metric).
    scoped_covered_entities = 0
    if scoped_wave_ids:
        scoped_covered_entities = (
            db.execute(
                select(func.count(distinct(WaveEntity.entity_id))).where(
                    WaveEntity.wave_id.in_(scoped_wave_ids)
                )
            ).scalar()
            or 0
        )

    # Entities covered by global waves (separate view — typically all of them).
    global_covered_entities = 0
    if global_wave_ids:
        global_covered_entities = (
            db.execute(
                select(func.count(distinct(WaveEntity.entity_id))).where(
                    WaveEntity.wave_id.in_(global_wave_ids)
                )
            ).scalar()
            or 0
        )

    # CCs analysed in scoped wave runs (the real progress metric).
    scoped_covered_cc = 0
    if scoped_wave_ids:
        scoped_covered_cc = (
            db.execute(
                select(func.count(distinct(CenterProposal.legacy_cc_id)))
                .join(AnalysisRun, CenterProposal.run_id == AnalysisRun.id)
                .join(
                    LegacyCostCenter,
                    CenterProposal.legacy_cc_id == LegacyCostCenter.id,
                )
                .where(
                    AnalysisRun.status == "completed",
                    AnalysisRun.wave_id.in_(scoped_wave_ids),
                    LegacyCostCenter.is_active.is_(True),
                )
            ).scalar()
            or 0
        )

    # CCs analysed by global runs — that's both the full-scope wave's
    # runs AND any wave_id-IS-NULL runs (e.g. from POST /api/runs/global).
    global_run_filters = []
    if global_wave_ids:
        global_run_filters.append(AnalysisRun.wave_id.in_(global_wave_ids))
    global_run_filters.append(AnalysisRun.wave_id.is_(None))

    global_covered_cc = (
        db.execute(
            select(func.count(distinct(CenterProposal.legacy_cc_id)))
            .join(AnalysisRun, CenterProposal.run_id == AnalysisRun.id)
            .join(
                LegacyCostCenter,
                CenterProposal.legacy_cc_id == LegacyCostCenter.id,
            )
            .where(
                AnalysisRun.status == "completed",
                LegacyCostCenter.is_active.is_(True),
            )
            .where(or_(*global_run_filters))
        ).scalar()
        or 0
    )

    # Per-wave breakdown — same shape as before but split into two arrays
    # so the UI can render them in separate sections.
    waves = db.execute(select(Wave).order_by(Wave.created_at.desc())).scalars().all()
    scoped_wave_details: list[dict] = []
    global_wave_details: list[dict] = []
    for w in waves:
        w_entity_count = (
            db.execute(select(func.count(WaveEntity.id)).where(WaveEntity.wave_id == w.id)).scalar()
            or 0
        )
        w_cc_count = (
            db.execute(
                select(func.count(distinct(CenterProposal.legacy_cc_id)))
                .join(AnalysisRun, CenterProposal.run_id == AnalysisRun.id)
                .join(
                    LegacyCostCenter,
                    CenterProposal.legacy_cc_id == LegacyCostCenter.id,
                )
                .where(
                    AnalysisRun.wave_id == w.id,
                    AnalysisRun.status == "completed",
                    LegacyCostCenter.is_active.is_(True),
                )
            ).scalar()
            or 0
        )
        entry = {
            "id": w.id,
            "code": w.code,
            "status": w.status,
            "entities_covered": w_entity_count,
            "cc_covered": w_cc_count,
            "is_full_scope": w.is_full_scope,
        }
        (global_wave_details if w.is_full_scope else scoped_wave_details).append(entry)

    # Wave-id-IS-NULL runs (typically POST /api/runs/global from the
    # Analytics Dashboard). Surfaced under the global block so they
    # don't get mixed into wave coverage.
    unassigned_cc = (
        db.execute(
            select(func.count(distinct(CenterProposal.legacy_cc_id)))
            .join(AnalysisRun, CenterProposal.run_id == AnalysisRun.id)
            .join(
                LegacyCostCenter,
                CenterProposal.legacy_cc_id == LegacyCostCenter.id,
            )
            .where(
                AnalysisRun.status == "completed",
                AnalysisRun.wave_id.is_(None),
                LegacyCostCenter.is_active.is_(True),
            )
        ).scalar()
        or 0
    )
    if unassigned_cc > 0:
        global_wave_details.append(
            {
                "id": None,
                "code": "Unassigned global runs",
                "status": "n/a",
                "entities_covered": 0,
                "cc_covered": unassigned_cc,
                "is_full_scope": False,
            }
        )

    scoped_entity_pct = (
        round(scoped_covered_entities / total_entities * 100, 1) if total_entities else 0
    )
    scoped_cc_pct = round(scoped_covered_cc / total_cc * 100, 1) if total_cc else 0
    global_entity_pct = (
        round(global_covered_entities / total_entities * 100, 1) if total_entities else 0
    )
    global_cc_pct = round(global_covered_cc / total_cc * 100, 1) if total_cc else 0

    return {
        "total_entities": total_entities,
        "total_cc": total_cc,
        # New: explicit split.
        "scoped": {
            "covered_entities": scoped_covered_entities,
            "covered_cc": scoped_covered_cc,
            "entity_pct": scoped_entity_pct,
            "cc_pct": scoped_cc_pct,
            "wave_count": len(scoped_wave_details),
        },
        "global": {
            "covered_entities": global_covered_entities,
            "covered_cc": global_covered_cc,
            "entity_pct": global_entity_pct,
            "cc_pct": global_cc_pct,
            "wave_count": len([w for w in global_wave_details if w["id"] is not None]),
            "unassigned_run_cc": unassigned_cc,
        },
        # Backwards-compat: the legacy top-level fields now reflect the
        # SCOPED subset (the meaningful "progress" metric). Existing
        # callers that just wanted a total can switch to ``global`` for
        # the equivalent of the old behaviour.
        "covered_entities": scoped_covered_entities,
        "covered_cc": scoped_covered_cc,
        "entity_pct": scoped_entity_pct,
        "cc_pct": scoped_cc_pct,
        # Per-wave breakdown — split.
        "scoped_waves": scoped_wave_details,
        "global_waves": global_wave_details,
        # Backwards-compat: the old combined ``waves`` array, scoped
        # first then global.
        "waves": scoped_wave_details + global_wave_details,
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
