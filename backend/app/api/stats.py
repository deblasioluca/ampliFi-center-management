"""Statistics endpoints (section 20.8)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.api.deps import require_role
from app.infra.db.session import get_db
from app.models.core import (
    AppUser,
    Balance,
    Entity,
    LegacyCostCenter,
    LegacyProfitCenter,
    TargetCostCenter,
    TargetProfitCenter,
    Wave,
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
