"""Housekeeping endpoints (section 11.9)."""

from __future__ import annotations

from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination, require_role
from app.infra.db.session import get_db
from app.models.core import AppUser, HousekeepingCycle, HousekeepingItem

router = APIRouter()


@router.get("/admin/housekeeping/cycles")
def list_cycles(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    total = db.execute(select(func.count(HousekeepingCycle.id))).scalar() or 0
    cycles = (
        db.execute(
            select(HousekeepingCycle)
            .order_by(HousekeepingCycle.created_at.desc())
            .offset((pag.page - 1) * pag.size)
            .limit(pag.size)
        )
        .scalars()
        .all()
    )
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [{"id": c.id, "period": c.period, "status": c.status} for c in cycles],
    }


@router.post("/admin/housekeeping/run")
def run_housekeeping(
    period: str | None = None,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Create and execute a housekeeping cycle."""
    import datetime

    from app.services.housekeeping import create_cycle, run_cycle

    if not period:
        period = datetime.datetime.now(datetime.UTC).strftime("%Y-%m")

    try:
        cycle = create_cycle(period=period, db=db)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from None

    db.flush()
    try:
        cycle = run_cycle(cycle.id, db)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e)) from None

    return {
        "id": cycle.id,
        "period": period,
        "status": cycle.status,
        "kpis": cycle.kpis,
    }


@router.post("/admin/housekeeping/cycles/{cycle_id}/notify")
def notify_owners(
    cycle_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Send email notifications to all owners with flagged items."""
    from app.services.housekeeping import send_notifications

    try:
        result = send_notifications(cycle_id, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    return result


@router.post("/admin/housekeeping/cycles/{cycle_id}/close")
def close_cycle(
    cycle_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Close a housekeeping cycle."""
    from app.services.housekeeping import close_cycle as do_close

    try:
        cycle = do_close(cycle_id, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    return {"id": cycle.id, "status": cycle.status}


@router.get("/admin/housekeeping/cycles/{cycle_id}")
def cycle_detail(
    cycle_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    cycle = db.get(HousekeepingCycle, cycle_id)
    if not cycle:
        raise HTTPException(status_code=404, detail="Cycle not found")
    item_count = (
        db.execute(
            select(func.count(HousekeepingItem.id)).where(HousekeepingItem.cycle_id == cycle.id)
        ).scalar()
        or 0
    )
    return {
        "id": cycle.id,
        "period": cycle.period,
        "status": cycle.status,
        "item_count": item_count,
        "kpis": cycle.kpis,
    }


class OwnerDecision(BaseModel):
    decision: str  # KEEP|CLOSE|DEFER
    comment: str | None = None


@router.get("/housekeeping/{cycle_id}/owner/{token}")
def owner_view(cycle_id: int, token: str, db: Session = Depends(get_db)) -> dict:
    items = (
        db.execute(
            select(HousekeepingItem).where(
                HousekeepingItem.cycle_id == cycle_id,
                HousekeepingItem.owner_token == token,
            )
        )
        .scalars()
        .all()
    )
    if not items:
        raise HTTPException(status_code=404, detail="No items for this token")
    return {
        "cycle_id": cycle_id,
        "items": [
            {"id": i.id, "flag": i.flag, "decision": i.decision, "target_cc_id": i.target_cc_id}
            for i in items
        ],
    }


@router.post("/housekeeping/{cycle_id}/owner/{token}/decide")
def owner_decide(
    cycle_id: int,
    token: str,
    item_id: int,
    body: OwnerDecision,
    db: Session = Depends(get_db),
) -> dict:
    item = db.get(HousekeepingItem, item_id)
    if not item or item.cycle_id != cycle_id or item.owner_token != token:
        raise HTTPException(status_code=404, detail="Item not found")
    from datetime import datetime

    item.decision = body.decision
    item.decision_comment = body.comment
    item.decided_at = datetime.now(UTC)
    db.commit()
    return {"status": "decided", "decision": body.decision}
