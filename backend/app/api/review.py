"""Reviewer API (section 11.8, token-scoped)."""

from __future__ import annotations

from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination
from app.infra.db.session import get_db
from app.models.core import ReviewItem, ReviewScope

router = APIRouter()


def _get_scope(token: str, db: Session) -> ReviewScope:
    from datetime import datetime

    scope = db.execute(select(ReviewScope).where(ReviewScope.token == token)).scalar_one_or_none()
    if not scope:
        raise HTTPException(status_code=404, detail="Invalid review token")
    if scope.token_expires_at < datetime.now(UTC):
        raise HTTPException(status_code=410, detail="Review token expired")
    if scope.status in ("revoked", "completed", "expired"):
        raise HTTPException(status_code=410, detail=f"Review scope is {scope.status}")
    return scope


@router.get("/{token}")
def scope_summary(token: str, db: Session = Depends(get_db)) -> dict:
    scope = _get_scope(token, db)
    total = (
        db.execute(
            select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == scope.id)
        ).scalar()
        or 0
    )
    decided = (
        db.execute(
            select(func.count(ReviewItem.id)).where(
                ReviewItem.scope_id == scope.id,
                ReviewItem.decision != "PENDING",
            )
        ).scalar()
        or 0
    )
    return {
        "scope_id": scope.id,
        "name": scope.name,
        "scope_type": scope.scope_type,
        "status": scope.status,
        "total_items": total,
        "decided_items": decided,
    }


@router.get("/{token}/items")
def scope_items(
    token: str,
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    decision: str | None = None,
) -> dict:
    scope = _get_scope(token, db)
    query = select(ReviewItem).where(ReviewItem.scope_id == scope.id)
    if decision:
        query = query.where(ReviewItem.decision == decision)
    total_q = select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == scope.id)
    if decision:
        total_q = total_q.where(ReviewItem.decision == decision)
    total = db.execute(total_q).scalar() or 0
    items = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {"id": i.id, "proposal_id": i.proposal_id, "decision": i.decision, "comment": i.comment}
            for i in items
        ],
    }


class DecisionBody(BaseModel):
    decision: str  # APPROVED|NOT_REQUIRED|COMMENTED
    comment: str | None = None


@router.post("/{token}/items/{item_id}/decide")
def decide_item(
    token: str,
    item_id: int,
    body: DecisionBody,
    db: Session = Depends(get_db),
) -> dict:
    scope = _get_scope(token, db)
    item = db.get(ReviewItem, item_id)
    if not item or item.scope_id != scope.id:
        raise HTTPException(status_code=404, detail="Item not found in scope")
    from datetime import datetime

    item.decision = body.decision
    item.comment = body.comment
    item.decided_at = datetime.now(UTC)
    db.commit()
    return {"status": "decided", "decision": body.decision}


class BulkDecision(BaseModel):
    item_ids: list[int]
    decision: str


@router.post("/{token}/items/bulk-decide")
def bulk_decide(
    token: str,
    body: BulkDecision,
    db: Session = Depends(get_db),
) -> dict:
    scope = _get_scope(token, db)
    from datetime import datetime

    now = datetime.now(UTC)
    count = 0
    for item_id in body.item_ids:
        item = db.get(ReviewItem, item_id)
        if item and item.scope_id == scope.id:
            item.decision = body.decision
            item.decided_at = now
            count += 1
    db.commit()
    return {"decided": count}


@router.post("/{token}/complete")
def complete_review(token: str, db: Session = Depends(get_db)) -> dict:
    scope = _get_scope(token, db)
    pending = (
        db.execute(
            select(func.count(ReviewItem.id)).where(
                ReviewItem.scope_id == scope.id,
                ReviewItem.decision == "PENDING",
            )
        ).scalar()
        or 0
    )
    if pending > 0:
        raise HTTPException(
            status_code=409,
            detail=f"{pending} items still pending",
        )
    from datetime import datetime

    scope.status = "completed"
    scope.completed_at = datetime.now(UTC)
    db.commit()
    return {"status": "completed"}
