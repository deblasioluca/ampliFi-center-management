"""Reviewer API (section 11.8, token-scoped)."""

from __future__ import annotations

from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from app.api.deps import PaginationParams, pagination
from app.infra.db.session import get_db
from app.models.core import CenterProposal, ReviewItem, ReviewScope

router = APIRouter()


def _get_scope(token: str, db: Session) -> ReviewScope:
    from datetime import datetime

    scope = db.execute(select(ReviewScope).where(ReviewScope.token == token)).scalar_one_or_none()
    if not scope:
        raise HTTPException(status_code=404, detail="Invalid review token")
    if scope.token_expires_at is not None and scope.token_expires_at < datetime.now(UTC):
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
    search: str | None = None,
) -> dict:
    scope = _get_scope(token, db)
    query = (
        select(ReviewItem)
        .where(ReviewItem.scope_id == scope.id)
        .options(joinedload(ReviewItem.proposal).joinedload(CenterProposal.legacy_cc))
    )
    if decision:
        query = query.where(ReviewItem.decision == decision)
    if search:
        from app.models.core import LegacyCostCenter

        query = (
            query.join(
                CenterProposal,
                ReviewItem.proposal_id == CenterProposal.id,
                isouter=True,
            )
            .join(
                LegacyCostCenter,
                CenterProposal.legacy_cc_id == LegacyCostCenter.id,
                isouter=True,
            )
            .where(
                LegacyCostCenter.cctr.ilike(f"%{search}%")
                | LegacyCostCenter.txtsh.ilike(f"%{search}%")
                | LegacyCostCenter.ccode.ilike(f"%{search}%")
            )
        )
    total_q = select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == scope.id)
    if decision:
        total_q = total_q.where(ReviewItem.decision == decision)
    total = db.execute(total_q).scalar() or 0
    result = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size))
    items = result.unique().scalars().all()
    enriched = []
    for i in items:
        row: dict = {
            "id": i.id,
            "proposal_id": i.proposal_id,
            "decision": i.decision,
            "comment": i.comment,
        }
        proposal = i.proposal
        if proposal:
            row["cleansing_outcome"] = proposal.cleansing_outcome
            row["target_object"] = proposal.target_object
            row["confidence"] = (
                str(proposal.confidence) if proposal.confidence else None
            )
            row["rule_path"] = proposal.rule_path
            row["merge_into_cctr"] = proposal.merge_into_cctr
            row["entity_code"] = proposal.entity_code
            cc = proposal.legacy_cc
            if cc:
                row["cctr"] = cc.cctr
                row["txtsh"] = cc.txtsh
                row["txtmi"] = cc.txtmi or cc.txtsh
                row["ccode"] = cc.ccode
                row["coarea"] = cc.coarea
                row["pctr"] = cc.pctr
                row["responsible"] = cc.responsible
                row["cctrcgy"] = cc.cctrcgy
                row["currency"] = cc.currency
        else:
            row["cleansing_outcome"] = None
            row["cctr"] = None
            row["txtsh"] = None
        enriched.append(row)
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": enriched,
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


class RequestChanges(BaseModel):
    comment: str


@router.post("/{token}/request-changes")
def request_changes(
    token: str,
    body: RequestChanges,
    db: Session = Depends(get_db),
) -> dict:
    scope = _get_scope(token, db)
    scope.status = "changes_requested"
    from app.domain.audit import write_audit

    write_audit(
        db,
        action="review.request_changes",
        entity_type="review_scope",
        entity_id=scope.id,
        actor_email=scope.reviewer_email,
        after={"comment": body.comment},
    )
    db.commit()
    return {"status": "changes_requested"}


class NewCenterRequest(BaseModel):
    purpose: str
    target_object: str = "CC"
    responsible: str | None = None
    bs_relevance: str | None = None


@router.post("/{token}/items/request-new")
def request_new_center(
    token: str,
    body: NewCenterRequest,
    db: Session = Depends(get_db),
) -> dict:
    """Reviewer requests a new center that wasn't proposed by analysis."""
    scope = _get_scope(token, db)
    from datetime import datetime

    comment_text = (
        f"NEW CENTER REQUEST \u2014 Purpose: {body.purpose}"
        f" | Target: {body.target_object}"
    )
    if body.responsible:
        comment_text += f" | Responsible: {body.responsible}"
    if body.bs_relevance:
        comment_text += f" | B/S Relevance: {body.bs_relevance}"

    item = ReviewItem(
        scope_id=scope.id,
        proposal_id=None,
        decision="NEW_REQUEST",
        comment=comment_text,
        decided_at=datetime.now(UTC),
    )
    db.add(item)
    from app.domain.audit import write_audit

    write_audit(
        db,
        action="review.request_new_center",
        entity_type="review_scope",
        entity_id=scope.id,
        actor_email=scope.reviewer_email,
        after={"purpose": body.purpose, "target_object": body.target_object},
    )
    db.commit()
    return {"status": "created", "item_id": item.id}


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
