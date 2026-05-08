"""Reviewer API (section 11.8, token-scoped)."""

from __future__ import annotations

from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException, Query
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


def _auto_populate_scope(scope: ReviewScope, db: Session) -> None:
    """Populate review items from proposals if scope is empty."""
    from app.models.core import HierarchyLeaf, LegacyCostCenter

    wave = scope.wave
    if not wave:
        return
    preferred_run_id = (wave.config or {}).get("preferred_run_id")
    if not preferred_run_id:
        # Try latest completed run
        from app.models.core import AnalysisRun

        latest_run = db.execute(
            select(AnalysisRun)
            .where(AnalysisRun.wave_id == wave.id, AnalysisRun.status == "completed")
            .order_by(AnalysisRun.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if latest_run:
            preferred_run_id = latest_run.id
        else:
            return

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == preferred_run_id))
        .scalars()
        .all()
    )
    if not proposals:
        return

    # Apply scope filter if present
    scope_filter = scope.scope_filter or {}
    scope_ccodes = scope_filter.get("entity_ccodes", [])
    hier_nodes = scope_filter.get("hierarchy_nodes", [])
    hier_cctrs: set[str] = set()
    if hier_nodes:
        for node_info in hier_nodes:
            node_name = node_info.get("node_name", "")
            if node_name:
                leaves = (
                    db.execute(select(HierarchyLeaf).where(HierarchyLeaf.setname == node_name))
                    .scalars()
                    .all()
                )
                for lf in leaves:
                    hier_cctrs.add(lf.value)

    count = 0
    for p in proposals:
        cc = db.get(LegacyCostCenter, p.legacy_cc_id) if p.legacy_cc_id else None
        if not cc:
            continue
        if scope_ccodes and cc.ccode not in scope_ccodes:
            continue
        if hier_cctrs and (cc.cctr or "") not in hier_cctrs:
            continue
        db.add(ReviewItem(scope_id=scope.id, proposal_id=p.id, decision="PENDING"))
        count += 1

    if count:
        db.commit()


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
    wave = scope.wave
    return {
        "scope_id": scope.id,
        "name": scope.name,
        "scope_type": scope.scope_type,
        "status": scope.status,
        "total_items": total,
        "decided_items": decided,
        "wave_id": wave.id if wave else None,
        "wave_code": wave.code if wave else None,
        "wave_name": wave.name if wave else None,
    }


@router.get("/{token}/items")
def scope_items(
    token: str,
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    decision: str | None = None,
    search: str | None = None,
    hierarchy_id: int | None = Query(None),
    search_values: str | None = Query(None),
) -> dict:
    scope = _get_scope(token, db)

    # Auto-populate review items if scope has none and wave has a preferred run
    existing_count = (
        db.execute(
            select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == scope.id)
        ).scalar()
        or 0
    )
    if existing_count == 0:
        _auto_populate_scope(scope, db)

    query = (
        select(ReviewItem)
        .where(ReviewItem.scope_id == scope.id)
        .options(joinedload(ReviewItem.proposal).joinedload(CenterProposal.legacy_cc))
    )
    if decision:
        query = query.where(ReviewItem.decision == decision)
    # Determine if we need the CC join (for search or search_values)
    needs_cc_join = bool(search or search_values)
    if needs_cc_join:
        from app.models.core import LegacyCostCenter

        query = query.join(
            CenterProposal,
            ReviewItem.proposal_id == CenterProposal.id,
            isouter=True,
        ).join(
            LegacyCostCenter,
            CenterProposal.legacy_cc_id == LegacyCostCenter.id,
            isouter=True,
        )
    if search:
        from app.models.core import LegacyCostCenter

        query = query.where(
            LegacyCostCenter.cctr.ilike(f"%{search}%")
            | LegacyCostCenter.txtsh.ilike(f"%{search}%")
            | LegacyCostCenter.ccode.ilike(f"%{search}%")
        )
    if search_values:
        from app.models.core import LegacyCostCenter

        vals = [v.strip() for v in search_values.split(",") if v.strip()]
        if vals:
            query = query.where(LegacyCostCenter.cctr.in_(vals))

    total_q = select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == scope.id)
    if decision:
        total_q = total_q.where(ReviewItem.decision == decision)
    if needs_cc_join:
        from app.models.core import LegacyCostCenter

        total_q = total_q.join(
            CenterProposal,
            ReviewItem.proposal_id == CenterProposal.id,
            isouter=True,
        ).join(
            LegacyCostCenter,
            CenterProposal.legacy_cc_id == LegacyCostCenter.id,
            isouter=True,
        )
    if search:
        from app.models.core import LegacyCostCenter

        total_q = total_q.where(
            LegacyCostCenter.cctr.ilike(f"%{search}%")
            | LegacyCostCenter.txtsh.ilike(f"%{search}%")
            | LegacyCostCenter.ccode.ilike(f"%{search}%")
        )
    if search_values:
        from app.models.core import LegacyCostCenter

        vals = [v.strip() for v in search_values.split(",") if v.strip()]
        if vals:
            total_q = total_q.where(LegacyCostCenter.cctr.in_(vals))
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
            row["confidence"] = str(proposal.confidence) if proposal.confidence else None
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
                row["cctr"] = None
                row["txtsh"] = None
                row["txtmi"] = None
                row["ccode"] = None
                row["coarea"] = None
                row["pctr"] = None
                row["responsible"] = None
                row["cctrcgy"] = None
                row["currency"] = None
        else:
            row["cleansing_outcome"] = None
            row["cctr"] = None
            row["txtsh"] = None
            row["txtmi"] = None
            row["target_object"] = None
            row["merge_into_cctr"] = None
            row["entity_code"] = None
            row["ccode"] = None
            row["coarea"] = None
            row["confidence"] = None
            row["rule_path"] = None
            row["pctr"] = None
            row["responsible"] = None
            row["cctrcgy"] = None
            row["currency"] = None
        enriched.append(row)

    # Resolve hierarchy paths if hierarchy_id is specified
    paths: dict[str, list[str]] = {}
    max_depth = 0
    if hierarchy_id is not None and enriched:
        from app.api.reference import _resolve_hierarchy_paths

        leaf_values = [r["cctr"] for r in enriched if r.get("cctr")]
        paths, max_depth = _resolve_hierarchy_paths(db, hierarchy_id, leaf_values)
        for row in enriched:
            row["levels"] = paths.get(row.get("cctr") or "", [])

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "hierarchy_id": hierarchy_id,
        "hierarchy_max_depth": max_depth,
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
    target_object: str = "CC"  # CC or PC
    responsible: str | None = None
    bs_relevance: str | None = None
    hierarchy_node: str | None = None  # hierarchy node under which to create
    hierarchy_id: int | None = None  # hierarchy ID for context
    clone_from: str | None = None  # existing cctr/pctr to clone from
    proposed_name: str | None = None  # proposed short text / name
    proposed_ccode: str | None = None  # entity code
    proposed_coarea: str | None = None  # controlling area


@router.post("/{token}/items/request-new")
def request_new_center(
    token: str,
    body: NewCenterRequest,
    db: Session = Depends(get_db),
) -> dict:
    """Reviewer requests a new center (add or clone) — goes to admin for approval."""
    scope = _get_scope(token, db)
    from datetime import datetime

    comment_text = f"NEW CENTER REQUEST — Purpose: {body.purpose} | Target: {body.target_object}"
    if body.responsible:
        comment_text += f" | Responsible: {body.responsible}"
    if body.bs_relevance:
        comment_text += f" | B/S Relevance: {body.bs_relevance}"
    if body.hierarchy_node:
        comment_text += f" | Under node: {body.hierarchy_node}"
    if body.clone_from:
        comment_text += f" | Cloned from: {body.clone_from}"
    if body.proposed_name:
        comment_text += f" | Name: {body.proposed_name}"
    if body.proposed_ccode:
        comment_text += f" | Entity: {body.proposed_ccode}"

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
        after={
            "purpose": body.purpose,
            "target_object": body.target_object,
            "hierarchy_node": body.hierarchy_node,
            "clone_from": body.clone_from,
            "proposed_name": body.proposed_name,
        },
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
