"""Review workflow service (§06.8).

Three viewing modes:
1. Flat list — paginated table of proposals
2. Hierarchy tree — grouped by hierarchy nodes
3. Proposed list — grouped by proposed target structure

Bulk operations: approve all under a hierarchy node, request changes,
request new center, final sign-off.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.core import (
    CenterProposal,
    ReviewItem,
    ReviewScope,
    Wave,
)

logger = structlog.get_logger()


def create_scope(
    wave_id: int,
    reviewer_user_id: int,
    scope_type: str,  # entity | hierarchy | list
    scope_filter: dict,
    db: Session,
) -> ReviewScope:
    """Assign a review scope to a reviewer (§06.8).

    scope_filter shapes:
      entity:    {"entity_codes": ["DE01", "US01"]}
      hierarchy: {"hierarchy_id": 5, "node_codes": ["H01", "H02"]}
      list:      {"legacy_cc_ids": [1, 2, 3]}
    """
    wave = db.get(Wave, wave_id)
    if not wave:
        raise ValueError(f"Wave {wave_id} not found")

    if wave.status not in ("locked", "in_review"):
        raise ValueError(f"Cannot assign scope: wave is {wave.status}")

    token = secrets.token_urlsafe(32)
    expires = datetime.now(UTC) + timedelta(days=30)
    scope = ReviewScope(
        wave_id=wave_id,
        reviewer_user_id=reviewer_user_id,
        name=f"Scope for wave {wave_id}",
        scope_type=scope_type,
        scope_filter=scope_filter,
        token=token,
        token_expires_at=expires,
    )
    db.add(scope)
    db.flush()

    # Now create ReviewItem rows for each matching proposal
    run_id = wave.preferred_run_id
    if not run_id:
        raise ValueError("Wave has no preferred run — run analysis first")

    proposals_q = select(CenterProposal).where(CenterProposal.run_id == run_id)

    if scope_type == "entity":
        entity_codes = scope_filter.get("entity_codes", [])
        if entity_codes:
            proposals_q = proposals_q.where(CenterProposal.entity_code.in_(entity_codes))
    elif scope_type == "list":
        cc_ids = scope_filter.get("legacy_cc_ids", [])
        if cc_ids:
            proposals_q = proposals_q.where(CenterProposal.legacy_cc_id.in_(cc_ids))

    proposals = db.execute(proposals_q).scalars().all()

    for proposal in proposals:
        item = ReviewItem(
            scope_id=scope.id,
            proposal_id=proposal.id,
        )
        db.add(item)

    scope.total_items = len(proposals)
    db.commit()
    db.refresh(scope)

    logger.info(
        "review.scope.created",
        scope_id=scope.id,
        wave_id=wave_id,
        reviewer=reviewer_user_id,
        type=scope_type,
        items=len(proposals),
    )
    return scope


def decide_item(
    item_id: int,
    decision: str,
    comment: str | None,
    user_id: int,
    db: Session,
) -> ReviewItem:
    """Record a reviewer's decision on a single item."""
    item = db.get(ReviewItem, item_id)
    if not item:
        raise ValueError(f"Review item {item_id} not found")

    if decision not in ("approve", "reject", "request_changes"):
        raise ValueError(f"Invalid decision: {decision}")

    scope = db.get(ReviewScope, item.scope_id)
    if scope and scope.reviewer_user_id != user_id:
        raise ValueError("Only the assigned reviewer can decide this item")

    item.decision = decision
    item.comment = comment
    item.decided_at = datetime.now(UTC)
    db.commit()
    db.refresh(item)
    return item


def bulk_approve(
    scope_id: int,
    item_ids: list[int] | None,
    comment: str | None,
    user_id: int,
    db: Session,
) -> dict:
    """Bulk approve items in a scope.

    If item_ids is None, approves ALL undecided items in scope.
    """
    scope = db.get(ReviewScope, scope_id)
    if not scope:
        raise ValueError(f"Scope {scope_id} not found")
    if scope.reviewer_user_id != user_id:
        raise ValueError("Only the assigned reviewer can approve")

    query = select(ReviewItem).where(
        ReviewItem.scope_id == scope_id,
        ReviewItem.decision == "PENDING",
    )
    if item_ids:
        query = query.where(ReviewItem.id.in_(item_ids))

    items = db.execute(query).scalars().all()
    now = datetime.now(UTC)

    for item in items:
        item.decision = "approve"
        item.comment = comment
        item.decided_at = now

    db.commit()
    return {"approved": len(items), "scope_id": scope_id}


def request_new_center(
    scope_id: int,
    request: dict,
    user_id: int,
    db: Session,
) -> dict:
    """Reviewer requests creation of a new center not in proposals."""
    scope = db.get(ReviewScope, scope_id)
    if not scope:
        raise ValueError(f"Scope {scope_id} not found")
    if scope.reviewer_user_id != user_id:
        raise ValueError("Only the assigned reviewer can request new centers")

    # Store the request as a special ReviewItem linked to a virtual proposal
    item = ReviewItem(
        scope_id=scope_id,
        proposal_id=None,
        decision="request_new",
        comment=f"Request: {request.get('description', '')}",
        decided_at=datetime.now(UTC),
    )
    db.add(item)
    db.commit()
    db.refresh(item)

    return {
        "item_id": item.id,
        "scope_id": scope_id,
        "request": request,
        "status": "pending_admin_review",
    }


def sign_off(scope_id: int, user_id: int, db: Session) -> ReviewScope:
    """Final sign-off on a review scope (§06.8.6)."""
    scope = db.get(ReviewScope, scope_id)
    if not scope:
        raise ValueError(f"Scope {scope_id} not found")
    if scope.reviewer_user_id != user_id:
        raise ValueError("Only the assigned reviewer can sign off")

    undecided = (
        db.execute(
            select(func.count())
            .select_from(ReviewItem)
            .where(
                ReviewItem.scope_id == scope_id,
                ReviewItem.decision == "PENDING",
            )
        ).scalar()
        or 0
    )

    if undecided > 0:
        raise ValueError(f"Cannot sign off: {undecided} items still undecided")

    scope.status = "signed_off"
    scope.signed_off_at = datetime.now(UTC)
    db.commit()
    db.refresh(scope)

    # Check if all scopes for this wave are signed off
    _check_wave_complete(scope.wave_id, db)

    return scope


def _check_wave_complete(wave_id: int, db: Session) -> None:
    """Check if all review scopes are signed off → wave can be signed_off."""
    total = (
        db.execute(
            select(func.count()).select_from(ReviewScope).where(ReviewScope.wave_id == wave_id)
        ).scalar()
        or 0
    )

    signed = (
        db.execute(
            select(func.count())
            .select_from(ReviewScope)
            .where(
                ReviewScope.wave_id == wave_id,
                ReviewScope.status == "signed_off",
            )
        ).scalar()
        or 0
    )

    if total > 0 and total == signed:
        wave = db.get(Wave, wave_id)
        if wave and wave.status == "in_review":
            wave.status = "signed_off"
            wave.signed_off_at = datetime.now(UTC)
            db.commit()
            logger.info("wave.all_scopes_signed_off", wave_id=wave_id)


def get_scope_progress(scope_id: int, db: Session) -> dict:
    """Get review progress for a scope."""
    total = (
        db.execute(
            select(func.count()).select_from(ReviewItem).where(ReviewItem.scope_id == scope_id)
        ).scalar()
        or 0
    )

    decided = (
        db.execute(
            select(func.count())
            .select_from(ReviewItem)
            .where(
                ReviewItem.scope_id == scope_id,
                ReviewItem.decision != "PENDING",
            )
        ).scalar()
        or 0
    )

    approved = (
        db.execute(
            select(func.count())
            .select_from(ReviewItem)
            .where(
                ReviewItem.scope_id == scope_id,
                ReviewItem.decision == "approve",
            )
        ).scalar()
        or 0
    )

    rejected = (
        db.execute(
            select(func.count())
            .select_from(ReviewItem)
            .where(
                ReviewItem.scope_id == scope_id,
                ReviewItem.decision == "reject",
            )
        ).scalar()
        or 0
    )

    changes_requested = (
        db.execute(
            select(func.count())
            .select_from(ReviewItem)
            .where(
                ReviewItem.scope_id == scope_id,
                ReviewItem.decision == "request_changes",
            )
        ).scalar()
        or 0
    )

    return {
        "total": total,
        "decided": decided,
        "pending": total - decided,
        "approved": approved,
        "rejected": rejected,
        "changes_requested": changes_requested,
        "progress_pct": round(decided / total * 100, 1) if total > 0 else 0,
    }
