"""Activity feed API — recent actions and notifications."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.infra.db.session import get_db
from app.models.core import ActivityFeedEntry, AppUser

router = APIRouter()


@router.get("")
def list_activity(
    limit: int = 50,
    unread_only: bool = False,
    db: Session = Depends(get_db),
    user: AppUser = Depends(get_current_user),
) -> dict:
    query = select(ActivityFeedEntry).order_by(ActivityFeedEntry.id.desc())
    if unread_only:
        query = query.where(ActivityFeedEntry.is_read.is_(False))
    # Show user's own activity + system notifications
    query = query.where(
        (ActivityFeedEntry.user_id == user.id) | (ActivityFeedEntry.user_id.is_(None))
    )
    rows = db.execute(query.limit(limit)).scalars().all()
    # Only count user-owned unread entries (system notifications are shared
    # rows that mark_all_read cannot clear, so exclude them from the badge).
    unread_count = (
        db.execute(
            select(func.count(ActivityFeedEntry.id))
            .where(ActivityFeedEntry.is_read.is_(False))
            .where(ActivityFeedEntry.user_id == user.id)
        ).scalar()
        or 0
    )
    return {
        "items": [
            {
                "id": r.id,
                "action": r.action,
                "entity_type": r.entity_type,
                "entity_id": r.entity_id,
                "summary": r.summary,
                "is_read": r.is_read,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
        "unread_count": unread_count,
    }


@router.get("/unread-count")
def unread_count(
    db: Session = Depends(get_db),
    user: AppUser = Depends(get_current_user),
) -> dict:
    """Lightweight count-only endpoint for the navbar's activity badge.

    The frontend's ``Layout.astro`` polls this on every page load to
    decide whether to render the red dot on the Activity link. The full
    ``GET /api/activity`` endpoint also returns this number, but the
    navbar wants just the count without the overhead of fetching and
    serialising 50 activity rows on every page transition. Without this
    endpoint the navbar fetch returned 404 (the frontend swallows it
    silently, but the badge never lit up even when there were unread
    items).

    Same predicate as ``list_activity``: a user sees their own entries
    plus shared system notifications (``user_id IS NULL``).
    """
    count = db.execute(
        select(func.count(ActivityFeedEntry.id))
        .where(ActivityFeedEntry.is_read.is_(False))
        .where((ActivityFeedEntry.user_id == user.id) | (ActivityFeedEntry.user_id.is_(None)))
    ).scalar()
    return {"count": int(count or 0)}


@router.post("/mark-read")
def mark_all_read(
    db: Session = Depends(get_db),
    user: AppUser = Depends(get_current_user),
) -> dict:
    # Only mark user-owned entries as read; system notifications (user_id=NULL)
    # are shared rows and must not be mutated per-user.
    db.execute(
        update(ActivityFeedEntry)
        .where(ActivityFeedEntry.user_id == user.id)
        .where(ActivityFeedEntry.is_read.is_(False))
        .values(is_read=True)
    )
    db.commit()
    return {"status": "ok"}


def log_activity(
    db: Session,
    action: str,
    summary: str,
    user_id: int | None = None,
    entity_type: str | None = None,
    entity_id: int | None = None,
    detail: dict | None = None,
) -> None:
    """Helper to insert activity feed entries from any service."""
    db.add(
        ActivityFeedEntry(
            user_id=user_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            summary=summary,
            detail=detail,
        )
    )
