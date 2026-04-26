"""Activity feed API — recent actions and notifications."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select, update
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
    unread = (
        db.execute(
            select(ActivityFeedEntry)
            .where(ActivityFeedEntry.is_read.is_(False))
            .where((ActivityFeedEntry.user_id == user.id) | (ActivityFeedEntry.user_id.is_(None)))
        )
        .scalars()
        .all()
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
        "unread_count": len(unread),
    }


@router.post("/mark-read")
def mark_all_read(
    db: Session = Depends(get_db),
    user: AppUser = Depends(get_current_user),
) -> dict:
    db.execute(
        update(ActivityFeedEntry)
        .where(
            (ActivityFeedEntry.user_id == user.id) | (ActivityFeedEntry.user_id.is_(None))
        )
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
