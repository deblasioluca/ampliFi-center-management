"""Chat assistant API (section 16.7)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.infra.db.session import get_db
from app.models.chat import ChatMessage, ChatThread
from app.models.core import AppUser

router = APIRouter()


def _require_user(user: AppUser | None = Depends(get_current_user)) -> AppUser:
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def _get_user_thread(thread_id: int, user: AppUser, db: Session) -> ChatThread:
    thread = db.get(ChatThread, thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if thread.user_id != user.id:
        raise HTTPException(status_code=403, detail="Not your thread")
    return thread


class ThreadCreate(BaseModel):
    surface: str  # analyst|reviewer
    wave_id: int | None = None
    run_id: int | None = None
    scope_id: int | None = None


class MessageCreate(BaseModel):
    content: str


@router.post("/threads")
def create_thread(
    body: ThreadCreate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(_require_user),
) -> dict:
    thread = ChatThread(
        surface=body.surface,
        user_id=user.id,
        wave_id=body.wave_id,
        run_id=body.run_id,
        scope_id=body.scope_id,
    )
    db.add(thread)
    db.commit()
    db.refresh(thread)
    return {"id": thread.id, "surface": thread.surface}


@router.get("/threads/{thread_id}")
def get_thread(
    thread_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(_require_user),
) -> dict:
    thread = _get_user_thread(thread_id, user, db)
    messages = (
        db.execute(
            select(ChatMessage)
            .where(ChatMessage.thread_id == thread_id)
            .order_by(ChatMessage.created_at)
        )
        .scalars()
        .all()
    )
    return {
        "id": thread.id,
        "surface": thread.surface,
        "pinned": thread.pinned,
        "messages": [
            {"id": m.id, "role": m.role, "content": m.content, "created_at": str(m.created_at)}
            for m in messages
        ],
    }


@router.post("/threads/{thread_id}/messages")
def send_message(
    thread_id: int,
    body: MessageCreate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(_require_user),
) -> dict:
    _get_user_thread(thread_id, user, db)  # ownership check
    msg = ChatMessage(thread_id=thread_id, role="user", content=body.content)
    db.add(msg)
    db.commit()
    db.refresh(msg)
    # In production, this would trigger async LLM call and stream response
    assistant_msg = ChatMessage(
        thread_id=thread_id,
        role="assistant",
        content="I'm the ampliFi chat assistant. LLM integration is pending configuration.",
    )
    db.add(assistant_msg)
    db.commit()
    return {"user_message_id": msg.id, "assistant_message_id": assistant_msg.id}


@router.post("/threads/{thread_id}/pin")
def pin_thread(
    thread_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(_require_user),
) -> dict:
    thread = _get_user_thread(thread_id, user, db)
    thread.pinned = not thread.pinned
    db.commit()
    return {"pinned": thread.pinned}


@router.delete("/threads/{thread_id}")
def delete_thread(
    thread_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(_require_user),
) -> dict:
    thread = _get_user_thread(thread_id, user, db)
    db.delete(thread)
    db.commit()
    return {"status": "deleted"}
