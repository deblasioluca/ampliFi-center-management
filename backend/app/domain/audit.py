"""Audit logging helper — writes to cleanup.audit_log on every state-changing action."""

from __future__ import annotations

from fastapi import Request
from sqlalchemy.orm import Session

from app.models.core import AuditLog


def write_audit(
    db: Session,
    *,
    action: str,
    entity_type: str | None = None,
    entity_id: str | int | None = None,
    actor_id: int | None = None,
    actor_email: str | None = None,
    before: dict | None = None,
    after: dict | None = None,
    request: Request | None = None,
) -> AuditLog:
    ip = None
    request_id = None
    if request:
        ip = request.client.host if request.client else None
        request_id = request.headers.get("x-request-id")

    entry = AuditLog(
        action=action,
        entity_type=entity_type,
        entity_id=str(entity_id) if entity_id is not None else None,
        actor_id=actor_id,
        actor_email=actor_email,
        before=before,
        after=after,
        ip_address=ip,
        request_id=request_id,
    )
    db.add(entry)
    return entry
