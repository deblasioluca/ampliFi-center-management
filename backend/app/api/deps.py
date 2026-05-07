"""Shared API dependencies (auth, DB session, pagination)."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.infra.db.session import get_db


@dataclass
class PaginationParams:
    page: int
    size: int


def pagination(
    page: int = Query(1, ge=1),
    size: int = Query(100, ge=1, le=10000),
) -> PaginationParams:
    return PaginationParams(page=page, size=size)


def get_current_user(request: Request, db: Session = Depends(get_db)):  # type: ignore[no-untyped-def]
    from app.auth.service import get_user_from_request

    return get_user_from_request(request, db)


# Legacy alias: "analyst" is treated as "data_manager"
_ROLE_ALIASES: dict[str, str] = {"analyst": "data_manager"}


def _user_roles(user) -> set[str]:  # type: ignore[no-untyped-def]
    """Parse comma-separated role string into a set, applying aliases."""
    raw = {r.strip() for r in (user.role or "").split(",") if r.strip()}
    resolved: set[str] = set()
    for r in raw:
        resolved.add(_ROLE_ALIASES.get(r, r))
    return resolved


def require_role(*roles: str):  # type: ignore[no-untyped-def]
    # Expand required role set with aliases
    expanded = set(roles)
    for alias, canonical in _ROLE_ALIASES.items():
        if canonical in expanded:
            expanded.add(alias)
        if alias in expanded:
            expanded.add(canonical)

    def _check(user=Depends(get_current_user)):  # type: ignore[no-untyped-def]
        if user is None:
            raise HTTPException(status_code=401, detail="Not authenticated")
        user_roles = _user_roles(user)
        if not user_roles & expanded:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user

    return _check
