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
    size: int = Query(100, ge=1, le=1000),
) -> PaginationParams:
    return PaginationParams(page=page, size=size)


def get_current_user(request: Request, db: Session = Depends(get_db)):  # type: ignore[no-untyped-def]
    from app.auth.service import get_user_from_request

    return get_user_from_request(request, db)


# Role aliases: data_manager has the same access as analyst
_ROLE_ALIASES: dict[str, str] = {"data_manager": "analyst"}


def require_role(*roles: str):  # type: ignore[no-untyped-def]
    # Expand role set: if "analyst" is accepted, "data_manager" is too (and vice versa)
    expanded = set(roles)
    for alias, canonical in _ROLE_ALIASES.items():
        if canonical in expanded:
            expanded.add(alias)
        if alias in expanded:
            expanded.add(canonical)

    def _check(user=Depends(get_current_user)):  # type: ignore[no-untyped-def]
        if user is None:
            raise HTTPException(status_code=401, detail="Not authenticated")
        if user.role not in expanded:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user

    return _check
