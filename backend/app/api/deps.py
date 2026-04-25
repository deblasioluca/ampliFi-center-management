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


def require_role(*roles: str):  # type: ignore[no-untyped-def]
    def _check(user=Depends(get_current_user)):  # type: ignore[no-untyped-def]
        if user is None:
            raise HTTPException(status_code=401, detail="Not authenticated")
        if user.role not in roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user

    return _check
