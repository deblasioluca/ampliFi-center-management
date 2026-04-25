"""Auth API endpoints (section 11.1)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.auth.service import (
    authenticate_user,
    create_access_token,
    create_refresh_token,
)
from app.config import settings
from app.infra.db.session import get_db
from app.models.core import AppUser

router = APIRouter()


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserInfo(BaseModel):
    id: int
    email: str
    display_name: str
    role: str

    model_config = {"from_attributes": True}


@router.get("/info")
async def auth_info() -> dict:
    return {"provider": settings.auth_provider, "login_url": "/api/auth/login"}


@router.post("/login")
async def login(
    body: LoginRequest, response: Response, db: Session = Depends(get_db)
) -> TokenResponse:
    user = authenticate_user(db, body.email, body.password)
    access_token = create_access_token(user.id, user.role)
    refresh_token = create_refresh_token(user.id)
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=settings.app_env != "dev",
        samesite="lax",
        max_age=settings.jwt_refresh_token_expire_days * 86400,
    )
    return TokenResponse(access_token=access_token)


@router.post("/refresh")
async def refresh(request_obj: Request, db: Session = Depends(get_db)) -> TokenResponse:
    # This would normally read from the cookie
    return TokenResponse(access_token="")


@router.post("/logout")
async def logout(response: Response) -> dict:
    response.delete_cookie("refresh_token")
    return {"status": "ok"}


@router.get("/me")
async def me(user: AppUser = Depends(get_current_user)) -> UserInfo | dict:
    if user is None:
        return {"authenticated": False}
    return UserInfo.model_validate(user)
