"""Auth API endpoints (section 11.1)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.auth.service import (
    authenticate_user,
    create_access_token,
    create_refresh_token,
    decode_token,
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
    token = request_obj.cookies.get("refresh_token")
    if not token:
        raise HTTPException(status_code=401, detail="No refresh token")
    payload = decode_token(token)
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid token type")
    user_id = int(payload["sub"])
    user = db.get(AppUser, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    access_token = create_access_token(user.id, user.role)
    return TokenResponse(access_token=access_token)


@router.post("/logout")
async def logout(response: Response) -> dict:
    response.delete_cookie("refresh_token")
    return {"status": "ok"}


@router.get("/me")
async def me(user: AppUser = Depends(get_current_user)) -> UserInfo | dict:
    if user is None:
        return {"authenticated": False}
    return UserInfo.model_validate(user)


# --- Azure EntraID OIDC routes (§10.1.2) ---

_OIDC_COOKIE = "oidc_pkce"


@router.get("/oidc/start")
async def oidc_start(request: Request, response: Response, db: Session = Depends(get_db)) -> dict:
    """Start OIDC authorization code flow with PKCE."""
    import json
    import secrets as sec

    from app.auth.entraid import EntraIDConfig, build_auth_url
    from app.models.core import AppConfig

    if settings.auth_provider != "entraid":
        raise HTTPException(status_code=400, detail="EntraID auth not enabled")

    cfg_row = db.execute(
        select(AppConfig).where(AppConfig.key == "auth.entraid")
    ).scalar_one_or_none()
    if not cfg_row or not cfg_row.value:
        raise HTTPException(status_code=500, detail="EntraID not configured")

    cfg = EntraIDConfig(cfg_row.value)
    state = sec.token_urlsafe(32)
    nonce = sec.token_urlsafe(32)
    auth_url, code_verifier = build_auth_url(cfg, state, nonce)

    # Store state + verifier in a secure HttpOnly cookie (not in response body)
    pkce_data = json.dumps({"state": state, "code_verifier": code_verifier})
    response.set_cookie(
        key=_OIDC_COOKIE,
        value=pkce_data,
        httponly=True,
        secure=settings.app_env != "dev",
        samesite="lax",
        max_age=600,  # 10 min expiry
    )

    return {"auth_url": auth_url}


@router.get("/oidc/callback")
async def oidc_callback(
    code: str,
    state: str,
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
) -> TokenResponse:
    """Handle OIDC callback and exchange code for tokens."""
    import json

    from app.auth.entraid import (
        EntraIDConfig,
        exchange_code,
        upsert_user_from_claims,
        validate_id_token,
    )
    from app.models.core import AppConfig

    if settings.auth_provider != "entraid":
        raise HTTPException(status_code=400, detail="EntraID auth not enabled")

    # Retrieve and validate PKCE state from secure cookie
    pkce_raw = request.cookies.get(_OIDC_COOKIE)
    if not pkce_raw:
        raise HTTPException(status_code=400, detail="Missing OIDC session cookie")

    try:
        pkce_data = json.loads(pkce_raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid OIDC session cookie") from exc

    expected_state = pkce_data.get("state", "")
    code_verifier = pkce_data.get("code_verifier", "")

    if not expected_state or state != expected_state:
        raise HTTPException(status_code=400, detail="OIDC state mismatch (CSRF)")

    # Clear the PKCE cookie
    response.delete_cookie(_OIDC_COOKIE)

    cfg_row = db.execute(
        select(AppConfig).where(AppConfig.key == "auth.entraid")
    ).scalar_one_or_none()
    if not cfg_row or not cfg_row.value:
        raise HTTPException(status_code=500, detail="EntraID not configured")

    cfg = EntraIDConfig(cfg_row.value)
    token_response = exchange_code(cfg, code, code_verifier)
    id_token = token_response.get("id_token", "")
    if not id_token:
        raise HTTPException(status_code=401, detail="No id_token in response")

    claims = validate_id_token(cfg, id_token)
    user = upsert_user_from_claims(claims, cfg, db)

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
