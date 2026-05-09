"""Auth API endpoints (section 11.1)."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
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
    username: str | None = None
    email: str | None = None
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserInfo(BaseModel):
    id: int
    username: str
    email: str | None = None
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
    login_id = body.username or body.email
    if not login_id:
        raise HTTPException(status_code=422, detail="Username is required")
    user = authenticate_user(db, login_id, body.password)
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


# --- Azure EntraID configuration & SPA flow ---


@router.get("/entra/config")
async def entra_config(request: Request) -> dict:
    """Return Entra ID configuration for MSAL.js SPA login."""
    enabled = bool(settings.entraid_client_id and settings.entraid_tenant_id)
    result: dict = {"enabled": enabled}
    if enabled:
        result["client_id"] = settings.entraid_client_id
        result["tenant_id"] = settings.entraid_tenant_id
        # SPA flow: no client secret needed, MSAL.js handles everything
        has_secret = bool(settings.entraid_client_secret.get_secret_value())
        result["auth_mode"] = "server" if has_secret else "spa"
        result["show_claims"] = settings.entraid_show_claims
        # SPA redirect URI → the login page itself
        # Respects TLS settings: external_url > direct > proxy > off
        if settings.tls_external_url:
            base = settings.tls_external_url.rstrip("/")
            result["spa_redirect_uri"] = f"{base}/login"
        else:
            tls_mode = settings.tls_mode.lower()
            if tls_mode == "direct":
                proto = "https"
            else:
                proto = request.headers.get("x-forwarded-proto", request.url.scheme)
            host = request.headers.get(
                "x-forwarded-host",
                request.headers.get("host", "localhost"),
            )
            result["spa_redirect_uri"] = f"{proto}://{host}/login"
    return result


class _SpaTokenRequest(BaseModel):
    id_token: str
    access_token: str = ""


@router.post("/entra/token")
async def entra_spa_token(
    body: _SpaTokenRequest,
    response: Response,
    db: Session = Depends(get_db),
) -> TokenResponse:
    """Exchange an MSAL.js id_token for an app session JWT (SPA flow)."""
    from app.auth.entraid import (
        EntraIDConfig,
        upsert_user_from_claims,
        validate_id_token,
    )
    from app.models.core import AppConfig

    if not settings.entraid_client_id:
        raise HTTPException(status_code=500, detail="EntraID not configured")

    # Read full config from DB (includes role_map for group-based role assignment)
    cfg_row = db.execute(
        select(AppConfig).where(AppConfig.key == "auth.entraid")
    ).scalar_one_or_none()
    if cfg_row and cfg_row.value:
        cfg = EntraIDConfig(cfg_row.value)
    else:
        cfg = EntraIDConfig(
            {
                "tenant_id": settings.entraid_tenant_id,
                "client_id": settings.entraid_client_id,
                "client_secret": (settings.entraid_client_secret.get_secret_value()),
                "redirect_uri": "",
            }
        )

    claims = validate_id_token(cfg, body.id_token)
    user = upsert_user_from_claims(claims, cfg, db)

    if not user.is_active:
        raise HTTPException(status_code=401, detail="Account disabled")
    if user.locked_until and user.locked_until > datetime.now(UTC):
        raise HTTPException(status_code=423, detail="Account locked")

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


# --- Azure EntraID OIDC routes (§10.1.2 — server-side flow) ---

_OIDC_COOKIE = "oidc_pkce"


def _get_fernet():
    """Derive a Fernet key from the app secret for encrypting OIDC cookies."""
    import base64
    import hashlib

    from cryptography.fernet import Fernet

    key_bytes = hashlib.sha256(settings.app_secret_key.get_secret_value().encode()).digest()
    return Fernet(base64.urlsafe_b64encode(key_bytes))


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

    # Store state + verifier in an encrypted HttpOnly cookie
    pkce_json = json.dumps({"state": state, "code_verifier": code_verifier, "nonce": nonce})
    pkce_data = _get_fernet().encrypt(pkce_json.encode()).decode()
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
        pkce_data = json.loads(_get_fernet().decrypt(pkce_raw.encode()).decode())
    except Exception as exc:
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

    # Verify nonce to prevent token replay attacks
    expected_nonce = pkce_data.get("nonce", "")
    if not expected_nonce:
        raise HTTPException(status_code=400, detail="Missing nonce in OIDC session")
    if claims.get("nonce") != expected_nonce:
        raise HTTPException(status_code=400, detail="OIDC nonce mismatch")

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
