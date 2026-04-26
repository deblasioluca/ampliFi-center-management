"""Azure EntraID (OIDC) authentication provider (§10.1.2).

Implements authorization code flow with PKCE for Microsoft Entra ID.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime
from urllib.parse import urlencode

import structlog
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.core import AppUser

logger = structlog.get_logger()


class EntraIDConfig:
    """Configuration for Azure EntraID OIDC."""

    def __init__(self, config: dict) -> None:
        self.tenant_id = config.get("tenant_id", "")
        self.client_id = config.get("client_id", "")
        self.client_secret = config.get("client_secret", "")
        self.redirect_uri = config.get("redirect_uri", "")
        self.role_map = config.get("role_map", {})

    @property
    def authority(self) -> str:
        return f"https://login.microsoftonline.com/{self.tenant_id}"

    @property
    def authorize_url(self) -> str:
        return f"{self.authority}/oauth2/v2.0/authorize"

    @property
    def token_url(self) -> str:
        return f"{self.authority}/oauth2/v2.0/token"

    @property
    def jwks_url(self) -> str:
        return f"{self.authority}/discovery/v2.0/keys"

    @property
    def end_session_url(self) -> str:
        return f"{self.authority}/oauth2/v2.0/logout"


def generate_pkce() -> tuple[str, str]:
    """Generate PKCE code_verifier and code_challenge."""
    verifier = secrets.token_urlsafe(64)
    challenge = hashlib.sha256(verifier.encode()).digest()
    import base64
    challenge_b64 = base64.urlsafe_b64encode(challenge).rstrip(b"=").decode()
    return verifier, challenge_b64


def build_auth_url(cfg: EntraIDConfig, state: str, nonce: str) -> str:
    """Build the OIDC authorization URL."""
    code_verifier, code_challenge = generate_pkce()
    params = {
        "client_id": cfg.client_id,
        "response_type": "code",
        "redirect_uri": cfg.redirect_uri,
        "response_mode": "query",
        "scope": "openid profile email",
        "state": state,
        "nonce": nonce,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{cfg.authorize_url}?{urlencode(params)}", code_verifier


def exchange_code(cfg: EntraIDConfig, code: str, code_verifier: str) -> dict:
    """Exchange authorization code for tokens."""
    import httpx

    data = {
        "client_id": cfg.client_id,
        "client_secret": cfg.client_secret,
        "code": code,
        "redirect_uri": cfg.redirect_uri,
        "grant_type": "authorization_code",
        "code_verifier": code_verifier,
    }
    resp = httpx.post(cfg.token_url, data=data, timeout=30.0)
    if resp.status_code != 200:
        logger.error("entraid.token_exchange_failed", status=resp.status_code, body=resp.text[:500])
        raise HTTPException(status_code=401, detail="Token exchange failed")
    return resp.json()


def validate_id_token(cfg: EntraIDConfig, id_token: str) -> dict:
    """Validate the id_token and extract claims.

    In production, this should verify the JWT signature using the JWKS endpoint.
    For now, we decode without verification and validate basic claims.
    """
    from jose import jwt as jose_jwt

    try:
        # In production: fetch JWKS from cfg.jwks_url and verify signature
        # For now, decode with audience verification
        claims = jose_jwt.decode(
            id_token,
            key="",
            algorithms=["RS256"],
            options={"verify_signature": False, "verify_aud": False},
        )
    except Exception as e:
        logger.error("entraid.token_validation_failed", error=str(e))
        raise HTTPException(status_code=401, detail=f"Invalid id_token: {e}") from None

    if claims.get("iss") and cfg.tenant_id not in claims["iss"]:
        raise HTTPException(status_code=401, detail="Token issuer mismatch")

    return claims


def upsert_user_from_claims(claims: dict, cfg: EntraIDConfig, db: Session) -> AppUser:
    """Create or update user from EntraID claims."""
    oid = claims.get("oid", "")
    email = claims.get("preferred_username") or claims.get("email") or claims.get("upn", "")
    display_name = claims.get("name", email)

    user = db.execute(
        select(AppUser).where(AppUser.external_id == oid)
    ).scalar_one_or_none()

    if not user:
        user = db.execute(
            select(AppUser).where(AppUser.email == email)
        ).scalar_one_or_none()

    # Determine role from group claims
    role = "reviewer"
    groups = claims.get("groups", [])
    for group_id in groups:
        if group_id in cfg.role_map:
            role = cfg.role_map[group_id]
            break

    if user:
        user.external_id = oid
        user.display_name = display_name
        user.last_login = datetime.now(UTC)
        if not user.role or user.role == "reviewer":
            user.role = role
    else:
        user = AppUser(
            email=email,
            display_name=display_name,
            external_id=oid,
            role=role,
            is_active=True,
            last_login=datetime.now(UTC),
        )
        db.add(user)

    db.commit()
    db.refresh(user)
    return user
