"""Authentication service — local (bcrypt + JWT) provider."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, Request
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.models.core import AppUser

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(user_id: int, role: str) -> str:
    expires = datetime.now(UTC) + timedelta(minutes=settings.jwt_access_token_expire_minutes)
    payload = {
        "sub": str(user_id),
        "role": role,
        "exp": expires,
        "type": "access",
    }
    return jwt.encode(
        payload, settings.app_secret_key.get_secret_value(), algorithm=settings.jwt_algorithm
    )


def create_refresh_token(user_id: int) -> str:
    expires = datetime.now(UTC) + timedelta(days=settings.jwt_refresh_token_expire_days)
    payload = {
        "sub": str(user_id),
        "exp": expires,
        "type": "refresh",
    }
    return jwt.encode(
        payload, settings.app_secret_key.get_secret_value(), algorithm=settings.jwt_algorithm
    )


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(
            token, settings.app_secret_key.get_secret_value(), algorithms=[settings.jwt_algorithm]
        )
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}") from None


def authenticate_user(db: Session, email: str, password: str) -> AppUser:
    stmt = select(AppUser).where(AppUser.email == email)
    user = db.execute(stmt).scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not user.is_active:
        raise HTTPException(status_code=401, detail="Account disabled")
    if user.locked_until and user.locked_until > datetime.now(UTC):
        raise HTTPException(status_code=423, detail="Account locked")
    if user.password_hash is None or not verify_password(password, user.password_hash):
        user.failed_logins += 1
        if user.failed_logins >= 5:
            user.locked_until = datetime.now(UTC) + timedelta(minutes=15)
        db.commit()
        raise HTTPException(status_code=401, detail="Invalid credentials")
    user.failed_logins = 0
    user.last_login = datetime.now(UTC)
    db.commit()
    return user


def get_user_from_request(request: Request, db: Session) -> AppUser | None:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        token = request.cookies.get("access_token")
        if not token:
            return None
    else:
        token = auth_header[7:]
    payload = decode_token(token)
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")
    user_id = int(payload["sub"])
    stmt = select(AppUser).where(AppUser.id == user_id, AppUser.is_active.is_(True))
    user = db.execute(stmt).scalar_one_or_none()
    return user
