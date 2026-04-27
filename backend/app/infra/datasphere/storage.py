"""Data storage routing layer — routes reads/writes to local DB or Datasphere.

Usage:
    from app.infra.datasphere.storage import get_storage_mode, is_datasphere_enabled

    mode = get_storage_mode("cost_center")  # "local" or "datasphere"
    if is_datasphere_enabled("balance"):
        # Route to HANA instead of PostgreSQL
        ...
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _get_ds_config(db: Session) -> Any | None:
    """Load the singleton DatasphereConfig record (cached per request)."""
    from app.models.core import DatasphereConfig

    return db.query(DatasphereConfig).first()


def get_storage_mode(domain: str, db: Session | None = None) -> str:
    """Return 'local' or 'datasphere' for a given data domain.

    Falls back to 'local' if:
    - Datasphere feature flag is off
    - No DatasphereConfig exists
    - DatasphereConfig.is_active is False
    - Domain is not enabled in domain_config
    """
    from app.config import settings

    if not settings.feature_datasphere:
        return "local"

    from app.models.core import LOCAL_ONLY_DOMAINS

    if domain in LOCAL_ONLY_DOMAINS:
        return "local"

    if db is None:
        from app.infra.db.session import SessionLocal

        db = SessionLocal()
        try:
            return _resolve_mode(domain, db)
        finally:
            db.close()
    return _resolve_mode(domain, db)


def _resolve_mode(domain: str, db: Session) -> str:
    config = _get_ds_config(db)
    if not config or not config.is_active:
        return "local"

    domain_settings = (config.domain_config or {}).get(domain, {})
    if domain_settings.get("enabled"):
        return "datasphere"
    return "local"


def is_datasphere_enabled(domain: str, db: Session | None = None) -> bool:
    """Check if a domain is routed to Datasphere."""
    return get_storage_mode(domain, db) == "datasphere"


def get_datasphere_table(domain: str, db: Session | None = None) -> str | None:
    """Return the configured Datasphere table name for a domain, or None."""

    if db is None:
        from app.infra.db.session import SessionLocal

        db = SessionLocal()
        try:
            return _resolve_table(domain, db)
        finally:
            db.close()
    return _resolve_table(domain, db)


def _resolve_table(domain: str, db: Session) -> str | None:
    from app.infra.datasphere.ddl import DEFAULT_TABLE_NAMES

    config = _get_ds_config(db)
    if not config or not config.is_active:
        return None

    domain_settings = (config.domain_config or {}).get(domain, {})
    if domain_settings.get("enabled"):
        return domain_settings.get("table_name") or DEFAULT_TABLE_NAMES.get(domain)
    return None


def get_datasphere_client(db: Session | None = None) -> Any | None:
    """Create a DatasphereClient from the stored config. Returns None if not configured."""
    if db is None:
        from app.infra.db.session import SessionLocal

        db = SessionLocal()
        try:
            return _build_client(db)
        finally:
            db.close()
    return _build_client(db)


def _build_client(db: Session) -> Any | None:
    from app.infra.datasphere.client import DatasphereClient

    config = _get_ds_config(db)
    if not config or not config.ds_url:
        return None

    password = ""
    if config.ds_password_encrypted:
        try:
            from app.infra.sap.encryption import decrypt_password

            password = decrypt_password(config.ds_password_encrypted)
        except Exception:
            logger.warning("Failed to decrypt Datasphere password")

    return DatasphereClient(
        url=config.ds_url,
        schema=config.ds_schema,
        user=config.ds_user or "",
        password=password,
        use_ssl=config.ds_use_ssl,
    )
