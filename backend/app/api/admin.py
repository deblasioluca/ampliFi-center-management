"""Admin API endpoints (sections 11.2-11.5, 11.11)."""

from __future__ import annotations

import time
import uuid

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination, require_role
from app.auth.service import hash_password
from app.infra.db.session import get_db
from app.models.core import (
    ALL_CATEGORIES,
    ALL_SCOPES,
    SCOPE_CLEANUP,
    SCOPE_UPLOAD_RULES,
    AppConfig,
    AppUser,
    AuditLog,
    Routine,
    SAPConnection,
    SAPConnectionProbe,
    TaskRun,
    UploadBatch,
    UploadError,
)

router = APIRouter()


# --- Users ---


class UserCreate(BaseModel):
    username: str
    email: str | None = None
    display_name: str
    password: str
    role: str = "data_manager"


class UserUpdate(BaseModel):
    display_name: str | None = None
    role: str | None = None
    is_active: bool | None = None


class UserOut(BaseModel):
    id: int
    username: str
    email: str | None = None
    display_name: str
    role: str
    is_active: bool

    model_config = {"from_attributes": True}


@router.get("/users")
def list_users(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    total = db.execute(select(func.count(AppUser.id))).scalar() or 0
    stmt = select(AppUser).offset((pag.page - 1) * pag.size).limit(pag.size)
    users = db.execute(stmt).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [UserOut.model_validate(u).model_dump() for u in users],
    }


@router.post("/users")
def create_user(
    body: UserCreate,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> UserOut:
    existing = db.execute(
        select(AppUser).where(AppUser.username == body.username)
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Username already taken")
    user = AppUser(
        username=body.username,
        email=body.email,
        display_name=body.display_name,
        password_hash=hash_password(body.password),
        role=body.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return UserOut.model_validate(user)


@router.get("/users/{user_id}")
def get_user(
    user_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> UserOut:
    user = db.get(AppUser, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserOut.model_validate(user)


@router.patch("/users/{user_id}")
def update_user(
    user_id: int,
    body: UserUpdate,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> UserOut:
    user = db.get(AppUser, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.id == _user.id:
        if body.is_active is not None and not body.is_active:
            raise HTTPException(status_code=409, detail="Cannot deactivate your own account")
        if body.role is not None:
            new_roles = {r.strip() for r in body.role.split(",")}
            if "admin" not in new_roles:
                raise HTTPException(
                    status_code=409,
                    detail="Cannot remove admin from own account",
                )
    if body.display_name is not None:
        user.display_name = body.display_name
    if body.role is not None:
        user.role = body.role
    if body.is_active is not None:
        user.is_active = body.is_active
    db.commit()
    db.refresh(user)
    return UserOut.model_validate(user)


class PasswordReset(BaseModel):
    new_password: str


@router.post("/users/{user_id}/reset-password")
def reset_user_password(
    user_id: int,
    body: PasswordReset,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    if len(body.new_password) < 4:
        raise HTTPException(status_code=422, detail="Password must be at least 4 characters")
    user = db.get(AppUser, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.password_hash = hash_password(body.new_password)
    user.failed_logins = 0
    user.locked_until = None
    db.commit()
    return {"status": "password_reset", "username": user.username}


@router.delete("/users/{user_id}")
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    user = db.get(AppUser, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.id == _user.id:
        raise HTTPException(status_code=409, detail="Cannot deactivate your own account")
    user.is_active = False
    db.commit()
    return {"status": "deactivated"}


# --- Configuration ---


class ConfigValue(BaseModel):
    value: dict


@router.get("/config/{key}")
def get_config(
    key: str,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    cfg = db.execute(select(AppConfig).where(AppConfig.key == key)).scalar_one_or_none()
    if not cfg:
        return {"key": key, "value": {}}
    return {"key": cfg.key, "value": cfg.value}


@router.put("/config/{key}")
def set_config(
    key: str,
    body: ConfigValue,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    cfg = db.execute(select(AppConfig).where(AppConfig.key == key)).scalar_one_or_none()
    if cfg:
        cfg.value = body.value
        cfg.updated_by = _user.id
    else:
        cfg = AppConfig(key=key, value=body.value, updated_by=_user.id)
        db.add(cfg)
    db.commit()
    return {"key": key, "value": body.value}


# --- Email test ---


@router.post("/config/email/test")
def test_email_connection(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    cfg = db.execute(select(AppConfig).where(AppConfig.key == "email")).scalar_one_or_none()
    if not cfg or not cfg.value:
        return {"status": "error", "error": "No email configuration found"}
    v = cfg.value
    from app.infra.email.engine import EmailEngine

    engine = EmailEngine(
        host=v.get("host", "localhost"),
        port=v.get("port", 587),
        username=v.get("username", ""),
        password=v.get("password", ""),
        use_tls=v.get("tls", "none") != "none",
        from_address=v.get("from_address", "noreply@amplifi.dev"),
        from_name=v.get("from_name", "ampliFi"),
    )
    return engine.test_connection()


class SendTestEmail(BaseModel):
    to: str


@router.post("/config/email/send-test")
def send_test_email(
    body: SendTestEmail,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    cfg = db.execute(select(AppConfig).where(AppConfig.key == "email")).scalar_one_or_none()
    if not cfg or not cfg.value:
        return {"sent": False, "error": "No email configuration found"}
    v = cfg.value
    from app.infra.email.engine import EmailEngine

    engine = EmailEngine(
        host=v.get("host", "localhost"),
        port=v.get("port", 587),
        username=v.get("username", ""),
        password=v.get("password", ""),
        use_tls=v.get("tls", "none") != "none",
        from_address=v.get("from_address", "noreply@amplifi.dev"),
        from_name=v.get("from_name", "ampliFi"),
    )
    result = engine.send(
        to=body.to,
        template_name="password_reset",
        context={
            "user_name": _user.display_name,
            "reset_url": "https://amplifi.dev/test",
            "expires_minutes": "60",
        },
    )
    return {"sent": result}


# --- SAP Connections ---


class SAPConnectionCreate(BaseModel):
    name: str
    description: str | None = None
    system_type: str
    landscape_type: str | None = None
    base_url: str
    host: str | None = None
    port: str | None = None
    conn_protocol: str | None = "https"
    client: str = "100"
    language: str = "EN"
    username: str
    password: str
    protocol: str = "odata"
    verify_ssl: bool = True
    use_proxy: bool = False
    saml2_disabled: bool = False
    allowed_tables: str | None = None
    fiori_launchpad_url: str | None = None
    webgui_url: str | None = None
    # Web Dispatcher
    webdisp_host: str | None = None
    webdisp_port: str | None = None
    webdisp_protocol: str | None = "https"
    use_webdisp: bool = False
    adt_use_webdisp: bool | None = None
    soap_use_webdisp: bool | None = None
    odata_use_webdisp: bool | None = None
    # Per-endpoint overrides
    adt_verify_ssl: bool | None = None
    adt_use_proxy: bool | None = None
    adt_saml2_disabled: bool | None = None
    soap_verify_ssl: bool | None = None
    soap_use_proxy: bool | None = None
    soap_saml2_disabled: bool | None = None
    odata_verify_ssl: bool | None = None
    odata_use_proxy: bool | None = None
    odata_saml2_disabled: bool | None = None
    # ICF aliases
    use_icf_aliases: bool = False
    adt_icf_source: str | None = None
    soap_icf_source: str | None = None
    odata_icf_source: str | None = None
    adt_icf_cert: str | None = None
    soap_icf_cert: str | None = None
    odata_icf_cert: str | None = None
    adt_icf_basic: str | None = None
    soap_icf_basic: str | None = None
    odata_icf_basic: str | None = None
    # Per-endpoint certificate source
    adt_cert_source: str | None = None
    soap_cert_source: str | None = None
    odata_cert_source: str | None = None
    # Principal Propagation
    pp_enabled: bool = False
    pp_sap_oauth_token_url: str | None = "/sap/bc/sec/oauth2/token"
    pp_sap_oauth_client_id: str | None = None
    pp_sap_oauth_client_secret: str | None = None
    pp_saml_issuer: str | None = None
    pp_saml_audience: str | None = None
    pp_user_mapping: str | None = "email"


class SAPConnectionUpdate(SAPConnectionCreate):
    password: str | None = None  # optional on update


class SAPConnectionOut(BaseModel):
    id: int
    name: str
    description: str | None
    system_type: str
    landscape_type: str | None
    base_url: str
    host: str | None = None
    port: str | None = None
    conn_protocol: str | None = None
    client: str
    language: str
    username: str
    protocol: str
    verify_ssl: bool
    use_proxy: bool
    saml2_disabled: bool
    is_active: bool
    allowed_tables: str | None
    fiori_launchpad_url: str | None = None
    webgui_url: str | None = None
    webdisp_host: str | None = None
    webdisp_port: str | None = None
    webdisp_protocol: str | None = None
    use_webdisp: bool = False
    adt_use_webdisp: bool | None = None
    soap_use_webdisp: bool | None = None
    odata_use_webdisp: bool | None = None
    adt_verify_ssl: bool | None = None
    adt_use_proxy: bool | None = None
    adt_saml2_disabled: bool | None = None
    soap_verify_ssl: bool | None = None
    soap_use_proxy: bool | None = None
    soap_saml2_disabled: bool | None = None
    odata_verify_ssl: bool | None = None
    odata_use_proxy: bool | None = None
    odata_saml2_disabled: bool | None = None
    use_icf_aliases: bool = False
    adt_icf_source: str | None = None
    soap_icf_source: str | None = None
    odata_icf_source: str | None = None
    adt_icf_cert: str | None = None
    soap_icf_cert: str | None = None
    odata_icf_cert: str | None = None
    adt_icf_basic: str | None = None
    soap_icf_basic: str | None = None
    odata_icf_basic: str | None = None
    adt_cert_source: str | None = None
    soap_cert_source: str | None = None
    odata_cert_source: str | None = None
    pp_enabled: bool = False
    pp_sap_oauth_token_url: str | None = None
    pp_sap_oauth_client_id: str | None = None
    pp_saml_issuer: str | None = None
    pp_saml_audience: str | None = None
    pp_user_mapping: str | None = None

    model_config = {"from_attributes": True}


@router.get("/sap")
def list_sap_connections(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> list[SAPConnectionOut]:
    conns = db.execute(select(SAPConnection)).scalars().all()
    return [SAPConnectionOut.model_validate(c) for c in conns]


@router.post("/sap")
def create_sap_connection(
    body: SAPConnectionCreate,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> SAPConnectionOut:
    from app.infra.sap.encryption import encrypt_password

    pp_secret_enc = None
    if body.pp_sap_oauth_client_secret:
        pp_secret_enc = encrypt_password(body.pp_sap_oauth_client_secret)
    conn = SAPConnection(
        name=body.name,
        description=body.description,
        system_type=body.system_type,
        landscape_type=body.landscape_type,
        base_url=body.base_url,
        host=body.host,
        port=body.port,
        conn_protocol=body.conn_protocol,
        client=body.client,
        language=body.language,
        username=body.username,
        password_encrypted=encrypt_password(body.password),
        protocol=body.protocol,
        verify_ssl=body.verify_ssl,
        use_proxy=body.use_proxy,
        saml2_disabled=body.saml2_disabled,
        allowed_tables=body.allowed_tables,
        fiori_launchpad_url=body.fiori_launchpad_url,
        webgui_url=body.webgui_url,
        webdisp_host=body.webdisp_host,
        webdisp_port=body.webdisp_port,
        webdisp_protocol=body.webdisp_protocol,
        use_webdisp=body.use_webdisp,
        adt_use_webdisp=body.adt_use_webdisp,
        soap_use_webdisp=body.soap_use_webdisp,
        odata_use_webdisp=body.odata_use_webdisp,
        adt_verify_ssl=body.adt_verify_ssl,
        adt_use_proxy=body.adt_use_proxy,
        adt_saml2_disabled=body.adt_saml2_disabled,
        soap_verify_ssl=body.soap_verify_ssl,
        soap_use_proxy=body.soap_use_proxy,
        soap_saml2_disabled=body.soap_saml2_disabled,
        odata_verify_ssl=body.odata_verify_ssl,
        odata_use_proxy=body.odata_use_proxy,
        odata_saml2_disabled=body.odata_saml2_disabled,
        use_icf_aliases=body.use_icf_aliases,
        adt_icf_source=body.adt_icf_source,
        soap_icf_source=body.soap_icf_source,
        odata_icf_source=body.odata_icf_source,
        adt_icf_cert=body.adt_icf_cert,
        soap_icf_cert=body.soap_icf_cert,
        odata_icf_cert=body.odata_icf_cert,
        adt_icf_basic=body.adt_icf_basic,
        soap_icf_basic=body.soap_icf_basic,
        odata_icf_basic=body.odata_icf_basic,
        adt_cert_source=body.adt_cert_source,
        soap_cert_source=body.soap_cert_source,
        odata_cert_source=body.odata_cert_source,
        pp_enabled=body.pp_enabled,
        pp_sap_oauth_token_url=body.pp_sap_oauth_token_url,
        pp_sap_oauth_client_id=body.pp_sap_oauth_client_id,
        pp_sap_oauth_client_secret_enc=pp_secret_enc,
        pp_saml_issuer=body.pp_saml_issuer,
        pp_saml_audience=body.pp_saml_audience,
        pp_user_mapping=body.pp_user_mapping,
    )
    db.add(conn)
    db.commit()
    db.refresh(conn)
    return SAPConnectionOut.model_validate(conn)


@router.get("/sap/{conn_id}")
def get_sap_connection(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> SAPConnectionOut:
    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")
    return SAPConnectionOut.model_validate(conn)


@router.put("/sap/{conn_id}")
def update_sap_connection(
    conn_id: int,
    body: SAPConnectionUpdate,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> SAPConnectionOut:
    from app.infra.sap.encryption import encrypt_password

    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")
    conn.name = body.name
    conn.description = body.description
    conn.system_type = body.system_type
    conn.landscape_type = body.landscape_type
    conn.base_url = body.base_url
    conn.host = body.host
    conn.port = body.port
    conn.conn_protocol = body.conn_protocol
    conn.client = body.client
    conn.language = body.language
    conn.username = body.username
    if body.password:
        conn.password_encrypted = encrypt_password(body.password)
    conn.protocol = body.protocol
    conn.verify_ssl = body.verify_ssl
    conn.use_proxy = body.use_proxy
    conn.saml2_disabled = body.saml2_disabled
    conn.allowed_tables = body.allowed_tables
    conn.fiori_launchpad_url = body.fiori_launchpad_url
    conn.webgui_url = body.webgui_url
    conn.webdisp_host = body.webdisp_host
    conn.webdisp_port = body.webdisp_port
    conn.webdisp_protocol = body.webdisp_protocol
    conn.use_webdisp = body.use_webdisp
    conn.adt_use_webdisp = body.adt_use_webdisp
    conn.soap_use_webdisp = body.soap_use_webdisp
    conn.odata_use_webdisp = body.odata_use_webdisp
    conn.adt_verify_ssl = body.adt_verify_ssl
    conn.adt_use_proxy = body.adt_use_proxy
    conn.adt_saml2_disabled = body.adt_saml2_disabled
    conn.soap_verify_ssl = body.soap_verify_ssl
    conn.soap_use_proxy = body.soap_use_proxy
    conn.soap_saml2_disabled = body.soap_saml2_disabled
    conn.odata_verify_ssl = body.odata_verify_ssl
    conn.odata_use_proxy = body.odata_use_proxy
    conn.odata_saml2_disabled = body.odata_saml2_disabled
    conn.use_icf_aliases = body.use_icf_aliases
    conn.adt_icf_source = body.adt_icf_source
    conn.soap_icf_source = body.soap_icf_source
    conn.odata_icf_source = body.odata_icf_source
    conn.adt_icf_cert = body.adt_icf_cert
    conn.soap_icf_cert = body.soap_icf_cert
    conn.odata_icf_cert = body.odata_icf_cert
    conn.adt_icf_basic = body.adt_icf_basic
    conn.soap_icf_basic = body.soap_icf_basic
    conn.odata_icf_basic = body.odata_icf_basic
    conn.adt_cert_source = body.adt_cert_source
    conn.soap_cert_source = body.soap_cert_source
    conn.odata_cert_source = body.odata_cert_source
    conn.pp_enabled = body.pp_enabled
    conn.pp_sap_oauth_token_url = body.pp_sap_oauth_token_url
    conn.pp_sap_oauth_client_id = body.pp_sap_oauth_client_id
    if body.pp_sap_oauth_client_secret:
        conn.pp_sap_oauth_client_secret_enc = encrypt_password(body.pp_sap_oauth_client_secret)
    conn.pp_saml_issuer = body.pp_saml_issuer
    conn.pp_saml_audience = body.pp_saml_audience
    conn.pp_user_mapping = body.pp_user_mapping
    db.commit()
    db.refresh(conn)
    return SAPConnectionOut.model_validate(conn)


@router.delete("/sap/{conn_id}")
def delete_sap_connection(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")
    db.delete(conn)
    db.commit()
    return {"status": "deleted"}


@router.get("/sap/{conn_id}/probes")
def list_sap_probes(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> list[dict]:
    probes = (
        db.execute(
            select(SAPConnectionProbe)
            .where(SAPConnectionProbe.connection_id == conn_id)
            .order_by(SAPConnectionProbe.probed_at.desc())
            .limit(20)
        )
        .scalars()
        .all()
    )
    return [
        {
            "id": p.id,
            "status": p.status,
            "protocol": p.protocol,
            "latency_ms": p.latency_ms,
            "details": p.details,
            "probed_at": str(p.probed_at),
        }
        for p in probes
    ]


@router.post("/sap/{conn_id}/test")
def test_sap_connection(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")
    from app.infra.sap.client import test_connection

    result = test_connection(conn)
    probe = SAPConnectionProbe(
        connection_id=conn.id,
        status="ok" if result["success"] else "error",
        protocol=conn.protocol,
        latency_ms=result.get("latency_ms"),
        details=result,
        probed_by=_user.id,
    )
    db.add(probe)
    db.commit()
    return result


@router.post("/sap/{conn_id}/trial")
def run_sap_trial(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Full connection trial — tests ALL endpoints (ADT, OData, SOAP).

    Adopted from sap-ai-consultant. Returns a result matrix with per-endpoint
    probes, login check, and recommendations.
    """
    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")
    from app.infra.sap.client import test_connection_full

    result = test_connection_full(conn)

    # Persist per-endpoint probe results
    for probe_data in result.get("probes", []):
        probe = SAPConnectionProbe(
            connection_id=conn.id,
            status="ok" if probe_data.get("success") else "error",
            protocol=probe_data.get("endpoint", "unknown"),
            latency_ms=probe_data.get("latency_ms"),
            details=probe_data,
            probed_by=_user.id,
        )
        db.add(probe)
    db.commit()
    return result


@router.get("/sap/{conn_id}/discover/services")
def discover_sap_services(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Discover available OData services on the SAP gateway."""
    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")
    from app.infra.sap.client import discover_odata_services

    try:
        services = discover_odata_services(conn)
        return {"connection": conn.name, "services": services, "count": len(services)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


# --- Object Bindings ---


class ObjectBindingCreate(BaseModel):
    object_type: str  # cost_center|profit_center|hierarchy|balance|gl_account|employee
    scope: str = SCOPE_CLEANUP
    data_category: str = "legacy"
    entity_set: str | None = None
    path: str | None = None
    params: dict | None = None
    schedule_cron: str | None = None
    enabled: bool = True


@router.get("/sap/{conn_id}/bindings")
def list_object_bindings(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager")),
) -> list[dict]:
    """List object bindings for a SAP connection."""
    from app.models.core import SAPObjectBinding

    bindings = (
        db.execute(select(SAPObjectBinding).where(SAPObjectBinding.connection_id == conn_id))
        .scalars()
        .all()
    )
    return [
        {
            "id": b.id,
            "object_type": b.object_type,
            "scope": b.scope,
            "data_category": b.data_category,
            "entity_set": b.entity_set,
            "path": b.path,
            "params": b.params,
            "schedule_cron": b.schedule_cron,
            "enabled": b.enabled,
        }
        for b in bindings
    ]


@router.post("/sap/{conn_id}/bindings")
def create_object_binding(
    conn_id: int,
    body: ObjectBindingCreate,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Create an object binding (what data to extract from this SAP system)."""
    from app.models.core import SAPObjectBinding

    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")
    if body.scope not in ALL_SCOPES:
        raise HTTPException(status_code=400, detail=f"Invalid scope: {body.scope}")
    if body.data_category not in ALL_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"Invalid data_category: {body.data_category}")

    binding = SAPObjectBinding(
        connection_id=conn_id,
        object_type=body.object_type,
        scope=body.scope,
        data_category=body.data_category,
        entity_set=body.entity_set,
        path=body.path,
        params=body.params,
        schedule_cron=body.schedule_cron,
        enabled=body.enabled,
    )
    db.add(binding)
    db.commit()
    db.refresh(binding)
    return {
        "id": binding.id,
        "object_type": binding.object_type,
        "scope": binding.scope,
        "data_category": binding.data_category,
        "entity_set": binding.entity_set,
        "enabled": binding.enabled,
    }


@router.delete("/sap/{conn_id}/bindings/{binding_id}")
def delete_object_binding(
    conn_id: int,
    binding_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.models.core import SAPObjectBinding

    binding = db.get(SAPObjectBinding, binding_id)
    if not binding or binding.connection_id != conn_id:
        raise HTTPException(status_code=404, detail="Object binding not found")
    db.delete(binding)
    db.commit()
    return {"status": "deleted"}


@router.post("/sap/{conn_id}/bindings/{binding_id}/test")
def test_object_binding(
    conn_id: int,
    binding_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Test whether the retrieval method for a binding actually works."""
    from app.models.core import SAPObjectBinding

    binding = db.get(SAPObjectBinding, binding_id)
    if not binding or binding.connection_id != conn_id:
        raise HTTPException(status_code=404, detail="Object binding not found")

    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")

    raw = binding.entity_set or binding.path or ""
    # Parse protocol prefix (e.g. "odata:CostCenterSet", "adt:/sap/...", "rfc:BAPI_...")
    if ":" in raw and not raw.startswith("/"):
        proto, path = raw.split(":", 1)
        proto = proto.lower()
    else:
        proto, path = "odata", raw

    try:
        if proto == "odata":
            from app.infra.sap.client import fetch_odata

            result = fetch_odata(conn, path, params={"$top": "1"})
            row_count = len(result) if result else 0
        elif proto == "adt":
            from urllib.parse import parse_qs, urlparse

            from app.infra.sap.client import fetch_adt_table

            parsed = urlparse(path)
            table_name = parse_qs(parsed.query).get("table", [path.split(":")[-1]])[0]
            result = fetch_adt_table(conn, table_name, max_rows=1)
            row_count = len(result) if result else 0
        elif proto == "rfc":
            parts = path.split(":")
            fm_name = parts[0] if parts else "RFC_READ_TABLE"

            if fm_name == "RFC_READ_TABLE" and len(parts) > 1:
                from app.infra.sap.client import call_rfc_read_table

                table_name = parts[-1]
                result = call_rfc_read_table(conn, table_name, max_rows=1)
                row_count = len(result) if result else 0
            elif fm_name.startswith("BAPI_"):
                from app.infra.sap.client import call_bapi

                bapi_result = call_bapi(conn, fm_name)
                if not bapi_result.get("success"):
                    return {
                        "success": False,
                        "error": bapi_result.get("error_message", "BAPI call failed"),
                        "entity_set": raw,
                        "protocol": proto,
                    }
                row_count = sum(len(rows) for rows in bapi_result.get("tables", {}).values())
            else:
                from app.infra.sap.client import call_soap_rfc

                rfc_result = call_soap_rfc(conn, fm_name)
                if not rfc_result.get("success"):
                    return {
                        "success": False,
                        "error": rfc_result.get("error_message", "RFC call failed"),
                        "entity_set": raw,
                        "protocol": proto,
                    }
                row_count = sum(len(rows) for rows in rfc_result.get("tables", {}).values())
        else:
            return {
                "success": False,
                "error": f"Unknown protocol prefix: {proto}",
                "entity_set": raw,
                "protocol": proto,
            }
        return {
            "success": True,
            "message": (
                f"Binding test OK — retrieved {row_count}"
                f" test row(s) via {proto.upper()} from {path}"
            ),
            "entity_set": raw,
            "protocol": proto,
        }
    except Exception as exc:
        return {
            "success": False,
            "error": f"Test failed: {exc}",
            "entity_set": raw,
            "protocol": proto,
        }


class ExtractionParams(BaseModel):
    co_area: str | None = None
    controlling_area: str | None = None
    hierarchy_name: str | None = None
    set_name: str | None = None
    company_code: str | None = None
    period_from: str | None = None
    period_to: str | None = None
    ledger: str | None = None
    gaap: str | None = None


@router.post("/sap/{conn_id}/bindings/{binding_id}/extract")
def extract_via_binding(
    conn_id: int,
    binding_id: int,
    body: ExtractionParams | None = None,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager")),
) -> dict:
    """Extract data from SAP using a specific binding's configuration."""
    from app.models.core import SAPObjectBinding

    binding = db.get(SAPObjectBinding, binding_id)
    if not binding or binding.connection_id != conn_id:
        raise HTTPException(status_code=404, detail="Object binding not found")

    from app.services.sap_extraction import extract_from_sap

    # Parse binding entity_set to determine protocol
    raw = binding.entity_set or binding.path or ""
    retrieval_method = None
    if ":" in raw and not raw.startswith("/"):
        retrieval_method = raw

    # Merge binding.params with request body params (body takes precedence)
    merged_params = dict(binding.params or {})
    if body:
        for k, v in body.model_dump(exclude_none=True).items():
            merged_params[k] = v

    try:
        result = extract_from_sap(
            db,
            conn_id,
            binding.object_type,
            merged_params or None,
            retrieval_method=retrieval_method,
            scope=getattr(binding, "scope", None) or "cleanup",
            data_category=getattr(binding, "data_category", None) or "legacy",
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"SAP extraction failed: {e}",
        ) from e


# --- SAP Value Lookups (dynamic selectors) ---


@router.get("/sap/{conn_id}/lookup/co-areas")
def lookup_co_areas(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager")),
) -> list[dict]:
    """Fetch available controlling areas from SAP (table TKA01)."""
    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")

    from app.infra.sap.client import fetch_adt_table

    try:
        rows = fetch_adt_table(conn, "TKA01", max_rows=500)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Cannot read CO areas: {exc}") from exc

    # Try to get descriptions from text table TKA02
    desc_map: dict[str, str] = {}
    try:
        text_rows = fetch_adt_table(conn, "TKA02", max_rows=500, where="SPRAS = 'E'")
        for tr in text_rows:
            k = tr.get("KOKRS", "").strip()
            d = tr.get("BEZEI", "").strip()
            if k and d:
                desc_map[k] = d
    except Exception:  # noqa: S110
        pass

    result = []
    seen: set[str] = set()
    for row in rows:
        kokrs = row.get("KOKRS", "").strip()
        if not kokrs or kokrs in seen:
            continue
        seen.add(kokrs)
        result.append(
            {
                "co_area": kokrs,
                "description": desc_map.get(kokrs, ""),
            }
        )
    result.sort(key=lambda x: x["co_area"])
    return result


@router.get("/sap/{conn_id}/lookup/hierarchies")
def lookup_hierarchies(
    conn_id: int,
    co_area: str | None = None,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager")),
) -> list[dict]:
    """Fetch available hierarchies from SAP (table SETHEADER)."""
    conn = db.get(SAPConnection, conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="SAP connection not found")

    from app.infra.sap.client import fetch_adt_table

    # SETCLASS 0101 = cost center hierarchies, 0104 = profit center hierarchies
    where = "SETCLASS = '0101' OR SETCLASS = '0104'"
    if co_area:
        import re

        if not re.match(r"^[A-Za-z0-9_\-./]+$", co_area):
            raise HTTPException(status_code=400, detail="Invalid CO area value")
        where = f"(SETCLASS = '0101' OR SETCLASS = '0104') AND SUBCLASS = '{co_area}'"

    try:
        rows = fetch_adt_table(conn, "SETHEADER", max_rows=1000, where=where)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Cannot read hierarchies: {exc}") from exc

    result = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        setname = row.get("SETNAME", "").strip()
        setclass = row.get("SETCLASS", "").strip()
        if not setname or (setclass, setname) in seen:
            continue
        seen.add((setclass, setname))
        kind = (
            "cost_center"
            if setclass == "0101"
            else "profit_center"
            if setclass == "0104"
            else setclass
        )
        result.append(
            {
                "set_name": setname,
                "description": row.get("DESSION", "").strip() or row.get("LTEXT", "").strip(),
                "set_class": setclass,
                "kind": kind,
                "co_area": row.get("SUBCLASS", "").strip(),
            }
        )
    result.sort(key=lambda x: (x["kind"], x["set_name"]))
    return result


# --- Hierarchy Management ---

_HIER_CLASS_LABELS = {
    "0101": "Cost Center",
    "0104": "Profit Center",
    "0106": "Entity",
    "GCRS": "Entity Hierarchy",
    "FLAT": "Flat Hierarchy",
}


def _hier_label(h: object, labels: dict[str, str] | None = None) -> str:
    """Build display label for a hierarchy."""
    if getattr(h, "label", None):
        return h.label  # type: ignore[return-value]
    cls = labels or _HIER_CLASS_LABELS
    base = f"{cls.get(h.setclass, h.setclass)}: {h.setname}"  # type: ignore[attr-defined]
    if h.description:  # type: ignore[attr-defined]
        base += f" — {h.description}"
    return base


@router.get("/hierarchies")
def list_hierarchies(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
    scope: str | None = None,
    data_category: str | None = None,
) -> list[dict]:
    """List all hierarchies with their labels."""
    from app.models.core import Hierarchy

    query = select(Hierarchy).order_by(Hierarchy.setclass, Hierarchy.setname)
    if scope:
        query = query.where(Hierarchy.scope == scope)
    if data_category:
        query = query.where(Hierarchy.data_category == data_category)
    rows = db.execute(query).scalars().all()
    class_labels = {
        "0101": "Cost Center",
        "0104": "Profit Center",
        "0106": "Entity",
        "GCRS": "Entity Hierarchy",
        "FLAT": "Flat Hierarchy",
    }
    return [
        {
            "id": h.id,
            "setclass": h.setclass,
            "setname": h.setname,
            "label": h.label or "",
            "display_label": _hier_label(h, class_labels),
            "description": h.description,
            "coarea": h.coarea,
            "is_active": h.is_active,
            "type_label": class_labels.get(h.setclass, h.setclass),
            "attrs": h.attrs or {},
        }
        for h in rows
    ]


@router.patch("/hierarchies/{hier_id}")
def update_hierarchy(
    hier_id: int,
    label: str | None = None,
    is_active: bool | None = None,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Update hierarchy label or active status."""
    from app.models.core import Hierarchy

    h = db.get(Hierarchy, hier_id)
    if not h:
        raise HTTPException(status_code=404, detail="Hierarchy not found")
    if label is not None:
        h.label = label if label else None
    if is_active is not None:
        h.is_active = is_active
    db.commit()
    db.refresh(h)
    return {
        "id": h.id,
        "setclass": h.setclass,
        "setname": h.setname,
        "label": h.label or "",
        "display_label": _hier_label(h),
        "description": h.description,
        "coarea": h.coarea,
        "is_active": h.is_active,
    }


# --- Uploads ---


@router.get("/upload-rules")
def get_upload_rules(
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    """Return allowed scope/category/object combinations for the upload form."""
    return {"rules": SCOPE_UPLOAD_RULES}


@router.post("/uploads")
def create_upload(
    kind: str = Query(...),
    scope: str = Query(default=SCOPE_CLEANUP),
    data_category: str = Query(default="legacy"),
    sheet_name: str | None = Query(default=None),
    header_row: int | None = Query(default=None),
    load_cc: bool = Query(default=True),
    load_ext_hier: bool = Query(default=True),
    load_cema_hier: bool = Query(default=True),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    import json
    import pathlib

    from app.config import settings

    if scope not in ALL_SCOPES:
        raise HTTPException(status_code=400, detail=f"Invalid scope: {scope}")
    if data_category not in ALL_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"Invalid data_category: {data_category}")

    content = file.file.read()
    storage_dir = pathlib.Path(settings.storage_local_path).resolve() / "uploads"
    storage_dir.mkdir(parents=True, exist_ok=True)
    fname = pathlib.Path(file.filename or "unknown").name  # strip directory components
    unique_prefix = f"{int(time.time())}_{uuid.uuid4().hex[:8]}_"
    dest = storage_dir / (unique_prefix + fname)
    if not dest.resolve().is_relative_to(storage_dir):
        raise HTTPException(status_code=400, detail="Invalid filename")
    dest.write_bytes(content)

    # For cc_with_hierarchy, store options in source_detail as JSON
    if kind == "cc_with_hierarchy":
        source_detail = json.dumps(
            {
                "sheet_name": sheet_name or "Database",
                "header_row": header_row if header_row is not None else 2,
                "load_cc": load_cc,
                "load_ext_hier": load_ext_hier,
                "load_cema_hier": load_cema_hier,
            }
        )
    else:
        source_detail = fname

    batch = UploadBatch(
        kind=kind,
        scope=scope,
        data_category=data_category,
        source_method="file",
        source_detail=source_detail,
        filename=fname,
        status="uploaded",
        uploaded_by=_user.id,
        storage_uri=str(dest.resolve()),
    )
    db.add(batch)
    db.commit()
    db.refresh(batch)
    return {
        "id": batch.id,
        "status": batch.status,
        "filename": batch.filename,
        "scope": batch.scope,
        "data_category": batch.data_category,
    }


@router.get("/uploads")
def list_uploads(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    total = db.execute(select(func.count(UploadBatch.id))).scalar() or 0
    stmt = (
        select(UploadBatch)
        .order_by(UploadBatch.created_at.desc())
        .offset((pag.page - 1) * pag.size)
        .limit(pag.size)
    )
    batches = db.execute(stmt).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": b.id,
                "kind": b.kind,
                "scope": b.scope,
                "data_category": b.data_category,
                "source_method": b.source_method,
                "source_detail": b.source_detail,
                "filename": b.filename,
                "status": b.status,
                "rows_total": b.rows_total,
                "rows_valid": b.rows_valid,
                "rows_error": b.rows_error,
                "rows_loaded": b.rows_loaded,
                "rows_processed": b.rows_processed,
                "created_at": str(b.created_at) if b.created_at else None,
            }
            for b in batches
        ],
    }


@router.get("/uploads/{batch_id}")
def get_upload(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Upload batch not found")
    return {
        "id": batch.id,
        "kind": batch.kind,
        "filename": batch.filename,
        "status": batch.status,
        "rows_total": batch.rows_total,
        "rows_valid": batch.rows_valid,
        "rows_error": batch.rows_error,
        "rows_loaded": batch.rows_loaded,
        "rows_processed": batch.rows_processed,
        "storage_uri": batch.storage_uri,
        "created_at": str(batch.created_at) if batch.created_at else None,
        "validated_at": str(batch.validated_at) if batch.validated_at else None,
        "loaded_at": str(batch.loaded_at) if batch.loaded_at else None,
    }


def _run_validate_in_background(batch_id: int) -> None:
    """Run validate_upload in a background thread with its own DB session."""
    import logging as _logging

    from app.infra.db.session import SessionLocal
    from app.services.upload_processor import validate_upload

    _log = _logging.getLogger(__name__)
    db = SessionLocal()
    try:
        batch = db.get(UploadBatch, batch_id)
        if not batch or batch.status != "validating":
            _log.info(
                "Validate batch %s aborted — status: %s",
                batch_id,
                batch.status if batch else "deleted",
            )
            return
        result = validate_upload(batch_id, db)
        _log.info("Validate batch %s completed: %s", batch_id, result)
    except Exception:
        _log.exception("Validate batch %s failed in background", batch_id)
        try:
            db.rollback()
            batch = db.get(UploadBatch, batch_id)
            if batch:
                batch.status = "failed"
                db.commit()
        except Exception:
            db.rollback()
    finally:
        db.close()


def _run_load_in_background(batch_id: int) -> None:
    """Run load_upload in a background thread with its own DB session."""
    import logging as _logging

    from app.infra.db.session import SessionLocal
    from app.services.upload_processor import load_upload

    _log = _logging.getLogger(__name__)
    db = SessionLocal()
    try:
        batch = db.get(UploadBatch, batch_id)
        if not batch or batch.status != "loading":
            _log.info(
                "Load batch %s aborted — status: %s",
                batch_id,
                batch.status if batch else "deleted",
            )
            return
        result = load_upload(batch_id, db)
        _log.info("Load batch %s completed: %s", batch_id, result)
    except Exception:
        _log.exception("Load batch %s failed in background", batch_id)
        try:
            db.rollback()
            batch = db.get(UploadBatch, batch_id)
            if batch:
                batch.status = "failed"
                db.commit()
        except Exception:
            db.rollback()
    finally:
        db.close()


@router.post("/uploads/{batch_id}/validate")
def validate_upload_batch(
    batch_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    if batch.status not in ("uploaded", "failed"):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot validate batch in status '{batch.status}'",
        )
    batch.status = "validating"
    batch.rows_processed = 0
    db.commit()
    background_tasks.add_task(_run_validate_in_background, batch_id)
    return {"status": "validating", "batch_id": batch_id}


@router.post("/uploads/{batch_id}/load")
def load_upload_batch(
    batch_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    if batch.status != "validated":
        raise HTTPException(
            status_code=400,
            detail=f"Batch must be validated first (status: {batch.status})",
        )
    batch.status = "loading"
    batch.rows_processed = 0
    db.commit()
    background_tasks.add_task(_run_load_in_background, batch_id)
    return {"status": "loading", "batch_id": batch_id}


@router.post("/uploads/{batch_id}/rollback")
def rollback_upload_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    from app.services.upload_processor import rollback_upload

    try:
        return rollback_upload(batch_id, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@router.post("/uploads/{batch_id}/reset")
def reset_upload_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    """Reset a stuck batch (validating/loading) back to a retryable state."""
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    if batch.status == "validating":
        batch.status = "uploaded"
        db.commit()
        return {"status": "uploaded", "message": "Reset to uploaded — you can re-validate."}
    if batch.status == "loading":
        batch.status = "validated"
        db.commit()
        return {"status": "validated", "message": "Reset to validated — you can re-load."}
    raise HTTPException(
        status_code=400,
        detail=f"Batch is not stuck (status: {batch.status})",
    )


@router.get("/uploads/{batch_id}/errors")
def list_upload_errors(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    total = (
        db.execute(
            select(func.count(UploadError.id)).where(UploadError.batch_id == batch_id)
        ).scalar()
        or 0
    )
    errors = (
        db.execute(
            select(UploadError)
            .where(UploadError.batch_id == batch_id)
            .order_by(UploadError.row_number)
            .offset((pag.page - 1) * pag.size)
            .limit(pag.size)
        )
        .scalars()
        .all()
    )
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "row_number": e.row_number,
                "column_name": e.column_name,
                "error_code": e.error_code,
                "message": e.message,
            }
            for e in errors
        ],
    }


# --- Routines ---


@router.get("/routines")
def list_routines(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> list[dict]:
    routines = db.execute(select(Routine).order_by(Routine.order)).scalars().all()
    return [
        {
            "code": r.code,
            "name": r.name,
            "kind": r.kind,
            "tree": r.tree,
            "source": r.source,
            "enabled": r.enabled,
            "order": r.order,
        }
        for r in routines
    ]


@router.patch("/routines/{code}")
def toggle_routine(
    code: str,
    enabled: bool = Query(...),
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    routine = db.execute(select(Routine).where(Routine.code == code)).scalar_one_or_none()
    if not routine:
        raise HTTPException(status_code=404, detail="Routine not found")
    routine.enabled = enabled
    db.commit()
    return {"code": code, "enabled": enabled}


# --- Audit log ---


@router.get("/audit")
def list_audit_logs(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "auditor")),
    pag: PaginationParams = Depends(pagination),
    action: str | None = None,
) -> dict:
    query = select(AuditLog).order_by(AuditLog.created_at.desc())
    if action:
        query = query.where(AuditLog.action == action)
    total_q = select(func.count(AuditLog.id))
    if action:
        total_q = total_q.where(AuditLog.action == action)
    total = db.execute(total_q).scalar() or 0
    logs = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": entry.id,
                "action": entry.action,
                "entity_type": entry.entity_type,
                "entity_id": entry.entity_id,
                "actor_email": entry.actor_email,
                "created_at": str(entry.created_at),
            }
            for entry in logs
        ],
    }


# --- Jobs ---


@router.get("/jobs")
def list_jobs(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    total = db.execute(select(func.count(TaskRun.id))).scalar() or 0
    tasks = (
        db.execute(
            select(TaskRun)
            .order_by(TaskRun.created_at.desc())
            .offset((pag.page - 1) * pag.size)
            .limit(pag.size)
        )
        .scalars()
        .all()
    )
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": t.id,
                "task_name": t.task_name,
                "task_id": t.task_id,
                "status": t.status,
                "started_at": str(t.started_at) if t.started_at else None,
            }
            for t in tasks
        ],
    }


# --- Sample data ---


@router.post("/sample-data")
def generate_sample(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.services.seed import generate_sample_data

    counts = generate_sample_data(db)
    return {"status": "created", "counts": counts}


@router.delete("/sample-data")
def remove_sample(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.services.seed import delete_sample_data

    counts = delete_sample_data(db)
    return {"status": "deleted", "counts": counts}


@router.get("/sample-data")
def sample_status(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.services.seed import sample_data_counts

    return sample_data_counts(db)


# ── Upload templates ─────────────────────────────────────────────────────

UPLOAD_TEMPLATES: dict[str, dict] = {
    "cost_centers": {
        "filename": "template_cost_centers.csv",
        "description": "Cost center upload — SAP CSKS/CSKT full structure",
        "columns": [
            "MANDT",
            "KOKRS",
            "KOSTL",
            "DATBI",
            "DATAB",
            "BKZKP",
            "PKZKP",
            "BUKRS",
            "GSBER",
            "KOSAR",
            "VERAK",
            "VERAK_USER",
            "WAERS",
            "KALSM",
            "TXJCD",
            "PRCTR",
            "WERKS",
            "LOGSYSTEM",
            "ERSDA",
            "USNAM",
            "BKZKS",
            "BKZER",
            "BKZOB",
            "PKZKS",
            "PKZER",
            "VMETH",
            "MGEFL",
            "ABTEI",
            "NKOST",
            "KVEWE",
            "KAPPL",
            "KOSZSCHL",
            "LAND1",
            "ANRED",
            "NAME1",
            "NAME2",
            "NAME3",
            "NAME4",
            "ORT01",
            "ORT02",
            "STRAS",
            "PFACH",
            "PSTLZ",
            "PSTL2",
            "REGIO",
            "SPRAS",
            "TELBX",
            "TELF1",
            "TELF2",
            "TELFX",
            "TELTX",
            "TELX1",
            "DATLT",
            "DRNAM",
            "KHINR",
            "CCKEY",
            "KOMPL",
            "STAKZ",
            "OBJNR",
            "FUNKT",
            "AFUNK",
            "CPI_TEMPL",
            "CPD_TEMPL",
            "FUNC_AREA",
            "SCI_TEMPL",
            "SCD_TEMPL",
            "SKI_TEMPL",
            "SKD_TEMPL",
            "ZZCUEMNCFU",
            "ZZCUEABACC",
            "ZZCUEGBCD",
            "ZZCUEUBCD",
            "ZZCUENKOS",
            "ZZSTRPCTYP",
            "ZZSTRKKLAS",
            "ZZSTRAAGCD",
            "ZZSTRGFD",
            "ZZSTRFST",
            "ZZSTRMACVE",
            "ZZSTRABUKR",
            "ZZSTRUGCD",
            "ZZSTRINADT",
            "ZZSTRKSTYP",
            "ZZSTRVERIK",
            "ZZSTRCURR2",
            "ZZSTRLCCID",
            "ZZSTRMALOC",
            "ZZSTRTAXCD",
            "ZZSTRGRPID",
            "ZZSTRREGCODE",
            "ZZSTRTAXAREA",
            "ZZSTRREPSIT",
            "ZZSTRGSM",
            "ZZCEMAPAR",
            "ZZLEDGER",
            "ZZHDSTAT",
            "ZZHDTYPE",
            "ZZFMD",
            "ZZFMDCC",
            "ZZFMDNODE",
            "ZZSTATE",
            "ZZTAX",
            "ZZSTRENTSA",
            "ZZSTRENTZU",
            "XBLNR",
            "VNAME",
            "RECID",
            "ETYPE",
            "JV_OTYPE",
            "JV_JIBCL",
            "JV_JIBSA",
            "FERC_IND",
            "KTEXT",
            "LTEXT",
        ],
        "sample_row": [],
    },
    "profit_centers": {
        "filename": "template_profit_centers.csv",
        "description": "Profit center upload — SAP CEPC/CEPCT full structure",
        "columns": [
            "MANDT",
            "PRCTR",
            "DATBI",
            "KOKRS",
            "DATAB",
            "ERSDA",
            "USNAM",
            "MERKMAL",
            "ABTEI",
            "VERAK",
            "VERAK_USER",
            "WAERS",
            "NPRCTR",
            "LAND1",
            "ANRED",
            "NAME1",
            "NAME2",
            "NAME3",
            "NAME4",
            "ORT01",
            "ORT02",
            "STRAS",
            "PFACH",
            "PSTLZ",
            "PSTL2",
            "SPRAS",
            "TELBX",
            "TELF1",
            "TELF2",
            "TELFX",
            "TELTX",
            "TELX1",
            "DATLT",
            "DRNAM",
            "KHINR",
            "BUKRS",
            "VNAME",
            "RECID",
            "ETYPE",
            "TXJCD",
            "REGIO",
            "KVEWE",
            "KAPPL",
            "KALSM",
            "LOGSYSTEM",
            "LOCK_IND",
            "PCA_TEMPLATE",
            "SEGMENT",
            "KTEXT",
            "LTEXT",
        ],
        "sample_row": [],
    },
    "balances": {
        "filename": "template_balances.csv",
        "description": "Cost center balance upload template",
        "columns": [
            "COAREA",
            "CCTR",
            "CCODE",
            "FISCAL_YEAR",
            "PERIOD",
            "ACCOUNT",
            "ACCOUNT_CLASS",
            "TC_AMT",
            "GC_AMT",
            "GC2_AMT",
            "CURRENCY_TC",
            "CURRENCY_GC",
            "CURRENCY_GC2",
            "POSTING_COUNT",
        ],
        "sample_row": [
            "1000",
            "0000100001",
            "DE01",
            "2025",
            "1",
            "400000",
            "OPEX",
            "15000.00",
            "15000.00",
            "16500.00",
            "EUR",
            "EUR",
            "USD",
            "42",
        ],
    },
    "balances_gcr": {
        "filename": "template_balances_gcr.csv",
        "description": "Balance upload — GCR aggregated format (company code + center + amounts)",
        "columns": [
            "COMPANY_CODE",
            "SAP_MANAGEMENT_CENTER",
            "CURR_CODE_ISO_TC",
            "SUM(P.GCR_POSTING_AMT_TC)",
            "SUM(P.GCR_POSTING_AMT_GC2)",
            "COUNT(*)",
        ],
        "sample_row": [],
    },
    "entities": {
        "filename": "template_entities.csv",
        "description": "Entity (company code) upload — SAP T001 full structure",
        "columns": [
            "MANDT",
            "BUKRS",
            "BUTXT",
            "ORT01",
            "LAND1",
            "WAERS",
            "SPRAS",
            "KTOPL",
            "WAABW",
            "PERIV",
            "KOKFI",
            "RCOMP",
            "ADRNR",
            "STCEG",
            "FIKRS",
            "XFMCO",
            "XFMCB",
            "XFMCA",
            "TXJCD",
            "FMHRDATE",
            "BUVAR",
            "FDBUK",
            "XFDIS",
            "XVALV",
            "XSKFN",
            "KKBER",
            "XMWSN",
            "MREGL",
            "XGSBE",
            "XGJRV",
            "XKDFT",
            "XPROD",
            "XEINK",
            "XJVAA",
            "XVVWA",
            "XSLTA",
            "XFDMM",
            "XFDSD",
            "XEXTB",
            "EBUKR",
            "KTOP2",
            "UMKRS",
            "BUKRS_GLOB",
            "FSTVA",
            "OPVAR",
            "XCOVR",
            "TXKRS",
            "WFVAR",
            "XBBBF",
            "XBBBE",
            "XBBBA",
            "XBBKO",
            "XSTDT",
            "MWSKV",
            "MWSKA",
            "IMPDA",
            "XNEGP",
            "XKKBI",
            "WT_NEWWT",
            "PP_PDATE",
            "INFMT",
            "FSTVARE",
            "KOPIM",
            "DKWEG",
            "OFFSACCT",
            "BAPOVAR",
            "XCOS",
            "XCESSION",
            "XSPLT",
            "SURCCM",
            "DTPROV",
            "DTAMTC",
            "DTTAXC",
            "DTTDSP",
            "DTAXR",
            "XVATDATE",
            "PST_PER_VAR",
            "XBBSC",
            "F_OBSOLETE",
        ],
        "sample_row": [],
    },
    "hierarchies": {
        "filename": "template_hierarchies.csv",
        "description": "Hierarchy upload template (SETHEADER/SETNODE/SETLEAF rows)",
        "columns": [
            "ROW_TYPE",
            "SETCLASS",
            "SETNAME",
            "DESCRIPTION",
            "COAREA",
            "PARENT_SETNAME",
            "CHILD_SETNAME",
            "VALUE",
            "SEQ",
        ],
        "sample_row_header": [
            "SETHEADER",
            "0101",
            "STDHIER",
            "Standard Hierarchy",
            "1000",
            "",
            "",
            "",
            "",
        ],
        "sample_row_node": ["SETNODE", "0101", "STDHIER", "", "1000", "STDHIER", "ADMIN", "", "1"],
        "sample_row_leaf": [
            "SETLEAF",
            "0101",
            "STDHIER",
            "",
            "1000",
            "ADMIN",
            "",
            "0000100001",
            "1",
        ],
    },
    "hierarchies_flat": {
        "filename": "template_hierarchies_flat.csv",
        "description": "Hierarchy upload — SAP flat node export (parent-child IDs)",
        "columns": [
            "MANDT",
            "PERIOD",
            "NODEID",
            "NODETYPE",
            "NODENAME",
            "PARENTID",
            "CHILDID",
            "NEXTID",
            "NODETEXT",
        ],
        "sample_row": [
            "100",
            "000",
            "00000001",
            "N",
            "TOTAL",
            "",
            "00000002",
            "",
            "Total Group",
        ],
    },
    "entity_hierarchy": {
        "filename": "template_entity_hierarchy.csv",
        "description": "Entity hierarchy upload — SAP ZUHL_GRD_GCRS_C (GCRS entity structure)",
        "columns": [
            "MANDT",
            "PERIOD",
            "NODEID",
            "NODETYPE",
            "NODENAME",
            "PARENTID",
            "CHILDID",
            "NEXTID",
            "NODETEXT",
        ],
        "sample_row": [
            "100",
            "202601",
            "00000001",
            "N",
            "ROOT",
            "",
            "00000002",
            "",
            "Root Entity Node",
        ],
    },
    "employees": {
        "filename": "template_employees.csv",
        "description": "Employee upload — SAP ZUHL_GRD_GPF full structure",
        "columns": [
            "MANDT",
            "GPN",
            "NAME",
            "VORNAME",
            "SPRACHENSCHLUESS",
            "ANREDECODE",
            "USERID",
            "EINTRITTSDATUM",
            "OE_LEITER",
            "INT_TEL_NR_1AP",
            "EXT_TEL_NR_1AP",
            "NL_CODE_GEB_1AP",
            "STRASSE_GEB_1AP",
            "STOCKWERK_1AP",
            "BUERONUMMER_1AP",
            "KSTST",
            "KSTST_TEXT",
            "OE_OBJEKT_ID",
            "OE_CODE",
            "OE_TEXT",
            "SAP_BUKRS",
            "SAP_BUKRS_TEXT",
            "T_NUMMER",
            "INSTRAD_1",
            "INSTRAD_2",
            "KSTST_EINSATZ_OE",
            "PERSONALBER_TEXT",
            "NL_OE_MA",
            "NL_TEXT",
            "GSFLD_OE_MA",
            "GSFLD_OE_MA_TEXT",
            "MA_GRUPPE",
            "MA_GRUPPE_TEXT",
            "MA_KREIS",
            "MA_KREIS_TEXT",
            "RANG_CODE",
            "RANG_TEXT",
            "AKADEMISCHER_TIT",
            "UBS_FUNK",
            "UBS_FUNK_TEXT",
            "GPN_VG_MA",
            "NAME_VG_MA",
            "UEG_OE_OBJEKTID",
            "UEG_OE_BEZ",
            "UEG_OE_KRZ",
            "BSCHGRAD",
            "PERSONALBEREICH",
            "FAX_EXT_1AP",
            "EMAIL_ADRESSE",
            "MA_KZ",
            "FIRMA_EXT_MA",
            "BEGDAT_ORGWECHS",
            "AUSTRITT_DATUM",
            "NATEL_NUMMER",
            "PAGER_NUMMER",
            "PLZ_GEB_1AP",
            "ORT_GEB_1AP",
            "EINSATZ_OE_KRZ",
            "EINSATZ_OE_TEXT",
            "DIVISION",
            "GEB_COD_1AP",
            "RANG_KRZ",
            "SYSTEMDATUM",
            "AP_NUMMER",
            "EINSATZ_OE_OBJID",
            "INT_TEL_NR_2AP",
            "EXT_TEL_NR_2AP",
            "BUERONUMMER_2AP",
            "GEB_COD_2AP",
            "STRASSE_GEB_2AP",
            "PLZ_GEB_2AP",
            "ORT_GEB_2AP",
            "GEB_COD_GEB_2AP",
            "FAX_NR_2AP",
            "STOCKWERK_2AP",
            "GPIN_NUMMER",
            "NAT",
            "LAND_GEB_1AP",
            "REG_NR_1AP",
            "POSTF_1AP",
            "PLZ_POSTFADR_1AP",
            "ORT_POSTFADR_1AP",
            "LAND_GEB_2AP",
            "REG_NR_2AP",
            "POSTF_2AP",
            "PLZ_POSTFADR_2AP",
            "ORT_POSTFADR_2AP",
            "LETZTER_ARB_TAG",
            "ABAC_NL_AG_EINOE",
            "VERTR_ENDE_EXMA",
            "UNTERGRP_CODE",
            "BS_FIRST_NAME",
            "BS_LAST_NAME",
            "NAME_UC",
            "VORNAME_UC",
            "NAME_PH",
            "VORNAME_PH",
            "MA_OE",
            "UPDATED_ID",
            "MA_KSTST",
            "BUSINESS_NAME",
            "JOB_CATEG_CODE",
            "JOB_CATEG_DESCR",
            "COSTCENTER_CODE",
            "COSTCENTER_DESCR",
            "MANACS_FUNC_CODE",
            "MANACS_FUNC_DESC",
            "MANACS_SEGM_CODE",
            "MANACS_SEGM_DESC",
            "MANACS_SECT_CODE",
            "MANACS_SECT_DESC",
            "MANACS_BSAR_CODE",
            "MANACS_BSAR_DESC",
            "MANACS_BSUN_CODE",
            "MANACS_BSUN_DESC",
            "MANACS_BSGP_CODE",
            "MANACS_BSGP_DESC",
            "MANACS_REG_CODE",
            "MANACS_REG_DESCR",
            "MANACS_LOC_CODE",
            "MANACS_LOC_DESCR",
            "REGULATORY_REG",
            "SUPERVISORS_GPIN",
            "UUNAME",
            "WEB_SSO",
            "SAP_USER",
            "HR_COMPANY",
            "REGULATORY_REGST",
            "GLOBAL_CC",
        ],
        "sample_row": [],
    },
    "gl_accounts_ska1": {
        "filename": "template_gl_accounts_ska1.csv",
        "description": "GL Account upload — SAP SKA1 (Chart of Accounts level)",
        "columns": [
            "MANDT",
            "KTOPL",
            "SAKNR",
            "XBILK",
            "SAKAN",
            "BILKT",
            "ERDAT",
            "ERNAM",
            "GVTYP",
            "KTOKS",
            "MUSTR",
            "VBUND",
            "XLOEV",
            "XSPEA",
            "XSPEB",
            "XSPEP",
            "MCOD1",
            "FUNC_AREA",
            "GLACCOUNT_TYPE",
            "GLACCOUNT_SUBTYPE",
            "MAIN_SAKNR",
            "LAST_CHANGED_TS",
            "TXT20",
            "TXT50",
        ],
        "sample_row": [],
    },
    "gl_accounts_skb1": {
        "filename": "template_gl_accounts_skb1.csv",
        "description": "GL Account upload — SAP SKB1 (Company Code level)",
        "columns": [
            "MANDT",
            "BUKRS",
            "SAKNR",
            "BEGRU",
            "BUSAB",
            "DATLZ",
            "ERDAT",
            "ERNAM",
            "FDGRV",
            "FDLEV",
            "FIPLS",
            "FSTAG",
            "HBKID",
            "HKTID",
            "KDFSL",
            "MITKZ",
            "MWSKZ",
            "STEXT",
            "VZSKZ",
            "WAERS",
            "WMETH",
            "XGKON",
            "XINTB",
            "XKRES",
            "XLOEB",
            "XNKON",
            "XOPVW",
            "XSPEB",
            "ZINDT",
            "ZINRT",
            "ZUAWA",
            "ALTKT",
            "XMITK",
            "RECID",
            "FIPOS",
            "XMWNO",
            "XSALH",
            "BEWGP",
            "INFKY",
            "TOGRU",
            "XLGCLR",
            "X_UJ_CLR",
            "MCAKEY",
            "COCHANGED",
            "LAST_CHANGED_TS",
        ],
        "sample_row": [],
    },
    "target_cost_centers": {
        "filename": "template_target_cost_centers.csv",
        "description": "Target (ampliFi) cost center upload",
        "columns": [
            "COAREA",
            "CCTR",
            "TXTSH",
            "TXTMI",
            "RESPONSIBLE",
            "CCODE",
            "CCTRCGY",
            "CURRENCY",
            "PCTR",
            "IS_ACTIVE",
            "MDG_STATUS",
            "MDG_CHANGE_REQUEST_ID",
        ],
        "sample_row": [],
    },
    "target_profit_centers": {
        "filename": "template_target_profit_centers.csv",
        "description": "Target (ampliFi) profit center upload",
        "columns": [
            "COAREA",
            "PCTR",
            "TXTSH",
            "TXTMI",
            "RESPONSIBLE",
            "CCODE",
            "DEPARTMENT",
            "CURRENCY",
            "IS_ACTIVE",
        ],
        "sample_row": [],
    },
    "center_mapping": {
        "filename": "template_center_mapping.csv",
        "description": "Legacy → Target center mapping upload",
        "columns": [
            "OBJECT_TYPE",
            "LEGACY_COAREA",
            "LEGACY_CENTER",
            "LEGACY_NAME",
            "TARGET_COAREA",
            "TARGET_CENTER",
            "TARGET_NAME",
            "MAPPING_TYPE",
            "NOTES",
        ],
        "sample_row": [
            "cost_center",
            "1000",
            "0001234567",
            "Legacy CC Name",
            "1000",
            "0009876543",
            "Target CC Name",
            "1:1",
            "",
        ],
    },
}


@router.get("/upload-templates")
def list_upload_templates(
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    """List available upload templates."""
    return {
        "templates": [
            {"kind": k, "filename": v["filename"], "description": v["description"]}
            for k, v in UPLOAD_TEMPLATES.items()
        ]
    }


@router.get("/upload-templates/{kind}")
def download_upload_template(
    kind: str,
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    """Get CSV content for an upload template."""
    tmpl = UPLOAD_TEMPLATES.get(kind)
    if not tmpl:
        raise HTTPException(status_code=404, detail=f"No template for kind: {kind}")

    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(tmpl["columns"])

    if kind == "hierarchies":
        if "sample_row_header" in tmpl:
            writer.writerow(tmpl["sample_row_header"])
        if "sample_row_node" in tmpl:
            writer.writerow(tmpl["sample_row_node"])
        if "sample_row_leaf" in tmpl:
            writer.writerow(tmpl["sample_row_leaf"])
    elif "sample_row" in tmpl:
        writer.writerow(tmpl["sample_row"])

    return {
        "kind": kind,
        "filename": tmpl["filename"],
        "content": output.getvalue(),
        "content_type": "text/csv",
    }


# --- LLM Usage & Cost Tracking ---


@router.get("/llm/usage")
def llm_usage_summary(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Get LLM usage summary (daily/monthly spend, call counts)."""
    from app.infra.llm.guardrails import CostGuardrail

    # Load guardrail config from app_config
    cfg = db.execute(
        select(AppConfig).where(AppConfig.key == "llm.guardrails")
    ).scalar_one_or_none()
    guardrail_config = cfg.value if cfg else {}
    guardrail = CostGuardrail(
        max_cost_per_call=guardrail_config.get("max_cost_per_call", 1.0),
        daily_cap_usd=guardrail_config.get("daily_cap_usd", 50.0),
        monthly_cap_usd=guardrail_config.get("monthly_cap_usd", 500.0),
    )
    return guardrail.get_usage_summary(db)


# --- Datasphere Integration ---


class DatasphereConfigUpdate(BaseModel):
    ds_url: str | None = None
    ds_schema: str = "ACM"
    ds_user: str | None = None
    ds_password: str | None = None
    ds_use_ssl: bool = True
    is_active: bool = False
    domain_config: dict | None = None


@router.get("/datasphere/config")
def get_datasphere_config(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Get current Datasphere configuration."""
    from app.models.core import DATASPHERE_DOMAINS, LOCAL_ONLY_DOMAINS, DatasphereConfig

    config = db.query(DatasphereConfig).first()
    if not config:
        return {
            "configured": False,
            "ds_url": "",
            "ds_schema": "ACM",
            "ds_user": "",
            "ds_use_ssl": True,
            "is_active": False,
            "domain_config": {},
            "datasphere_domains": DATASPHERE_DOMAINS,
            "local_only_domains": LOCAL_ONLY_DOMAINS,
        }

    return {
        "configured": True,
        "ds_url": config.ds_url or "",
        "ds_schema": config.ds_schema,
        "ds_user": config.ds_user or "",
        "ds_use_ssl": config.ds_use_ssl,
        "is_active": config.is_active,
        "domain_config": config.domain_config or {},
        "datasphere_domains": DATASPHERE_DOMAINS,
        "local_only_domains": LOCAL_ONLY_DOMAINS,
    }


@router.put("/datasphere/config")
def update_datasphere_config(
    body: DatasphereConfigUpdate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Create or update Datasphere configuration."""
    from app.models.core import DatasphereConfig

    config = db.query(DatasphereConfig).first()
    if not config:
        config = DatasphereConfig(ds_schema="ACM")
        db.add(config)

    provided = body.model_fields_set
    if "ds_url" in provided:
        config.ds_url = body.ds_url
    if "ds_schema" in provided:
        config.ds_schema = body.ds_schema
    if "ds_user" in provided:
        config.ds_user = body.ds_user
    if "ds_use_ssl" in provided:
        config.ds_use_ssl = body.ds_use_ssl
    if "is_active" in provided:
        config.is_active = body.is_active
    config.updated_by = user.id

    if "ds_password" in provided and body.ds_password:
        from app.infra.sap.encryption import encrypt_password

        config.ds_password_encrypted = encrypt_password(body.ds_password)

    if "domain_config" in provided and body.domain_config is not None:
        config.domain_config = body.domain_config

    db.commit()
    return {"success": True, "message": "Datasphere configuration updated"}


@router.post("/datasphere/test")
def test_datasphere_connection(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Test the configured Datasphere connection."""
    from app.infra.datasphere.storage import get_datasphere_client

    client = get_datasphere_client(db)
    if not client:
        return {
            "success": False,
            "message": "Datasphere not configured",
        }
    return client.test_connection()


@router.get("/datasphere/ddl")
def get_datasphere_ddl(
    schema: str = Query("ACM", description="Target HANA schema"),
    domain: str | None = Query(None, description="Single domain, or all"),
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Generate HANA column-store DDL for Datasphere tables."""
    import re

    if not re.match(r"^[A-Za-z0-9_]+$", schema):
        raise HTTPException(400, "Invalid schema name (only A-Z, 0-9, _ allowed)")

    from app.infra.datasphere.ddl import generate_all_ddl, generate_full_ddl

    if domain:
        tables = generate_all_ddl(schema)
        if domain not in tables:
            raise HTTPException(404, f"Unknown domain: {domain}")
        return {"domain": domain, "ddl": tables[domain]}

    return {"ddl": generate_full_ddl(schema)}


@router.get("/datasphere/domains")
def list_datasphere_domains(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """List all data domains with their storage mode (local vs datasphere)."""
    from app.infra.datasphere.ddl import DEFAULT_TABLE_NAMES
    from app.infra.datasphere.storage import get_storage_mode
    from app.models.core import DATASPHERE_DOMAINS, LOCAL_ONLY_DOMAINS

    domains = []
    for d in DATASPHERE_DOMAINS:
        mode = get_storage_mode(d, db)
        domains.append(
            {
                "domain": d,
                "mode": mode,
                "movable": True,
                "default_table": DEFAULT_TABLE_NAMES.get(d, d.upper()),
            }
        )
    for d in LOCAL_ONLY_DOMAINS:
        domains.append(
            {
                "domain": d,
                "mode": "local",
                "movable": False,
                "default_table": None,
            }
        )
    return {"domains": domains}


# ── Explorer Source Config ────────────────────────────────────────────────


class ExplorerSourceIn(BaseModel):
    object_type: str
    area: str = "legacy"
    label: str
    source_system: str = "local_db"
    protocol: str = "db_query"
    mode: str = "replicated"
    connection_ref: str | None = None
    endpoint: str | None = None
    replication_cron: str | None = None
    extra_config: dict | None = None
    enabled: bool = True
    display_order: int = 0


class ExplorerSourceOut(BaseModel):
    id: int
    object_type: str
    area: str
    label: str
    source_system: str
    protocol: str
    mode: str
    connection_ref: str | None = None
    endpoint: str | None = None
    replication_cron: str | None = None
    extra_config: dict | None = None
    enabled: bool
    display_order: int

    model_config = {"from_attributes": True}


@router.get("/explorer-sources")
def list_explorer_sources(
    area: str | None = Query(None),
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """List all data source configs for the Data Explorer."""
    from app.models.core import ExplorerSourceConfig

    stmt = select(ExplorerSourceConfig).order_by(
        ExplorerSourceConfig.area, ExplorerSourceConfig.display_order
    )
    if area:
        stmt = stmt.where(ExplorerSourceConfig.area == area)
    rows = db.execute(stmt).scalars().all()
    return {"items": [ExplorerSourceOut.model_validate(r).model_dump() for r in rows]}


@router.post("/explorer-sources")
def create_explorer_source(
    body: ExplorerSourceIn,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Create a new explorer data source config."""
    from app.models.core import ExplorerSourceConfig

    existing = db.execute(
        select(ExplorerSourceConfig).where(
            ExplorerSourceConfig.object_type == body.object_type,
            ExplorerSourceConfig.area == body.area,
        )
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(409, f"Source config for {body.area}/{body.object_type} already exists")
    row = ExplorerSourceConfig(
        **body.model_dump(),
        updated_by=user.id,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return ExplorerSourceOut.model_validate(row).model_dump()


@router.put("/explorer-sources/{src_id}")
def update_explorer_source(
    src_id: int,
    body: ExplorerSourceIn,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Update an explorer data source config."""
    from app.models.core import ExplorerSourceConfig

    row = db.get(ExplorerSourceConfig, src_id)
    if not row:
        raise HTTPException(404, "Source config not found")
    for k, v in body.model_dump(exclude={"object_type", "area"}, exclude_unset=True).items():
        setattr(row, k, v)
    row.updated_by = user.id
    db.commit()
    db.refresh(row)
    return ExplorerSourceOut.model_validate(row).model_dump()


@router.delete("/explorer-sources/{src_id}")
def delete_explorer_source(
    src_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Delete an explorer data source config."""
    from app.models.core import ExplorerSourceConfig

    row = db.get(ExplorerSourceConfig, src_id)
    if not row:
        raise HTTPException(404, "Source config not found")
    db.delete(row)
    db.commit()
    return {"ok": True}


# --- Explorer Display Config (global attribute selection) ---


class DisplayConfigIn(BaseModel):
    object_type: str
    table_columns: list[str] = []
    detail_columns: list[str] = []
    column_labels: dict[str, str] = {}
    default_sort_column: str | None = None
    default_sort_dir: str | None = "asc"


class DisplayConfigOut(BaseModel):
    id: int
    object_type: str
    table_columns: list[str]
    detail_columns: list[str]
    column_labels: dict[str, str] = {}
    default_sort_column: str | None = None
    default_sort_dir: str | None = "asc"

    model_config = {"from_attributes": True}


@router.get("/explorer-display-config")
def list_display_configs(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """List all display configurations."""
    from app.models.core import ExplorerDisplayConfig

    rows = (
        db.execute(select(ExplorerDisplayConfig).order_by(ExplorerDisplayConfig.object_type))
        .scalars()
        .all()
    )
    return {"items": [DisplayConfigOut.model_validate(r).model_dump() for r in rows]}


@router.get("/explorer-display-config/{object_type}")
def get_display_config(
    object_type: str,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Get display config for a specific object type."""
    from app.models.core import ExplorerDisplayConfig

    row = db.execute(
        select(ExplorerDisplayConfig).where(ExplorerDisplayConfig.object_type == object_type)
    ).scalar_one_or_none()
    if not row:
        return {
            "object_type": object_type,
            "table_columns": [],
            "detail_columns": [],
            "column_labels": {},
        }
    return DisplayConfigOut.model_validate(row).model_dump()


@router.put("/explorer-display-config/{object_type}")
def upsert_display_config(
    object_type: str,
    body: DisplayConfigIn,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Create or update display config for an object type."""
    from app.models.core import ExplorerDisplayConfig

    row = db.execute(
        select(ExplorerDisplayConfig).where(ExplorerDisplayConfig.object_type == object_type)
    ).scalar_one_or_none()
    if row:
        row.table_columns = body.table_columns
        row.detail_columns = body.detail_columns
        row.column_labels = body.column_labels
        row.default_sort_column = body.default_sort_column
        row.default_sort_dir = body.default_sort_dir
        row.updated_by = user.id
    else:
        row = ExplorerDisplayConfig(
            object_type=object_type,
            table_columns=body.table_columns,
            detail_columns=body.detail_columns,
            column_labels=body.column_labels,
            default_sort_column=body.default_sort_column,
            default_sort_dir=body.default_sort_dir,
            updated_by=user.id,
        )
        db.add(row)
    db.commit()
    db.refresh(row)
    return DisplayConfigOut.model_validate(row).model_dump()


@router.get("/explorer-available-columns/{object_type}")
def get_available_columns(
    object_type: str,
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Return all available columns for an object type."""
    from sqlalchemy import inspect as sa_inspect

    from app.api.explore import _DEFAULT_COLUMN_LABELS, _OBJECT_MODELS

    model = _OBJECT_MODELS.get(object_type)
    if not model:
        return {"columns": [], "default_labels": {}}
    mapper = sa_inspect(model)
    cols = [c.key for c in mapper.column_attrs if c.key not in ("id", "created_at", "updated_at")]
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in cols}
    return {"object_type": object_type, "columns": cols, "default_labels": labels}


# --- Application Logs ---


@router.get("/logs")
def get_application_logs(
    limit: int = Query(200, ge=1, le=5000),
    level: str | None = Query(None),
    since: str | None = Query(None),
    search: str | None = Query(None),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.infra.logging import get_recent_logs

    entries = get_recent_logs(limit=limit, level=level, since=since, search=search)
    return {"total": len(entries), "items": entries}


# --- Rule Catalog Q&A (LLM-powered, read-only) ---


class RuleCatalogQARequest(BaseModel):
    """Body for /rule-catalog/qa.

    ``question`` is the user prompt.
    ``rule_code`` (optional) narrows the LLM's grounding context to a single
    catalog entry — useful when the user has clicked into a specific rule and
    wants to ask follow-ups about it.
    ``history`` (optional) is a list of prior user/assistant turns from the
    same UI session, so multi-turn follow-ups make sense without server-side
    persistence. Server is stateless: the client tracks the history.
    """

    question: str
    rule_code: str | None = None
    history: list[dict] = []


def _build_rule_catalog_grounding(rule_code: str | None) -> str:
    """Return a compact string description of the rule catalog used as
    grounding for the LLM. Two modes:

    * ``rule_code is None`` — short index of every rule (code, label, tree, kind)
    * ``rule_code`` set      — full metadata for that one rule, plus the same
      short index so the LLM can refer to siblings if useful
    """
    from app.domain.decision_tree.rule_catalog import (
        get_rule_metadata,
        list_rule_catalog,
    )

    catalog = list_rule_catalog()
    short_lines = []
    for entry in catalog:
        tree = entry.get("tree") or "any"
        kind = entry.get("kind") or "rule"
        short_lines.append(
            f"- {entry['code']} ({tree}/{kind}): {entry.get('business_label') or entry['name']}"
        )
    short_index = "\n".join(short_lines)

    if rule_code is None:
        return f"## Rule catalog (index)\n{short_index}"

    meta = get_rule_metadata(rule_code) or {}
    detail_lines = [f"## Rule in focus: {rule_code}"]
    if meta.get("business_label"):
        detail_lines.append(f"Label: {meta['business_label']}")
    if meta.get("description"):
        detail_lines.append(f"Description: {meta['description']}")
    if meta.get("decides"):
        detail_lines.append(f"Decides: {', '.join(meta['decides'])}")
    if meta.get("verdict_meanings"):
        vm = "; ".join(f"{k}={v}" for k, v in meta["verdict_meanings"].items())
        detail_lines.append(f"Verdict meanings: {vm}")
    if meta.get("params"):
        # params is a dict of {param_name: {default, ...}} — keep terse
        params_summary = ", ".join(meta["params"].keys())
        detail_lines.append(f"Tunable params: {params_summary}")
    detail = "\n".join(detail_lines)

    return f"{detail}\n\n## Other rules in the catalog (for reference)\n{short_index}"


def _build_qa_system_prompt(grounding: str) -> str:
    return (
        "You are an expert assistant for the ampliFi cost-center cleanup tool. "
        "Your role is to answer questions about the built-in decision-tree rule "
        "catalog so analysts and admins can understand what each rule does, what "
        "verdicts it produces, and how to combine rules into pipelines.\n\n"
        "Rules below are read-only — they are defined in Python code. The user "
        "cannot edit them here; they can tune parameters per pipeline variant on "
        "the Decision Tree Variants page (/admin/configs) and build custom "
        "no-code rules on the Rule Builder page (/admin/rules).\n\n"
        "Ground every answer in the catalog provided. If asked about a rule "
        "that isn't in the catalog, say so — do not invent rule codes or "
        "behaviors. Cite specific rule codes (in backticks) when referring to "
        "them. Keep answers concise: 2-5 short paragraphs maximum, plain prose, "
        "no markdown headers.\n\n"
        f"{grounding}"
    )


@router.post("/rule-catalog/qa")
def rule_catalog_qa(
    body: RuleCatalogQARequest,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """LLM-grounded Q&A over the built-in rule catalog (read-only).

    Stateless — the client passes prior turns in ``history`` if multi-turn
    context is needed. Returns ``{available: false, reason: ...}`` when the
    LLM provider is not configured, so the frontend can show a clear message.
    """
    from app.infra.llm.provider import (
        AzureOpenAIProvider,
        Message,
        SapBtpProvider,
    )

    cfg = db.execute(select(AppConfig).where(AppConfig.key == "llm")).scalar_one_or_none()
    if not cfg or not cfg.value:
        return {
            "available": False,
            "reason": "LLM provider not configured",
            "answer": None,
        }

    llm_config = cfg.value
    provider_type = llm_config.get("provider", "azure")
    try:
        if provider_type == "azure":
            provider = AzureOpenAIProvider(llm_config)
        elif provider_type == "btp":
            provider = SapBtpProvider(llm_config)
        else:
            return {
                "available": False,
                "reason": f"Unknown provider type: {provider_type}",
                "answer": None,
            }
    except Exception as exc:  # pragma: no cover — defensive
        return {
            "available": False,
            "reason": f"Provider init failed: {type(exc).__name__}",
            "answer": None,
        }

    grounding = _build_rule_catalog_grounding(body.rule_code)
    messages: list[Message] = [
        Message(role="system", content=_build_qa_system_prompt(grounding)),
    ]
    # Replay prior turns from this UI session (best-effort sanitization)
    for turn in body.history[-10:]:  # cap to last 10 turns to bound prompt size
        role = turn.get("role")
        content = turn.get("content")
        if role in ("user", "assistant") and isinstance(content, str) and content.strip():
            messages.append(Message(role=role, content=content[:4000]))
    messages.append(Message(role="user", content=body.question[:4000]))

    model = llm_config.get("model", "gpt-4o")
    try:
        completion = provider.complete(
            model=model,
            messages=messages,
            temperature=float(llm_config.get("qa_temperature", 0.2)),
            max_tokens=int(llm_config.get("qa_max_tokens", 600)),
            metadata={"feature": "rule_catalog_qa", "rule_code": body.rule_code},
        )
    except Exception as exc:
        return {
            "available": False,
            "reason": f"LLM call failed: {type(exc).__name__}",
            "answer": None,
        }

    return {
        "available": True,
        "answer": completion.text,
        "scoped_to": body.rule_code,
        "model": model,
        "tokens_in": getattr(completion, "tokens_in", 0),
        "tokens_out": getattr(completion, "tokens_out", 0),
    }


# --- LLM-powered Config Drafter & Configurator (PR #72) ---


class ConfigDrafterRequest(BaseModel):
    """Body for /configs/draft-from-description.

    ``description`` — user's plain-language description of the desired
    pipeline goal.
    ``engine`` — 'v1' (cleansing) or 'v2' (CEMA migration). Determines
    which routines the LLM is allowed to choose from.
    """

    description: str
    engine: str = "v1"


class ConfigConfiguratorRequest(BaseModel):
    """Body for /configs/configure-stepwise.

    ``step``:
    - 'clarify' — ask the LLM for clarifying questions about the user's goal
    - 'propose' — produce a draft based on description + clarifications
    - 'refine' — adjust an existing draft based on user feedback

    ``description`` is required for clarify and propose.
    ``clarifications`` (Q/A pairs from the clarify step) feed into propose.
    ``draft`` and ``user_feedback`` are required for refine.
    """

    step: str  # 'clarify' | 'propose' | 'refine'
    engine: str = "v1"
    description: str | None = None
    clarifications: list[dict] = []  # [{question, answer}, ...]
    draft: dict | None = None
    user_feedback: str | None = None


def _build_pipeline_grounding(engine: str) -> str:
    """Catalog grounding tailored for pipeline drafting — emphasizes the
    routines available in the chosen engine and their tunable params."""
    from app.domain.decision_tree.rule_catalog import list_rule_catalog

    tree_filter = engine.lower()
    catalog = list_rule_catalog()
    if tree_filter == "v1":
        catalog = [e for e in catalog if not e["code"].startswith("v2.")]
    elif tree_filter == "v2":
        catalog = [e for e in catalog if e["code"].startswith("v2.")]

    blocks = []
    for e in catalog:
        params = e.get("params") or {}
        param_lines = []
        for pname, pmeta in params.items():
            default = pmeta.get("default") if isinstance(pmeta, dict) else None
            help_text = pmeta.get("help") if isinstance(pmeta, dict) else None
            param_lines.append(
                f"    - {pname}: default={default}" + (f" ({help_text})" if help_text else "")
            )
        param_block = "\n".join(param_lines) if param_lines else "    (no tunable parameters)"
        decides = ", ".join(e.get("decides") or []) or "—"
        blocks.append(
            f"### {e['code']} — {e.get('business_label') or e['name']}\n"
            f"{e.get('description', '').strip()}\n"
            f"  Decides: {decides}\n"
            f"  Tunable params:\n{param_block}"
        )
    return "\n\n".join(blocks)


def _build_drafter_system_prompt(engine: str, grounding: str) -> str:
    return (
        "You are an expert assistant for the ampliFi cost-center cleanup tool. "
        "Your job is to draft an analysis pipeline configuration based on the "
        "user's plain-language description.\n\n"
        f"Engine: {engine.upper()}. You may ONLY pick routines from the catalog "
        "below — every routine code in your output MUST appear in the catalog "
        "exactly. Inventing codes is a hard error.\n\n"
        "Output format — return a single JSON object with this exact shape and "
        "nothing else (no prose, no markdown fences):\n"
        "{\n"
        '  "rationale": "<2-4 sentence plain-prose explanation '
        'of why this pipeline matches the goal>",\n'
        '  "config": {\n'
        '    "pipeline": [\n'
        '      {"routine": "<code>", "enabled": true, "params": {<key>: <value>, ...}},\n'
        "      ...\n"
        "    ],\n"
        '    "params": {<top-level params if relevant>}\n'
        "  },\n"
        '  "warnings": ["<any caveats or assumptions you made>"]\n'
        "}\n\n"
        "Choose routines that are actually relevant to the goal. Don't pad. "
        "Use param defaults unless the user's description implies a different "
        "value (e.g. 'aggressively retire' → tighten thresholds; 'be conservative' "
        "→ loosen them). For V1 pipelines, ALWAYS include "
        "'aggregate.combine_outcomes' as the last routine — it's the combiner.\n\n"
        "## Available routines\n"
        f"{grounding}"
    )


def _build_configurator_clarify_prompt(engine: str, grounding: str) -> str:
    return (
        "You are an expert assistant for the ampliFi cost-center cleanup tool. "
        "The user has described a goal for a pipeline configuration but the "
        "description may be ambiguous. Before drafting, ask 1-3 SHORT clarifying "
        "questions that would meaningfully change the resulting pipeline.\n\n"
        "Return a single JSON object with this exact shape and nothing else:\n"
        "{\n"
        '  "questions": [\n'
        '    {"key": "<short_snake_case_id>", "question": "<plain question>", '
        '"options": ["<short answer 1>", "<short answer 2>", ...]},\n'
        "    ...\n"
        "  ]\n"
        "}\n\n"
        "If the user's description is clear enough that no clarification helps, "
        "return an empty questions list. Don't ask trivia. Each option must be "
        "a short phrase (≤4 words) the user can tap.\n\n"
        f"Engine: {engine.upper()}. Available routines:\n{grounding}"
    )


def _build_configurator_refine_prompt(engine: str, grounding: str) -> str:
    return (
        "You are an expert assistant for the ampliFi cost-center cleanup tool. "
        "You previously drafted a pipeline configuration. The user has provided "
        "feedback. Produce a REVISED config that addresses the feedback while "
        "keeping the parts they didn't object to.\n\n"
        f"Engine: {engine.upper()}. Output format — same JSON shape as the "
        "drafter (rationale, config, warnings). Routine codes must come from "
        "the catalog below.\n\n"
        "## Available routines\n"
        f"{grounding}"
    )


def _call_llm_json(
    db: Session,
    system_prompt: str,
    user_message: str,
    metadata: dict,
    history: list | None = None,
    max_tokens: int = 1500,
    temperature: float = 0.1,
) -> dict:
    """Shared helper: call the configured LLM, parse a JSON response,
    return ``{available, parsed?, raw?, model, tokens_in, tokens_out, reason?}``.

    Robust to: no LLM config, unknown provider, init failure, network
    failure, malformed JSON. All collapse to ``available: false``.
    """
    import json
    import re

    from app.infra.llm.provider import (
        AzureOpenAIProvider,
        Message,
        SapBtpProvider,
    )

    cfg = db.execute(select(AppConfig).where(AppConfig.key == "llm")).scalar_one_or_none()
    if not cfg or not cfg.value:
        return {"available": False, "reason": "LLM provider not configured"}

    llm_config = cfg.value
    provider_type = llm_config.get("provider", "azure")
    try:
        if provider_type == "azure":
            provider = AzureOpenAIProvider(llm_config)
        elif provider_type == "btp":
            provider = SapBtpProvider(llm_config)
        else:
            return {
                "available": False,
                "reason": f"Unknown provider type: {provider_type}",
            }
    except Exception as exc:
        return {
            "available": False,
            "reason": f"Provider init failed: {type(exc).__name__}",
        }

    messages: list[Message] = [Message(role="system", content=system_prompt)]
    for turn in (history or [])[-10:]:
        role = turn.get("role")
        content = turn.get("content")
        if role in ("user", "assistant") and isinstance(content, str) and content.strip():
            messages.append(Message(role=role, content=content[:6000]))
    messages.append(Message(role="user", content=user_message[:8000]))

    model = llm_config.get("model", "gpt-4o")
    try:
        completion = provider.complete(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            metadata=metadata,
        )
    except Exception as exc:
        return {"available": False, "reason": f"LLM call failed: {type(exc).__name__}"}

    raw = completion.text or ""
    # Strip markdown code fences if present
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        return {
            "available": False,
            "reason": f"LLM returned non-JSON: {type(exc).__name__}",
            "raw": raw,
            "model": model,
            "tokens_in": getattr(completion, "tokens_in", 0),
            "tokens_out": getattr(completion, "tokens_out", 0),
        }

    return {
        "available": True,
        "parsed": parsed,
        "raw": raw,
        "model": model,
        "tokens_in": getattr(completion, "tokens_in", 0),
        "tokens_out": getattr(completion, "tokens_out", 0),
    }


def _validate_pipeline_config(parsed: dict, engine: str) -> dict:
    """Validate that every routine code in the proposed config exists in
    the rule catalog. Returns ``{ok, invalid_codes, valid_codes}``."""
    from app.domain.decision_tree.rule_catalog import list_rule_catalog

    tree_filter = engine.lower()
    catalog = list_rule_catalog()
    if tree_filter == "v1":
        valid = {e["code"] for e in catalog if not e["code"].startswith("v2.")}
    elif tree_filter == "v2":
        valid = {e["code"] for e in catalog if e["code"].startswith("v2.")}
    else:
        valid = {e["code"] for e in catalog}

    config = parsed.get("config") or {}
    pipeline = config.get("pipeline") or []
    proposed_codes = [step.get("routine") for step in pipeline if isinstance(step, dict)]
    invalid = [c for c in proposed_codes if c and c not in valid]
    return {
        "ok": not invalid,
        "invalid_codes": invalid,
        "valid_codes": [c for c in proposed_codes if c in valid],
    }


@router.post("/configs/draft-from-description")
def llm_draft_config(
    body: ConfigDrafterRequest,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Drafter (single-shot): plain-language description → editable config draft.

    Read-only — never persists. Returns the draft so the frontend can show
    it for review before the user saves it through the regular config-create
    endpoint.
    """
    if body.engine.lower() not in ("v1", "v2"):
        raise HTTPException(status_code=400, detail="engine must be 'v1' or 'v2'")

    grounding = _build_pipeline_grounding(body.engine)
    system_prompt = _build_drafter_system_prompt(body.engine, grounding)
    result = _call_llm_json(
        db,
        system_prompt=system_prompt,
        user_message=body.description,
        metadata={"feature": "config_drafter", "engine": body.engine},
        max_tokens=2000,
    )
    if not result.get("available"):
        return {
            "available": False,
            "reason": result.get("reason"),
            "raw": result.get("raw"),
            "draft": None,
        }

    parsed = result["parsed"]
    validation = _validate_pipeline_config(parsed, body.engine)
    return {
        "available": True,
        "draft": parsed.get("config"),
        "rationale": parsed.get("rationale"),
        "warnings": parsed.get("warnings", []),
        "validation": validation,
        "model": result["model"],
        "tokens_in": result["tokens_in"],
        "tokens_out": result["tokens_out"],
    }


@router.post("/configs/configure-stepwise")
def llm_configure_stepwise(
    body: ConfigConfiguratorRequest,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Configurator (multi-step): clarify → propose → refine.

    Stateless — the client carries clarifications/draft between steps.
    Read-only. The propose step ends with an editable draft, same shape
    as the drafter endpoint.
    """
    if body.engine.lower() not in ("v1", "v2"):
        raise HTTPException(status_code=400, detail="engine must be 'v1' or 'v2'")
    if body.step not in ("clarify", "propose", "refine"):
        raise HTTPException(status_code=400, detail="step must be clarify|propose|refine")

    grounding = _build_pipeline_grounding(body.engine)

    if body.step == "clarify":
        if not body.description:
            raise HTTPException(status_code=400, detail="description required for clarify")
        result = _call_llm_json(
            db,
            system_prompt=_build_configurator_clarify_prompt(body.engine, grounding),
            user_message=body.description,
            metadata={"feature": "config_configurator", "step": "clarify"},
            max_tokens=600,
        )
        if not result.get("available"):
            return {
                "available": False,
                "reason": result.get("reason"),
                "raw": result.get("raw"),
                "questions": [],
            }
        return {
            "available": True,
            "step": "clarify",
            "questions": result["parsed"].get("questions", []),
            "model": result["model"],
            "tokens_in": result["tokens_in"],
            "tokens_out": result["tokens_out"],
        }

    if body.step == "propose":
        if not body.description:
            raise HTTPException(status_code=400, detail="description required for propose")
        # Fold clarifications into the user message
        clar_lines = "\n".join(
            f"Q: {c.get('question')}\nA: {c.get('answer')}"
            for c in body.clarifications or []
            if isinstance(c, dict) and c.get("question") and c.get("answer")
        )
        user_msg = body.description + ("\n\nClarifications:\n" + clar_lines if clar_lines else "")
        result = _call_llm_json(
            db,
            system_prompt=_build_drafter_system_prompt(body.engine, grounding),
            user_message=user_msg,
            metadata={"feature": "config_configurator", "step": "propose"},
            max_tokens=2000,
        )
        if not result.get("available"):
            return {
                "available": False,
                "reason": result.get("reason"),
                "raw": result.get("raw"),
                "draft": None,
            }
        parsed = result["parsed"]
        validation = _validate_pipeline_config(parsed, body.engine)
        return {
            "available": True,
            "step": "propose",
            "draft": parsed.get("config"),
            "rationale": parsed.get("rationale"),
            "warnings": parsed.get("warnings", []),
            "validation": validation,
            "model": result["model"],
            "tokens_in": result["tokens_in"],
            "tokens_out": result["tokens_out"],
        }

    # refine
    if not body.draft or not body.user_feedback:
        raise HTTPException(status_code=400, detail="draft and user_feedback required for refine")
    import json as _json

    user_msg = (
        "Current draft (JSON):\n"
        + _json.dumps(body.draft)
        + "\n\nUser feedback:\n"
        + body.user_feedback
    )
    result = _call_llm_json(
        db,
        system_prompt=_build_configurator_refine_prompt(body.engine, grounding),
        user_message=user_msg,
        metadata={"feature": "config_configurator", "step": "refine"},
        max_tokens=2000,
    )
    if not result.get("available"):
        return {
            "available": False,
            "reason": result.get("reason"),
            "raw": result.get("raw"),
            "draft": None,
        }
    parsed = result["parsed"]
    validation = _validate_pipeline_config(parsed, body.engine)
    return {
        "available": True,
        "step": "refine",
        "draft": parsed.get("config"),
        "rationale": parsed.get("rationale"),
        "warnings": parsed.get("warnings", []),
        "validation": validation,
        "model": result["model"],
        "tokens_in": result["tokens_in"],
        "tokens_out": result["tokens_out"],
    }
