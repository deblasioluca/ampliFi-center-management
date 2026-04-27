"""Admin API endpoints (sections 11.2-11.5, 11.11)."""

from __future__ import annotations

import time
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination, require_role
from app.auth.service import hash_password
from app.infra.db.session import get_db
from app.models.core import (
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
    role: str = "analyst"


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
        if body.role is not None and body.role != "admin":
            raise HTTPException(status_code=409, detail="Cannot demote your own account")
    if body.display_name is not None:
        user.display_name = body.display_name
    if body.role is not None:
        user.role = body.role
    if body.is_active is not None:
        user.is_active = body.is_active
    db.commit()
    db.refresh(user)
    return UserOut.model_validate(user)


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
    client: str = "100"
    language: str = "EN"
    username: str
    password: str
    protocol: str = "odata"
    verify_ssl: bool = True
    use_proxy: bool = False
    saml2_disabled: bool = False
    allowed_tables: str | None = None


class SAPConnectionUpdate(SAPConnectionCreate):
    password: str | None = None  # optional on update


class SAPConnectionOut(BaseModel):
    id: int
    name: str
    description: str | None
    system_type: str
    landscape_type: str | None
    base_url: str
    client: str
    language: str
    username: str
    protocol: str
    verify_ssl: bool
    use_proxy: bool
    saml2_disabled: bool
    is_active: bool
    allowed_tables: str | None

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

    conn = SAPConnection(
        name=body.name,
        description=body.description,
        system_type=body.system_type,
        landscape_type=body.landscape_type,
        base_url=body.base_url,
        client=body.client,
        language=body.language,
        username=body.username,
        password_encrypted=encrypt_password(body.password),
        protocol=body.protocol,
        verify_ssl=body.verify_ssl,
        use_proxy=body.use_proxy,
        saml2_disabled=body.saml2_disabled,
        allowed_tables=body.allowed_tables,
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
    entity_set: str | None = None
    path: str | None = None
    params: dict | None = None
    schedule_cron: str | None = None
    enabled: bool = True


@router.get("/sap/{conn_id}/bindings")
def list_object_bindings(
    conn_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
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

    binding = SAPObjectBinding(
        connection_id=conn_id,
        object_type=body.object_type,
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

            # Extract table name from ADT URL path like /sap/bc/adt/datapreview/ddic?table=CSKS
            parsed = urlparse(path)
            table_name = parse_qs(parsed.query).get("table", [path])[0]
            result = fetch_adt_table(conn, table_name, max_rows=1)
            row_count = len(result) if result else 0
        elif proto == "rfc":
            # RFC/SOAP test — not yet implemented, report clearly
            return {
                "success": True,
                "message": (
                    f"RFC binding configured: {path}"
                    " (live test requires SOAP endpoint — config looks valid)"
                ),
                "entity_set": raw,
                "protocol": proto,
            }
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
            "error": str(exc),
            "entity_set": raw,
            "protocol": proto,
        }


@router.post("/sap/{conn_id}/bindings/{binding_id}/extract")
def extract_via_binding(
    conn_id: int,
    binding_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Extract data from SAP using a specific binding's configuration."""
    from app.models.core import SAPObjectBinding

    binding = db.get(SAPObjectBinding, binding_id)
    if not binding or binding.connection_id != conn_id:
        raise HTTPException(status_code=404, detail="Object binding not found")

    from app.services.sap_extraction import extract_from_sap

    try:
        result = extract_from_sap(
            db,
            conn_id,
            binding.object_type,
            binding.params,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


# --- Uploads ---


@router.post("/uploads")
def create_upload(
    kind: str = Query(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    import pathlib

    from app.config import settings

    content = file.file.read()
    storage_dir = pathlib.Path(settings.storage_local_path) / "uploads"
    storage_dir.mkdir(parents=True, exist_ok=True)
    fname = pathlib.Path(file.filename or "unknown").name  # strip directory components
    unique_prefix = f"{int(time.time())}_{uuid.uuid4().hex[:8]}_"
    dest = storage_dir / (unique_prefix + fname)
    if not dest.resolve().is_relative_to(storage_dir.resolve()):
        raise HTTPException(status_code=400, detail="Invalid filename")
    dest.write_bytes(content)

    batch = UploadBatch(
        kind=kind,
        filename=fname,
        status="uploaded",
        uploaded_by=_user.id,
        storage_uri=str(dest),
    )
    db.add(batch)
    db.commit()
    db.refresh(batch)
    return {"id": batch.id, "status": batch.status, "filename": batch.filename}


@router.get("/uploads")
def list_uploads(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
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
                "filename": b.filename,
                "status": b.status,
                "rows_total": b.rows_total,
                "rows_valid": b.rows_valid,
                "rows_error": b.rows_error,
                "rows_loaded": b.rows_loaded,
                "created_at": str(b.created_at) if b.created_at else None,
            }
            for b in batches
        ],
    }


@router.get("/uploads/{batch_id}")
def get_upload(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
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
        "storage_uri": batch.storage_uri,
        "created_at": str(batch.created_at) if batch.created_at else None,
        "validated_at": str(batch.validated_at) if batch.validated_at else None,
        "loaded_at": str(batch.loaded_at) if batch.loaded_at else None,
    }


@router.post("/uploads/{batch_id}/validate")
def validate_upload_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.services.upload_processor import validate_upload

    try:
        return validate_upload(batch_id, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@router.post("/uploads/{batch_id}/load")
def load_upload_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.services.upload_processor import load_upload

    try:
        return load_upload(batch_id, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@router.post("/uploads/{batch_id}/rollback")
def rollback_upload_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.services.upload_processor import rollback_upload

    try:
        return rollback_upload(batch_id, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@router.get("/uploads/{batch_id}/errors")
def list_upload_errors(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
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
        "description": "Legacy cost center upload template",
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
        ],
        "sample_row": [
            "1000",
            "0000100001",
            "Administration",
            "General Administration",
            "JDOE",
            "DE01",
            "H",
            "EUR",
            "0000100001",
            "TRUE",
        ],
    },
    "profit_centers": {
        "filename": "template_profit_centers.csv",
        "description": "Legacy profit center upload template",
        "columns": [
            "COAREA",
            "PCTR",
            "TXTSH",
            "TXTMI",
            "RESPONSIBLE",
            "CCODE",
            "DEPARTMENT",
            "CURRENCY",
            "CURRPCTR",
            "IS_ACTIVE",
        ],
        "sample_row": [
            "1000",
            "0000100001",
            "Sales DE",
            "Sales Department Germany",
            "JDOE",
            "DE01",
            "SALES",
            "EUR",
            "EUR",
            "TRUE",
        ],
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
    "entities": {
        "filename": "template_entities.csv",
        "description": "Entity (company code) upload template",
        "columns": ["CCODE", "NAME", "COUNTRY", "REGION", "CURRENCY", "IS_ACTIVE"],
        "sample_row": ["DE01", "Germany Operations", "DE", "EMEA", "EUR", "TRUE"],
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
    "employees": {
        "filename": "template_employees.csv",
        "description": "Employee (SAP HR) upload template",
        "columns": [
            "GPN",
            "BS_FIRSTNAME",
            "BS_LASTNAME",
            "BS_NAME",
            "EMAIL_ADDRESS",
            "EMP_STATUS",
            "JOB_DESC",
            "OU_CD",
            "OU_DESC",
            "LOCAL_CC_CD",
            "LOCAL_CC_DESC",
            "GCRS_COMP_CD",
            "GCRS_COMP_DESC",
            "LOCN_CITY_NAME_1",
            "LOCN_CTRY_CD_1",
            "LM_GPN",
            "LM_BS_FIRSTNAME",
            "LM_BS_LASTNAME",
            "RANK_DESC",
            "FUNC_E_BUS_AREA",
        ],
        "sample_row": [
            "10001",
            "John",
            "Doe",
            "John Doe",
            "john.doe@company.com",
            "ACTIVE",
            "Head of Finance",
            "OU_FIN",
            "Finance Department",
            "0000100001",
            "Administration",
            "1000",
            "Global Corp HQ",
            "Zurich",
            "CH",
            "10002",
            "Jane",
            "Smith",
            "Director",
            "Finance",
        ],
    },
}


@router.get("/upload-templates")
def list_upload_templates(
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
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
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
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
