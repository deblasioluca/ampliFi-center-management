"""Admin API endpoints (sections 11.2-11.5, 11.11)."""

from __future__ import annotations

import time
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel, EmailStr
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
    email: EmailStr
    display_name: str
    password: str
    role: str = "analyst"


class UserUpdate(BaseModel):
    display_name: str | None = None
    role: str | None = None
    is_active: bool | None = None


class UserOut(BaseModel):
    id: int
    email: str
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
    existing = db.execute(select(AppUser).where(AppUser.email == body.email)).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")
    user = AppUser(
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
    body: SAPConnectionCreate,
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

    return validate_upload(batch_id, db)


@router.post("/uploads/{batch_id}/load")
def load_upload_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    from app.services.upload_processor import load_upload

    return load_upload(batch_id, db)


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
