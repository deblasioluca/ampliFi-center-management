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
    _user: AppUser = Depends(require_role("admin", "analyst")),
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
    _user: AppUser = Depends(require_role("admin", "analyst")),
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
    _user: AppUser = Depends(require_role("admin", "analyst")),
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
                "description": row.get("DESSION", "").strip(),
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
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
) -> list[dict]:
    """List all hierarchies with their labels."""
    from app.models.core import Hierarchy

    rows = (
        db.execute(select(Hierarchy).order_by(Hierarchy.setclass, Hierarchy.setname))
        .scalars()
        .all()
    )
    class_labels = {"0101": "Cost Center", "0104": "Profit Center", "0106": "Entity"}
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


@router.post("/uploads")
def create_upload(
    kind: str = Query(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
) -> dict:
    import pathlib

    from app.config import settings

    content = file.file.read()
    storage_dir = pathlib.Path(settings.storage_local_path).resolve() / "uploads"
    storage_dir.mkdir(parents=True, exist_ok=True)
    fname = pathlib.Path(file.filename or "unknown").name  # strip directory components
    unique_prefix = f"{int(time.time())}_{uuid.uuid4().hex[:8]}_"
    dest = storage_dir / (unique_prefix + fname)
    if not dest.resolve().is_relative_to(storage_dir):
        raise HTTPException(status_code=400, detail="Invalid filename")
    dest.write_bytes(content)

    batch = UploadBatch(
        kind=kind,
        filename=fname,
        status="uploaded",
        uploaded_by=_user.id,
        storage_uri=str(dest.resolve()),
    )
    db.add(batch)
    db.commit()
    db.refresh(batch)
    return {"id": batch.id, "status": batch.status, "filename": batch.filename}


@router.get("/uploads")
def list_uploads(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
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
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
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
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
) -> dict:
    import logging as _logging

    from app.services.upload_processor import validate_upload

    _log = _logging.getLogger(__name__)
    try:
        result = validate_upload(batch_id, db)
        _log.info("Validate batch %s: %s", batch_id, result)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except Exception as exc:
        _log.exception("Validate batch %s failed unexpectedly", batch_id)
        try:
            db.rollback()
            batch = db.get(UploadBatch, batch_id)
            if batch:
                batch.status = "failed"
                db.commit()
        except Exception:
            db.rollback()
        raise HTTPException(status_code=500, detail=f"Validation error: {exc}") from None


@router.post("/uploads/{batch_id}/load")
def load_upload_batch(
    batch_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
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
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
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
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
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
