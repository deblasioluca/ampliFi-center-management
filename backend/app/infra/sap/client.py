"""SAP connection client — OData, ADT, SOAP/RFC protocols.

Adopted from sap-ai-consultant patterns.  Uses the unified HTTP client
factory and connection trial engine for consistent behaviour across all
three protocols.
"""

from __future__ import annotations

import time
from typing import Any
from xml.sax.saxutils import escape as xml_escape

import httpx
import structlog

from app.infra.sap.base import ACCEPT_HEADERS, SSO_KEYWORDS
from app.infra.sap.connection_trial import (
    _create_sap_client,
    _resolve_base_url,
    run_connection_trial,
)
from app.infra.sap.encryption import decrypt_password
from app.models.core import SAPConnection

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Simple per-protocol tests (kept for backward compat)
# ---------------------------------------------------------------------------


def test_connection(conn: SAPConnection) -> dict[str, Any]:
    """Run a simple single-protocol test (legacy API).

    For the full multi-endpoint trial use run_connection_trial() instead.
    """
    start = time.monotonic()
    try:
        password = decrypt_password(conn.password_encrypted)
        if conn.protocol == "odata":
            return _test_odata(conn, password, start)
        elif conn.protocol == "adt":
            return _test_adt(conn, password, start)
        elif conn.protocol in ("soap", "soap_rfc"):
            return _test_soap(conn, password, start)
        else:
            return {"success": False, "error": f"Unsupported protocol: {conn.protocol}"}
    except Exception as e:
        elapsed = int((time.monotonic() - start) * 1000)
        logger.error("sap.connection.test.failed", name=conn.name, error=str(e))
        return {"success": False, "error": str(e), "latency_ms": elapsed}


def test_connection_full(conn: SAPConnection) -> dict[str, Any]:
    """Run a full connection trial testing ALL endpoints (ADT, OData, SOAP).

    This is the sap-ai-consultant-style comprehensive trial that produces
    a result matrix with per-endpoint recommendations.
    """
    result = run_connection_trial(conn)
    return result.to_dict()


def _test_odata(conn: SAPConnection, password: str, start: float) -> dict[str, Any]:
    """Test OData connectivity via CATALOGSERVICE discovery."""
    url = f"{_resolve_base_url(conn)}/sap/opu/odata/iwfnd/CATALOGSERVICE;v=2/ServiceCollection"
    headers = {
        "sap-client": conn.client,
        "Accept": "application/json",
        "x-csrf-token": "Fetch",
    }
    with httpx.Client(verify=conn.verify_ssl, timeout=30.0, follow_redirects=False) as client:
        resp = client.get(url, headers=headers, auth=(conn.username, password))
        elapsed = int((time.monotonic() - start) * 1000)

        # Check for SSO redirect
        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("location", "")
            if any(kw in location.lower() for kw in SSO_KEYWORDS):
                return {
                    "success": False,
                    "latency_ms": elapsed,
                    "protocol": "odata",
                    "status_code": resp.status_code,
                    "error": f"SSO redirect to {location}",
                    "sso_redirect": True,
                    "detail": "System requires SSO — add saml2=disabled parameter "
                    "or configure SSO credentials",
                }

        if resp.status_code == 200:
            csrf_token = resp.headers.get("x-csrf-token", "")
            try:
                data = resp.json()
                services = data.get("d", {}).get("results", [])
                service_names = [s.get("Title", s.get("ID", "")) for s in services[:10]]
            except Exception:
                service_names = []
            return {
                "success": True,
                "latency_ms": elapsed,
                "protocol": "odata",
                "csrf_available": bool(csrf_token and csrf_token.lower() != "required"),
                "status_code": 200,
                "services_found": len(service_names),
                "services": service_names,
                "detail": f"OData catalog OK — {len(service_names)} services discovered",
            }
        if resp.status_code == 403:
            return {
                "success": True,
                "latency_ms": elapsed,
                "protocol": "odata",
                "status_code": 403,
                "detail": "OData reachable — user lacks catalog authorization "
                "(check S_SERVICE auth object in SAP)",
            }
        if resp.status_code == 401:
            return {
                "success": False,
                "latency_ms": elapsed,
                "protocol": "odata",
                "status_code": 401,
                "error": "Authentication failed — wrong username or password",
            }
        return {
            "success": False,
            "latency_ms": elapsed,
            "protocol": "odata",
            "status_code": resp.status_code,
            "error": resp.text[:500],
        }


def _test_adt(conn: SAPConnection, password: str, start: float) -> dict[str, Any]:
    """Test ADT connectivity via /sap/bc/adt/discovery."""
    url = f"{_resolve_base_url(conn)}/sap/bc/adt/discovery"
    headers = {
        "sap-client": conn.client,
        "Accept": ACCEPT_HEADERS["adt"],
        "x-csrf-token": "Fetch",
    }
    with httpx.Client(verify=conn.verify_ssl, timeout=30.0, follow_redirects=False) as client:
        resp = client.get(url, headers=headers, auth=(conn.username, password))
        elapsed = int((time.monotonic() - start) * 1000)

        # SSO redirect check
        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("location", "")
            if any(kw in location.lower() for kw in SSO_KEYWORDS):
                return {
                    "success": False,
                    "latency_ms": elapsed,
                    "protocol": "adt",
                    "status_code": resp.status_code,
                    "error": f"SSO redirect to {location}",
                    "sso_redirect": True,
                }

        if resp.status_code == 200:
            csrf_token = resp.headers.get("x-csrf-token", "")
            body = resp.text or ""
            has_services = "<app:service" in body or "<atom:" in body
            return {
                "success": True,
                "latency_ms": elapsed,
                "protocol": "adt",
                "status_code": 200,
                "csrf_available": bool(csrf_token and csrf_token.lower() != "required"),
                "services_discovered": has_services,
                "detail": "ADT discovery OK — CSRF token obtained"
                + (", services listed" if has_services else ""),
            }
        if resp.status_code == 403:
            return {
                "success": False,
                "latency_ms": elapsed,
                "protocol": "adt",
                "status_code": 403,
                "error": "ADT reachable but user lacks S_DEVELOP authorization. "
                "Check auth objects S_DEVELOP, S_ADT_RES in transaction SU01",
            }
        if resp.status_code == 401:
            return {
                "success": False,
                "latency_ms": elapsed,
                "protocol": "adt",
                "status_code": 401,
                "error": "Authentication failed — wrong username or password",
            }
        if resp.status_code == 404:
            return {
                "success": False,
                "latency_ms": elapsed,
                "protocol": "adt",
                "status_code": 404,
                "error": "ADT service not activated. Activate /sap/bc/adt in transaction SICF",
            }
        return {
            "success": False,
            "latency_ms": elapsed,
            "protocol": "adt",
            "status_code": resp.status_code,
            "error": resp.text[:500],
        }


def _test_soap(conn: SAPConnection, password: str, start: float) -> dict[str, Any]:
    """Test SOAP/RFC connectivity via /sap/bc/soap/rfc."""
    url = f"{_resolve_base_url(conn)}/sap/bc/soap/rfc"
    headers = {
        "sap-client": conn.client,
        "Content-Type": "text/xml; charset=utf-8",
        "Accept": ACCEPT_HEADERS["soap"],
    }
    with httpx.Client(verify=conn.verify_ssl, timeout=30.0, follow_redirects=False) as client:
        # POST with empty body — we expect 400/401/403/405/500 (service alive)
        resp = client.post(url, content="", headers=headers, auth=(conn.username, password))
        elapsed = int((time.monotonic() - start) * 1000)

        # SSO redirect check
        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("location", "")
            if any(kw in location.lower() for kw in SSO_KEYWORDS):
                return {
                    "success": False,
                    "latency_ms": elapsed,
                    "protocol": "soap",
                    "status_code": resp.status_code,
                    "error": f"SSO redirect to {location}",
                    "sso_redirect": True,
                }

        if resp.status_code in (200, 400, 405, 500):
            return {
                "success": True,
                "latency_ms": elapsed,
                "protocol": "soap",
                "status_code": resp.status_code,
                "reachable": True,
                "detail": f"SOAP/RFC endpoint reachable (HTTP {resp.status_code}). "
                "RFC function modules accessible via SOAP web services",
            }
        if resp.status_code == 401:
            return {
                "success": False,
                "latency_ms": elapsed,
                "protocol": "soap",
                "status_code": 401,
                "error": "Authentication failed — wrong username or password",
            }
        if resp.status_code == 403:
            return {
                "success": True,
                "latency_ms": elapsed,
                "protocol": "soap",
                "status_code": 403,
                "detail": "SOAP endpoint reachable but user lacks S_RFC authorization. "
                "Assign S_RFC auth object for RFC_READ_TABLE, BAPI_COSTCENTER_GETLIST, etc.",
            }
        if resp.status_code == 404:
            return {
                "success": False,
                "latency_ms": elapsed,
                "protocol": "soap",
                "status_code": 404,
                "error": "SOAP/RFC service not activated. Activate /sap/bc/soap/rfc in SICF",
            }
        return {
            "success": False,
            "latency_ms": elapsed,
            "protocol": "soap",
            "status_code": resp.status_code,
            "error": resp.text[:500],
        }


# ---------------------------------------------------------------------------
# OData extraction (with paging, CSRF, retry)
# ---------------------------------------------------------------------------


def fetch_odata(
    conn: SAPConnection,
    entity_set: str,
    params: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch data from an SAP OData service with server-driven paging."""
    password = decrypt_password(conn.password_encrypted)
    url = f"{_resolve_base_url(conn)}/sap/opu/odata/sap/{entity_set}"
    headers = {
        "sap-client": conn.client,
        "Accept": "application/json",
        "Accept-Language": conn.language,
    }
    all_results: list[dict[str, Any]] = []

    with httpx.Client(
        verify=conn.verify_ssl,
        timeout=60.0,
        follow_redirects=False,
    ) as client:
        next_url: str | None = url
        page = 0
        while next_url:
            resp = client.get(
                next_url,
                headers=headers,
                params=params if next_url == url else None,
                auth=(conn.username, password),
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("d", {}).get("results", [])
            all_results.extend(results)
            next_url = data.get("d", {}).get("__next")
            params = None
            page += 1
            if page % 10 == 0:
                logger.info(
                    "sap.odata.paging",
                    entity_set=entity_set,
                    page=page,
                    rows=len(all_results),
                )

    logger.info("sap.odata.fetch.complete", entity_set=entity_set, rows=len(all_results))
    return all_results


# ---------------------------------------------------------------------------
# ADT data extraction (table read via datapreview)
# ---------------------------------------------------------------------------

_ADT_ACCEPT = "application/xml, application/atom+xml, application/atomsvc+xml, text/xml, */*;q=0.8"
_RETRYABLE_CODES = (400, 403, 404, 405, 406)


def fetch_adt_table(
    conn: SAPConnection,
    table_name: str,
    *,
    max_rows: int = 10000,
    where: str = "",
    select: str = "*",
) -> list[dict[str, Any]]:
    """Read SAP table data via ADT datapreview endpoint.

    Tries multiple endpoint strategies (adopted from sap-ai-consultant):
    1. datapreview/freestyle (S/4HANA 1909+)
    2. datapreview/ddic?dataSourceName=TABLE
    3. datapreview/TABLE (legacy)

    Falls back to RFC_READ_TABLE via SOAP if all ADT endpoints fail
    with 403/404.
    """
    from app.infra.sap.xml_parser import parse_datapreview

    password = decrypt_password(conn.password_encrypted)
    base = _resolve_base_url(conn)

    # Build SQL-like query
    query_parts = [f"SELECT {select}", f"FROM {table_name.upper()}"]
    if where:
        query_parts.append(f"WHERE {where}")
    query_str = " ".join(query_parts)

    with _create_sap_client(
        base_url=base,
        user=conn.username,
        password=password,
        sap_client=conn.client,
        language=conn.language,
        verify_ssl=conn.verify_ssl,
        saml2_disabled=conn.saml2_disabled,
        endpoint_type="adt",
    ) as client:
        # CSRF token fetch
        csrf_resp = client.get(
            "/sap/bc/adt/discovery",
            headers={"x-csrf-token": "Fetch"},
        )
        csrf_token = csrf_resp.headers.get("x-csrf-token", "")

        base_headers = {
            "Content-Type": "text/plain",
            "Accept": _ADT_ACCEPT,
            "x-csrf-token": csrf_token,
        }
        params_base = {"rowNumber": str(max_rows)}
        resp = None

        # Strategy 1: freestyle endpoint (S/4HANA 1909+)
        try:
            resp = client.post(
                "/sap/bc/adt/datapreview/freestyle",
                content=query_str,
                headers=base_headers,
                params=params_base,
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in _RETRYABLE_CODES:
                raise
            resp = None

        # Strategy 2: ddic with dataSourceName
        if resp is None:
            try:
                resp = client.post(
                    "/sap/bc/adt/datapreview/ddic",
                    content=query_str,
                    headers=base_headers,
                    params={**params_base, "dataSourceName": table_name.upper()},
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in _RETRYABLE_CODES:
                    raise
                resp = None

        # Strategy 3: table-specific path (legacy)
        if resp is None:
            try:
                resp = client.post(
                    f"/sap/bc/adt/datapreview/{table_name.upper()}",
                    content=query_str,
                    headers=base_headers,
                    params=params_base,
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in _RETRYABLE_CODES:
                    raise
                resp = None

        # If all ADT strategies failed, fall back to RFC_READ_TABLE
        if resp is None:
            logger.info(
                "sap.adt.fallback_to_rfc",
                table=table_name,
                note="All ADT datapreview endpoints failed, trying RFC_READ_TABLE via SOAP",
            )
            return call_rfc_read_table(conn, table_name, where=where, max_rows=max_rows)

        # Parse XML response
        raw = parse_datapreview(resp.content)
        results = raw.get("rows", [])
        logger.info("sap.adt.table.fetch", table=table_name, rows=len(results))
        return results


# ---------------------------------------------------------------------------
# SOAP/RFC — call function modules via /sap/bc/soap/rfc
# ---------------------------------------------------------------------------


def call_soap_rfc(
    conn: SAPConnection,
    fm_name: str,
    *,
    imports: dict[str, str] | None = None,
    tables: dict[str, list[dict[str, str]]] | None = None,
) -> dict[str, Any]:
    """Execute an RFC-enabled function module via SOAP over HTTP.

    Adopted from sap-ai-consultant. Calls /sap/bc/soap/rfc which is
    active by default on most S/4HANA systems.

    Returns: dict with 'success', 'exports', 'tables', 'error_message'.
    """
    from app.infra.sap.xml_parser import parse_soap_rfc_response

    imports = imports or {}
    tables = tables or {}
    password = decrypt_password(conn.password_encrypted)
    base = _resolve_base_url(conn)

    # Build SOAP envelope
    soap_body_parts: list[str] = []
    for param_name, param_value in imports.items():
        soap_body_parts.append(f"      <{param_name}>{xml_escape(param_value)}</{param_name}>")

    for table_name, table_rows in tables.items():
        if not table_rows:
            continue
        table_items: list[str] = []
        for row in table_rows:
            fields = "".join(f"<{k}>{xml_escape(v)}</{k}>" for k, v in row.items())
            table_items.append(f"        <item>{fields}</item>")
        soap_body_parts.append(
            f"      <{table_name}>\n" + "\n".join(table_items) + f"\n      </{table_name}>"
        )

    soap_envelope = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"'
        ' xmlns:ns1="urn:sap-com:document:sap:rfc:functions">\n'
        "  <SOAP-ENV:Body>\n"
        f"    <ns1:{fm_name}>\n" + "\n".join(soap_body_parts) + f"\n    </ns1:{fm_name}>\n"
        "  </SOAP-ENV:Body>\n"
        "</SOAP-ENV:Envelope>"
    )

    with httpx.Client(
        verify=conn.verify_ssl,
        timeout=120.0,
        follow_redirects=False,
    ) as client:
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f"urn:sap-com:document:sap:rfc:functions:{fm_name}",
            "sap-client": conn.client,
            "Accept": "text/xml, application/xml",
        }

        resp = client.post(
            f"{base}/sap/bc/soap/rfc",
            content=soap_envelope,
            headers=headers,
            auth=(conn.username, password),
        )

        # CSRF retry on 403
        if resp.status_code == 403:
            csrf_resp = client.get(
                f"{base}/sap/bc/soap/rfc",
                headers={
                    "x-csrf-token": "Fetch",
                    "sap-client": conn.client,
                },
                auth=(conn.username, password),
            )
            csrf_token = csrf_resp.headers.get("x-csrf-token", "")
            headers["x-csrf-token"] = csrf_token
            resp = client.post(
                f"{base}/sap/bc/soap/rfc",
                content=soap_envelope,
                headers=headers,
                auth=(conn.username, password),
            )

        if resp.status_code >= 400:
            return {
                "success": False,
                "exports": [],
                "tables": {},
                "error_message": f"HTTP {resp.status_code}: {resp.text[:500]}",
            }

        parsed = parse_soap_rfc_response(resp.content)
        if parsed.get("error_message"):
            return {
                "success": False,
                "exports": parsed.get("exports", []),
                "tables": parsed.get("tables", {}),
                "error_message": parsed["error_message"],
            }

        return {
            "success": True,
            "exports": parsed.get("exports", []),
            "tables": parsed.get("tables", {}),
            "error_message": "",
        }


def call_rfc_read_table(
    conn: SAPConnection,
    table_name: str,
    *,
    where: str = "",
    max_rows: int = 10000,
    fields: list[str] | None = None,
    delimiter: str = "|",
) -> list[dict[str, Any]]:
    """Read an SAP table via RFC_READ_TABLE over SOAP.

    Falls back method when ADT datapreview is not available.
    """
    imports = {
        "QUERY_TABLE": table_name.upper(),
        "DELIMITER": delimiter,
        "ROWCOUNT": str(max_rows),
    }

    tables_param: dict[str, list[dict[str, str]]] = {}
    if fields:
        tables_param["FIELDS"] = [{"FIELDNAME": f} for f in fields]
    if where:
        # Split long WHERE into 72-char chunks (SAP limitation)
        options = []
        remaining = where
        while remaining:
            chunk = remaining[:72]
            remaining = remaining[72:]
            options.append({"TEXT": chunk})
        tables_param["OPTIONS"] = options

    result = call_soap_rfc(conn, "RFC_READ_TABLE", imports=imports, tables=tables_param)

    if not result["success"]:
        error = result.get("error_message", "RFC_READ_TABLE failed")
        raise RuntimeError(f"RFC_READ_TABLE failed for {table_name}: {error}")

    # Parse the DATA table — rows are delimited strings
    data_rows = result.get("tables", {}).get("DATA", [])
    field_names_raw = result.get("tables", {}).get("FIELDS", [])
    col_names = [f.get("FIELDNAME", "").strip() for f in field_names_raw]

    rows: list[dict[str, Any]] = []
    for data_row in data_rows:
        wa = data_row.get("WA", "")
        values = wa.split(delimiter)
        row_dict: dict[str, Any] = {}
        for i, col in enumerate(col_names):
            row_dict[col] = values[i].strip() if i < len(values) else ""
        rows.append(row_dict)

    logger.info("sap.rfc_read_table", table=table_name, rows=len(rows))
    return rows


def call_bapi(
    conn: SAPConnection,
    bapi_name: str,
    imports: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Call a BAPI via SOAP and return the result.

    Convenience wrapper around call_soap_rfc for BAPI calls like
    BAPI_COSTCENTER_GETLIST, BAPI_PROFITCENTER_GETLIST, etc.
    """
    return call_soap_rfc(conn, bapi_name, imports=imports)


# ---------------------------------------------------------------------------
# Discover available hierarchies / services
# ---------------------------------------------------------------------------


def discover_odata_services(conn: SAPConnection) -> list[dict[str, str]]:
    """List available OData services on the SAP gateway.

    Queries the CATALOGSERVICE to discover what OData services are
    available. Uses v2 first, then falls back to v1.
    """
    password = decrypt_password(conn.password_encrypted)
    base = _resolve_base_url(conn)
    catalog_paths = [
        "/sap/opu/odata/iwfnd/CATALOGSERVICE;v=2/ServiceCollection",
        "/sap/opu/odata/iwfnd/CATALOGSERVICE/ServiceCollection",
    ]

    with httpx.Client(
        verify=conn.verify_ssl,
        timeout=30.0,
        follow_redirects=False,
    ) as client:
        for cat_path in catalog_paths:
            try:
                resp = client.get(
                    f"{base}{cat_path}",
                    headers={
                        "sap-client": conn.client,
                        "Accept": "application/json",
                        "Accept-Language": conn.language,
                    },
                    params={"$format": "json"},
                    auth=(conn.username, password),
                )
                if resp.status_code in (404, 500):
                    continue
                resp.raise_for_status()
                data = resp.json()
                results = data.get("d", {}).get("results", [])
                return [
                    {
                        "title": svc.get("Title", ""),
                        "id": svc.get("ID", ""),
                        "url": svc.get("ServiceUrl", ""),
                        "version": svc.get("ServiceVersion", ""),
                        "description": svc.get("Description", ""),
                    }
                    for svc in results
                ]
            except Exception:  # noqa: S112
                logger.debug("OData catalog path failed", path=cat_path)
                continue

    return []
