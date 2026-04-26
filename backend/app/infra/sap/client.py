"""SAP connection client — OData, ADT, SOAP/RFC protocols.

Adopted from sap-ai-consultant patterns.  Uses the unified HTTP client
factory and connection trial engine for consistent behaviour across all
three protocols.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
import structlog

from app.infra.sap.base import ACCEPT_HEADERS, SSO_KEYWORDS
from app.infra.sap.connection_trial import (
    _create_sap_client,
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
    password = decrypt_password(conn.password_encrypted)
    start = time.monotonic()
    try:
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
    url = f"{conn.base_url.rstrip('/')}/sap/opu/odata/iwfnd/CATALOGSERVICE;v=2/ServiceCollection"
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
    url = f"{conn.base_url.rstrip('/')}/sap/bc/adt/discovery"
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
    url = f"{conn.base_url.rstrip('/')}/sap/bc/soap/rfc"
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
    url = f"{conn.base_url.rstrip('/')}/sap/opu/odata/sap/{entity_set}"
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


def fetch_adt_table(
    conn: SAPConnection,
    table_name: str,
    *,
    max_rows: int = 10000,
    where: str = "",
) -> list[dict[str, Any]]:
    """Read SAP table data via ADT datapreview endpoint.

    Uses POST /sap/bc/adt/datapreview/ddic with XML body specifying
    the table and optional WHERE clause. Results come back as XML
    which we parse into dicts.
    """
    password = decrypt_password(conn.password_encrypted)
    base = conn.base_url.rstrip("/")

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
        # Step 1: CSRF token fetch
        csrf_resp = client.get(
            "/sap/bc/adt/discovery",
            headers={"x-csrf-token": "Fetch"},
        )
        csrf_token = csrf_resp.headers.get("x-csrf-token", "")

        # Step 2: Datapreview query
        url = f"/sap/bc/adt/datapreview/ddic?rowNumber={max_rows}"
        where_element = ""
        if where:
            where_element = f"\n  <dataPreview:whereClause>{where}</dataPreview:whereClause>"
        body = f"""<?xml version="1.0" encoding="utf-8"?>
<dataPreview:tableData xmlns:dataPreview="http://www.sap.com/adt/dataPreview">
  <dataPreview:table>{table_name}</dataPreview:table>
  <dataPreview:maxRows>{max_rows}</dataPreview:maxRows>{where_element}
</dataPreview:tableData>"""

        headers = {
            "Content-Type": "application/xml",
            "Accept": "application/json",
            "x-csrf-token": csrf_token,
        }
        resp = client.post(url, content=body, headers=headers)
        resp.raise_for_status()

        # Parse JSON response
        data = resp.json()
        columns = data.get("columns", [])
        rows_raw = data.get("dataPreview", [])

        results: list[dict[str, Any]] = []
        for row in rows_raw:
            row_dict = {}
            values = row.get("column", [])
            for i, col in enumerate(columns):
                col_name = col.get("name", f"col_{i}")
                row_dict[col_name] = values[i] if i < len(values) else ""
            results.append(row_dict)

        logger.info("sap.adt.table.fetch", table=table_name, rows=len(results))
        return results


# ---------------------------------------------------------------------------
# Discover available hierarchies / services
# ---------------------------------------------------------------------------


def discover_odata_services(conn: SAPConnection) -> list[dict[str, str]]:
    """List available OData services on the SAP gateway.

    Queries the CATALOGSERVICE to discover what OData services are
    available. Uses v2 first, then falls back to v1.
    """
    password = decrypt_password(conn.password_encrypted)
    base = conn.base_url.rstrip("/")
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
