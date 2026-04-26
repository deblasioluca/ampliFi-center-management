"""SAP connection client — OData, ADT, SOAP/RFC protocols."""

from __future__ import annotations

import time
from typing import Any

import httpx
import structlog

from app.infra.sap.encryption import decrypt_password
from app.models.core import SAPConnection

logger = structlog.get_logger()


def test_connection(conn: SAPConnection) -> dict[str, Any]:
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


def _test_odata(conn: SAPConnection, password: str, start: float) -> dict[str, Any]:
    url = f"{conn.base_url}/sap/opu/odata/sap/API_COSTCENTER_SRV/"
    headers = {"sap-client": conn.client, "Accept": "application/json"}
    with httpx.Client(verify=conn.verify_ssl, timeout=30.0) as client:
        # CSRF fetch
        csrf_resp = client.get(
            url,
            headers={**headers, "x-csrf-token": "Fetch"},
            auth=(conn.username, password),
        )
        elapsed = int((time.monotonic() - start) * 1000)
        if csrf_resp.status_code in (200, 403):
            csrf_token = csrf_resp.headers.get("x-csrf-token", "")
            return {
                "success": True,
                "latency_ms": elapsed,
                "protocol": "odata",
                "csrf_available": bool(csrf_token),
                "status_code": csrf_resp.status_code,
                "services_found": True,
            }
        return {
            "success": False,
            "latency_ms": elapsed,
            "status_code": csrf_resp.status_code,
            "error": csrf_resp.text[:500],
        }


def _test_adt(conn: SAPConnection, password: str, start: float) -> dict[str, Any]:
    url = f"{conn.base_url}/sap/bc/adt/discovery"
    headers = {"sap-client": conn.client, "Accept": "application/xml"}
    with httpx.Client(verify=conn.verify_ssl, timeout=30.0) as client:
        resp = client.get(url, headers=headers, auth=(conn.username, password))
        elapsed = int((time.monotonic() - start) * 1000)
        return {
            "success": resp.status_code == 200,
            "latency_ms": elapsed,
            "protocol": "adt",
            "status_code": resp.status_code,
        }


def _test_soap(conn: SAPConnection, password: str, start: float) -> dict[str, Any]:
    """Test SOAP/RFC connectivity via an HTTP SOAP ping."""
    url = f"{conn.base_url}/sap/bc/srt/wsdl/flv_10002A111AD1/bndg_url/sap/bc/srt/rfc/sap"
    headers = {"sap-client": conn.client, "Content-Type": "text/xml; charset=utf-8"}
    with httpx.Client(verify=conn.verify_ssl, timeout=30.0) as client:
        resp = client.get(url, headers=headers, auth=(conn.username, password))
        elapsed = int((time.monotonic() - start) * 1000)
        return {
            "success": resp.status_code in (200, 401, 403, 500),
            "latency_ms": elapsed,
            "protocol": "soap",
            "status_code": resp.status_code,
            "reachable": resp.status_code != 0,
        }


def fetch_odata(
    conn: SAPConnection,
    entity_set: str,
    params: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    password = decrypt_password(conn.password_encrypted)
    url = f"{conn.base_url}/sap/opu/odata/sap/{entity_set}"
    headers = {"sap-client": conn.client, "Accept": "application/json"}
    all_results: list[dict[str, Any]] = []

    with httpx.Client(verify=conn.verify_ssl, timeout=60.0) as client:
        next_url: str | None = url
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

    logger.info("sap.odata.fetch", entity_set=entity_set, rows=len(all_results))
    return all_results
