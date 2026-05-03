"""Connection trial engine — adopted from sap-ai-consultant.

Tests combinations of endpoint × SSL × proxy × SAML2 against a SAP system
to determine the working configuration for each protocol (ADT, OData, SOAP).
Returns a condensed result matrix with recommendations.
"""

from __future__ import annotations

import time

import httpx
import structlog

from app.infra.sap.base import (
    ACCEPT_HEADERS,
    ENDPOINT_PATHS,
    ENDPOINTS,
    ODATA_CATALOG_FALLBACKS,
    SSO_KEYWORDS,
    EndpointRecommendation,
    LoginCheckResult,
    ProbeResult,
    TrialResult,
)
from app.infra.sap.encryption import decrypt_password
from app.models.core import SAPConnection

logger = structlog.get_logger()

PROBE_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=15.0)


def _create_sap_client(
    *,
    base_url: str,
    user: str,
    password: str,
    sap_client: str,
    language: str = "EN",
    verify_ssl: bool = True,
    saml2_disabled: bool = False,
    endpoint_type: str = "default",
    timeout: httpx.Timeout | None = None,
) -> httpx.Client:
    """Create a configured httpx.Client for SAP — sync version.

    Mirrors the sap-ai-consultant create_sap_client() factory but uses
    synchronous httpx since ampliFi backend is sync FastAPI.
    """
    accept = ACCEPT_HEADERS.get(endpoint_type, ACCEPT_HEADERS["default"])
    headers: dict[str, str] = {
        "Accept": accept,
        "Accept-Language": language,
        "sap-client": sap_client,
    }
    params: dict[str, str] = {}
    if saml2_disabled:
        params["saml2"] = "disabled"

    return httpx.Client(
        base_url=base_url.rstrip("/"),
        auth=(user, password),
        verify=verify_ssl,
        headers=headers,
        params=params,
        timeout=timeout or DEFAULT_TIMEOUT,
        follow_redirects=False,
    )


# ---------------------------------------------------------------------------
# Login check (from sap-ai-consultant)
# ---------------------------------------------------------------------------


def check_login(
    base_url: str,
    username: str,
    password: str,
    sap_client: str,
    language: str = "EN",
    verify_ssl: bool = True,
    saml2_disabled: bool = False,
) -> LoginCheckResult:
    """Validate user credentials against SAP system.

    Tries /sap/bc/adt/discovery first (activated on virtually all systems),
    then falls back to /sap/bc/ping.
    """
    login_paths = ["/sap/bc/adt/discovery", "/sap/bc/ping"]
    t0 = time.monotonic()

    try:
        with _create_sap_client(
            base_url=base_url,
            user=username,
            password=password,
            sap_client=sap_client,
            language=language,
            verify_ssl=verify_ssl,
            saml2_disabled=saml2_disabled,
            endpoint_type="default",
            timeout=PROBE_TIMEOUT,
        ) as client:
            sso_seen = False
            sso_location = ""
            forbidden_seen = False
            last_resp: httpx.Response | None = None
            last_path = login_paths[0]

            for path in login_paths:
                last_path = path
                resp = client.get(path)
                last_resp = resp

                if resp.status_code == 200:
                    elapsed = round((time.monotonic() - t0) * 1000)
                    return LoginCheckResult(
                        success=True,
                        status_code=200,
                        latency_ms=elapsed,
                        detail=f"Login successful — user/password accepted ({path})",
                        url=f"{base_url.rstrip('/')}{path}",
                    )

                if resp.status_code == 401:
                    elapsed = round((time.monotonic() - t0) * 1000)
                    return LoginCheckResult(
                        success=False,
                        status_code=401,
                        error="HTTP 401 Unauthorized",
                        latency_ms=elapsed,
                        detail="Invalid credentials — wrong username or password",
                        url=f"{base_url.rstrip('/')}{path}",
                    )

                if resp.status_code == 403:
                    forbidden_seen = True
                    continue

                if resp.status_code in (301, 302, 303, 307, 308):
                    location = resp.headers.get("location", "")
                    if any(kw in location.lower() for kw in SSO_KEYWORDS):
                        sso_seen = True
                        sso_location = location
                    continue

                if resp.status_code == 404:
                    continue

                if resp.status_code == 500:
                    elapsed = round((time.monotonic() - t0) * 1000)
                    return LoginCheckResult(
                        success=True,
                        status_code=500,
                        latency_ms=elapsed,
                        detail=f"Login likely OK (HTTP 500 on {path} — server error, "
                        "but user was authenticated)",
                        url=f"{base_url.rstrip('/')}{path}",
                    )

            elapsed = round((time.monotonic() - t0) * 1000)

            if forbidden_seen:
                return LoginCheckResult(
                    success=True,
                    status_code=403,
                    latency_ms=elapsed,
                    detail="Login OK — credentials accepted (HTTP 403 — "
                    "ICF service may not be activated or user lacks endpoint authorization)",
                    url=f"{base_url.rstrip('/')}{last_path}",
                )

            if sso_seen:
                return LoginCheckResult(
                    success=False,
                    status_code=302,
                    error=f"SSO redirect to {sso_location}",
                    latency_ms=elapsed,
                    detail="System requires SSO — basic auth not accepted",
                    sso_redirect=True,
                    url=f"{base_url.rstrip('/')}{login_paths[0]}",
                )

            if last_resp is not None:
                return LoginCheckResult(
                    success=False,
                    status_code=last_resp.status_code,
                    error=f"HTTP {last_resp.status_code}",
                    latency_ms=elapsed,
                    detail=f"Unexpected response: HTTP {last_resp.status_code} on {last_path}",
                    url=f"{base_url.rstrip('/')}{last_path}",
                )

            return LoginCheckResult(
                success=False,
                error="No response from any login endpoint",
                latency_ms=elapsed,
                detail="Could not reach any login endpoint",
                url=f"{base_url.rstrip('/')}{login_paths[0]}",
            )

    except httpx.ConnectError as exc:
        elapsed = round((time.monotonic() - t0) * 1000)
        return LoginCheckResult(
            success=False,
            error=f"Connection failed: {exc}",
            latency_ms=elapsed,
            detail="Cannot connect to SAP system — check hostname, port, and network",
            url=f"{base_url.rstrip('/')}{login_paths[0]}",
        )
    except Exception as exc:
        elapsed = round((time.monotonic() - t0) * 1000)
        return LoginCheckResult(
            success=False,
            error=str(exc)[:300],
            latency_ms=elapsed,
            detail=f"Login check error: {str(exc)[:100]}",
            url=f"{base_url.rstrip('/')}{login_paths[0]}",
        )


# ---------------------------------------------------------------------------
# Per-endpoint probing (from sap-ai-consultant)
# ---------------------------------------------------------------------------


def _probe_endpoint(
    base_url: str,
    endpoint: str,
    username: str,
    password: str,
    sap_client: str,
    language: str = "EN",
    verify_ssl: bool = True,
    saml2_disabled: bool = False,
    icf_path: str | None = None,
) -> ProbeResult:
    """Probe a single endpoint (ADT, OData, SOAP)."""
    path = icf_path or ENDPOINT_PATHS[endpoint]
    t0 = time.monotonic()

    try:
        with _create_sap_client(
            base_url=base_url,
            user=username,
            password=password,
            sap_client=sap_client,
            language=language,
            verify_ssl=verify_ssl,
            saml2_disabled=saml2_disabled,
            endpoint_type=endpoint,
            timeout=PROBE_TIMEOUT,
        ) as client:
            if endpoint == "adt":
                resp = client.get(path, headers={"x-csrf-token": "Fetch"})
            elif endpoint == "soap":
                resp = client.post(
                    path,
                    content="",
                    headers={
                        "Content-Type": "text/xml; charset=utf-8",
                        "sap-client": sap_client,
                    },
                )
            else:  # odata
                resp = client.get(
                    path,
                    headers={"Accept": "application/json", "x-csrf-token": "Fetch"},
                )
                if resp.status_code in (404, 500):
                    for fallback_path in ODATA_CATALOG_FALLBACKS:
                        fb_resp = client.get(
                            fallback_path,
                            headers={"Accept": "application/json", "x-csrf-token": "Fetch"},
                        )
                        if fb_resp.status_code not in (404, 500):
                            resp = fb_resp
                            path = fallback_path
                            break

            elapsed = round((time.monotonic() - t0) * 1000)

            # Check SSO redirect
            if resp.status_code in (301, 302, 303, 307, 308):
                location = resp.headers.get("location", "")
                if any(kw in location.lower() for kw in SSO_KEYWORDS):
                    return ProbeResult(
                        endpoint=endpoint,
                        success=False,
                        status_code=resp.status_code,
                        error=f"SSO redirect to {location}",
                        latency_ms=elapsed,
                        sso_redirect=True,
                        redirect_location=location,
                        auth_ok=False,
                        auth_detail="SSO redirect — basic auth not accepted",
                        url=str(resp.url),
                    )

            # Authorization validation per endpoint
            auth_ok: bool | None = None
            auth_detail = ""
            csrf_token = ""
            services: list[str] = []

            if endpoint == "adt":
                if resp.status_code == 200:
                    csrf_token = resp.headers.get("x-csrf-token", "")
                    body = resp.text or ""
                    has_csrf = bool(csrf_token and csrf_token.lower() != "required")
                    has_xml = "<app:service" in body or "<atom:" in body
                    if has_csrf and has_xml:
                        auth_ok = True
                        auth_detail = "ADT discovery OK — CSRF token obtained, services listed"
                    elif has_csrf:
                        auth_ok = True
                        auth_detail = "ADT reachable — CSRF token obtained"
                    else:
                        auth_ok = True
                        auth_detail = "ADT reachable (HTTP 200)"
                elif resp.status_code == 403:
                    auth_ok = False
                    auth_detail = "ADT reachable but user lacks S_DEVELOP authorization"
                elif resp.status_code == 401:
                    auth_ok = False
                    auth_detail = "Authentication failed"

            elif endpoint == "odata":
                if resp.status_code == 200:
                    csrf_token = resp.headers.get("x-csrf-token", "")
                    try:
                        data = resp.json()
                        results = data.get("d", {}).get("results", [])
                        services = [svc.get("Title", svc.get("ID", "")) for svc in results[:20]]
                        auth_ok = True
                        auth_detail = f"OData catalog OK — {len(results)} services found"
                    except Exception:
                        auth_ok = True
                        auth_detail = "OData reachable (HTTP 200)"
                elif resp.status_code == 403:
                    auth_ok = False
                    auth_detail = "OData reachable but user lacks S_SERVICE authorization"

            elif endpoint == "soap":
                if resp.status_code in (200, 400, 405, 500):
                    auth_ok = True
                    auth_detail = f"SOAP/RFC endpoint reachable (HTTP {resp.status_code})"
                elif resp.status_code == 401:
                    auth_ok = False
                    auth_detail = "Authentication failed"
                elif resp.status_code == 403:
                    auth_ok = False
                    auth_detail = "SOAP endpoint reachable but user lacks S_RFC authorization"

            success = resp.status_code in (200, 400, 405, 500) or (
                resp.status_code == 403 and endpoint != "adt"
            )
            if endpoint == "adt":
                success = resp.status_code == 200

            return ProbeResult(
                endpoint=endpoint,
                success=success,
                status_code=resp.status_code,
                latency_ms=elapsed,
                detail=auth_detail or f"HTTP {resp.status_code}",
                auth_ok=auth_ok,
                auth_detail=auth_detail,
                url=str(resp.url),
                csrf_token=csrf_token,
                services_discovered=services,
                response_body_preview=resp.text[:300] if resp.text else "",
            )

    except httpx.ConnectError as exc:
        elapsed = round((time.monotonic() - t0) * 1000)
        return ProbeResult(
            endpoint=endpoint,
            success=False,
            error=f"Connection failed: {exc}",
            latency_ms=elapsed,
            detail="Cannot connect — check hostname, port, and network",
            url=f"{base_url.rstrip('/')}{path}",
        )
    except Exception as exc:
        elapsed = round((time.monotonic() - t0) * 1000)
        return ProbeResult(
            endpoint=endpoint,
            success=False,
            error=str(exc)[:300],
            latency_ms=elapsed,
            detail=f"Probe error: {str(exc)[:100]}",
            url=f"{base_url.rstrip('/')}{path}",
        )


# ---------------------------------------------------------------------------
# Full connection trial
# ---------------------------------------------------------------------------


def _resolve_base_url(conn: SAPConnection) -> str:
    """Build the base URL from host/port/protocol or fall back to base_url."""
    if conn.host:
        proto = conn.conn_protocol or "https"
        if conn.port:
            return f"{proto}://{conn.host}:{conn.port}"
        return f"{proto}://{conn.host}"
    return conn.base_url.rstrip("/")


def _resolve_webdisp_url(conn: SAPConnection) -> str | None:
    """Build the Web Dispatcher URL if configured."""
    if not conn.webdisp_host:
        return None
    proto = conn.webdisp_protocol or "https"
    if conn.webdisp_port:
        return f"{proto}://{conn.webdisp_host}:{conn.webdisp_port}"
    return f"{proto}://{conn.webdisp_host}"


def _ep_verify_ssl(conn: SAPConnection, ep: str) -> bool:
    """Resolve per-endpoint verify_ssl (with fallback to global)."""
    override = getattr(conn, f"{ep}_verify_ssl", None)
    return override if override is not None else conn.verify_ssl


def _ep_use_proxy(conn: SAPConnection, ep: str) -> bool:
    """Resolve per-endpoint use_proxy (with fallback to global)."""
    override = getattr(conn, f"{ep}_use_proxy", None)
    return override if override is not None else conn.use_proxy


def _ep_saml2_disabled(conn: SAPConnection, ep: str) -> bool:
    """Resolve per-endpoint saml2_disabled (with fallback to global)."""
    override = getattr(conn, f"{ep}_saml2_disabled", None)
    return override if override is not None else conn.saml2_disabled


def _ep_use_webdisp(conn: SAPConnection, ep: str) -> bool:
    """Resolve per-endpoint use_webdisp (with fallback to global)."""
    override = getattr(conn, f"{ep}_use_webdisp", None)
    return override if override is not None else conn.use_webdisp


def _ep_icf_path(conn: SAPConnection, ep: str) -> str | None:
    """Resolve per-endpoint ICF path override (cert or basic alias)."""
    if not conn.use_icf_aliases:
        return None
    source = getattr(conn, f"{ep}_icf_source", None) or "standard"
    if source == "standard":
        return None
    basic_path: str | None = getattr(conn, f"{ep}_icf_basic", None)
    cert_path: str | None = getattr(conn, f"{ep}_icf_cert", None)
    cert_src = getattr(conn, f"{ep}_cert_source", None) or "none"
    if cert_src in ("global", "local") and cert_path:
        return cert_path
    if basic_path:
        return basic_path
    return None


def run_connection_trial(conn: SAPConnection) -> TrialResult:
    """Run a full connection trial on all 3 endpoints.

    This is the main entry point — mirrors the sap-ai-consultant connection
    trial engine. Tests login, then probes ADT, OData, and SOAP endpoints
    separately with per-endpoint settings (SSL, proxy, SAML2, ICF paths,
    Web Dispatcher routing), and builds recommendations.
    """
    password = decrypt_password(conn.password_encrypted)
    base_url = _resolve_base_url(conn)
    webdisp_url = _resolve_webdisp_url(conn)

    result = TrialResult(
        connection_name=conn.name,
        base_url=base_url,
    )

    # Step 1: Login check (uses global settings)
    result.login_check = check_login(
        base_url=base_url,
        username=conn.username,
        password=password,
        sap_client=conn.client,
        language=conn.language,
        verify_ssl=conn.verify_ssl,
        saml2_disabled=conn.saml2_disabled,
    )

    # Step 2: Probe each endpoint with per-endpoint overrides
    for ep in ENDPOINTS:
        ep_ssl = _ep_verify_ssl(conn, ep)
        ep_saml2 = _ep_saml2_disabled(conn, ep)
        ep_webdisp = _ep_use_webdisp(conn, ep)
        icf_path = _ep_icf_path(conn, ep)

        target_url = webdisp_url if (ep_webdisp and webdisp_url) else base_url

        probe = _probe_endpoint(
            base_url=target_url,
            endpoint=ep,
            username=conn.username,
            password=password,
            sap_client=conn.client,
            language=conn.language,
            verify_ssl=ep_ssl,
            saml2_disabled=ep_saml2,
            icf_path=icf_path,
        )
        result.probes.append(probe)

        # If app-server probe failed and webdisp is available, try the other path
        if not probe.success and webdisp_url and not ep_webdisp:
            wd_probe = _probe_endpoint(
                base_url=webdisp_url,
                endpoint=ep,
                username=conn.username,
                password=password,
                sap_client=conn.client,
                language=conn.language,
                verify_ssl=ep_ssl,
                saml2_disabled=ep_saml2,
                icf_path=icf_path,
            )
            if wd_probe.success:
                wd_probe.detail = f"[WebDisp] {wd_probe.detail}"
                result.probes.append(wd_probe)

    # Step 3: Build recommendations (pick best probe per endpoint)
    for ep in ENDPOINTS:
        ep_probes = [p for p in result.probes if p.endpoint == ep]
        best = next((p for p in ep_probes if p.success), None)
        if best:
            result.recommendations.append(
                EndpointRecommendation(
                    endpoint=ep,
                    reachable=True,
                    verify_ssl=_ep_verify_ssl(conn, ep),
                    use_proxy=_ep_use_proxy(conn, ep),
                    saml2_disabled=_ep_saml2_disabled(conn, ep),
                    note=best.detail,
                )
            )
        else:
            error = ep_probes[0].error if ep_probes else "Not tested"
            result.recommendations.append(
                EndpointRecommendation(
                    endpoint=ep,
                    reachable=False,
                    note=error,
                )
            )

    # Step 4: Proposed config
    working = [r for r in result.recommendations if r.reachable]
    if working:
        preferred = "odata" if any(r.endpoint == "odata" for r in working) else working[0].endpoint
        result.proposed_config = {
            "preferred_protocol": preferred,
            "endpoints_available": [r.endpoint for r in working],
            "verify_ssl": conn.verify_ssl,
            "saml2_disabled": conn.saml2_disabled,
            "use_webdisp": conn.use_webdisp,
        }

    logger.info(
        "sap.connection_trial.complete",
        connection=conn.name,
        login_ok=result.login_check.success if result.login_check else None,
        endpoints_ok=[p.endpoint for p in result.probes if p.success],
    )

    return result
