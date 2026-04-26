"""SAP protocol abstraction — adopted from sap-ai-consultant patterns.

Defines the common SAPProtocol interface so domain code never cares which
wire protocol (OData, ADT, SOAP/RFC) is used.  Connection configuration
is protocol-aware (per-endpoint settings for SSL, proxy, SAML2).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ProbeResult:
    """Result of probing a single SAP endpoint."""

    endpoint: str  # adt | odata | soap
    success: bool = False
    status_code: int = 0
    latency_ms: int = 0
    error: str = ""
    detail: str = ""
    sso_redirect: bool = False
    redirect_location: str = ""
    auth_ok: bool | None = None
    auth_detail: str = ""
    url: str = ""
    response_headers: dict[str, str] = field(default_factory=dict)
    response_body_preview: str = ""
    csrf_token: str = ""
    services_discovered: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "success": self.success,
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "detail": self.detail,
            "sso_redirect": self.sso_redirect,
            "redirect_location": self.redirect_location,
            "auth_ok": self.auth_ok,
            "auth_detail": self.auth_detail,
            "url": self.url,
            "csrf_token": bool(self.csrf_token),
            "services_discovered": self.services_discovered,
        }


@dataclass
class LoginCheckResult:
    """Result of a basic login / credential validation."""

    success: bool = False
    status_code: int = 0
    error: str = ""
    latency_ms: int = 0
    detail: str = ""
    sso_redirect: bool = False
    url: str = ""
    response_headers: dict[str, str] = field(default_factory=dict)
    response_body_preview: str = ""


@dataclass
class EndpointRecommendation:
    """Recommended settings for a single endpoint."""

    endpoint: str
    reachable: bool
    verify_ssl: bool = True
    use_proxy: bool = False
    saml2_disabled: bool = False
    protocol: str = "https"
    note: str = ""


@dataclass
class TrialResult:
    """Full connection trial result for an SAP system."""

    connection_name: str
    base_url: str
    login_check: LoginCheckResult | None = None
    probes: list[ProbeResult] = field(default_factory=list)
    recommendations: list[EndpointRecommendation] = field(default_factory=list)
    proposed_config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "connection_name": self.connection_name,
            "base_url": self.base_url,
            "login_check": {
                "success": self.login_check.success,
                "status_code": self.login_check.status_code,
                "error": self.login_check.error,
                "latency_ms": self.login_check.latency_ms,
                "detail": self.login_check.detail,
                "sso_redirect": self.login_check.sso_redirect,
            }
            if self.login_check
            else None,
            "probes": [p.to_dict() for p in self.probes],
            "recommendations": [
                {
                    "endpoint": r.endpoint,
                    "reachable": r.reachable,
                    "verify_ssl": r.verify_ssl,
                    "use_proxy": r.use_proxy,
                    "saml2_disabled": r.saml2_disabled,
                    "note": r.note,
                }
                for r in self.recommendations
            ],
            "proposed_config": self.proposed_config,
        }


# ---------------------------------------------------------------------------
# Endpoint constants (from sap-ai-consultant)
# ---------------------------------------------------------------------------

ENDPOINTS = ("adt", "soap", "odata")

ENDPOINT_PATHS: dict[str, str] = {
    "adt": "/sap/bc/adt/discovery",
    "soap": "/sap/bc/soap/rfc",
    "odata": "/sap/opu/odata/iwfnd/CATALOGSERVICE;v=2/ServiceCollection",
}

ODATA_CATALOG_FALLBACKS: list[str] = [
    "/sap/opu/odata/iwfnd/CATALOGSERVICE/ServiceCollection",
    "/sap/opu/odata/sap/",
]

ENDPOINT_LABELS: dict[str, str] = {
    "adt": "ADT (/sap/bc/adt/)",
    "soap": "SOAP/RFC (/sap/bc/soap/rfc)",
    "odata": "OData (/sap/opu/odata/iwfnd/CATALOGSERVICE)",
}

# Accept headers per endpoint type (from sap-ai-consultant http_client)
ACCEPT_HEADERS: dict[str, str] = {
    "adt": "application/xml, application/atom+xml, application/atomsvc+xml, text/xml, */*",
    "soap": "text/xml, */*",
    "odata": "application/json",
    "default": "application/xml, application/atom+xml, text/xml, */*",
}

# SSO detection keywords
SSO_KEYWORDS = ("login", "sso", "saml", "logon")


# ---------------------------------------------------------------------------
# Protocol interface
# ---------------------------------------------------------------------------


@runtime_checkable
class SAPProtocol(Protocol):
    """Interface that every SAP protocol adapter must implement."""

    name: str  # 'odata' | 'adt' | 'soap_rfc'

    def test(self, conn: Any) -> ProbeResult:
        """Run a connectivity probe."""
        ...

    def discover(self, conn: Any, what: str, params: dict) -> list[dict]:
        """Discover available objects (hierarchies, services, tables)."""
        ...

    def pull(self, conn: Any, binding: Any) -> Iterable[dict]:
        """Extract rows from SAP via this protocol."""
        ...
