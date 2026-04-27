"""SAP Datasphere / HANA Cloud client.

Provides:
- Connection management via hdbcli (SAP HANA client) or fallback HTTP SQL API
- DDL generation for HANA column-store tables
- Read/write routing for domain data
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class DatasphereClient:
    """Client for SAP Datasphere (HANA Cloud) database operations."""

    def __init__(
        self,
        url: str,
        schema: str,
        user: str,
        password: str,
        use_ssl: bool = True,
    ) -> None:
        self.url = url
        self.schema = schema
        self.user = user
        self.password = password
        self.use_ssl = use_ssl
        self._conn: Any = None

    def _parse_host_port(self) -> tuple[str, int]:
        """Extract host and port from URL like 'host:port' or 'https://host:port'."""
        url = self.url
        if "://" in url:
            url = url.split("://", 1)[1]
        url = url.split("/", 1)[0]  # strip path
        if ":" in url:
            host, port_str = url.rsplit(":", 1)
            return host, int(port_str)
        return url, 443

    def connect(self) -> Any:
        """Establish HANA connection. Uses hdbcli if available, else raises."""
        try:
            from hdbcli import dbapi  # type: ignore[import-untyped]
        except ImportError as exc:
            msg = (
                "hdbcli not installed. Install with: pip install hdbcli  (requires SAP HANA Client)"
            )
            raise ImportError(msg) from exc

        host, port = self._parse_host_port()
        self._conn = dbapi.connect(
            address=host,
            port=port,
            user=self.user,
            password=self.password,
            encrypt=self.use_ssl,
            sslValidateCertificate=False,
        )
        return self._conn

    def test_connection(self) -> dict[str, Any]:
        """Test the Datasphere connection and return status info."""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_SCHEMA, CURRENT_USER FROM DUMMY")
            row = cursor.fetchone()
            return {
                "success": True,
                "schema": row[0] if row else None,
                "user": row[1] if row else None,
                "message": "Connected to HANA Cloud successfully",
            }
        except ImportError:
            return {
                "success": False,
                "message": "hdbcli not installed (pip install hdbcli)",
            }
        except Exception as exc:
            return {
                "success": False,
                "message": f"Connection failed: {exc}",
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            self._conn = None

    def execute_ddl(self, ddl: str) -> dict[str, Any]:
        """Execute DDL statement(s) on the Datasphere schema."""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            for stmt in ddl.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cursor.execute(stmt)
            conn.commit()
            return {"success": True, "message": "DDL executed successfully"}
        except Exception as exc:
            return {"success": False, "message": f"DDL execution failed: {exc}"}
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            self._conn = None

    def close(self) -> None:
        if self._conn:
            import contextlib

            with contextlib.suppress(Exception):
                self._conn.close()
            self._conn = None
