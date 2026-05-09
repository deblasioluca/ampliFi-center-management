"""FastAPI application entry point."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.infra.logging import setup_logging

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    setup_logging()
    logger.info("amplifi_cleanup.starting", env=settings.app_env)

    # Boot routine registry
    from app.domain.decision_tree.registry import boot_registry

    boot_registry()

    yield
    logger.info("amplifi_cleanup.stopping")


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

_cors_origins: list[str] = (
    ["*"]
    if settings.app_env == "dev"
    else [o.strip() for o in settings.cors_allowed_origins.split(",") if o.strip()]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In proxy TLS mode, trust X-Forwarded-* headers from the reverse proxy
if settings.tls_mode.lower() == "proxy":
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")


@app.middleware("http")
async def request_id_middleware(request: Request, call_next) -> Response:  # type: ignore[no-untyped-def]
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)
    response: Response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# --- health endpoints ---


@app.get("/api/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/readyz")
async def readyz() -> Response:
    checks: dict[str, str] = {}
    try:
        from sqlalchemy import text as sa_text

        from app.infra.db.session import engine as db_engine

        with db_engine.connect() as conn:
            conn.execute(sa_text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as exc:
        checks["db"] = f"error: {exc}"
    try:
        import redis as redis_lib

        r = redis_lib.from_url(settings.redis_url)
        r.ping()
        r.close()
        checks["redis"] = "ok"
    except Exception:
        checks["redis"] = "unavailable (optional)"

    # Redis is optional — only DB is required for healthy status
    all_ok = checks.get("db") == "ok"
    return JSONResponse(
        content={"status": "ready" if all_ok else "degraded", "checks": checks},
        status_code=200 if all_ok else 503,
    )


# --- API routers ---

from app.api import admin as admin_router  # noqa: E402
from app.api import auth as auth_router  # noqa: E402
from app.api import chat as chat_router  # noqa: E402
from app.api import configs as configs_router  # noqa: E402
from app.api import data_management as data_mgmt_router  # noqa: E402
from app.api import housekeeping as housekeeping_router  # noqa: E402
from app.api import reference as reference_router  # noqa: E402
from app.api import review as review_router  # noqa: E402
from app.api import runs as runs_router  # noqa: E402
from app.api import stats as stats_router  # noqa: E402
from app.api import waves as waves_router  # noqa: E402

app.include_router(auth_router.router, prefix="/api/auth", tags=["auth"])
app.include_router(admin_router.router, prefix="/api/admin", tags=["admin"])
app.include_router(waves_router.router, prefix="/api/waves", tags=["waves"])
app.include_router(configs_router.router, prefix="/api/configs", tags=["configs"])
app.include_router(runs_router.router, prefix="/api/runs", tags=["runs"])
app.include_router(review_router.router, prefix="/api/review", tags=["review"])
app.include_router(reference_router.router, prefix="/api", tags=["reference"])
app.include_router(stats_router.router, prefix="/api/stats", tags=["stats"])
app.include_router(chat_router.router, prefix="/api/chat", tags=["chat"])
app.include_router(housekeeping_router.router, prefix="/api", tags=["housekeeping"])
app.include_router(data_mgmt_router.router, prefix="/api/data", tags=["data-management"])

from app.api import activity as activity_router  # noqa: E402
from app.api import docs_help as docs_help_router  # noqa: E402
from app.api import explore as explore_router  # noqa: E402

app.include_router(activity_router.router, prefix="/api/activity", tags=["activity"])
app.include_router(docs_help_router.router, prefix="/api", tags=["help"])
app.include_router(explore_router.router, prefix="/api/explore", tags=["explore"])

from app.api import dq as dq_router  # noqa: E402

app.include_router(dq_router.router, prefix="/api/dq", tags=["data-quality"])


# --- Prometheus metrics ---


@app.get("/api/metrics")
async def prometheus_metrics() -> Response:
    """Prometheus-compatible metrics endpoint (§18)."""

    from sqlalchemy import text as sa_text

    from app.infra.db.session import engine as db_engine

    lines: list[str] = []
    lines.append("# HELP amplifi_up Application is running")
    lines.append("# TYPE amplifi_up gauge")
    lines.append("amplifi_up 1")

    try:
        with db_engine.connect() as conn:
            # Wave counts by status
            rows = conn.execute(
                sa_text("SELECT status, COUNT(*) FROM cleanup.wave GROUP BY status")
            ).all()
            lines.append("# HELP amplifi_waves_total Number of waves by status")
            lines.append("# TYPE amplifi_waves_total gauge")
            for status, count in rows:
                lines.append(f'amplifi_waves_total{{status="{status}"}} {count}')

            # Cost center count
            cc_count = (
                conn.execute(sa_text("SELECT COUNT(*) FROM cleanup.legacy_cost_center")).scalar()
                or 0
            )
            lines.append("# HELP amplifi_cost_centers_total Total cost centers loaded")
            lines.append("# TYPE amplifi_cost_centers_total gauge")
            lines.append(f"amplifi_cost_centers_total {cc_count}")

            # Balance count
            bal_count = conn.execute(sa_text("SELECT COUNT(*) FROM cleanup.balance")).scalar() or 0
            lines.append("# HELP amplifi_balances_total Total balance records")
            lines.append("# TYPE amplifi_balances_total gauge")
            lines.append(f"amplifi_balances_total {bal_count}")

            # Analysis run counts
            run_rows = conn.execute(
                sa_text("SELECT status, COUNT(*) FROM cleanup.analysis_run GROUP BY status")
            ).all()
            lines.append("# HELP amplifi_analysis_runs_total Analysis runs by status")
            lines.append("# TYPE amplifi_analysis_runs_total gauge")
            for status, count in run_rows:
                lines.append(f'amplifi_analysis_runs_total{{status="{status}"}} {count}')

            # User count
            user_count = (
                conn.execute(
                    sa_text("SELECT COUNT(*) FROM cleanup.app_user WHERE is_active = true")
                ).scalar()
                or 0
            )
            lines.append("# HELP amplifi_active_users Total active users")
            lines.append("# TYPE amplifi_active_users gauge")
            lines.append(f"amplifi_active_users {user_count}")
    except Exception:
        logger.debug("metrics.db_query_failed")

    return Response(content="\n".join(lines) + "\n", media_type="text/plain")


# --- Serve static frontend build (if exists) ---
# Mount AFTER all API routes so /api/* takes priority
_frontend_dist = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"
if _frontend_dist.is_dir():
    from fastapi.responses import HTMLResponse

    # SPA-style catch-all for /review/{token} — serves the review index page
    _review_html = _frontend_dist / "review" / "index.html"
    if _review_html.is_file():
        _review_content = _review_html.read_text()

        @app.get("/review/{token:path}", response_class=HTMLResponse)
        async def _review_catchall(token: str) -> HTMLResponse:
            return HTMLResponse(content=_review_content)

    app.mount("/", StaticFiles(directory=str(_frontend_dist), html=True), name="frontend")
