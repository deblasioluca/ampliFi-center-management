"""FastAPI application entry point."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.infra.logging import setup_logging

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    setup_logging()
    logger.info("amplifi_cleanup.starting", env=settings.app_env)
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.app_env == "dev" else [],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    except Exception as exc:
        checks["redis"] = f"error: {exc}"

    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        content={"status": "ready" if all_ok else "degraded", "checks": checks},
        status_code=200 if all_ok else 503,
    )


# --- API routers ---

from app.api import admin as admin_router  # noqa: E402
from app.api import auth as auth_router  # noqa: E402
from app.api import chat as chat_router  # noqa: E402
from app.api import configs as configs_router  # noqa: E402
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
