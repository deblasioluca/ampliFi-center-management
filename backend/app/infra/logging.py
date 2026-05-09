"""Structured logging configuration (section 18 of spec)."""

from __future__ import annotations

import logging
import sys
import threading
from collections import deque
from datetime import UTC, datetime

import structlog

from app.config import settings

MAX_LOG_ENTRIES = 5000


class _LogEntry:
    __slots__ = ("timestamp", "level", "logger_name", "message", "source")

    def __init__(
        self,
        timestamp: str,
        level: str,
        logger_name: str,
        message: str,
        source: str = "backend",
    ):
        self.timestamp = timestamp
        self.level = level
        self.logger_name = logger_name
        self.message = message
        self.source = source


_log_buffer: deque[_LogEntry] = deque(maxlen=MAX_LOG_ENTRIES)
_lock = threading.Lock()


class RingBufferHandler(logging.Handler):
    """Captures log records into an in-memory ring buffer for the admin UI."""

    def emit(self, record: logging.LogRecord) -> None:
        ts = datetime.fromtimestamp(record.created, tz=UTC).isoformat()
        if ts.endswith("+00:00"):
            ts = ts[:-6] + "Z"
        entry = _LogEntry(
            timestamp=ts,
            level=record.levelname,
            logger_name=record.name,
            message=self.format(record),
        )
        with _lock:
            _log_buffer.append(entry)


def add_client_log(
    level: str,
    message: str,
    logger_name: str = "frontend",
    url: str | None = None,
) -> None:
    """Add a log entry originating from the browser client."""
    ts = datetime.now(tz=UTC).isoformat()
    if ts.endswith("+00:00"):
        ts = ts[:-6] + "Z"
    full_msg = f"[{url}] {message}" if url else message
    entry = _LogEntry(
        timestamp=ts,
        level=level.upper(),
        logger_name=logger_name,
        message=full_msg,
        source="frontend",
    )
    with _lock:
        _log_buffer.append(entry)


def flush_logs() -> int:
    """Clear the in-memory log buffer. Returns the number of entries removed."""
    with _lock:
        count = len(_log_buffer)
        _log_buffer.clear()
    return count


def get_recent_logs(
    limit: int = 200,
    level: str | None = None,
    since: str | None = None,
    search: str | None = None,
    source: str | None = None,
) -> list[dict]:
    """Return recent log entries, newest first."""
    with _lock:
        entries = list(_log_buffer)
    entries.reverse()

    if level:
        level_upper = level.upper()
        entries = [e for e in entries if e.level == level_upper]
    if source:
        entries = [e for e in entries if e.source == source]
    if since:
        since_normalized = since.replace("Z", "+00:00") if since.endswith("Z") else since
        try:
            since_dt = datetime.fromisoformat(since_normalized)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=UTC)
        except ValueError:
            since_dt = None
        if since_dt:
            entries = [
                e
                for e in entries
                if datetime.fromisoformat(e.timestamp.replace("Z", "+00:00")) >= since_dt
            ]
    if search:
        search_lower = search.lower()
        entries = [e for e in entries if search_lower in e.message.lower()]

    return [
        {
            "timestamp": e.timestamp,
            "level": e.level,
            "logger": e.logger_name,
            "message": e.message,
            "source": e.source,
        }
        for e in entries[:limit]
    ]


SENSITIVE_KEYS = frozenset(
    {
        "password",
        "api_key",
        "client_secret",
        "authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "csrf_token",
        "secret_key",
        "access_token",
        "refresh_token",
    }
)


def _redact_sensitive(_logger: object, _method: str, event_dict: dict) -> dict:
    for key in list(event_dict.keys()):
        if key.lower() in SENSITIVE_KEYS:
            event_dict[key] = "***"
    return event_dict


def setup_logging() -> None:
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _redact_sensitive,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.app_env == "dev":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    ring_handler = RingBufferHandler()
    ring_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.addHandler(ring_handler)
    root.setLevel(logging.DEBUG if settings.debug else logging.INFO)

    for name in ("uvicorn", "uvicorn.access", "sqlalchemy.engine"):
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).propagate = True

    # Silence verbose multipart parser debug output
    logging.getLogger("multipart").setLevel(logging.WARNING)
    logging.getLogger("python_multipart").setLevel(logging.WARNING)
