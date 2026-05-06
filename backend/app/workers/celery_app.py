"""Celery application configuration.

Includes a beat schedule for monthly housekeeping cycles and weekly review
reminders. Beat is started separately:

    celery -A app.workers.celery_app beat -l info

The schedule is defined in code (vs DB-driven) so it deploys with the
codebase. Operators can override individual schedules via environment
variables (HOUSEKEEPING_CRON_*).
"""

from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab

from app.config import settings

celery = Celery(
    "amplifi_cleanup",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_routes={
        "app.workers.tasks.run_analysis": {"queue": "default"},
        "app.workers.tasks.run_v2_analysis": {"queue": "default"},
        "app.workers.tasks.run_ml_scoring": {"queue": "ml"},
        "app.workers.tasks.run_llm_review": {"queue": "llm"},
        "app.workers.tasks.send_email_batch": {"queue": "email"},
        "app.workers.tasks.sap_pull": {"queue": "odata"},
        "app.workers.tasks.housekeeping_monthly": {"queue": "default"},
        "app.workers.tasks.housekeeping_send_reminders": {"queue": "email"},
        "app.workers.tasks.review_send_reminders": {"queue": "email"},
    },
)


def _crontab_from_env(prefix: str, default: dict) -> crontab:
    """Build a crontab from env vars, falling back to defaults.

    e.g. HOUSEKEEPING_CRON_MONTHLY_HOUR=3, HOUSEKEEPING_CRON_MONTHLY_DAY=1
    """
    return crontab(
        minute=os.environ.get(f"{prefix}_MINUTE", default.get("minute", "0")),
        hour=os.environ.get(f"{prefix}_HOUR", default.get("hour", "3")),
        day_of_month=os.environ.get(f"{prefix}_DAY", default.get("day_of_month", "*")),
        day_of_week=os.environ.get(f"{prefix}_DAY_OF_WEEK", default.get("day_of_week", "*")),
        month_of_year=os.environ.get(f"{prefix}_MONTH", default.get("month_of_year", "*")),
    )


# ── Beat schedule ────────────────────────────────────────────────────────
# Default: housekeeping cycle on the 1st of every month at 03:00 UTC.
# Reminder ticks run daily — the task itself decides which cycles need
# reminders this run based on cycle.config.reminder_days.

celery.conf.beat_schedule = {
    "housekeeping-monthly": {
        "task": "app.workers.tasks.housekeeping_monthly",
        "schedule": _crontab_from_env(
            "HOUSEKEEPING_CRON_MONTHLY",
            {"hour": "3", "minute": "0", "day_of_month": "1"},
        ),
    },
    "housekeeping-reminders-daily": {
        "task": "app.workers.tasks.housekeeping_send_reminders",
        "schedule": _crontab_from_env(
            "HOUSEKEEPING_CRON_REMINDERS",
            {"hour": "9", "minute": "0"},
        ),
    },
    "review-reminders-daily": {
        "task": "app.workers.tasks.review_send_reminders",
        "schedule": _crontab_from_env(
            "REVIEW_CRON_REMINDERS",
            {"hour": "9", "minute": "30"},
        ),
    },
}

celery.autodiscover_tasks(["app.workers"])
