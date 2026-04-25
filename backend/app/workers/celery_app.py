"""Celery application configuration."""

from __future__ import annotations

from celery import Celery

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
        "app.workers.tasks.run_ml_scoring": {"queue": "ml"},
        "app.workers.tasks.run_llm_review": {"queue": "llm"},
        "app.workers.tasks.send_email_batch": {"queue": "email"},
        "app.workers.tasks.sap_pull": {"queue": "odata"},
    },
)

celery.autodiscover_tasks(["app.workers"])
