"""Celery task definitions."""

from __future__ import annotations

import structlog

from app.workers.celery_app import celery

logger = structlog.get_logger()


@celery.task(name="app.workers.tasks.run_analysis", bind=True)
def run_analysis(self, run_id: int) -> dict:  # type: ignore[no-untyped-def]
    logger.info("task.run_analysis.start", run_id=run_id, task_id=self.request.id)
    # Placeholder — will orchestrate decision tree + ML + LLM
    return {"run_id": run_id, "status": "completed"}


@celery.task(name="app.workers.tasks.run_ml_scoring", bind=True)
def run_ml_scoring(self, run_id: int, model_name: str) -> dict:  # type: ignore[no-untyped-def]
    logger.info("task.run_ml_scoring.start", run_id=run_id, model=model_name)
    return {"run_id": run_id, "model": model_name, "status": "completed"}


@celery.task(name="app.workers.tasks.run_llm_review", bind=True)
def run_llm_review(self, run_id: int, mode: str = "SINGLE") -> dict:  # type: ignore[no-untyped-def]
    logger.info("task.run_llm_review.start", run_id=run_id, mode=mode)
    return {"run_id": run_id, "mode": mode, "status": "completed"}


@celery.task(name="app.workers.tasks.send_email_batch", bind=True)
def send_email_batch(self, template: str, recipients: list[str]) -> dict:  # type: ignore[no-untyped-def]
    logger.info("task.send_email_batch.start", template=template, count=len(recipients))
    return {"template": template, "sent": len(recipients)}


@celery.task(name="app.workers.tasks.sap_pull", bind=True)
def sap_pull(self, connection_id: int, binding_id: int) -> dict:  # type: ignore[no-untyped-def]
    logger.info("task.sap_pull.start", connection_id=connection_id, binding_id=binding_id)
    return {"connection_id": connection_id, "binding_id": binding_id, "status": "completed"}
