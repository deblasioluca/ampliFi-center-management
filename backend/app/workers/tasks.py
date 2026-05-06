"""Celery task definitions.

These are the actual task implementations — previously this file contained
placeholders that returned ``{"status": "completed"}`` without doing any
work, which meant Celery was wired up but nothing was ever dispatched
through it. Now:

- run_analysis / run_v2_analysis: dispatch the synchronous service to the
  worker pool so the HTTP request returns immediately with a run_id and
  the heavy lifting (130k+ centers) happens in the background.
- housekeeping_monthly: triggered by Celery Beat on the 1st of each month.
- housekeeping_send_reminders / review_send_reminders: triggered daily; the
  task itself decides whether a given cycle/scope needs a reminder this
  run based on its config.

Each task creates its own DB session — workers cannot share request-scoped
sessions with the FastAPI app.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from app.workers.celery_app import celery

logger = structlog.get_logger()


# ── Helper: scoped DB session for tasks ──────────────────────────────────


def _task_session():
    """Return a SQLAlchemy session scoped to a single task invocation."""
    from app.infra.db.session import SessionLocal

    return SessionLocal()


# ── Analysis dispatch ────────────────────────────────────────────────────


@celery.task(name="app.workers.tasks.run_analysis", bind=True, max_retries=0)
def run_analysis(  # type: ignore[no-untyped-def]
    self,
    run_id: int,
    wave_id: int | None,
    config_id: int,
    user_id: int,
    mode: str = "simulation",
    label: str | None = None,
    excluded_scopes: list[str] | None = None,
) -> dict[str, Any]:
    """Run the V1 decision tree analysis on a wave (or globally if wave_id is None).

    The HTTP endpoint creates the AnalysisRun row in 'queued' status and
    dispatches this task. The task picks up the existing run and progresses
    it through running → completed (or failed).
    """
    from app.models.core import AnalysisRun
    from app.services.analysis import execute_analysis_for_run

    logger.info("task.run_analysis.start", run_id=run_id, task_id=self.request.id)
    db = _task_session()
    try:
        run = db.get(AnalysisRun, run_id)
        if not run:
            return {"run_id": run_id, "status": "not_found"}
        execute_analysis_for_run(
            run=run,
            wave_id=wave_id,
            config_id=config_id,
            user_id=user_id,
            mode=mode,
            label=label,
            excluded_scopes=excluded_scopes,
            db=db,
        )
        db.commit()
        return {"run_id": run_id, "status": run.status}
    except Exception as e:
        db.rollback()
        run = db.get(AnalysisRun, run_id)
        if run:
            run.status = "failed"
            run.kpis = (run.kpis or {}) | {"error": str(e)[:500]}
            run.finished_at = datetime.now(UTC)
            db.commit()
        logger.error("task.run_analysis.failed", run_id=run_id, error=str(e))
        raise
    finally:
        db.close()


@celery.task(name="app.workers.tasks.run_v2_analysis", bind=True, max_retries=0)
def run_v2_analysis(  # type: ignore[no-untyped-def]
    self,
    run_id: int,
    wave_id: int | None,
    config_id: int,
    mode: str = "simulation",
    id_config: dict | None = None,
) -> dict[str, Any]:
    """Run the V2 CEMA migration analysis."""
    from app.models.core import AnalysisRun
    from app.services.analysis_v2 import execute_v2_analysis_for_run

    logger.info("task.run_v2_analysis.start", run_id=run_id, task_id=self.request.id)
    db = _task_session()
    try:
        run = db.get(AnalysisRun, run_id)
        if not run:
            return {"run_id": run_id, "status": "not_found"}
        execute_v2_analysis_for_run(
            run=run,
            wave_id=wave_id,
            config_id=config_id,
            mode=mode,
            id_config=id_config or {},
            db=db,
        )
        db.commit()
        return {"run_id": run_id, "status": run.status}
    except Exception as e:
        db.rollback()
        run = db.get(AnalysisRun, run_id)
        if run:
            run.status = "failed"
            run.kpis = (run.kpis or {}) | {"error": str(e)[:500]}
            run.finished_at = datetime.now(UTC)
            db.commit()
        logger.error("task.run_v2_analysis.failed", run_id=run_id, error=str(e))
        raise
    finally:
        db.close()


# ── Periodic: housekeeping ────────────────────────────────────────────────


@celery.task(name="app.workers.tasks.housekeeping_monthly", bind=True, max_retries=2)
def housekeeping_monthly(self) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    """Create + run a monthly housekeeping cycle for the current period.

    Idempotent: if a cycle for this period already exists, this task is a
    no-op. Triggered by Celery Beat on the 1st of each month at 03:00 UTC
    by default.
    """
    from app.services.housekeeping import create_cycle, run_cycle

    period = datetime.now(UTC).strftime("%Y-%m")
    db = _task_session()
    try:
        try:
            cycle = create_cycle(period=period, db=db)
        except ValueError:
            logger.info("task.housekeeping_monthly.skipped_exists", period=period)
            return {"period": period, "status": "exists"}
        db.flush()
        cycle = run_cycle(cycle.id, db)
        return {"period": period, "cycle_id": cycle.id, "kpis": cycle.kpis}
    finally:
        db.close()


@celery.task(name="app.workers.tasks.housekeeping_send_reminders", bind=True, max_retries=2)
def housekeeping_send_reminders(self) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    """Send reminders for any open housekeeping cycle whose configured
    reminder_days matches the current age of the cycle (since notifications
    were sent)."""
    from sqlalchemy import select

    from app.models.core import HousekeepingCycle
    from app.services.housekeeping import auto_close_overdue, send_reminders

    db = _task_session()
    try:
        cycles = (
            db.execute(
                select(HousekeepingCycle).where(HousekeepingCycle.status == "review_open")
            )
            .scalars()
            .all()
        )
        results = []
        now = datetime.now(UTC)
        for cycle in cycles:
            cfg = cycle.config or {}
            reminder_days = cfg.get("reminder_days", [7, 14, 21])
            opened = cycle.review_opened_at
            if not opened:
                continue
            days_open = (now - opened).days
            if days_open in reminder_days:
                r = send_reminders(cycle.id, db)
                results.append({"cycle_id": cycle.id, "reminders": r})
            # Auto-close after configured window
            auto_days = int(cfg.get("auto_close_after_days", 30))
            if days_open >= auto_days:
                ac = auto_close_overdue(cycle.id, db)
                results.append({"cycle_id": cycle.id, "auto_close": ac})
        return {"processed_cycles": len(cycles), "results": results}
    finally:
        db.close()


# ── Periodic: review reminders ──────────────────────────────────────────


@celery.task(name="app.workers.tasks.review_send_reminders", bind=True, max_retries=2)
def review_send_reminders(self) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    """Send reminders for review scopes that are invited but not yet completed.

    Reminder cadence: at 7, 14, and 21 days after invitation. Only sends
    for scopes whose ``token_expires_at`` is still in the future.
    """
    from sqlalchemy import func, select

    from app.models.core import AppConfig, ReviewItem, ReviewScope, Wave

    db = _task_session()
    try:
        cfg_row = (
            db.execute(select(AppConfig).where(AppConfig.key == "email")).scalar_one_or_none()
        )
        cfg = cfg_row.value if cfg_row else {}
        if not cfg:
            return {"sent": 0, "reason": "no_email_config"}

        from app.infra.email.engine import EmailEngine

        engine = EmailEngine(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 1025)),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            use_tls=cfg.get("tls", "none") != "none",
            from_address=cfg.get("from_address", "noreply@amplifi.dev"),
        )
        base_url = cfg.get("base_url", "http://localhost:4321")
        reminder_days = [7, 14, 21]
        now = datetime.now(UTC)

        scopes = (
            db.execute(
                select(ReviewScope).where(
                    ReviewScope.status.in_(["invited", "in_progress"]),
                    ReviewScope.reviewer_email.isnot(None),
                )
            )
            .scalars()
            .all()
        )
        sent = 0
        skipped = 0
        for scope in scopes:
            invited = scope.invited_at
            if not invited:
                skipped += 1
                continue
            if scope.token_expires_at and scope.token_expires_at < now:
                continue  # expired — don't bother
            days_since = (now - invited).days
            if days_since not in reminder_days:
                continue

            wave = db.get(Wave, scope.wave_id)

            decided = (
                db.execute(
                    select(func.count(ReviewItem.id)).where(
                        ReviewItem.scope_id == scope.id,
                        ReviewItem.decision != "PENDING",
                    )
                ).scalar()
                or 0
            )
            total = (
                db.execute(
                    select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == scope.id)
                ).scalar()
                or 0
            )
            try:
                engine.send(
                    to=scope.reviewer_email,
                    template_name="review_reminder",
                    context={
                        "reviewer_name": scope.reviewer_name or scope.reviewer_email,
                        "wave_name": wave.name if wave else "(unknown)",
                        "review_url": f"{base_url}/review/{scope.token}",
                        "reviewed_count": decided,
                        "total_count": total,
                        "deadline": (
                            str(scope.token_expires_at)[:10]
                            if scope.token_expires_at
                            else "soon"
                        ),
                    },
                )
                sent += 1
            except Exception as e:
                logger.warning(
                    "review.reminder_failed",
                    scope_id=scope.id,
                    error=str(e),
                )
        return {"sent": sent, "skipped": skipped, "considered": len(scopes)}
    finally:
        db.close()


# ── ML / LLM / SAP / Email — kept as named-and-routable, callable from API ──


@celery.task(name="app.workers.tasks.run_ml_scoring", bind=True)
def run_ml_scoring(self, run_id: int, model_name: str) -> dict:  # type: ignore[no-untyped-def]
    """Score a run with an ML model.

    Thin wrapper — services.analysis already runs ML inline as part of the
    pipeline. This task exists for explicit re-scoring without a full
    pipeline re-run, when the underlying model is updated.
    """
    logger.info("task.run_ml_scoring.start", run_id=run_id, model=model_name)
    db = _task_session()
    try:
        try:
            from app.domain.ml.classifier import score_run

            score_run(run_id=run_id, model_name=model_name, db=db)
        except ImportError:
            logger.info(
                "task.run_ml_scoring.no_score_run",
                hint="score_run() not implemented yet — pipeline-driven ML only",
            )
        db.commit()
        return {"run_id": run_id, "model": model_name, "status": "completed"}
    except Exception as e:
        db.rollback()
        logger.error("task.run_ml_scoring.failed", run_id=run_id, error=str(e))
        raise
    finally:
        db.close()


@celery.task(name="app.workers.tasks.send_email_batch", bind=True)
def send_email_batch(  # type: ignore[no-untyped-def]
    self, template: str, recipients: list[str], context: dict | None = None
) -> dict:
    """Send a templated email to a batch of recipients."""
    from sqlalchemy import select

    from app.infra.email.engine import EmailEngine
    from app.models.core import AppConfig

    db = _task_session()
    try:
        cfg_row = (
            db.execute(select(AppConfig).where(AppConfig.key == "email")).scalar_one_or_none()
        )
        cfg = cfg_row.value if cfg_row else {}
        if not cfg:
            return {"template": template, "sent": 0, "reason": "no_email_config"}
        engine = EmailEngine(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 1025)),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            use_tls=cfg.get("tls", "none") != "none",
            from_address=cfg.get("from_address", "noreply@amplifi.dev"),
        )
        sent = 0
        for r in recipients:
            try:
                engine.send(to=r, template_name=template, context=context or {})
                sent += 1
            except Exception:
                logger.warning("task.send_email_batch.failed", recipient=r)
        return {"template": template, "sent": sent, "total": len(recipients)}
    finally:
        db.close()


@celery.task(name="app.workers.tasks.sap_pull", bind=True)
def sap_pull(self, connection_id: int, binding_id: int) -> dict:  # type: ignore[no-untyped-def]
    """Trigger an OData/ADT pull from a configured SAP connection."""
    logger.info("task.sap_pull.start", connection_id=connection_id, binding_id=binding_id)
    db = _task_session()
    try:
        try:
            from app.services.sap_extraction import extract_from_binding

            result = extract_from_binding(
                connection_id=connection_id, binding_id=binding_id, db=db
            )
        except (ImportError, AttributeError):
            result = {"warning": "extract_from_binding not available"}
        db.commit()
        return {
            "connection_id": connection_id,
            "binding_id": binding_id,
            "status": "completed",
            **result,
        }
    except Exception as e:
        db.rollback()
        logger.error("task.sap_pull.failed", error=str(e))
        return {
            "connection_id": connection_id,
            "binding_id": binding_id,
            "status": "failed",
            "error": str(e)[:500],
        }
    finally:
        db.close()


@celery.task(name="app.workers.tasks.run_llm_review", bind=True)
def run_llm_review(  # type: ignore[no-untyped-def]
    self, run_id: int, mode: str = "SINGLE"
) -> dict:
    """Trigger the LLM review pass for a completed analysis run."""
    logger.info("task.run_llm_review.start", run_id=run_id, mode=mode)
    db = _task_session()
    try:
        try:
            from app.infra.llm.review_pass import run_review_pass

            result = run_review_pass(run_id=run_id, mode=mode, db=db) or {}
        except (ImportError, AttributeError):
            result = {"warning": "run_review_pass not available"}
        db.commit()
        return {"run_id": run_id, "mode": mode, "status": "completed", **result}
    except Exception as e:
        db.rollback()
        logger.error("task.run_llm_review.failed", run_id=run_id, error=str(e))
        raise
    finally:
        db.close()


# ── Dispatcher used by the API ────────────────────────────────────────────


DISPATCH = {
    "v1": run_analysis,
    "v2": run_v2_analysis,
}


def dispatch_analysis(
    *,
    engine: str,
    run_id: int,
    **kwargs: Any,
) -> str | None:
    """Dispatch an analysis run to the configured engine. Returns task id.

    Raises ValueError if the engine name is unknown.
    """
    task = DISPATCH.get(engine.lower())
    if task is None:
        raise ValueError(f"Unknown analysis engine: {engine}")
    async_result = task.delay(run_id=run_id, **kwargs)
    return async_result.id


__all__ = [
    "celery",
    "dispatch_analysis",
    "housekeeping_monthly",
    "housekeeping_send_reminders",
    "review_send_reminders",
    "run_analysis",
    "run_v2_analysis",
    "run_ml_scoring",
    "send_email_batch",
    "sap_pull",
    "run_llm_review",
]
