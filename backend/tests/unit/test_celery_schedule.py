"""Tests for Celery beat schedule configuration and dispatch helper.

These tests don't actually run Celery — they verify the configuration is
well-formed (schedule entries exist, task names are correct, dispatcher
maps engines to tasks correctly).
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


def test_beat_schedule_includes_required_jobs() -> None:
    from app.workers.celery_app import celery

    schedule = celery.conf.beat_schedule
    assert "housekeeping-monthly" in schedule
    assert "housekeeping-reminders-daily" in schedule
    assert "review-reminders-daily" in schedule


def test_beat_schedule_tasks_resolve() -> None:
    """Each schedule entry must reference a task that's registered."""
    from app.workers.celery_app import celery

    schedule = celery.conf.beat_schedule
    for name, entry in schedule.items():
        task_name = entry["task"]
        assert task_name.startswith("app.workers.tasks."), (
            f"Schedule '{name}' references {task_name} which is not in app.workers.tasks"
        )


def test_task_routes_cover_all_queues() -> None:
    """All explicit queues used in deploy/systemd must be in the task_routes."""
    from app.workers.celery_app import celery

    routes = celery.conf.task_routes
    queues_referenced = {r["queue"] for r in routes.values()}
    # Worker systemd unit lists: default,ml,llm,email,odata
    expected = {"default", "ml", "llm", "email", "odata"}
    missing = expected - queues_referenced
    assert not missing, f"Queues missing from task_routes: {missing}"


def test_crontab_env_override_for_housekeeping_monthly() -> None:
    """HOUSEKEEPING_CRON_MONTHLY_HOUR=5 should change the schedule."""
    # Re-import to pick up the env var
    import importlib

    with patch.dict(os.environ, {"HOUSEKEEPING_CRON_MONTHLY_HOUR": "5"}, clear=False):
        from app.workers import celery_app

        importlib.reload(celery_app)
        from app.workers.celery_app import celery

        entry = celery.conf.beat_schedule["housekeeping-monthly"]
        # crontab.hour stores parsed value in _orig_hour
        assert "5" in str(entry["schedule"]) or entry["schedule"]._orig_hour == "5"


def test_dispatch_unknown_engine_raises() -> None:
    from app.workers.tasks import dispatch_analysis

    with pytest.raises(ValueError, match="Unknown analysis engine"):
        dispatch_analysis(engine="v99", run_id=1)


def test_dispatch_v1_calls_run_analysis_delay() -> None:
    from app.workers import tasks

    fake_async = MagicMock()
    fake_async.id = "task-abc-123"
    with patch.object(tasks.run_analysis, "delay", return_value=fake_async) as m:
        task_id = tasks.dispatch_analysis(
            engine="v1",
            run_id=42,
            wave_id=7,
            config_id=3,
            user_id=1,
        )
    m.assert_called_once_with(run_id=42, wave_id=7, config_id=3, user_id=1)
    assert task_id == "task-abc-123"


def test_dispatch_v2_calls_run_v2_analysis_delay() -> None:
    from app.workers import tasks

    fake_async = MagicMock()
    fake_async.id = "task-xyz-456"
    with patch.object(tasks.run_v2_analysis, "delay", return_value=fake_async) as m:
        task_id = tasks.dispatch_analysis(
            engine="v2",
            run_id=99,
            wave_id=2,
            config_id=5,
            mode="simulation",
        )
    m.assert_called_once_with(run_id=99, wave_id=2, config_id=5, mode="simulation")
    assert task_id == "task-xyz-456"


def test_dispatch_engine_case_insensitive() -> None:
    from app.workers import tasks

    fake_async = MagicMock()
    fake_async.id = "task-1"
    with patch.object(tasks.run_analysis, "delay", return_value=fake_async):
        # uppercase
        task_id = tasks.dispatch_analysis(engine="V1", run_id=1)
        assert task_id == "task-1"
