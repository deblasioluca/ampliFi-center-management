"""Tests for the threaded /api/runs/global endpoint and the progress
fields exposed by GET /api/runs/{run_id}.

Covers:
* The endpoint creates the AnalysisRun row in 'queued' status, dispatches
  a daemon thread, and returns the run_id immediately (no waiting for the
  pipeline).
* GET /api/runs/{run_id} surfaces total_centers + completed_centers so
  the frontend can render a progress bar.
* The thread runner handles "run not found" and analysis exceptions
  gracefully (sets status=failed with error text).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api.runs import (
    RunOut,
    _run_global_in_thread,
    get_run,
    run_global_analysis,
)

# ── helpers ──────────────────────────────────────────────────────────────


def _make_run(
    run_id: int = 1,
    status: str = "queued",
    total: int = 0,
    completed: int = 0,
    kpis: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=run_id,
        wave_id=None,
        config_id=42,
        status=status,
        kpis=kpis,
        started_at=None,
        finished_at=None,
        total_centers=total,
        completed_centers=completed,
        error=None,
    )


# ── POST /api/runs/global — async dispatch ───────────────────────────────


def test_global_run_returns_immediately_without_waiting_for_pipeline() -> None:
    """The endpoint must NOT call execute_analysis directly; it just
    creates the row, dispatches a thread, and returns. We verify by
    mocking the threading.Thread class and checking the response."""
    db = MagicMock()
    user = SimpleNamespace(id=99)

    # When db.add(run) is called and db.refresh(run) runs, simulate
    # the run getting an id assigned.
    def assign_id(obj):
        obj.id = 7
        obj.started_at = None

    db.add.side_effect = assign_id
    db.refresh.side_effect = lambda obj: None

    with (
        patch("app.api.runs.threading.Thread") as mock_thread,
        patch("app.services.analysis.get_or_create_default_config") as get_cfg,
    ):
        get_cfg.return_value = SimpleNamespace(id=42)
        result = run_global_analysis(config_id=None, db=db, user=user)

    assert result["run_id"] == 7
    assert result["status"] == "queued"
    # Thread must have been started — exactly one
    mock_thread.assert_called_once()
    instance = mock_thread.return_value
    instance.start.assert_called_once()
    # Pipeline execution must NOT have been called from the request handler
    db.commit.assert_called_once()  # only the row creation


def test_global_run_uses_provided_config_id_when_set() -> None:
    """When the caller provides a config_id, we shouldn't call
    get_or_create_default_config (don't create a default unnecessarily)."""
    db = MagicMock()
    user = SimpleNamespace(id=99)
    db.add.side_effect = lambda obj: setattr(obj, "id", 5)

    with (
        patch("app.api.runs.threading.Thread"),
        patch("app.services.analysis.get_or_create_default_config") as get_cfg,
    ):
        result = run_global_analysis(config_id=42, db=db, user=user)

    get_cfg.assert_not_called()
    assert result["run_id"] == 5


# ── GET /api/runs/{run_id} — progress fields ─────────────────────────────


def test_get_run_returns_progress_fields() -> None:
    """The frontend needs total_centers + completed_centers to render a
    progress bar without holding the original POST connection open."""
    run = _make_run(run_id=10, status="running", total=127, completed=42)
    db = MagicMock()
    db.get.return_value = run

    out = get_run(run_id=10, db=db)

    assert out.id == 10
    assert out.status == "running"
    assert out.total_centers == 127
    assert out.completed_centers == 42


def test_get_run_handles_zero_progress_gracefully() -> None:
    """A queued run hasn't started yet; total/completed should be 0."""
    run = _make_run(run_id=1, status="queued", total=0, completed=0)
    db = MagicMock()
    db.get.return_value = run

    out = get_run(run_id=1, db=db)

    assert out.total_centers == 0
    assert out.completed_centers == 0


def test_get_run_404_for_unknown_run() -> None:
    db = MagicMock()
    db.get.return_value = None

    with pytest.raises(HTTPException) as exc:
        get_run(run_id=999, db=db)
    assert exc.value.status_code == 404


# ── RunOut Pydantic schema ───────────────────────────────────────────────


def test_run_out_progress_defaults_to_zero() -> None:
    """If a caller omits the progress fields (e.g. an older client model),
    they default to 0 rather than None — keeping the int contract simple."""
    out = RunOut(id=1, config_id=42, status="queued")
    assert out.total_centers == 0
    assert out.completed_centers == 0


# ── Thread runner — graceful failure handling ────────────────────────────


def test_thread_runner_marks_run_failed_when_pipeline_raises() -> None:
    """If execute_analysis_for_run raises, the thread must catch it,
    mark the run as failed with the error text, and not crash."""
    run = _make_run(run_id=10, status="running")
    db = MagicMock()
    # First db.get returns the run; subsequent get inside except branch
    # also returns the same run so it can be marked failed.
    db.get.return_value = run

    with (
        patch("app.infra.db.session.SessionLocal", return_value=db),
        patch("app.services.analysis.execute_analysis_for_run") as exec_fn,
    ):
        exec_fn.side_effect = ValueError("synthetic pipeline failure")
        # Should NOT raise — the thread runner catches everything
        _run_global_in_thread(run_id=10, config_id=42, user_id=1)

    assert run.status == "failed"
    assert run.error is not None
    assert "synthetic pipeline failure" in run.error
    db.commit.assert_called()
    db.close.assert_called_once()


def test_thread_runner_handles_run_not_found() -> None:
    """If the run row is gone by the time the thread starts (e.g. the
    user deleted it racing with the dispatch), don't crash."""
    db = MagicMock()
    db.get.return_value = None

    with patch("app.infra.db.session.SessionLocal", return_value=db):
        # Should NOT raise
        _run_global_in_thread(run_id=999, config_id=42, user_id=1)

    db.close.assert_called_once()
    # Pipeline never ran — nothing to mark failed
