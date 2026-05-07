"""Tests for the V2 async dispatch pattern in the wave analyse-v2 endpoint.

PR #81 converted ``POST /api/waves/{id}/analyse-v2`` from a blocking
synchronous handler to a queued-async pattern: the request creates the
``AnalysisRun`` row in ``queued`` status, dispatches the V2 pipeline
to a daemon thread via ``_run_v2_in_thread``, and returns immediately.
Progress is surfaced via ``run.completed_centers`` updates inside the
loop and the frontend polls ``GET /api/runs/{id}`` every 2 seconds.

These tests cover the dispatch helper directly (without spinning up a
real FastAPI app) since the actual endpoint just composes:
  build run row → start thread → return run_id

The thread helper is the interesting unit — it owns its own DB session,
calls ``execute_v2_analysis_for_run``, and is responsible for marking
the run as ``failed`` on exception so the UI doesn't see runs stuck
in ``running`` forever.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.api.waves import _run_v2_in_thread


def _mock_run(run_id: int = 99, status: str = "queued") -> MagicMock:
    run = MagicMock()
    run.id = run_id
    run.status = status
    return run


def test_v2_thread_helper_calls_execute_with_correct_args() -> None:
    """The thread helper should hand the exact run row + parameters to
    ``execute_v2_analysis_for_run`` so the engine writes results into
    the row that was created by the request handler."""
    fake_run = _mock_run()
    fake_db = MagicMock()
    fake_db.get.return_value = fake_run

    with (
        patch("app.infra.db.session.SessionLocal") as mock_session,
        patch("app.services.analysis_v2.execute_v2_analysis_for_run") as mock_execute,
    ):
        mock_session.return_value = fake_db

        _run_v2_in_thread(
            run_id=99,
            wave_id=7,
            config_id=42,
            user_id=1,
            mode="simulation",
            id_config={"pc_start": 137, "cc_start": 1},
        )

    # Engine was called with the run row from the helper's own session
    mock_execute.assert_called_once()
    kwargs = mock_execute.call_args.kwargs
    assert kwargs["run"] is fake_run
    assert kwargs["wave_id"] == 7
    assert kwargs["config_id"] == 42
    assert kwargs["mode"] == "simulation"
    assert kwargs["id_config"] == {"pc_start": 137, "cc_start": 1}
    # The session must be closed afterwards regardless
    fake_db.close.assert_called_once()


def test_v2_thread_helper_marks_run_failed_on_exception() -> None:
    """If the engine raises, the helper must transition the run row to
    ``failed`` so the frontend's progress poller stops thinking the run
    is still in flight. Without this, a crash would leave the row in
    ``running`` indefinitely."""
    fake_run = _mock_run(status="running")
    fake_db = MagicMock()
    fake_db.get.return_value = fake_run

    with (
        patch("app.infra.db.session.SessionLocal") as mock_session,
        patch("app.services.analysis_v2.execute_v2_analysis_for_run") as mock_execute,
    ):
        mock_session.return_value = fake_db
        mock_execute.side_effect = RuntimeError("pipeline blew up")

        # Exception must NOT propagate — the helper runs in a daemon
        # thread, so an uncaught exception would just disappear into
        # the void. The helper logs + marks failed instead.
        _run_v2_in_thread(
            run_id=99,
            wave_id=7,
            config_id=42,
            user_id=1,
            mode="simulation",
            id_config=None,
        )

    assert fake_run.status == "failed"
    fake_db.commit.assert_called()
    fake_db.close.assert_called_once()


def test_v2_thread_helper_does_not_overwrite_terminal_status() -> None:
    """If the engine raises AFTER the run was cleanly cancelled (status
    set to 'cancelled' by the cooperative-cancel path), the helper
    should not flip it to 'failed'. Same for already-completed runs."""
    fake_run = _mock_run(status="cancelled")
    fake_db = MagicMock()
    fake_db.get.return_value = fake_run

    with (
        patch("app.infra.db.session.SessionLocal") as mock_session,
        patch("app.services.analysis_v2.execute_v2_analysis_for_run") as mock_execute,
    ):
        mock_session.return_value = fake_db
        mock_execute.side_effect = RuntimeError("late blow-up")

        _run_v2_in_thread(
            run_id=99,
            wave_id=7,
            config_id=42,
            user_id=1,
            mode="simulation",
            id_config=None,
        )

    # Status was already 'cancelled' — the helper's ``if run.status not
    # in ('completed', 'cancelled')`` guard should leave it alone.
    assert fake_run.status == "cancelled"


def test_v2_thread_helper_handles_missing_run_row_gracefully() -> None:
    """If the run row was deleted between dispatch and thread start,
    the helper must not blow up — it should log and return cleanly."""
    fake_db = MagicMock()
    fake_db.get.return_value = None  # row not found

    with (
        patch("app.infra.db.session.SessionLocal") as mock_session,
        patch("app.services.analysis_v2.execute_v2_analysis_for_run") as mock_execute,
    ):
        mock_session.return_value = fake_db

        # Should not raise
        _run_v2_in_thread(
            run_id=999,
            wave_id=7,
            config_id=42,
            user_id=1,
            mode="simulation",
            id_config=None,
        )

    mock_execute.assert_not_called()
    fake_db.close.assert_called_once()
