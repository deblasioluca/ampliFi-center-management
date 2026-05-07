"""Tests for the async cluster duplicate-check (PR #88).

The cluster explorer endpoint went from a synchronous "block the
worker for many seconds while embeddings run" to a dispatch-and-poll
pair:

* ``POST /api/data/duplicate-check`` — dispatches a daemon thread,
  registers a job in the in-memory ``_cluster_jobs`` dict, returns
  ``{job_id, status: "queued"}`` immediately.
* ``GET /api/data/duplicate-check/jobs/{job_id}`` — returns the job
  record (status / progress / result) or 404 if unknown.

We test the contract — what the frontend depends on — without trying
to actually run the embedding work end-to-end. The thread target is
patched to a no-op fake so the tests stay fast and deterministic.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException


def test_dispatch_returns_job_id_immediately() -> None:
    """POST creates a job entry and returns ``{job_id, status: queued}``
    without blocking on the actual embedding work."""
    from app.api import reference

    started: list = []

    class _FakeThread:
        """Stand-in for threading.Thread that records start() but does
        nothing. We don't want a real worker thread fighting our
        in-memory dict during the assertions."""

        def __init__(self, target=None, args=(), daemon=False, **_kwargs) -> None:
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self) -> None:
            started.append((self.target, self.args, self.daemon))

    with (
        patch.object(reference, "_cluster_jobs", {}),
        # Patch threading.Thread inside reference.py so the dispatch
        # records the call without actually spinning a worker.
        patch("threading.Thread", _FakeThread),
    ):
        out = reference.check_duplicates(
            coarea="1000",
            threshold=0.85,
            limit=100,
            db=MagicMock(),
        )

    assert "job_id" in out
    assert out["status"] == "queued"
    assert isinstance(out["job_id"], str)
    assert len(out["job_id"]) > 0

    # Thread was dispatched as a daemon (so a process restart doesn't
    # wait for it) and pointed at the real worker function.
    assert started, "Thread.start() was never called"
    target, args, daemon = started[0]
    assert daemon is True
    # First positional arg is the job_id we just got back
    assert args[0] == out["job_id"]


def test_get_job_returns_full_record() -> None:
    """Polling endpoint returns the live job record so the frontend
    can render progress (status, stage, total, progress) and pick up
    ``result`` on completion."""
    from app.api import reference

    fake_jobs = {
        "abc123": {
            "status": "running",
            "progress": 42,
            "total": 100,
            "stage": "embedding",
            "result": None,
            "error": None,
            "params": {"coarea": "1000", "threshold": 0.85, "limit": 100},
        }
    }
    with patch.object(reference, "_cluster_jobs", fake_jobs):
        out = reference.get_duplicate_check_job(job_id="abc123")

    assert out["status"] == "running"
    assert out["progress"] == 42
    assert out["total"] == 100
    assert out["stage"] == "embedding"


def test_get_job_completed_includes_result() -> None:
    """When the worker finishes successfully it sets ``status='done'``
    and stuffs the pairs payload into ``result``. The frontend reads
    that out of the same polling endpoint — no separate "fetch result"
    round-trip."""
    from app.api import reference

    finished_payload = {
        "total": 3,
        "pairs": [
            {
                "id_a": 1,
                "id_b": 2,
                "name_a": "Marketing EMEA",
                "name_b": "EMEA Marketing",
                "similarity": 0.92,
            },
            {
                "id_a": 5,
                "id_b": 9,
                "name_a": "Sales DACH",
                "name_b": "DACH Sales",
                "similarity": 0.89,
            },
            {
                "id_a": 11,
                "id_b": 14,
                "name_a": "Ops APAC",
                "name_b": "APAC Operations",
                "similarity": 0.86,
            },
        ],
    }
    fake_jobs = {
        "done-job": {
            "status": "done",
            "progress": 100,
            "total": 100,
            "stage": "complete",
            "result": finished_payload,
            "error": None,
            "params": {"coarea": "1000", "threshold": 0.85, "limit": 100},
        }
    }
    with patch.object(reference, "_cluster_jobs", fake_jobs):
        out = reference.get_duplicate_check_job(job_id="done-job")

    assert out["status"] == "done"
    assert out["result"] is finished_payload
    assert len(out["result"]["pairs"]) == 3


def test_get_job_unknown_returns_404() -> None:
    """A poll for a job_id that doesn't exist (or has been GC'd) gets
    a 404 — the frontend's polling loop treats this as a hard stop."""
    from app.api import reference

    with (
        patch.object(reference, "_cluster_jobs", {}),
        pytest.raises(HTTPException) as ei,
    ):
        reference.get_duplicate_check_job(job_id="never-existed")

    assert ei.value.status_code == 404


def test_failed_job_carries_error_string() -> None:
    """If the worker raises, the job is marked ``failed`` with the
    exception string. The frontend renders ``error`` directly so
    operators can see what went wrong (instead of a generic
    "something failed")."""
    from app.api import reference

    fake_jobs = {
        "boom": {
            "status": "failed",
            "progress": 0,
            "total": 0,
            "stage": "embedding",
            "result": None,
            "error": "OOM while loading sentence-transformer model",
            "params": {"coarea": "1000", "threshold": 0.85, "limit": 100},
        }
    }
    with patch.object(reference, "_cluster_jobs", fake_jobs):
        out = reference.get_duplicate_check_job(job_id="boom")

    assert out["status"] == "failed"
    assert "OOM" in out["error"]
