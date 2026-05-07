"""Tests for the KPI fallback path in /api/runs/{id} (PR #88).

Operator report (with screenshot) showed the Analytics Dashboard
displaying "Total 18,490 / KEEP 0 / RETIRE 0 / MERGE_MAP 0 / REDESIGN 0
/ Reduction 100%" while the Data Browser tab below clearly showed
many cost centers tagged KEEP. Root cause: the run's cached ``kpis``
JSONB blob was empty for that particular run (a couple of older runs
in production didn't persist it on the success path).

The fix recomputes outcome counts from the proposals table whenever
the cached outcomes are all zero. These tests pin that behaviour:

* When ``run.kpis`` already has populated outcomes → use them as-is,
  don't run the fallback query.
* When ``run.kpis`` is empty or all zero AND proposals exist → fall
  back to the recomputed counts.
* When everything is empty (no kpis, no proposals) → return the empty
  shape rather than crashing.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def _make_run(*, kpis: dict | None, completed_centers: int = 0) -> MagicMock:
    """Build a minimal AnalysisRun-shaped mock for the get_run handler.

    Only the fields the handler reads need to be set; the rest are
    harmless MagicMock attributes that get coerced to strings."""
    run = MagicMock()
    run.id = 21
    run.wave_id = None
    run.config_id = 6
    run.status = "completed"
    run.kpis = kpis
    run.started_at = None
    run.finished_at = None
    run.total_centers = 18490 if completed_centers else 0
    run.completed_centers = completed_centers
    return run


def _grouped_outcome_rows(**counts: int) -> list[tuple[str, int]]:
    """Helper: build the GROUP BY rows the recompute query expects.
    Keys are CleansingOutcome enum string values (uppercase)."""
    return list(counts.items())


def test_populated_kpis_are_returned_unchanged() -> None:
    """When the cached kpis dict has populated outcome counts the
    handler returns them as-is — no fallback query. This is the hot
    path: the vast majority of runs persist kpis correctly."""
    from app.api.runs import get_run

    run = _make_run(
        kpis={
            "total_centers": 100,
            "keep": 70,
            "retire": 20,
            "merge_map": 7,
            "redesign": 3,
        },
        completed_centers=100,
    )
    db = MagicMock()
    db.get.return_value = run

    out = get_run(run_id=21, db=db)

    # The proposals query is NOT issued — only the db.get(AnalysisRun, 21)
    # call. db.execute should never be called because cached kpis are
    # already populated.
    db.execute.assert_not_called()
    assert out.kpis["keep"] == 70
    assert out.kpis["retire"] == 20
    assert out.kpis["merge_map"] == 7
    assert out.kpis["redesign"] == 3


def test_empty_kpis_with_proposals_fall_back_to_recomputed_counts() -> None:
    """The reported bug: kpis blob exists but every outcome is 0,
    while the proposals table actually has KEEP rows. Fallback should
    recompute from proposals so the dashboard headline matches the
    data browser body."""
    from app.api.runs import get_run

    run = _make_run(
        kpis={
            "total_centers": 18490,
            "keep": 0,
            "retire": 0,
            "merge_map": 0,
            "redesign": 0,
        },
        completed_centers=18490,
    )
    db = MagicMock()
    db.get.return_value = run

    # The recompute query is a single GROUP BY whose result we mock
    # directly. The handler iterates ``db.execute(...).all()`` so the
    # mock chain mirrors that.
    db.execute.return_value.all.return_value = _grouped_outcome_rows(
        KEEP=18000,
        RETIRE=400,
        MERGE_MAP=70,
        REDESIGN=20,
    )

    out = get_run(run_id=21, db=db)

    # Recomputed values shadow the zeros from the cached blob
    assert out.kpis["keep"] == 18000
    assert out.kpis["retire"] == 400
    assert out.kpis["merge_map"] == 70
    assert out.kpis["redesign"] == 20
    # total_centers from the cached kpis is preserved (it wasn't 0)
    assert out.kpis["total_centers"] == 18490


def test_no_kpis_at_all_falls_back() -> None:
    """``run.kpis`` is None — older runs that never persisted the dict.
    Fallback still kicks in and produces a useful response."""
    from app.api.runs import get_run

    run = _make_run(kpis=None, completed_centers=50)
    db = MagicMock()
    db.get.return_value = run
    db.execute.return_value.all.return_value = _grouped_outcome_rows(KEEP=40, RETIRE=10)

    out = get_run(run_id=21, db=db)

    assert out.kpis["keep"] == 40
    assert out.kpis["retire"] == 10
    # merge_map / redesign not present in proposals → default to 0
    assert out.kpis["merge_map"] == 0
    assert out.kpis["redesign"] == 0
    # total_centers picked up from the proposal count since the cached
    # value was missing
    assert out.kpis["total_centers"] == 50


def test_empty_kpis_falls_back_even_when_completed_is_zero() -> None:
    """PR #88 widening: previously the fallback was gated on
    ``completed_centers > 0``, which excluded older runs where the
    field was never tracked. Operators saw "0 KEEP, 0 RETIRE..." in
    the dashboard while the data browser below clearly had KEEP rows
    — the new behaviour is to recompute whenever cached outcomes are
    zero, regardless of completed_centers.
    """
    from app.api.runs import get_run

    run = _make_run(
        kpis={
            "total_centers": 18490,
            "keep": 0,
            "retire": 0,
            "merge_map": 0,
            "redesign": 0,
        },
        completed_centers=0,  # ← key: gate would have skipped fallback
    )
    db = MagicMock()
    db.get.return_value = run
    db.execute.return_value.all.return_value = _grouped_outcome_rows(
        KEEP=15000,
        RETIRE=2000,
    )

    out = get_run(run_id=21, db=db)

    # Fallback DID fire, recomputed values present
    assert out.kpis["keep"] == 15000
    assert out.kpis["retire"] == 2000


def test_empty_kpis_and_no_proposals_returns_empty_safely() -> None:
    """A run with empty kpis AND no proposals at all (e.g. a freshly
    queued run that hasn't started, or one where the recompute query
    legitimately returns nothing) shouldn't crash — it should return
    whatever empty shape we have."""
    from app.api.runs import get_run

    run = _make_run(kpis={}, completed_centers=0)
    db = MagicMock()
    db.get.return_value = run
    db.execute.return_value.all.return_value = []  # no proposals

    out = get_run(run_id=21, db=db)

    # No exception, no synthesised counts. The kpis dict stays empty
    # so the frontend renders "--" rather than misleading zeros that
    # might be confused for confirmed-zero outcomes.
    assert isinstance(out.kpis, dict)


def test_run_not_found_returns_404() -> None:
    """Sanity: the existing 404 path still works after the fallback
    refactor."""
    from fastapi import HTTPException

    from app.api.runs import get_run

    db = MagicMock()
    db.get.return_value = None

    with pytest.raises(HTTPException) as ei:
        get_run(run_id=999, db=db)

    assert ei.value.status_code == 404
