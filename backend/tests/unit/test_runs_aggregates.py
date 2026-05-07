"""Tests for ``GET /api/runs/{id}/aggregates`` (PR #90).

The endpoint replaces the chart-data path that was capped at 5000
proposals — operator reported "Outcome distribution only shows 5k of
18,490". These tests cover the shape and the SQL aggregations.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_db():
    """A SQLAlchemy session double whose ``execute`` calls are scripted
    by the individual test. Each test sets ``mock_db._results`` to a
    list of values (one per execute call), or stubs ``execute``
    directly for finer control.
    """
    db = MagicMock()
    return db


def test_aggregates_returns_correct_shape_with_total_proposals():
    """Sanity: response carries every block the dashboard needs.

    The dashboard expects a single fetch per run, so a missing block
    means a chart silently disappears. Pinning the shape catches any
    accidental removal.
    """
    from app.api.runs import run_aggregates

    db = MagicMock()

    # Run row exists
    run = MagicMock(id=1, kpis={})
    db.get.return_value = run

    # outcome_counts: KEEP=18490
    # target_per_outcome: KEEP/CC_AND_PC=18490
    # outcome_by_entity: one row CH01/UBS AG/KEEP=781
    # confidence_histogram: empty
    # balance_activity: KEEP avg_postings=179
    db.execute.side_effect = [
        # 1) outcome_counts
        MagicMock(all=lambda: [("KEEP", 18490)]),
        # 2) target_per_outcome
        MagicMock(all=lambda: [("KEEP", "CC_AND_PC", 18490)]),
        # 3) outcome_by_entity
        MagicMock(all=lambda: [("CH01", "KEEP", 781)]),
        # 4) entity_names lookup
        MagicMock(all=lambda: [("CH01", "UBS AG")]),
        # 5) confidence_histogram
        MagicMock(all=list),
        # 6) balance_activity
        MagicMock(all=lambda: [("KEEP", 179.0, 1500.0, 18490)]),
    ]

    user = MagicMock()
    out = run_aggregates(run_id=1, db=db, _user=user)

    assert out["run_id"] == 1
    assert out["total_proposals"] == 18490
    assert out["outcome_counts"] == {"KEEP": 18490}
    assert out["target_per_outcome"] == {"KEEP": {"CC_AND_PC": 18490}}
    assert out["outcome_target_flow"] == {"KEEP → CC_AND_PC": 18490}
    assert out["outcome_by_entity"][0]["ccode"] == "CH01"
    assert out["outcome_by_entity"][0]["name"] == "UBS AG"
    assert out["outcome_by_entity"][0]["total"] == 781
    # 10 fixed bins regardless of input
    assert len(out["confidence_histogram"]) == 10
    assert out["confidence_count"] == 0
    assert out["balance_activity"]["KEEP"]["avg_postings"] == 179.0
    assert out["balance_activity"]["KEEP"]["n_centers"] == 18490


def test_aggregates_404_on_unknown_run():
    """Unknown run id raises 404, not 500."""
    from fastapi import HTTPException

    from app.api.runs import run_aggregates

    db = MagicMock()
    db.get.return_value = None

    user = MagicMock()
    with pytest.raises(HTTPException) as exc:
        run_aggregates(run_id=999, db=db, _user=user)
    assert exc.value.status_code == 404


def test_aggregates_buckets_confidence_into_10_bins_with_clamp_at_1():
    """Confidence histogram has exactly 10 bins covering [0.0, 1.0].

    Edge case: confidence=1.0 is clamped to bin 9 (not bin 10), since
    bin 10 would mean confidence > 1.0 which our schema doesn't permit.
    """
    from app.api.runs import run_aggregates

    db = MagicMock()
    db.get.return_value = MagicMock(id=1, kpis={})

    # Empty outcome_by_entity → entity_names lookup is skipped, so the
    # next execute() call goes straight to the histogram query.
    db.execute.side_effect = [
        MagicMock(all=list),  # outcome_counts
        MagicMock(all=list),  # target_per_outcome
        MagicMock(all=list),  # outcome_by_entity (empty → no entity_names)
        # confidence histogram bins: bin 0=5, bin 5=10, bin 9=2 (the 1.0 case)
        MagicMock(all=lambda: [(0, 5), (5, 10), (9, 2)]),
        MagicMock(all=list),  # balance_activity
    ]

    user = MagicMock()
    out = run_aggregates(run_id=1, db=db, _user=user)

    bins = out["confidence_histogram"]
    assert bins[0]["count"] == 5
    assert bins[5]["count"] == 10
    assert bins[9]["count"] == 2
    assert out["confidence_count"] == 17
    # All bins have a label like "0.0", "0.1", ..., "0.9"
    assert bins[0]["label"] == "0.0"
    assert bins[9]["label"] == "0.9"


def test_aggregates_outcome_by_entity_sorted_by_total_desc():
    """Heatmap rows must be sorted biggest-first; that's what the
    operator looks at — the entities with the most action."""
    from app.api.runs import run_aggregates

    db = MagicMock()
    db.get.return_value = MagicMock(id=1, kpis={})

    # Three entities with different totals; intentional out-of-order
    # input to verify sort.
    db.execute.side_effect = [
        MagicMock(all=lambda: [("KEEP", 1300)]),  # outcome_counts
        MagicMock(all=list),  # target_per_outcome
        MagicMock(
            all=lambda: [
                ("US88", "KEEP", 100),
                ("CH01", "KEEP", 781),
                ("CH04", "KEEP", 502),
            ]
        ),
        MagicMock(all=list),  # entity_names
        MagicMock(all=list),  # histogram
        MagicMock(all=list),  # balance_activity
    ]

    user = MagicMock()
    out = run_aggregates(run_id=1, db=db, _user=user)

    entities = out["outcome_by_entity"]
    assert [e["ccode"] for e in entities] == ["CH01", "CH04", "US88"]
    assert entities[0]["total"] == 781
