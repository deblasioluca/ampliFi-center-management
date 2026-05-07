"""Tests for the ``include_hierarchies`` lazy-load flag on the data
browser endpoint.

PR #81 added an ``include_hierarchies`` query parameter to
``GET /api/runs/{id}/data-browser`` that defaults to False. The
hierarchy block (every active hierarchy + every node + every leaf) is
the dominant cost on this endpoint and the tabular view doesn't need
it. Without this fix the endpoint took 30+ seconds on slower hardware
and the user perceived the page as 'hanging'.

These tests verify that:
* the default response omits hierarchies (fast path)
* hierarchies are returned when the flag is explicitly set
* turning the flag off doesn't break the rest of the response shape
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.api.runs import data_browser


def _mock_db(
    run_obj: MagicMock | None,
    proposals: list,
    ccs: list,
    balance_rows: list,
    hierarchies: list,
) -> MagicMock:
    """Mock a SQLAlchemy session matching the call sequence in
    ``data_browser``: get(run), execute(count), execute(proposals),
    execute(ccs), execute(balance), then optionally
    execute(hierarchies) and execute(nodes/leaves) per hierarchy.
    """
    db = MagicMock()
    db.get.return_value = run_obj

    # Each db.execute(...) returns a result object. We sequence them.
    results = []

    # 1) COUNT query for total_count
    count_result = MagicMock()
    count_result.scalar.return_value = len(proposals)
    results.append(count_result)

    # 2) Paginated proposals query
    proposals_result = MagicMock()
    proposals_result.scalars.return_value.all.return_value = proposals
    results.append(proposals_result)

    if proposals:
        ccs_result = MagicMock()
        ccs_result.scalars.return_value.all.return_value = ccs
        results.append(ccs_result)

    if ccs:
        bal_result = MagicMock()
        bal_result.all.return_value = balance_rows
        results.append(bal_result)

    # Hierarchy fetches (only consumed if include_hierarchies=True)
    hier_result = MagicMock()
    hier_result.scalars.return_value.all.return_value = hierarchies
    results.append(hier_result)

    # For each hierarchy, two more execute() calls (nodes, leaves)
    for _ in hierarchies:
        nodes_result = MagicMock()
        nodes_result.scalars.return_value.all.return_value = []
        leaves_result = MagicMock()
        leaves_result.scalars.return_value.all.return_value = []
        results.extend([nodes_result, leaves_result])

    db.execute.side_effect = results
    return db


def _mock_run() -> MagicMock:
    run = MagicMock()
    run.id = 42
    run.status = "completed"
    return run


def test_data_browser_default_omits_hierarchies() -> None:
    """Default call (no flag) returns an empty hierarchies list — the
    expensive query is skipped entirely."""
    run = _mock_run()
    db = _mock_db(run, proposals=[], ccs=[], balance_rows=[], hierarchies=[])

    result = data_browser(run_id=42, include_hierarchies=False, db=db, _user=MagicMock())

    assert result["hierarchies"] == []
    # The Hierarchy/HierarchyNode/HierarchyLeaf queries should NOT
    # have been executed. With no proposals there's the count query
    # + proposals SELECT (2 calls). Adding hierarchies would add more.
    assert db.execute.call_count == 2


def test_data_browser_with_flag_loads_hierarchies() -> None:
    """When the user switches to the Hierarchical view, the frontend
    re-fetches with ``?include_hierarchies=true``. That call should
    actually return the hierarchy trees."""
    run = _mock_run()
    fake_hier = MagicMock()
    fake_hier.id = 1
    fake_hier.setname = "ANALYSIS_PERIMETER"
    fake_hier.setclass = "0101"
    fake_hier.label = "Analysis perimeter"
    fake_hier.description = "test hierarchy"
    fake_hier.coarea = "1000"
    db = _mock_db(run, proposals=[], ccs=[], balance_rows=[], hierarchies=[fake_hier])

    result = data_browser(run_id=42, include_hierarchies=True, db=db, _user=MagicMock())

    assert len(result["hierarchies"]) == 1
    h = result["hierarchies"][0]
    assert h["id"] == 1
    assert h["setclass"] == "0101"
    # Even if the test mock returns no nodes/leaves, the shape should be present
    assert "nodes" in h
    assert "leaves" in h


def test_data_browser_404_when_run_missing() -> None:
    """If the run id doesn't exist, return 404 — same behaviour
    regardless of the flag."""
    db = _mock_db(None, proposals=[], ccs=[], balance_rows=[], hierarchies=[])

    from fastapi import HTTPException

    try:
        data_browser(run_id=999, include_hierarchies=False, db=db, _user=MagicMock())
    except HTTPException as e:
        assert e.status_code == 404
    else:
        raise AssertionError("Expected 404 HTTPException")


def test_data_browser_response_shape_consistent_across_flag() -> None:
    """The response shape (top-level keys) should be identical
    regardless of the flag — only the contents of ``hierarchies``
    differs. The frontend relies on this when toggling between tabs."""
    run = _mock_run()
    db1 = _mock_db(run, proposals=[], ccs=[], balance_rows=[], hierarchies=[])
    db2 = _mock_db(run, proposals=[], ccs=[], balance_rows=[], hierarchies=[])

    r1 = data_browser(run_id=42, include_hierarchies=False, db=db1, _user=MagicMock())
    r2 = data_browser(run_id=42, include_hierarchies=True, db=db2, _user=MagicMock())

    assert set(r1.keys()) == set(r2.keys())
    expected = {"run_id", "total", "page", "size", "items",
                "pc_target_groups", "hierarchies"}
    assert expected.issubset(r1.keys())
