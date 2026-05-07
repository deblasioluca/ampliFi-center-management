"""Tests for ``GET /api/legacy/balances/by-hierarchy`` (PR #89, A14).

The endpoint aggregates balance rows per cost center and returns the
hierarchy_path the frontend uses to nest them. We verify:

* Empty result (hierarchy with no leaves) returns the empty shape
* Aggregation pulls the GROUP BY rows + path resolution + txtsh lookup
  in the right order, with the expected fields on every item
* fiscal_year is propagated to the response so the frontend summary
  line can display it
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_empty_hierarchy_returns_empty_items() -> None:
    """A hierarchy with no balance rows on its leaves → 0 items, 0
    max_depth. No exception, no path-resolution call (early return)."""
    from app.api.reference import balances_by_hierarchy

    db = MagicMock()
    bal_result = MagicMock()
    bal_result.all.return_value = []  # no aggregated balance rows
    db.execute.side_effect = [bal_result]

    out = balances_by_hierarchy(
        hierarchy_id=7, fiscal_year=None, scope=None, data_category=None, db=db
    )

    assert out["hierarchy_id"] == 7
    assert out["total_items"] == 0
    assert out["items"] == []
    assert out["max_depth"] == 0


def test_aggregation_returns_per_cc_totals_with_paths() -> None:
    """Two CCs with balance rows in this hierarchy → two items with
    aggregated totals, paths attached."""
    from app.api.reference import balances_by_hierarchy

    db = MagicMock()

    # Mock the aggregated balance rows. Each row is a row-like with
    # the columns the SELECT lists in order.
    bal_row1 = MagicMock()
    bal_row1.cctr = "00010001"
    bal_row1.ccode = "1000"
    bal_row1.tc_amt = 12500.50
    bal_row1.posting_count = 24
    bal_row1.rows = 12
    bal_row2 = MagicMock()
    bal_row2.cctr = "00010002"
    bal_row2.ccode = "1000"
    bal_row2.tc_amt = 8200.00
    bal_row2.posting_count = 16
    bal_row2.rows = 8

    bal_result = MagicMock()
    bal_result.all.return_value = [bal_row1, bal_row2]

    txtsh_result = MagicMock()
    txtsh_result.all.return_value = [
        ("00010001", "Cost Center One"),
        ("00010002", "Cost Center Two"),
    ]

    db.execute.side_effect = [bal_result, txtsh_result]

    fake_paths = {
        "00010001": ["UBS", "DIV", "FN", "00010001"],
        "00010002": ["UBS", "DIV", "FN", "00010002"],
    }
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=(fake_paths, 4),
    ):
        out = balances_by_hierarchy(
            hierarchy_id=7,
            fiscal_year=2024,
            scope=None,
            data_category=None,
            db=db,
        )

    assert out["hierarchy_id"] == 7
    assert out["fiscal_year"] == 2024  # propagated for the frontend summary line
    assert out["max_depth"] == 4
    assert out["total_items"] == 2

    # Items carry cctr, ccode, txtsh, hierarchy_path, totals
    by_cctr = {it["cctr"]: it for it in out["items"]}
    assert by_cctr["00010001"]["txtsh"] == "Cost Center One"
    assert by_cctr["00010001"]["hierarchy_path"] == fake_paths["00010001"]
    assert by_cctr["00010001"]["totals"]["tc_amt"] == 12500.50
    assert by_cctr["00010001"]["totals"]["posting_count"] == 24
    assert by_cctr["00010001"]["totals"]["rows"] == 12

    assert by_cctr["00010002"]["totals"]["tc_amt"] == 8200.00


def test_cc_without_known_txtsh_gets_empty_string() -> None:
    """If the txtsh map doesn't have a CC (e.g. the row is from a
    coarea where the legacy CC table doesn't carry a short text), the
    item still renders with txtsh='' instead of None — the frontend
    string-templates this directly into a <td>.
    """
    from app.api.reference import balances_by_hierarchy

    db = MagicMock()

    bal_row = MagicMock()
    bal_row.cctr = "ORPHAN"
    bal_row.ccode = "1000"
    bal_row.tc_amt = 100.0
    bal_row.posting_count = 1
    bal_row.rows = 1

    bal_result = MagicMock()
    bal_result.all.return_value = [bal_row]
    txtsh_result = MagicMock()
    txtsh_result.all.return_value = []  # no txtsh known
    db.execute.side_effect = [bal_result, txtsh_result]

    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=({"ORPHAN": ["ROOT", "ORPHAN"]}, 2),
    ):
        out = balances_by_hierarchy(
            hierarchy_id=7, fiscal_year=None, scope=None, data_category=None, db=db
        )

    assert out["items"][0]["txtsh"] == ""
    # Path still resolves correctly
    assert out["items"][0]["hierarchy_path"] == ["ROOT", "ORPHAN"]


def test_unmatched_cc_in_paths_gets_empty_path() -> None:
    """A CC with a balance row but no entry in the resolver's paths
    dict gets an empty hierarchy_path list. The frontend's tree
    bucketizer puts these under "(unassigned)" rather than crashing.
    """
    from app.api.reference import balances_by_hierarchy

    db = MagicMock()

    bal_row = MagicMock()
    bal_row.cctr = "STRAY"
    bal_row.ccode = "1000"
    bal_row.tc_amt = 50.0
    bal_row.posting_count = 1
    bal_row.rows = 1

    bal_result = MagicMock()
    bal_result.all.return_value = [bal_row]
    txtsh_result = MagicMock()
    txtsh_result.all.return_value = [("STRAY", "Stray CC")]
    db.execute.side_effect = [bal_result, txtsh_result]

    # Resolver returns empty paths for this CC (it's listed as a leaf
    # in hierarchy_leaf but the resolver didn't reach it from any root)
    with patch("app.api.reference._resolve_hierarchy_paths", return_value=({}, 0)):
        out = balances_by_hierarchy(
            hierarchy_id=7, fiscal_year=None, scope=None, data_category=None, db=db
        )

    assert out["items"][0]["hierarchy_path"] == []
