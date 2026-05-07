"""Tests for the data-browser perf rework in PR #87.

The endpoint went from "load everything" to paginated + opt-in heavy
parts. The behaviours we want to lock down:

* Pagination cap — even with ``size`` not passed, results are bounded.
* ``include_balances=False`` (default) doesn't put a
  ``monthly_balances`` key on items.
* ``include_hierarchies=False`` (default) returns hierarchy *metadata*
  only — no ``nodes`` / ``leaves`` arrays — and the response carries
  ``hierarchies_inlined: False``.
* ``include_hierarchies=True`` flips ``hierarchies_inlined`` and
  populates ``nodes`` and ``leaves`` for each row.
* ``search=...`` pushes a LIKE filter to the DB.

Pagination correctness (offset/limit math) and SQL emission are
covered indirectly here; the main goal is to defend against
accidental regressions on the contract the frontend relies on.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.api.data_management import data_browser


def _empty_query_result() -> MagicMock:
    """A db.execute() return that yields zero rows for any chained call."""
    result = MagicMock()
    result.scalars.return_value.all.return_value = []
    result.all.return_value = []
    result.scalar.return_value = 0
    return result


def _make_db(*, total: int = 0, ccs: list | None = None, hiers: list | None = None) -> MagicMock:
    """Build a MagicMock session that returns canned results in the
    order the endpoint queries them.

    Order matters — ``data_browser`` issues queries in this sequence:

        1. count (cc_count_q.scalar())
        2. cc page (cc_q.scalars().all())
        3. PCs (only if any CC has pctr — skipped here)
        4. balances (only when include_balances=True)
        5. hierarchies (hier_q.scalars().all())
        6. hierarchy nodes (per hierarchy, only when include_hierarchies=True)
        7. hierarchy leaves (per hierarchy, only when include_hierarchies=True)
    """
    db = MagicMock()
    results = []
    # 1) count
    count_r = MagicMock()
    count_r.scalar.return_value = total
    results.append(count_r)
    # 2) ccs
    cc_r = MagicMock()
    cc_r.scalars.return_value.all.return_value = ccs or []
    results.append(cc_r)
    # 3) hierarchies (the test fixtures here have no PCs, no balances)
    hier_r = MagicMock()
    hier_r.scalars.return_value.all.return_value = hiers or []
    results.append(hier_r)
    db.execute.side_effect = results
    return db


def test_default_response_skips_balances_and_hierarchy_structure() -> None:
    """Default call: no balances, no hierarchy nodes/leaves. This is
    the fast-path that operators get on first navigation."""
    cc = MagicMock()
    cc.id = 1
    cc.cctr = "CC100"
    cc.txtsh = "Test CC"
    cc.txtmi = ""
    cc.ccode = "DE01"
    cc.coarea = "1000"
    cc.pctr = None  # no PC lookup, simplifies the mock chain
    cc.responsible = "alice"
    cc.cctrcgy = "1"
    cc.is_active = True

    hier = MagicMock()
    hier.id = 5
    hier.setname = "CCHIER"
    hier.setclass = "0101"
    hier.label = None
    hier.coarea = "1000"
    hier.description = None

    db = _make_db(total=1, ccs=[cc], hiers=[hier])

    out = data_browser(
        page=1,
        size=200,
        include_balances=False,
        include_hierarchies=False,
        db=db,
        _user=MagicMock(),
    )

    # Pagination metadata
    assert out["total"] == 1
    assert out["page"] == 1
    assert out["size"] == 200

    # Items don't have monthly_balances key when include_balances=False
    assert len(out["items"]) == 1
    assert "monthly_balances" not in out["items"][0]
    assert out["items"][0]["cctr"] == "CC100"

    # Hierarchies are metadata-only
    assert out["hierarchies_inlined"] is False
    assert len(out["hierarchies"]) == 1
    h = out["hierarchies"][0]
    assert "nodes" not in h
    assert "leaves" not in h
    assert h["id"] == 5
    assert h["setclass"] == "0101"
    # Label was synthesised from setclass + setname
    assert "Cost Center" in h["label"]


def test_include_hierarchies_true_inlines_nodes_and_leaves() -> None:
    """Operator drills into the hierarchical view → frontend re-fetches
    with include_hierarchies=true. The response now carries the full
    nodes + leaves payload that the tree renderer needs."""
    cc = MagicMock()
    cc.id = 1
    cc.cctr = "CC100"
    cc.txtsh = ""
    cc.txtmi = ""
    cc.ccode = "DE01"
    cc.coarea = "1000"
    cc.pctr = None
    cc.responsible = ""
    cc.cctrcgy = ""
    cc.is_active = True

    hier = MagicMock()
    hier.id = 5
    hier.setname = "CCHIER"
    hier.setclass = "0101"
    hier.label = "My CC tree"
    hier.coarea = "1000"
    hier.description = None

    node = MagicMock()
    node.parent_setname = "ROOT"
    node.child_setname = "EUROPE"
    node.seq = 0

    leaf = MagicMock()
    leaf.setname = "EUROPE"
    leaf.value = "CC100"
    leaf.seq = 0

    # When include_hierarchies=True, two extra queries fire per
    # hierarchy (nodes + leaves). Build the result chain manually.
    db = MagicMock()
    count_r = MagicMock()
    count_r.scalar.return_value = 1
    cc_r = MagicMock()
    cc_r.scalars.return_value.all.return_value = [cc]
    hier_r = MagicMock()
    hier_r.scalars.return_value.all.return_value = [hier]
    nodes_r = MagicMock()
    nodes_r.scalars.return_value.all.return_value = [node]
    leaves_r = MagicMock()
    leaves_r.scalars.return_value.all.return_value = [leaf]
    db.execute.side_effect = [count_r, cc_r, hier_r, nodes_r, leaves_r]

    out = data_browser(
        page=1,
        size=200,
        include_balances=False,
        include_hierarchies=True,
        db=db,
        _user=MagicMock(),
    )

    assert out["hierarchies_inlined"] is True
    h = out["hierarchies"][0]
    assert h["nodes"] == [{"parent": "ROOT", "child": "EUROPE", "seq": 0}]
    assert h["leaves"] == [{"setname": "EUROPE", "value": "CC100", "seq": 0}]


def test_pagination_size_capped_at_500() -> None:
    """The size parameter is bounded at 500. This is the max single
    page size we're willing to ship — beyond that we lose the
    'fast initial render' property, and a malicious or careless
    caller passing size=10000 could starve the server. FastAPI's
    ``Query(le=500)`` rejects out-of-range values with a 422."""

    # FastAPI validates Query bounds before the function body runs;
    # in unit tests we hit that by importing the validator directly.
    # Easiest assertion: confirm our default and that the function
    # signature carries the ge=1, le=500 bounds — a regression here
    # would silently widen the cap.
    import inspect

    sig = inspect.signature(data_browser)
    size_default = sig.parameters["size"].default
    assert size_default.default == 200
    # FastAPI's Query exposes le either as a direct attribute or via
    # the .metadata list (depends on FastAPI version). Try both so the
    # test isn't pinned to one release line.
    bound_max = getattr(size_default, "le", None)
    if bound_max is None:
        metadata = getattr(size_default, "metadata", None) or []
        for m in metadata:
            if hasattr(m, "le"):
                bound_max = m.le
                break
    assert bound_max == 500


def test_search_param_does_not_break_zero_results() -> None:
    """Search filter on a database with no matching rows returns
    total=0 and an empty items list — no exception."""
    db = _make_db(total=0, ccs=[], hiers=[])

    out = data_browser(
        page=1,
        size=200,
        search="ZZZ",
        include_balances=False,
        include_hierarchies=False,
        db=db,
        _user=MagicMock(),
    )

    assert out["total"] == 0
    assert out["items"] == []
    assert out["page"] == 1
