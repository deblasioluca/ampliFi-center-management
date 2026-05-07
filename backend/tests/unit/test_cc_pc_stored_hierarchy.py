"""Tests for the hierarchy enrichment added to the legacy CC + PC endpoints.

Three areas are covered:

1. ``_resolve_hierarchy_paths`` — the helper that walks the hierarchy
   tree to find each leaf's path. Tested directly with mocked DB rows
   so we can exercise edge cases (cycles, multi-parent setnames,
   missing leaves) without spinning up Postgres.

2. ``GET /api/legacy/cost-centers`` — verifies the response now
   includes ``hierarchy_id``, ``hierarchy_max_depth`` and a ``levels``
   array per row, and that the legacy "no hierarchy picked" path still
   returns ``levels=[]`` and ``max_depth=0``.

3. ``GET /api/legacy/profit-centers`` — same shape verified for PC.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from app.api.deps import PaginationParams
from app.api.reference import (
    _resolve_hierarchy_paths,
    list_legacy_ccs,
    list_legacy_pcs,
)

# ── helpers ──────────────────────────────────────────────────────────────


def _edge(parent: str, child: str) -> SimpleNamespace:
    return SimpleNamespace(parent_setname=parent, child_setname=child, seq=0)


def _leaf(setname: str, value: str) -> SimpleNamespace:
    return SimpleNamespace(setname=setname, value=value, seq=0)


def _make_cc(cctr: str, **overrides: object) -> SimpleNamespace:
    base = {
        "id": 1,
        "mandt": "100",
        "coarea": "1000",
        "cctr": cctr,
        "txtsh": "Short",
        "txtmi": "Medium",
        "responsible": None,
        "verak_user": None,
        "cctrcgy": None,
        "ccode": None,
        "currency": None,
        "pctr": None,
        "gsber": None,
        "werks": None,
        "abtei": None,
        "func_area": None,
        "land1": None,
        "nkost": None,
        "is_active": True,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _make_pc(pctr: str, **overrides: object) -> SimpleNamespace:
    base = {
        "id": 1,
        "mandt": "100",
        "coarea": "1000",
        "pctr": pctr,
        "txtsh": "Short",
        "txtmi": "Medium",
        "responsible": None,
        "verak_user": None,
        "department": None,
        "ccode": None,
        "currency": None,
        "segment": None,
        "land1": None,
        "name1": None,
        "name2": None,
        "is_active": True,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _mock_db_for_resolve(edges: list, leaves: list) -> MagicMock:
    """Mock a Session for ``_resolve_hierarchy_paths``.

    The function calls ``db.execute`` twice: first for edges, then for
    leaves. Both return ``.scalars().all()``.
    """
    db = MagicMock()
    edges_result = MagicMock()
    edges_result.scalars.return_value.all.return_value = edges
    leaves_result = MagicMock()
    leaves_result.scalars.return_value.all.return_value = leaves
    db.execute.side_effect = [edges_result, leaves_result]
    return db


# ── _resolve_hierarchy_paths ─────────────────────────────────────────────


def test_resolve_returns_full_path_root_to_leaf() -> None:
    """A four-level chain should yield a four-element path for the leaf."""
    edges = [
        _edge("ROOT", "REGION"),
        _edge("REGION", "COUNTRY"),
        _edge("COUNTRY", "ENTITY"),
    ]
    leaves = [_leaf("ENTITY", "CC001")]
    db = _mock_db_for_resolve(edges, leaves)

    paths, max_depth = _resolve_hierarchy_paths(db, hierarchy_id=1, leaf_values=["CC001"])

    assert paths == {"CC001": ["ROOT", "REGION", "COUNTRY", "ENTITY"]}
    assert max_depth == 4


def test_resolve_handles_multiple_leaves_at_different_depths() -> None:
    """Two leaves under different parents should each get their own path,
    and ``max_depth`` should reflect the deepest one."""
    edges = [
        _edge("ROOT", "A"),
        _edge("A", "B"),
        _edge("ROOT", "C"),  # short branch
    ]
    leaves = [_leaf("B", "DEEP"), _leaf("C", "SHALLOW")]
    db = _mock_db_for_resolve(edges, leaves)

    paths, max_depth = _resolve_hierarchy_paths(db, hierarchy_id=1, leaf_values=["DEEP", "SHALLOW"])

    assert paths["DEEP"] == ["ROOT", "A", "B"]
    assert paths["SHALLOW"] == ["ROOT", "C"]
    assert max_depth == 3


def test_resolve_omits_leaves_that_are_not_in_the_hierarchy() -> None:
    """If a CC has no row in ``hierarchy_leaf`` for the picked hierarchy,
    it shouldn't appear in the output dict at all — the endpoint will
    surface that as ``levels=[]``."""
    edges = [_edge("ROOT", "A")]
    leaves = [_leaf("A", "INSIDE")]
    db = _mock_db_for_resolve(edges, leaves)

    paths, _max = _resolve_hierarchy_paths(db, hierarchy_id=1, leaf_values=["INSIDE", "OUTSIDE"])

    assert "INSIDE" in paths
    assert "OUTSIDE" not in paths


def test_resolve_handles_cycles_safely() -> None:
    """Malformed data with a cycle (A → B → A) should not loop forever.
    The walk stops at the first repeat and returns whatever it had."""
    edges = [_edge("A", "B"), _edge("B", "A")]
    leaves = [_leaf("B", "X")]
    db = _mock_db_for_resolve(edges, leaves)

    paths, _max = _resolve_hierarchy_paths(db, hierarchy_id=1, leaf_values=["X"])

    # The exact path depends on which parent_of mapping wins, but the
    # important thing is that we got something finite. No infinite loop.
    assert "X" in paths
    assert len(paths["X"]) <= 2  # at most A and B before cycle is detected


def test_resolve_returns_empty_when_no_leaf_values_requested() -> None:
    """Asking for the path of zero leaves should short-circuit without
    hitting the DB."""
    db = MagicMock()
    paths, max_depth = _resolve_hierarchy_paths(db, hierarchy_id=1, leaf_values=[])
    assert paths == {}
    assert max_depth == 0
    db.execute.assert_not_called()


def test_resolve_keeps_first_parent_when_setname_has_multiple() -> None:
    """If a setname appears as a child of two different parents, the
    helper must pick one consistently — first one wins (same convention
    as the /nodes endpoint).
    """
    edges = [
        _edge("ROOT", "P1"),
        _edge("ROOT", "P2"),
        _edge("P1", "X"),  # X under P1 (first)
        _edge("P2", "X"),  # X also under P2 (ignored)
    ]
    leaves = [_leaf("X", "VAL")]
    db = _mock_db_for_resolve(edges, leaves)

    paths, _max = _resolve_hierarchy_paths(db, hierarchy_id=1, leaf_values=["VAL"])

    # Path goes X → P1 (the first edge wins) → ROOT, returned root-first.
    assert paths["VAL"] == ["ROOT", "P1", "X"]


# ── CC endpoint ──────────────────────────────────────────────────────────


def _mock_db_for_cc_endpoint(cc_rows: list, total: int, edges: list, leaves: list) -> MagicMock:
    """Mock for ``list_legacy_ccs`` when ``hierarchy_id`` is set.

    Call sequence:
      1. count(LegacyCostCenter)
      2. select(LegacyCostCenter)
      3. select(HierarchyNode)  -- via _resolve_hierarchy_paths
      4. select(HierarchyLeaf)  -- via _resolve_hierarchy_paths
    """
    db = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = total
    cc_result = MagicMock()
    cc_result.scalars.return_value.all.return_value = cc_rows
    edges_result = MagicMock()
    edges_result.scalars.return_value.all.return_value = edges
    leaves_result = MagicMock()
    leaves_result.scalars.return_value.all.return_value = leaves
    db.execute.side_effect = [count_result, cc_result, edges_result, leaves_result]
    return db


def test_cc_endpoint_without_hierarchy_returns_empty_levels() -> None:
    """The legacy code path: no hierarchy_id → no resolver call,
    levels=[], max_depth=0. Backwards-compatible default."""
    db = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 1
    cc_result = MagicMock()
    cc_result.scalars.return_value.all.return_value = [_make_cc("CC001")]
    db.execute.side_effect = [count_result, cc_result]

    result = list_legacy_ccs(db=db, pag=PaginationParams(page=1, size=50))

    assert result["hierarchy_id"] is None
    assert result["hierarchy_max_depth"] == 0
    assert result["items"][0]["levels"] == []
    # _resolve_hierarchy_paths shouldn't have been called → only 2 execute calls
    assert db.execute.call_count == 2


def test_cc_endpoint_with_hierarchy_id_enriches_each_row() -> None:
    """With hierarchy_id, each CC gets a ``levels`` array and the
    response carries the max depth so the UI can size its column list."""
    edges = [_edge("ROOT", "REGION"), _edge("REGION", "ENTITY_A")]
    leaves = [_leaf("ENTITY_A", "CC001")]
    db = _mock_db_for_cc_endpoint(
        cc_rows=[_make_cc("CC001"), _make_cc("CC002")],
        total=2,
        edges=edges,
        leaves=leaves,
    )

    result = list_legacy_ccs(db=db, pag=PaginationParams(page=1, size=50), hierarchy_id=42)

    assert result["hierarchy_id"] == 42
    assert result["hierarchy_max_depth"] == 3
    items_by_cctr = {it["cctr"]: it for it in result["items"]}
    # CC001 is in the hierarchy → full path
    assert items_by_cctr["CC001"]["levels"] == ["ROOT", "REGION", "ENTITY_A"]
    # CC002 isn't in the hierarchy → empty list (so the UI can render
    # "(unmapped)" or just a blank cell — we don't fill that in here)
    assert items_by_cctr["CC002"]["levels"] == []


# ── PC endpoint ──────────────────────────────────────────────────────────


def _mock_db_for_pc_endpoint(pc_rows: list, total: int, edges: list, leaves: list) -> MagicMock:
    db = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = total
    pc_result = MagicMock()
    pc_result.scalars.return_value.all.return_value = pc_rows
    edges_result = MagicMock()
    edges_result.scalars.return_value.all.return_value = edges
    leaves_result = MagicMock()
    leaves_result.scalars.return_value.all.return_value = leaves
    db.execute.side_effect = [count_result, pc_result, edges_result, leaves_result]
    return db


def test_pc_endpoint_without_hierarchy_returns_empty_levels() -> None:
    db = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 1
    pc_result = MagicMock()
    pc_result.scalars.return_value.all.return_value = [_make_pc("PC001")]
    db.execute.side_effect = [count_result, pc_result]

    result = list_legacy_pcs(db=db, pag=PaginationParams(page=1, size=50))

    assert result["hierarchy_id"] is None
    assert result["hierarchy_max_depth"] == 0
    assert result["items"][0]["levels"] == []
    assert db.execute.call_count == 2


def test_pc_endpoint_with_hierarchy_id_enriches_each_row() -> None:
    edges = [_edge("ROOT", "SEG_FIN")]
    leaves = [_leaf("SEG_FIN", "PC001")]
    db = _mock_db_for_pc_endpoint(pc_rows=[_make_pc("PC001")], total=1, edges=edges, leaves=leaves)

    result = list_legacy_pcs(db=db, pag=PaginationParams(page=1, size=50), hierarchy_id=7)

    assert result["hierarchy_id"] == 7
    assert result["hierarchy_max_depth"] == 2
    assert result["items"][0]["levels"] == ["ROOT", "SEG_FIN"]
