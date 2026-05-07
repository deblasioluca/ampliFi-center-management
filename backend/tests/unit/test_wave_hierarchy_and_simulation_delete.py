"""Tests for two endpoints touched by this PR:

1. ``GET /api/legacy/hierarchies/{id}/nodes`` — the rewritten endpoint
   that returns a flat list of setname-based nodes with computed level
   and parent (instead of the raw edge list it returned before).

2. ``DELETE /api/waves/simulations/{run_id}`` — the new hard-delete
   endpoint, including the "activated runs cannot be deleted" guard.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.api.reference import list_hierarchy_nodes
from app.api.waves import delete_simulation

# ── Helpers for the hierarchy-nodes tests ────────────────────────────────


def _make_edge(parent: str, child: str, seq: int = 0) -> SimpleNamespace:
    return SimpleNamespace(parent_setname=parent, child_setname=child, seq=seq)


def _make_hier(setname: str = "ROOT", description: str = "Test hierarchy") -> SimpleNamespace:
    return SimpleNamespace(id=1, setname=setname, description=description)


def _mock_db_for_nodes(hier: SimpleNamespace | None, edges: list) -> MagicMock:
    """Mock a Session for the /nodes endpoint.

    The endpoint does:
      1. db.get(Hierarchy, hier_id) → hier or None
      2. db.execute(select(HierarchyNode)...) → scalars().all() == edges
    """
    db = MagicMock()
    db.get.return_value = hier
    edges_result = MagicMock()
    edges_result.scalars.return_value.all.return_value = edges
    db.execute.return_value = edges_result
    return db


# ── Hierarchy nodes — happy path: simple two-level tree ──────────────────


def test_nodes_returns_root_plus_children_with_computed_levels() -> None:
    """A → B, A → C should yield three nodes: A (root, level 0) + B,C (level 1)."""
    hier = _make_hier(setname="A", description="Group hierarchy")
    edges = [_make_edge("A", "B", 0), _make_edge("A", "C", 1)]
    db = _mock_db_for_nodes(hier, edges)

    result = list_hierarchy_nodes(hier_id=1, db=db)

    assert result["hierarchy_id"] == 1
    assert result["setname"] == "A"
    items_by_setname = {it["setname"]: it for it in result["items"]}
    assert set(items_by_setname.keys()) == {"A", "B", "C"}
    assert items_by_setname["A"] == {
        "setname": "A",
        "parent": "",
        "level": 0,
        "description": "Group hierarchy",
    }
    assert items_by_setname["B"]["level"] == 1
    assert items_by_setname["B"]["parent"] == "A"
    assert items_by_setname["B"]["description"] is None
    assert items_by_setname["C"]["level"] == 1
    assert items_by_setname["C"]["parent"] == "A"


# ── Multi-level: a five-level UBS-style hierarchy ────────────────────────


def test_nodes_handles_five_level_group_region_country_type_entity() -> None:
    """Mimics UBS_GROUP_ENT: Group → Region → Country → Type → Entity.

    Verifies that BFS depth assignment scales beyond two levels and that
    the items are returned sorted by (level, setname).
    """
    hier = _make_hier(setname="GROUP")
    edges = [
        _make_edge("GROUP", "REGION_EU"),
        _make_edge("GROUP", "REGION_AM"),
        _make_edge("REGION_EU", "COUNTRY_CH"),
        _make_edge("REGION_EU", "COUNTRY_DE"),
        _make_edge("COUNTRY_CH", "TYPE_BANK"),
        _make_edge("TYPE_BANK", "ENTITY_UBS_AG"),
    ]
    db = _mock_db_for_nodes(hier, edges)

    items = list_hierarchy_nodes(hier_id=1, db=db)["items"]

    levels = {it["setname"]: it["level"] for it in items}
    assert levels["GROUP"] == 0
    assert levels["REGION_EU"] == 1
    assert levels["REGION_AM"] == 1
    assert levels["COUNTRY_CH"] == 2
    assert levels["COUNTRY_DE"] == 2
    assert levels["TYPE_BANK"] == 3
    assert levels["ENTITY_UBS_AG"] == 4

    # Sort order: by (level, setname) — items at the same level should
    # come out alphabetically.
    actual_order = [it["setname"] for it in items]
    expected_order = [
        "GROUP",
        "REGION_AM",
        "REGION_EU",
        "COUNTRY_CH",
        "COUNTRY_DE",
        "TYPE_BANK",
        "ENTITY_UBS_AG",
    ]
    assert actual_order == expected_order


# ── Missing hierarchy → 404 ──────────────────────────────────────────────


def test_nodes_returns_404_for_unknown_hierarchy() -> None:
    db = _mock_db_for_nodes(hier=None, edges=[])
    with pytest.raises(HTTPException) as exc:
        list_hierarchy_nodes(hier_id=999, db=db)
    assert exc.value.status_code == 404


# ── No edges at all → falls back to the hierarchy's own setname ──────────


def test_nodes_falls_back_to_hierarchy_setname_when_no_edges() -> None:
    """If hierarchy_node has no rows for this hierarchy, the only node
    we can show is the hierarchy's setname itself (as the lonely root)."""
    hier = _make_hier(setname="EMPTY_HIER", description="Hierarchy with no nodes")
    db = _mock_db_for_nodes(hier, edges=[])

    result = list_hierarchy_nodes(hier_id=1, db=db)

    assert len(result["items"]) == 1
    assert result["items"][0]["setname"] == "EMPTY_HIER"
    assert result["items"][0]["level"] == 0
    assert result["items"][0]["parent"] == ""


# ── Setname appearing under multiple parents: keep the first (shallowest) ─


def test_nodes_keeps_shallowest_depth_when_setname_has_multiple_parents() -> None:
    """``X`` is reachable as A→X (depth 1) and A→B→X (depth 2). The
    shallower path should win — that's the natural display position
    in a UI tree picker.
    """
    hier = _make_hier(setname="A")
    edges = [
        _make_edge("A", "B"),
        _make_edge("A", "X"),  # X at depth 1
        _make_edge("B", "X"),  # X also reachable at depth 2
    ]
    db = _mock_db_for_nodes(hier, edges)

    items_by_setname = {it["setname"]: it for it in list_hierarchy_nodes(hier_id=1, db=db)["items"]}
    assert items_by_setname["X"]["level"] == 1
    assert items_by_setname["X"]["parent"] == "A"


# ── Description only on the root, never on inner nodes ──────────────────


def test_nodes_description_only_on_root() -> None:
    """The hierarchy_node table doesn't have per-node descriptions, so
    the endpoint surfaces the hierarchy's description only on the root.
    Inner nodes return ``None`` for description."""
    hier = _make_hier(setname="ROOT", description="Top-level description")
    edges = [_make_edge("ROOT", "CHILD")]
    db = _mock_db_for_nodes(hier, edges)

    items_by_setname = {it["setname"]: it for it in list_hierarchy_nodes(hier_id=1, db=db)["items"]}
    assert items_by_setname["ROOT"]["description"] == "Top-level description"
    assert items_by_setname["CHILD"]["description"] is None


# ── Delete simulation endpoint ───────────────────────────────────────────


def _make_run(run_id: int = 1, mode: str = "simulation") -> SimpleNamespace:
    return SimpleNamespace(id=run_id, mode=mode)


def _mock_db_for_delete(run: SimpleNamespace | None) -> MagicMock:
    db = MagicMock()
    db.get.return_value = run
    return db


def test_delete_simulation_removes_the_run() -> None:
    run = _make_run(run_id=42, mode="simulation")
    db = _mock_db_for_delete(run)
    user = MagicMock()

    result = delete_simulation(run_id=42, db=db, _user=user)

    db.delete.assert_called_once_with(run)
    db.commit.assert_called_once()
    assert result == {"status": "deleted", "id": 42}


def test_delete_simulation_404_when_run_not_found() -> None:
    db = _mock_db_for_delete(run=None)
    user = MagicMock()

    with pytest.raises(HTTPException) as exc:
        delete_simulation(run_id=999, db=db, _user=user)
    assert exc.value.status_code == 404
    db.delete.assert_not_called()
    db.commit.assert_not_called()


def test_delete_simulation_409_when_run_is_activated() -> None:
    """Activated runs represent committed state and cannot be deleted
    through this endpoint — the operator needs an explicit revert flow."""
    run = _make_run(run_id=42, mode="activated")
    db = _mock_db_for_delete(run)
    user = MagicMock()

    with pytest.raises(HTTPException) as exc:
        delete_simulation(run_id=42, db=db, _user=user)
    assert exc.value.status_code == 409
    assert "activated" in exc.value.detail.lower()
    db.delete.assert_not_called()
    db.commit.assert_not_called()
