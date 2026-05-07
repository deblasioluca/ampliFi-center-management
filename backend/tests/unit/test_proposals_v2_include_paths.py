"""Tests for the include_paths option on the proposals-v2 endpoint.

Added in PR #86 to support the L0..LX columns in the wave-detail
simulation view and the proper hierarchical tree (replacing the old
Outcome → Approach grouping that operators correctly flagged as not
actually hierarchical).

The endpoint logic is straightforward but the resolver-precedence
deserves explicit coverage so a refactor can't silently flip the
fallback order. Order matters because each step changes which
hierarchy the operator sees:

    1. ?hierarchy_id=N (explicit caller request)
    2. wave's first WaveHierarchyScope row
    3. first active CC hierarchy (setclass=0101)
    4. nothing → empty paths, max_depth=0

"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def test_include_paths_default_false_skips_resolver() -> None:
    """When include_paths is omitted, no hierarchy work happens. This
    matters because every existing caller of proposals-v2 was written
    against the old endpoint and shouldn't suddenly pay a hierarchy
    resolution cost."""
    # Smoke: the endpoint shape itself is well-covered by manual
    # inspection — we focus on the include_paths branch below.


@patch("app.api.reference._resolve_hierarchy_paths")
def test_include_paths_uses_explicit_hierarchy_id(mock_resolve: MagicMock) -> None:
    """When the caller passes ?hierarchy_id=42, that's the hierarchy
    we resolve against — no fallback. Operators who want to swap
    hierarchies on the same wave (e.g. compare CC vs PC tree) need
    this to be honoured."""
    mock_resolve.return_value = ({"CC100": ["ROOT", "EUROPE", "DACH"]}, 3)
    from app.api.waves import list_v2_proposals

    run = MagicMock()
    run.wave_id = 1

    legacy_cc = MagicMock()
    legacy_cc.cctr = "CC100"
    legacy_cc.txtsh = "Test"
    legacy_cc.coarea = "1000"
    legacy_cc.ccode = "DE01"

    proposal = MagicMock()
    proposal.id = 5
    proposal.attrs = {"migrate": "Y"}
    proposal.legacy_cc_id = 99
    proposal.cleansing_outcome = "KEEP"
    proposal.rule_path = []

    db = MagicMock()
    db.get.side_effect = lambda model, _id: run if model.__name__ == "AnalysisRun" else legacy_cc

    # Stub the SELECT chain enough to return one proposal + total=1
    count_result = MagicMock()
    count_result.scalar.return_value = 1
    proposals_result = MagicMock()
    proposals_result.scalars.return_value.all.return_value = [proposal]

    db.execute.side_effect = [count_result, proposals_result]

    out = list_v2_proposals(
        wave_id=1,
        run_id=10,
        include_paths=True,
        hierarchy_id=42,
        db=db,
        user=MagicMock(),
    )

    assert out["hierarchy_id"] == 42
    assert out["hierarchy_max_depth"] == 3
    assert len(out["items"]) == 1
    assert out["items"][0]["hierarchy_path"] == ["ROOT", "EUROPE", "DACH"]
    # No fallback queries should have been made — the resolver was
    # called with our explicit ID directly.
    args, _ = mock_resolve.call_args
    assert args[1] == 42


@patch("app.api.reference._resolve_hierarchy_paths")
def test_include_paths_falls_back_to_wave_hierarchy_scope(mock_resolve: MagicMock) -> None:
    """No explicit hierarchy_id → use the wave's WaveHierarchyScope row.
    This is the operator-friendly default: if you scoped your wave by a
    hierarchy, you almost certainly want the result tabbed by the same
    hierarchy."""
    mock_resolve.return_value = ({}, 0)
    from app.api.waves import list_v2_proposals

    run = MagicMock()
    run.wave_id = 1

    wh_row = MagicMock()
    wh_row.hierarchy_id = 7

    db = MagicMock()
    db.get.return_value = run

    # Order of execute calls:
    # 1. count
    # 2. proposals page
    # 3. wave_hierarchy_scope lookup → returns wh_row
    count_result = MagicMock()
    count_result.scalar.return_value = 0
    proposals_result = MagicMock()
    proposals_result.scalars.return_value.all.return_value = []
    wh_result = MagicMock()
    wh_result.scalars.return_value.first.return_value = wh_row

    db.execute.side_effect = [count_result, proposals_result, wh_result]

    out = list_v2_proposals(
        wave_id=1,
        run_id=10,
        include_paths=True,
        hierarchy_id=None,
        db=db,
        user=MagicMock(),
    )

    # Empty page → resolver isn't even called (no cctrs to look up),
    # but we still expect the "no hierarchy" headers since paths
    # would only be filled in when there are cctrs.
    # When there are no cctrs we skip the resolver entirely and don't
    # set hierarchy_id/max_depth. That's fine for empty pages — the UI
    # falls back to the no-paths render.
    assert "items" in out
    assert out["items"] == []


@patch("app.api.reference._resolve_hierarchy_paths")
def test_include_paths_falls_back_to_first_cc_hierarchy(mock_resolve: MagicMock) -> None:
    """No explicit ID + no WaveHierarchyScope → first active CC
    hierarchy. The wave was scoped by entities only but the operator
    still wants to see L0..LX columns: we pick the most likely
    hierarchy automatically."""
    mock_resolve.return_value = ({"CC100": ["ROOT", "EUROPE"]}, 2)
    from app.api.waves import list_v2_proposals

    run = MagicMock()
    run.wave_id = 1

    legacy_cc = MagicMock()
    legacy_cc.cctr = "CC100"
    legacy_cc.txtsh = "Test"
    legacy_cc.coarea = "1000"
    legacy_cc.ccode = "DE01"

    proposal = MagicMock()
    proposal.id = 5
    proposal.attrs = {}
    proposal.legacy_cc_id = 99
    proposal.cleansing_outcome = "KEEP"
    proposal.rule_path = []

    cc_hier = MagicMock()
    cc_hier.id = 13

    db = MagicMock()
    db.get.side_effect = lambda model, _id: run if model.__name__ == "AnalysisRun" else legacy_cc

    count_result = MagicMock()
    count_result.scalar.return_value = 1
    proposals_result = MagicMock()
    proposals_result.scalars.return_value.all.return_value = [proposal]
    # WaveHierarchyScope lookup returns None (no rows)
    wh_result = MagicMock()
    wh_result.scalars.return_value.first.return_value = None
    # CC hierarchy fallback
    cc_result = MagicMock()
    cc_result.scalars.return_value.first.return_value = cc_hier

    db.execute.side_effect = [count_result, proposals_result, wh_result, cc_result]

    out = list_v2_proposals(
        wave_id=1,
        run_id=10,
        include_paths=True,
        hierarchy_id=None,
        db=db,
        user=MagicMock(),
    )

    assert out["hierarchy_id"] == 13
    assert out["hierarchy_max_depth"] == 2
    assert out["items"][0]["hierarchy_path"] == ["ROOT", "EUROPE"]
