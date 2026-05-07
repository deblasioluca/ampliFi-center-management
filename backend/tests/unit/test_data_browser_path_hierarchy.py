"""Test the ``?path_hierarchy_id=N`` parameter on /api/runs/{id}/data-browser
(PR #88).

When set, every item in the response carries a ``hierarchy_path`` array
(setname chain from L0 down to the leaf in the chosen hierarchy) and
the response carries ``path_max_depth`` so the frontend can size the
dynamic L0..LX column header list.

Tests use mocks all the way through — we're not exercising real DB
behaviour, just the wiring between the endpoint, the
``_resolve_hierarchy_paths`` helper, and the response shape.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_run(run_id: int = 21) -> MagicMock:
    run = MagicMock()
    run.id = run_id
    return run


def _make_proposal(pid: int, cc_id: int) -> MagicMock:
    p = MagicMock()
    p.id = pid
    p.legacy_cc_id = cc_id
    p.cleansing_outcome = "KEEP"
    p.target_object = None
    p.merge_into_cctr = None
    p.confidence = None
    p.override_outcome = None
    p.override_target = None
    p.rule_path = None
    p.llm_commentary = None
    p.run_id = 21
    return p


def _make_cc(cc_id: int, cctr: str) -> MagicMock:
    cc = MagicMock()
    cc.id = cc_id
    cc.cctr = cctr
    cc.txtsh = f"Center {cctr}"
    cc.txtmi = f"Cost Center {cctr}"
    cc.ccode = "1000"
    cc.coarea = "X100"
    cc.responsible = "Alice"
    cc.pctr = None
    cc.is_active = True
    return cc


def test_data_browser_without_path_hierarchy_id_returns_zero_max_depth() -> None:
    """Default behaviour: no path_hierarchy_id, no hierarchy_path on
    items, path_max_depth=0. This is the common case — the tabular view
    only adds L cols when the picker is non-empty."""
    from app.api.runs import data_browser

    run = _make_run()
    proposals = [_make_proposal(1, 100), _make_proposal(2, 200)]
    ccs = [_make_cc(100, "00010001"), _make_cc(200, "00010002")]

    db = MagicMock()
    db.get.return_value = run

    # Multiple db.execute calls for proposals/ccs/balances. Returns are
    # sequenced via side_effect.
    proposal_result = MagicMock()
    proposal_result.scalars.return_value.all.return_value = proposals
    cc_result = MagicMock()
    cc_result.scalars.return_value.all.return_value = ccs
    bal_result = MagicMock()
    bal_result.all.return_value = []  # no balance rows
    db.execute.side_effect = [proposal_result, cc_result, bal_result]

    out = data_browser(
        run_id=21,
        include_hierarchies=False,
        path_hierarchy_id=None,
        db=db,
        _user=MagicMock(),
    )

    assert out["path_hierarchy_id"] is None
    assert out["path_max_depth"] == 0
    assert len(out["items"]) == 2
    # No hierarchy_path on items when not requested
    for item in out["items"]:
        assert "hierarchy_path" not in item


def test_data_browser_with_path_hierarchy_id_attaches_paths() -> None:
    """When path_hierarchy_id is set, items get hierarchy_path arrays
    and the response carries path_max_depth from the resolver.
    """
    from app.api.runs import data_browser

    run = _make_run()
    proposals = [_make_proposal(1, 100), _make_proposal(2, 200)]
    ccs = [_make_cc(100, "00010001"), _make_cc(200, "00010002")]

    db = MagicMock()
    db.get.return_value = run

    proposal_result = MagicMock()
    proposal_result.scalars.return_value.all.return_value = proposals
    cc_result = MagicMock()
    cc_result.scalars.return_value.all.return_value = ccs
    bal_result = MagicMock()
    bal_result.all.return_value = []
    db.execute.side_effect = [proposal_result, cc_result, bal_result]

    # Stub the resolver — endpoint imports it inside the function so we
    # patch where it's looked up, not where it's defined.
    fake_paths = {
        "00010001": ["UBS_GROUP_CC", "DIV_CC", "FN_CC_Group_Operat", "00010001"],
        "00010002": ["UBS_GROUP_CC", "DIV_CC", "00010002"],
    }
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=(fake_paths, 4),
    ):
        out = data_browser(
            run_id=21,
            include_hierarchies=False,
            path_hierarchy_id=7,
            db=db,
            _user=MagicMock(),
        )

    assert out["path_hierarchy_id"] == 7
    assert out["path_max_depth"] == 4
    assert len(out["items"]) == 2
    # Each item carries the path corresponding to its cctr
    paths_by_cctr = {it["cctr"]: it["hierarchy_path"] for it in out["items"]}
    assert paths_by_cctr["00010001"] == fake_paths["00010001"]
    assert paths_by_cctr["00010002"] == fake_paths["00010002"]


def test_data_browser_path_hierarchy_unmatched_cctr_gets_empty_path() -> None:
    """If the resolver doesn't find a path for a given cctr (e.g. the
    CC isn't part of that hierarchy), the item still gets a
    ``hierarchy_path`` field — an empty list — so the frontend can
    render em-dashes in the L0..LX cells without conditional code.
    """
    from app.api.runs import data_browser

    run = _make_run()
    proposals = [_make_proposal(1, 100)]
    ccs = [_make_cc(100, "ORPHAN001")]

    db = MagicMock()
    db.get.return_value = run

    proposal_result = MagicMock()
    proposal_result.scalars.return_value.all.return_value = proposals
    cc_result = MagicMock()
    cc_result.scalars.return_value.all.return_value = ccs
    bal_result = MagicMock()
    bal_result.all.return_value = []
    db.execute.side_effect = [proposal_result, cc_result, bal_result]

    # Resolver returns empty paths dict — this CC isn't in the hierarchy
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=({}, 0),
    ):
        out = data_browser(
            run_id=21,
            include_hierarchies=False,
            path_hierarchy_id=7,
            db=db,
            _user=MagicMock(),
        )

    assert len(out["items"]) == 1
    # Empty list (not missing key) — the renderer can iterate safely
    assert out["items"][0]["hierarchy_path"] == []
