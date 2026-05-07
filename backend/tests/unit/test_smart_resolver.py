"""Tests for the setclass-aware hierarchy resolvers in
``backend/app/api/reference.py`` (PR #90).

Operator bug: selecting an Entity hierarchy on the Cost Centers tab
gave no L0..LX columns because ``_resolve_hierarchy_paths`` looked up
leaves by cctr but Entity-hierarchy leaves are ccodes. The new
``_resolve_paths_for_ccs`` and ``_resolve_paths_for_pcs`` helpers pick
the right lookup field based on the hierarchy's setclass.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _cc(cctr: str, ccode: str = "", pctr: str = ""):
    """Build a stand-in for LegacyCostCenter with just the fields the
    resolvers read."""
    m = MagicMock()
    m.cctr = cctr
    m.ccode = ccode
    m.pctr = pctr
    return m


def _pc(pctr: str, ccode: str = ""):
    m = MagicMock()
    m.pctr = pctr
    m.ccode = ccode
    return m


def test_resolve_paths_for_ccs_entity_hierarchy_uses_ccode():
    """setclass=0106 → look up via cc.ccode, return paths keyed by cc.cctr."""
    from app.api.reference import _resolve_paths_for_ccs

    db = MagicMock()
    hier = MagicMock(setclass="0106")
    db.get.return_value = hier

    ccs = [
        _cc(cctr="00001001", ccode="CH01"),
        _cc(cctr="00001002", ccode="CH01"),
        _cc(cctr="00001003", ccode="US88"),
    ]

    # Patch the underlying resolver — verify it's called with ccodes,
    # not cctrs, and that we map back to cctr keys correctly.
    fake_paths = {
        "CH01": ["UBS_GROUP", "EUR", "CH", "Bank", "CH01"],
        "US88": ["UBS_GROUP", "AMER", "US", "Bank", "US88"],
    }
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=(fake_paths, 5),
    ) as mock_inner:
        paths, max_depth = _resolve_paths_for_ccs(db, hierarchy_id=1, ccs=ccs)

    # The inner resolver was called with ccodes (de-duplicated)
    args, kwargs = mock_inner.call_args
    db_arg, hier_arg, keys_arg = args
    assert sorted(keys_arg) == ["CH01", "US88"]

    # Returned paths are keyed by cctr, not ccode
    assert paths == {
        "00001001": ["UBS_GROUP", "EUR", "CH", "Bank", "CH01"],
        "00001002": ["UBS_GROUP", "EUR", "CH", "Bank", "CH01"],
        "00001003": ["UBS_GROUP", "AMER", "US", "Bank", "US88"],
    }
    assert max_depth == 5


def test_resolve_paths_for_ccs_pc_hierarchy_uses_pctr():
    """setclass=0104 → look up via cc.pctr."""
    from app.api.reference import _resolve_paths_for_ccs

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="0104")

    ccs = [
        _cc(cctr="00001001", pctr="P0001001"),
        _cc(cctr="00001002", pctr="P0001002"),
        _cc(cctr="00001003", pctr=""),  # no pctr → not resolvable
    ]

    fake_paths = {
        "P0001001": ["PC_ROOT", "Tier1", "P0001001"],
        "P0001002": ["PC_ROOT", "Tier2", "P0001002"],
    }
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=(fake_paths, 3),
    ):
        paths, max_depth = _resolve_paths_for_ccs(db, hierarchy_id=2, ccs=ccs)

    assert paths == {
        "00001001": ["PC_ROOT", "Tier1", "P0001001"],
        "00001002": ["PC_ROOT", "Tier2", "P0001002"],
    }
    # cctr 00001003 doesn't appear — correct (no pctr)
    assert "00001003" not in paths
    assert max_depth == 3


def test_resolve_paths_for_ccs_cc_hierarchy_uses_cctr():
    """setclass=0101 → identity, look up via cc.cctr (preserves the
    pre-PR-#90 behaviour for CC hierarchies)."""
    from app.api.reference import _resolve_paths_for_ccs

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="0101")

    ccs = [_cc(cctr="00001001"), _cc(cctr="00001002")]

    fake_paths = {
        "00001001": ["CC_ROOT", "DIV1", "00001001"],
        "00001002": ["CC_ROOT", "DIV2", "00001002"],
    }
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=(fake_paths, 3),
    ) as mock_inner:
        paths, max_depth = _resolve_paths_for_ccs(db, hierarchy_id=3, ccs=ccs)

    # Inner called with cctrs
    args, _ = mock_inner.call_args
    assert sorted(args[2]) == ["00001001", "00001002"]
    assert paths == fake_paths
    assert max_depth == 3


def test_resolve_paths_for_ccs_unknown_setclass_falls_back_to_cctr():
    """Unknown setclass — fall through to cctr lookup so behaviour
    matches pre-PR-#90 for any custom CC-style hierarchy."""
    from app.api.reference import _resolve_paths_for_ccs

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="9999")

    ccs = [_cc(cctr="00001001")]
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=({"00001001": ["X"]}, 1),
    ) as mock_inner:
        _resolve_paths_for_ccs(db, hierarchy_id=4, ccs=ccs)

    args, _ = mock_inner.call_args
    assert args[2] == ["00001001"]


def test_resolve_paths_for_ccs_empty_returns_empty():
    from app.api.reference import _resolve_paths_for_ccs

    paths, depth = _resolve_paths_for_ccs(MagicMock(), hierarchy_id=1, ccs=[])
    assert paths == {}
    assert depth == 0


def test_resolve_paths_for_ccs_missing_hierarchy_returns_empty():
    """If the hierarchy itself doesn't exist (deleted between the
    operator's request and our query), return empty — not a 500."""
    from app.api.reference import _resolve_paths_for_ccs

    db = MagicMock()
    db.get.return_value = None
    paths, depth = _resolve_paths_for_ccs(db, hierarchy_id=999, ccs=[_cc(cctr="00001001")])
    assert paths == {}
    assert depth == 0


def test_resolve_paths_for_pcs_pc_hierarchy_uses_pctr():
    """setclass=0104 → look up by pc.pctr (the natural case)."""
    from app.api.reference import _resolve_paths_for_pcs

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="0104")

    pcs = [_pc(pctr="P0001001"), _pc(pctr="P0001002")]
    fake_paths = {"P0001001": ["A", "B"], "P0001002": ["A", "C"]}
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=(fake_paths, 2),
    ):
        paths, _ = _resolve_paths_for_pcs(db, hierarchy_id=1, pcs=pcs)

    assert paths == fake_paths


def test_resolve_paths_for_pcs_entity_hierarchy_uses_ccode():
    """Entity hierarchy on PC tab → resolve via pc.ccode."""
    from app.api.reference import _resolve_paths_for_pcs

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="0106")

    pcs = [_pc(pctr="P0001001", ccode="CH01"), _pc(pctr="P0001002", ccode="US88")]
    fake_paths = {"CH01": ["E", "CH01"], "US88": ["E", "US88"]}
    with patch(
        "app.api.reference._resolve_hierarchy_paths",
        return_value=(fake_paths, 2),
    ):
        paths, _ = _resolve_paths_for_pcs(db, hierarchy_id=1, pcs=pcs)

    # Returns paths keyed by pctr, not ccode
    assert paths == {
        "P0001001": ["E", "CH01"],
        "P0001002": ["E", "US88"],
    }


def test_resolve_paths_for_pcs_cc_hierarchy_returns_empty():
    """CC hierarchy on PC tab makes no sense — PCs aren't in CC trees.
    Returns empty rather than silently giving wrong results."""
    from app.api.reference import _resolve_paths_for_pcs

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="0101")

    pcs = [_pc(pctr="P0001001", ccode="CH01")]
    paths, depth = _resolve_paths_for_pcs(db, hierarchy_id=1, pcs=pcs)
    assert paths == {}
    assert depth == 0
