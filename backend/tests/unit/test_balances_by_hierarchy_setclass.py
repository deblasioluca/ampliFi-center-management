"""Tests for setclass-aware ``GET /api/legacy/balances/by-hierarchy``
(PR #90).

Operator request: "Balances should support entity AND cost center
hierarchies". The endpoint already worked for CC hierarchies; this
extends it to entity (0106) and PC (0104).
"""

from __future__ import annotations

from unittest.mock import MagicMock


def test_balances_by_hierarchy_returns_empty_for_unknown_hierarchy():
    """If the hierarchy id doesn't resolve, return an empty result
    block — not a 500, not stale data."""
    from app.api.reference import balances_by_hierarchy

    db = MagicMock()
    db.get.return_value = None

    out = balances_by_hierarchy(hierarchy_id=999, db=db)
    assert out["max_depth"] == 0
    assert out["total_items"] == 0
    assert out["items"] == []


def test_balances_by_hierarchy_cc_setclass_uses_cctr_join():
    """setclass=0101 (CC) — original behaviour, JOIN on Balance.cctr."""
    from app.api.reference import balances_by_hierarchy

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="0101")

    # Aggregation result: one cctr with totals
    from types import SimpleNamespace

    bal_row = SimpleNamespace(
        cctr="00001001", ccode="CH01", tc_amt=1000.0, posting_count=50, rows=10
    )
    # Three execute calls: aggregation, path resolution sub-call gets
    # mocked too via the response, txtsh lookup
    db.execute.side_effect = [
        MagicMock(all=lambda: [bal_row]),  # main aggregation
        # _resolve_hierarchy_paths internals — mocked to return empty
        # (we're testing the JOIN selection, not the resolution itself)
        MagicMock(scalars=lambda: MagicMock(all=list)),
        MagicMock(scalars=lambda: MagicMock(all=list)),
        MagicMock(all=lambda: [("00001001", "TM Operat 001")]),  # txtsh
    ]

    out = balances_by_hierarchy(hierarchy_id=1, db=db)
    assert out["hierarchy_setclass"] == "0101"
    assert out["total_items"] == 1
    assert out["items"][0]["cctr"] == "00001001"
    assert out["items"][0]["txtsh"] == "TM Operat 001"
    assert out["items"][0]["totals"]["tc_amt"] == 1000.0


def test_balances_by_hierarchy_entity_setclass_response_carries_setclass():
    """setclass=0106 (Entity) — the response includes the setclass so
    the frontend can label/render correctly."""
    from app.api.reference import balances_by_hierarchy

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="0106")
    db.execute.side_effect = [
        MagicMock(all=list),  # aggregation: no rows
    ]

    out = balances_by_hierarchy(hierarchy_id=1, db=db)
    assert out["hierarchy_setclass"] == "0106"
    assert out["items"] == []


def test_balances_by_hierarchy_pc_setclass_response_carries_setclass():
    """setclass=0104 (PC)."""
    from app.api.reference import balances_by_hierarchy

    db = MagicMock()
    db.get.return_value = MagicMock(setclass="0104")
    db.execute.side_effect = [
        MagicMock(all=list),
    ]

    out = balances_by_hierarchy(hierarchy_id=1, db=db)
    assert out["hierarchy_setclass"] == "0104"
