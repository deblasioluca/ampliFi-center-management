"""Tests for ``lock_proposals`` — verifies V1 and V2 engine behaviour.

Critical regression test: V2 proposals must produce m:1 PC grouping when
the same ``pc_id`` is shared across multiple proposals. V1 proposals must
preserve the legacy 1:1 mapping.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.domain.proposal.service import (
    _proposal_engine,
    _resolve_target_ids,
    get_effective_outcome,
)


class _FakeProposal:
    """Minimal stand-in for CenterProposal in unit tests."""

    def __init__(
        self,
        cleansing_outcome: str = "KEEP",
        target_object: str = "CC_AND_PC",
        attrs: dict | None = None,
        override_outcome: str | None = None,
        override_target: str | None = None,
    ) -> None:
        self.cleansing_outcome = cleansing_outcome
        self.target_object = target_object
        self.attrs = attrs
        self.override_outcome = override_outcome
        self.override_target = override_target


class _FakeLegacy:
    def __init__(
        self,
        cctr: str = "OLD123",
        pctr: str | None = "OLD_PC",
        coarea: str = "1000",
        txtsh: str = "Legacy CC",
    ) -> None:
        self.cctr = cctr
        self.pctr = pctr
        self.coarea = coarea
        self.txtsh = txtsh


# ---------- _proposal_engine ----------


def test_engine_defaults_to_v1_when_attrs_missing() -> None:
    p = _FakeProposal(attrs=None)
    assert _proposal_engine(p) == "v1"  # type: ignore[arg-type]


def test_engine_defaults_to_v1_when_attrs_empty() -> None:
    p = _FakeProposal(attrs={})
    assert _proposal_engine(p) == "v1"  # type: ignore[arg-type]


def test_engine_v2_when_attr_set() -> None:
    p = _FakeProposal(attrs={"engine_version": "v2"})
    assert _proposal_engine(p) == "v2"  # type: ignore[arg-type]


def test_engine_lowercased() -> None:
    p = _FakeProposal(attrs={"engine_version": "V2"})
    assert _proposal_engine(p) == "v2"  # type: ignore[arg-type]


# ---------- _resolve_target_ids: V1 preserves legacy IDs ----------


def test_v1_uses_legacy_cctr_and_pctr() -> None:
    p = _FakeProposal(attrs=None)
    legacy = _FakeLegacy(cctr="OLD123", pctr="OLD_PC")
    cctr, pctr, _, _ = _resolve_target_ids(p, legacy)  # type: ignore[arg-type]
    assert cctr == "OLD123"
    assert pctr == "OLD_PC"


def test_v1_pctr_falls_back_to_cctr_when_legacy_pctr_missing() -> None:
    p = _FakeProposal(attrs=None)
    legacy = _FakeLegacy(cctr="OLD123", pctr=None)
    _, pctr, _, _ = _resolve_target_ids(p, legacy)  # type: ignore[arg-type]
    assert pctr == "OLD123"


# ---------- _resolve_target_ids: V2 uses assigned IDs ----------


def test_v2_uses_assigned_cc_id_and_pc_id() -> None:
    p = _FakeProposal(
        attrs={
            "engine_version": "v2",
            "cc_id": "C00042",
            "pc_id": "P00007",
            "cc_name": "New CC Name",
            "pc_name": "New PC Group",
        }
    )
    legacy = _FakeLegacy(cctr="LEGACY", pctr="LEGACY_PC")
    cctr, pctr, cc_name, pc_name = _resolve_target_ids(p, legacy)  # type: ignore[arg-type]
    assert cctr == "C00042"
    assert pctr == "P00007"
    assert cc_name == "New CC Name"
    assert pc_name == "New PC Group"


def test_v2_falls_back_to_legacy_when_cc_id_missing() -> None:
    """Defensive: if assign_v2_ids did not run, fall back gracefully."""
    p = _FakeProposal(attrs={"engine_version": "v2"})  # no cc_id/pc_id
    legacy = _FakeLegacy(cctr="LEGACY", pctr="LEGACY_PC")
    cctr, pctr, _, _ = _resolve_target_ids(p, legacy)  # type: ignore[arg-type]
    assert cctr == "LEGACY"
    assert pctr == "LEGACY_PC"


# ---------- m:1 grouping behaviour ----------


def test_v2_m_to_1_multiple_proposals_share_pc_id() -> None:
    """Three V2 proposals with same group_key share one pc_id → one PC.

    This is the canonical m:1 model. The dedup at the DB layer (existing-row
    check on (coarea, pctr)) ensures only one TargetProfitCenter row, but the
    semantic guarantee is: the *same* pc_id is returned for all three.
    """
    shared_pc_id = "P00100"
    proposals = [
        _FakeProposal(
            attrs={
                "engine_version": "v2",
                "cc_id": f"C{i:05d}",
                "pc_id": shared_pc_id,
                "group_key": "GROUP_A",
                "approach": "1:n",
            }
        )
        for i in range(3)
    ]
    legacy = _FakeLegacy(coarea="1000")

    pc_ids = {_resolve_target_ids(p, legacy)[1] for p in proposals}  # type: ignore[arg-type]
    cc_ids = {_resolve_target_ids(p, legacy)[0] for p in proposals}  # type: ignore[arg-type]

    assert pc_ids == {shared_pc_id}, "All 1:n proposals must share the same pc_id"
    assert len(cc_ids) == 3, "Each proposal must have its own cc_id"


def test_v2_1_to_1_each_proposal_unique_pc_id() -> None:
    """Three V2 proposals with approach=1:1 each get unique pc_id."""
    proposals = [
        _FakeProposal(
            attrs={
                "engine_version": "v2",
                "cc_id": f"C{i:05d}",
                "pc_id": f"P{i:05d}",
                "approach": "1:1",
            }
        )
        for i in range(3)
    ]
    legacy = _FakeLegacy(coarea="1000")

    pc_ids = {_resolve_target_ids(p, legacy)[1] for p in proposals}  # type: ignore[arg-type]
    assert len(pc_ids) == 3, "1:1 proposals must each have a unique pc_id"


# ---------- get_effective_outcome ----------


def test_get_effective_outcome_returns_base_when_no_override() -> None:
    p = _FakeProposal(cleansing_outcome="KEEP", target_object="CC")
    outcome, target = get_effective_outcome(p)  # type: ignore[arg-type]
    assert outcome == "KEEP"
    assert target == "CC"


def test_get_effective_outcome_uses_override_when_set() -> None:
    p = _FakeProposal(
        cleansing_outcome="KEEP",
        target_object="CC",
        override_outcome="REDESIGN",
        override_target="PC_ONLY",
    )
    outcome, target = get_effective_outcome(p)  # type: ignore[arg-type]
    assert outcome == "REDESIGN"
    assert target == "PC_ONLY"


# ---------- Integration with mocked DB session ----------


def test_lock_creates_one_pc_per_group_for_v2() -> None:
    """End-to-end semantic: 5 V2 proposals in 2 groups → 2 PC rows added."""
    from app.domain.proposal.service import lock_proposals

    # Build mock session
    session = MagicMock()
    wave = MagicMock(id=1, status="proposed")
    session.get.side_effect = lambda model, _id: {
        "Wave": wave,
        "AnalysisRun": MagicMock(id=10, wave_id=1, engine_version="v2.cema_migration"),
    }.get(model.__name__)

    proposals_data = [
        ("CC_A", "GROUP_X", "P_X", "C_A"),
        ("CC_B", "GROUP_X", "P_X", "C_B"),  # same group → same pc_id
        ("CC_C", "GROUP_X", "P_X", "C_C"),  # same group → same pc_id
        ("CC_D", "GROUP_Y", "P_Y", "C_D"),  # different group
        ("CC_E", "GROUP_Y", "P_Y", "C_E"),  # same as D
    ]

    proposals = []
    for legacy_cctr, group_key, pc_id, cc_id in proposals_data:
        p = MagicMock()
        p.cleansing_outcome = "KEEP"
        p.target_object = "CC_AND_PC"
        p.override_outcome = None
        p.override_target = None
        p.attrs = {
            "engine_version": "v2",
            "cc_id": cc_id,
            "pc_id": pc_id,
            "cc_name": cc_id,
            "pc_name": pc_id,
            "group_key": group_key,
        }
        p.legacy_cc_id = legacy_cctr
        proposals.append(p)

    # Mock proposal query
    proposals_result = MagicMock()
    proposals_result.scalars.return_value.all.return_value = proposals

    # Mock legacy lookups
    def _get(model, _id):
        if model.__name__ == "Wave":
            return wave
        if model.__name__ == "AnalysisRun":
            r = MagicMock()
            r.wave_id = 1
            r.engine_version = "v2.cema_migration"
            return r
        if model.__name__ == "LegacyCostCenter":
            lc = MagicMock()
            lc.cctr = _id
            lc.pctr = "ignored_legacy_pctr"
            lc.coarea = "1000"
            lc.ccode = "C001"
            lc.txtsh = "x"
            lc.txtmi = "x"
            lc.responsible = ""
            lc.cctrcgy = "1"
            lc.currency = "EUR"
            return lc
        return None

    session.get.side_effect = _get

    # Track adds
    added: list = []
    seen_pc_keys: set[tuple[str, str]] = set()
    seen_cc_keys: set[tuple[str, str]] = set()

    def _track_add(obj):
        added.append(obj)

    session.add.side_effect = _track_add

    # Sequence of execute() calls:
    #   1. Query for all proposals
    #   2. For each KEEP+CC_AND_PC proposal:
    #      a. Existence check on TargetCostCenter (returns existing row or None)
    #      b. Existence check on TargetProfitCenter (returns existing row or None)
    #
    # We walk the proposals list in order and answer each existence check by
    # looking up our seen_*_keys sets.
    proposal_iter = iter(proposals_data)
    pending_call: dict = {}  # carries pc_id/cc_id between TCC and TPC checks

    def _execute(stmt):
        nonlocal pending_call
        # First call returns the proposals list
        if not pending_call.get("first_done"):
            pending_call["first_done"] = True
            return proposals_result

        # Need to know what stmt is being asked. Inspect the FROM clause name.
        froms = list(stmt.get_final_froms())
        table_name = froms[0].name if froms else ""

        m = MagicMock()
        if table_name == "target_cost_center":
            # Pull next CC info from proposals_iter
            try:
                _, _, _, cc_id = next(proposal_iter)
                pending_call["cc_id"] = cc_id
                # Reset for new proposal — also queue pc_id
            except StopIteration:
                cc_id = None
            key = ("1000", cc_id)
            if key in seen_cc_keys:
                m.scalar_one_or_none.return_value = MagicMock()
            else:
                seen_cc_keys.add(key)
                m.scalar_one_or_none.return_value = None
            return m
        if table_name == "target_profit_center":
            # The pc_id corresponds to the proposal whose CC we just checked
            # — we need to read it from proposals_data based on cc_id we
            # tracked. Find pc_id by matching cc_id in proposals_data.
            cc_id = pending_call.get("cc_id")
            pc_id = next((pc for (_cctr, _gk, pc, c) in proposals_data if c == cc_id), None)
            key = ("1000", pc_id)
            if key in seen_pc_keys:
                tpc_existing = MagicMock()
                tpc_existing.coarea = "1000"
                tpc_existing.pctr = pc_id
                m.scalar_one_or_none.return_value = tpc_existing
            else:
                seen_pc_keys.add(key)
                m.scalar_one_or_none.return_value = None
            return m
        m.scalar_one_or_none.return_value = None
        return m

    session.execute.side_effect = _execute

    # Run
    result = lock_proposals(wave_id=1, run_id=10, db=session)

    # 5 proposals → 5 CCs (each unique cc_id) but only 2 PCs (2 groups)
    assert result["target_cc_created"] == 5, f"expected 5 CCs, got {result['target_cc_created']}"
    assert result["target_pc_created"] == 2, f"expected 2 PCs, got {result['target_pc_created']}"
    assert result["pc_groups"] == 2  # 2 distinct (coarea, pc_id) tuples


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
