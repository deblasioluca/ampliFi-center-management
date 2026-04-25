"""Unit tests for the deterministic decision tree engine (section 04)."""

from __future__ import annotations

from app.domain.decision_tree.engine import (
    CenterFeatures,
    CleansingOutcome,
    TargetObject,
    evaluate_center,
    run_cleansing_tree,
    run_mapping_tree,
)


def _base_features(**overrides) -> CenterFeatures:
    defaults = {
        "coarea": "1000",
        "cctr": "0001000100",
        "ccode": "1000",
        "txtsh": "Admin HQ",
        "is_active": True,
        "months_since_last_posting": 0,
        "posting_count_window": 100,
        "hierarchy_membership_count": 1,
    }
    defaults.update(overrides)
    return CenterFeatures(**defaults)


class TestCleansingTree:
    def test_inactive_retires(self):
        f = _base_features(is_active=False)
        result = run_cleansing_tree(f)
        assert result.cleansing == CleansingOutcome.RETIRE
        assert "inactive=true" in result.rule_path[0]

    def test_no_postings_retires(self):
        f = _base_features(months_since_last_posting=36, posting_count_window=0)
        result = run_cleansing_tree(f)
        assert result.cleansing == CleansingOutcome.RETIRE

    def test_active_with_postings_keeps(self):
        f = _base_features()
        result = run_cleansing_tree(f)
        assert result.cleansing == CleansingOutcome.KEEP

    def test_duplicate_cluster_merges(self):
        f = _base_features(duplicate_cluster_id="CL001", duplicate_cluster_size=3)
        result = run_cleansing_tree(f)
        assert result.cleansing == CleansingOutcome.MERGE_MAP

    def test_hierarchy_strict_redesign(self):
        f = _base_features(hierarchy_membership_count=0)
        result = run_cleansing_tree(f, {"strict_hierarchy_compliance": True})
        assert result.cleansing == CleansingOutcome.REDESIGN


class TestMappingTree:
    def test_retire_returns_none(self):
        target = run_mapping_tree(_base_features(), CleansingOutcome.RETIRE)
        assert target == TargetObject.NONE

    def test_bs_relevance_cc_and_pc(self):
        f = _base_features(bs_amt=100.0, rev_amt=50.0)
        target = run_mapping_tree(f, CleansingOutcome.KEEP)
        assert target == TargetObject.CC_AND_PC

    def test_bs_only_pc_only(self):
        f = _base_features(bs_amt=100.0)
        target = run_mapping_tree(f, CleansingOutcome.KEEP)
        assert target == TargetObject.PC_ONLY

    def test_project_real(self):
        f = _base_features(is_project_related=True)
        target = run_mapping_tree(f, CleansingOutcome.KEEP)
        assert target == TargetObject.WBS_REAL

    def test_project_allocation_stat(self):
        f = _base_features(is_project_related=True, is_allocation_vehicle=True)
        target = run_mapping_tree(f, CleansingOutcome.KEEP)
        assert target == TargetObject.WBS_STAT

    def test_feeder_cc(self):
        f = _base_features(is_feeder=True)
        target = run_mapping_tree(f, CleansingOutcome.KEEP)
        assert target == TargetObject.CC

    def test_default_cc(self):
        f = _base_features()
        target = run_mapping_tree(f, CleansingOutcome.KEEP)
        assert target == TargetObject.CC


class TestEvaluateCenter:
    def test_full_pipeline(self):
        f = _base_features()
        result = evaluate_center(f)
        assert result.cleansing == CleansingOutcome.KEEP
        assert result.target_object == TargetObject.CC

    def test_determinism(self):
        """Same input must always produce same output (section 04 requirement)."""
        f = _base_features()
        results = [evaluate_center(f) for _ in range(100)]
        assert all(r.cleansing == results[0].cleansing for r in results)
        assert all(r.target_object == results[0].target_object for r in results)
        assert all(r.rule_path == results[0].rule_path for r in results)
