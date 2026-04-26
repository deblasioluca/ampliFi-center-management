"""Tests for decision tree: routines, registry, DSL, pipeline engine."""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.dsl import evaluate_dsl_rule
from app.domain.decision_tree.engine import (
    CenterFeatures,
    PipelineEngine,
    evaluate_center,
    evaluate_center_with_pipeline,
)
from app.domain.decision_tree.registry import boot_registry


def _make_ctx(**overrides: object) -> CenterContext:
    """Create a CenterContext with defaults, overriding specific fields."""
    defaults: dict = {
        "center_id": 1,
        "coarea": "1000",
        "cctr": "CC0100",
        "ccode": "DE01",
        "txtsh": "Test Center",
        "txtmi": "Test Center Long Description",
        "responsible": "JDOE",
        "currency": "EUR",
        "cctrcgy": "H",
        "pctr": "PC0100",
        "is_active": True,
        "months_since_last_posting": None,
        "posting_count_window": 10,
        "bs_amt": 0.0,
        "rev_amt": 0.0,
        "opex_amt": 1000.0,
        "total_balance": 5000.0,
        "hierarchy_membership_count": 1,
        "has_owner": True,
        "is_feeder": False,
        "is_allocation_vehicle": False,
        "is_project_related": False,
        "in_bw_extractors": False,
        "in_grc": False,
        "in_intercompany": False,
        "has_direct_revenue": False,
        "has_operational_costs": True,
        "collects_project_costs": False,
        "used_for_revenue_allocation": False,
        "used_for_cost_allocation": False,
        "used_for_info_only": False,
        "duplicate_cluster_id": None,
        "duplicate_cluster_size": 0,
        "attrs": {},
    }
    defaults.update(overrides)
    return CenterContext(**defaults)


def _make_features(**overrides: object) -> CenterFeatures:
    """Create a CenterFeatures with defaults."""
    defaults: dict = {
        "coarea": "1000",
        "cctr": "CC0100",
        "ccode": "DE01",
        "txtsh": "Test",
        "txtmi": "Test Long",
        "responsible": "JDOE",
        "currency": "EUR",
        "cctrcgy": "H",
        "pctr": "PC0100",
        "is_active": True,
        "months_since_last_posting": None,
        "posting_count_window": 10,
        "bs_amt": 0.0,
        "rev_amt": 0.0,
        "opex_amt": 1000.0,
        "total_balance": 5000.0,
        "hierarchy_membership_count": 1,
        "has_owner": True,
        "attrs": {},
    }
    defaults.update(overrides)
    return CenterFeatures(**defaults)


# ── Registry tests ─────────────────────────────────────────────────────


class TestRoutineRegistry:
    def test_boot_registers_builtins(self) -> None:
        reg = boot_registry()
        codes = reg.codes()
        assert "rule.posting_activity" in codes
        assert "rule.ownership" in codes
        assert "rule.redundancy" in codes
        assert "rule.hierarchy_compliance" in codes
        assert "rule.cross_system_dependency" in codes
        assert "aggregate.combine_outcomes" in codes

    def test_get_returns_routine(self) -> None:
        reg = boot_registry()
        routine = reg.get("rule.posting_activity")
        assert routine is not None
        assert routine.code == "rule.posting_activity"

    def test_get_unknown_returns_none(self) -> None:
        reg = boot_registry()
        assert reg.get("rule.nonexistent") is None

    def test_list_by_kind(self) -> None:
        reg = boot_registry()
        rules = reg.list(kind="rule")
        assert len(rules) > 0
        for r in rules:
            assert r.kind == "rule"


# ── Individual routine tests ───────────────────────────────────────────


class TestPostingActivityRoutine:
    def test_active_center_kept(self) -> None:
        ctx = _make_ctx(posting_count_window=50, months_since_last_posting=3)
        reg = boot_registry()
        routine = reg.get("rule.posting_activity")
        assert routine is not None
        params = {"posting_inactivity_threshold": 24, "posting_minimal_threshold": 0}
        result = routine.run(ctx, params)
        assert result.verdict == "PASS"

    def test_inactive_center_retired(self) -> None:
        ctx = _make_ctx(posting_count_window=0, months_since_last_posting=30)
        reg = boot_registry()
        routine = reg.get("rule.posting_activity")
        assert routine is not None
        params = {"posting_inactivity_threshold": 24, "posting_minimal_threshold": 0}
        result = routine.run(ctx, params)
        assert result.verdict == "RETIRE"


class TestOwnershipRoutine:
    def test_has_owner_kept(self) -> None:
        ctx = _make_ctx(has_owner=True)
        reg = boot_registry()
        routine = reg.get("rule.ownership")
        assert routine is not None
        result = routine.run(ctx, {})
        assert result.verdict == "PASS"

    def test_no_owner_retired(self) -> None:
        ctx = _make_ctx(has_owner=False)
        reg = boot_registry()
        routine = reg.get("rule.ownership")
        assert routine is not None
        result = routine.run(ctx, {})
        assert result.verdict == "RETIRE"


class TestRedundancyRoutine:
    def test_no_cluster_kept(self) -> None:
        ctx = _make_ctx(duplicate_cluster_id=None, duplicate_cluster_size=0)
        reg = boot_registry()
        routine = reg.get("rule.redundancy")
        assert routine is not None
        result = routine.run(ctx, {})
        assert result.verdict == "PASS"

    def test_in_cluster_merge(self) -> None:
        ctx = _make_ctx(duplicate_cluster_id="CLUSTER_A", duplicate_cluster_size=3)
        reg = boot_registry()
        routine = reg.get("rule.redundancy")
        assert routine is not None
        result = routine.run(ctx, {"min_cluster_size": 2})
        assert result.verdict == "MERGE_MAP"


class TestHierarchyComplianceRoutine:
    def test_single_membership_kept(self) -> None:
        ctx = _make_ctx(hierarchy_membership_count=1)
        reg = boot_registry()
        routine = reg.get("rule.hierarchy_compliance")
        assert routine is not None
        result = routine.run(ctx, {})
        assert result.verdict == "PASS"

    def test_no_membership_flagged(self) -> None:
        ctx = _make_ctx(hierarchy_membership_count=0)
        reg = boot_registry()
        routine = reg.get("rule.hierarchy_compliance")
        assert routine is not None
        result = routine.run(ctx, {})
        assert result.verdict == "MERGE_MAP"


# ── DSL rule engine tests ─────────────────────────────────────────────


class TestDSL:
    def test_simple_eq(self) -> None:
        ctx = _make_ctx(has_owner=True)
        expr = {"feature": "has_owner", "op": "==", "value": True}
        result = evaluate_dsl_rule(
            expression=expr,
            ctx=ctx,
            verdict_when_true={"outcome": "KEEP", "reason": "test.pass"},
            verdict_when_false="PASS",
            routine_code="dsl_test",
        )
        assert result.verdict == "KEEP"

    def test_all_combinator(self) -> None:
        ctx = _make_ctx(has_owner=True, posting_count_window=0)
        expr = {
            "all": [
                {"feature": "has_owner", "op": "==", "value": True},
                {"feature": "posting_count_window", "op": "<=", "value": 0},
            ]
        }
        result = evaluate_dsl_rule(
            expression=expr,
            ctx=ctx,
            verdict_when_true={"outcome": "RETIRE", "reason": "test.retire"},
            verdict_when_false="PASS",
            routine_code="dsl_all",
        )
        assert result.verdict == "RETIRE"

    def test_any_combinator(self) -> None:
        ctx = _make_ctx(has_owner=False, posting_count_window=50)
        expr = {
            "any": [
                {"feature": "has_owner", "op": "==", "value": False},
                {"feature": "posting_count_window", "op": "==", "value": 0},
            ]
        }
        result = evaluate_dsl_rule(
            expression=expr,
            ctx=ctx,
            verdict_when_true={"outcome": "FLAG", "reason": "test.flag"},
            verdict_when_false="PASS",
            routine_code="dsl_any",
        )
        assert result.verdict == "FLAG"

    def test_not_combinator(self) -> None:
        ctx = _make_ctx(has_owner=True)
        expr = {"not": {"feature": "has_owner", "op": "==", "value": False}}
        result = evaluate_dsl_rule(
            expression=expr,
            ctx=ctx,
            verdict_when_true={"outcome": "KEEP", "reason": "test.keep"},
            verdict_when_false="PASS",
            routine_code="dsl_not",
        )
        assert result.verdict == "KEEP"

    def test_condition_not_met_returns_passthrough(self) -> None:
        ctx = _make_ctx(has_owner=True)
        expr = {"feature": "has_owner", "op": "==", "value": False}
        result = evaluate_dsl_rule(
            expression=expr,
            ctx=ctx,
            verdict_when_true={"outcome": "RETIRE", "reason": "test.retire"},
            verdict_when_false="PASS",
            routine_code="dsl_neg",
        )
        assert result.verdict == "PASS"

    def test_is_null_operator(self) -> None:
        ctx = _make_ctx(months_since_last_posting=None)
        expr = {"feature": "months_since_last_posting", "op": "is_null"}
        result = evaluate_dsl_rule(
            expression=expr,
            ctx=ctx,
            verdict_when_true={"outcome": "FLAG", "reason": "test.null"},
            verdict_when_false="PASS",
            routine_code="dsl_null",
        )
        assert result.verdict == "FLAG"


# ── Pipeline engine tests ──────────────────────────────────────────────


class TestPipelineEngine:
    def test_basic_pipeline_execution(self) -> None:
        reg = boot_registry()
        ctx = _make_ctx(posting_count_window=50, has_owner=True, hierarchy_membership_count=1)
        config = {
            "pipeline": [
                {"routine": "rule.posting_activity", "enabled": True, "params": {}},
                {"routine": "rule.ownership", "enabled": True, "params": {}},
            ]
        }
        engine = PipelineEngine(reg)
        results = engine.execute(config, ctx)
        assert len(results) == 2
        assert all(isinstance(r, RoutineResult) for r in results)

    def test_disabled_routines_skipped(self) -> None:
        reg = boot_registry()
        ctx = _make_ctx()
        config = {
            "pipeline": [
                {"routine": "rule.posting_activity", "enabled": False, "params": {}},
                {"routine": "rule.ownership", "enabled": True, "params": {}},
            ]
        }
        engine = PipelineEngine(reg)
        results = engine.execute(config, ctx)
        assert len(results) == 1
        assert results[0].code == "rule.ownership"

    def test_short_circuit_stops_pipeline(self) -> None:
        reg = boot_registry()
        ctx = _make_ctx(posting_count_window=0, months_since_last_posting=30)
        config = {
            "pipeline": [
                {
                    "routine": "rule.posting_activity",
                    "enabled": True,
                    "params": {"posting_inactivity_threshold": 24, "posting_minimal_threshold": 0},
                },
                {"routine": "rule.ownership", "enabled": True, "params": {}},
            ]
        }
        engine = PipelineEngine(reg)
        results = engine.execute(config, ctx)
        # posting_activity should short_circuit with RETIRE
        assert results[0].verdict == "RETIRE"
        assert results[0].short_circuit is True
        # No more results after short_circuit
        assert len(results) == 1


# ── Backward compatibility tests ───────────────────────────────────────


class TestLegacyEvaluateCenter:
    def test_active_center_keep(self) -> None:
        features = _make_features(
            posting_count_window=50,
            has_owner=True,
            hierarchy_membership_count=1,
        )
        result = evaluate_center(features, {})
        assert result.cleansing.value == "KEEP"

    def test_inactive_center_retire(self) -> None:
        features = _make_features(
            posting_count_window=0,
            months_since_last_posting=30,
            has_owner=True,
        )
        params = {"inactivity_threshold_months": 24, "posting_threshold": 0}
        result = evaluate_center(features, params)
        assert result.cleansing.value == "RETIRE"


class TestPipelineEvaluation:
    def test_evaluate_center_with_pipeline(self) -> None:
        reg = boot_registry()
        ctx = _make_ctx(
            posting_count_window=50,
            has_owner=True,
            hierarchy_membership_count=1,
            has_operational_costs=True,
        )
        config = {
            "pipeline": [
                {"routine": "rule.posting_activity", "enabled": True, "params": {}},
                {"routine": "rule.ownership", "enabled": True, "params": {}},
                {"routine": "rule.hierarchy_compliance", "enabled": True, "params": {}},
                {"routine": "rule.has_operational_costs", "enabled": True, "params": {}},
                {"routine": "aggregate.combine_outcomes", "enabled": True, "params": {}},
            ]
        }
        result = evaluate_center_with_pipeline(ctx, config, reg)
        assert result.cleansing.value == "KEEP"
        assert len(result.routine_results) > 0
