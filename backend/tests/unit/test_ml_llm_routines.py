"""Tests for the ML and LLM routines."""

from __future__ import annotations

import pytest

from app.domain.decision_tree.context import CenterContext
from app.domain.decision_tree.registry import boot_registry, get_registry
from app.domain.decision_tree.routines.llm_advisor import LLMAdvisor, _parse_response
from app.domain.decision_tree.routines.ml_outcome_predictor import (
    MLOutcomePredictor,
    compute_population_anomalies,
)

# ── Test fixtures ────────────────────────────────────────────────────────


def _ctx_active() -> CenterContext:
    """A healthy active center — should lean KEEP."""
    return CenterContext(
        center_id=1,
        coarea="1000",
        cctr="C001",
        ccode="DE01",
        txtsh="Active center",
        is_active=True,
        months_since_last_posting=1,
        posting_count_window=120,
        bs_amt=50000.0,
        rev_amt=200000.0,
        opex_amt=80000.0,
        total_balance=170000.0,
        hierarchy_membership_count=2,
        has_owner=True,
    )


def _ctx_inactive() -> CenterContext:
    """A long-dead center — should lean RETIRE."""
    return CenterContext(
        center_id=2,
        coarea="1000",
        cctr="C002",
        ccode="DE01",
        txtsh="Old retired",
        is_active=False,
        months_since_last_posting=48,
        posting_count_window=0,
        bs_amt=0.0,
        rev_amt=0.0,
        opex_amt=0.0,
        total_balance=0.0,
        hierarchy_membership_count=0,
        has_owner=False,
    )


def _ctx_duplicate() -> CenterContext:
    """A duplicate cluster member — should lean MERGE_MAP."""
    ctx = CenterContext(
        center_id=3,
        coarea="1000",
        cctr="C003",
        ccode="DE01",
        txtsh="Dup of C001",
        is_active=True,
        months_since_last_posting=2,
        posting_count_window=60,
        bs_amt=10000.0,
        rev_amt=0.0,
        opex_amt=20000.0,
        total_balance=30000.0,
        hierarchy_membership_count=1,
        has_owner=True,
    )
    ctx.duplicate_cluster_id = "cluster-A"
    ctx.duplicate_cluster_size = 4
    return ctx


# ── ML routine tests ─────────────────────────────────────────────────────


def test_ml_routine_registered() -> None:
    boot_registry()
    reg = get_registry()
    routine = reg.get("ml.outcome_predictor")
    assert routine is not None
    assert routine.kind == "ml"


def test_ml_predicts_keep_for_healthy_center() -> None:
    routine = MLOutcomePredictor()
    result = routine.run(_ctx_active(), {})
    assert result.verdict == "KEEP"
    assert result.score is not None
    assert 0.25 < result.score <= 1.0  # softmax over 4 classes
    assert "probs" in result.payload
    assert (
        sum(result.payload["probs"].values()) == 1
        or abs(sum(result.payload["probs"].values()) - 1) < 1e-6
    )


def test_ml_predicts_retire_for_dead_center() -> None:
    routine = MLOutcomePredictor()
    result = routine.run(_ctx_inactive(), {})
    assert result.verdict == "RETIRE"
    # RETIRE confidence should clearly dominate the others.
    assert result.payload["probs"]["RETIRE"] > result.payload["probs"]["KEEP"]
    assert result.payload["probs"]["RETIRE"] > result.payload["probs"]["MERGE_MAP"]


def test_ml_predicts_merge_for_duplicate_cluster() -> None:
    routine = MLOutcomePredictor()
    result = routine.run(_ctx_duplicate(), {})
    # Duplicate signal should push MERGE_MAP probability above KEEP.
    assert result.payload["probs"]["MERGE_MAP"] > result.payload["probs"]["KEEP"]


def test_ml_writes_back_to_context() -> None:
    routine = MLOutcomePredictor()
    ctx = _ctx_active()
    routine.run(ctx, {})
    assert ctx.ml_outcome_probs  # populated
    assert "KEEP" in ctx.ml_outcome_probs
    assert ctx.ml_anomaly_score is not None


def test_population_anomalies_handles_small_population() -> None:
    # Below the 10-center threshold → returns empty dict, no crash.
    assert compute_population_anomalies([_ctx_active()]) == {}
    assert compute_population_anomalies([_ctx_active() for _ in range(5)]) == {}


def test_population_anomalies_runs_on_real_population() -> None:
    pytest.importorskip("sklearn", reason="scikit-learn not installed in this venv")
    contexts = [_ctx_active() for _ in range(15)]
    # Make one outlier
    contexts[0].months_since_last_posting = 999
    contexts[0].bs_amt = 10_000_000_000.0
    scores = compute_population_anomalies(contexts)
    assert len(scores) == 15
    assert all(0 <= s <= 1 for s in scores.values())
    # Outlier should be at or near the top of the distribution.
    sorted_scores = sorted(scores.values(), reverse=True)
    assert scores[contexts[0].center_id] >= sorted_scores[2]  # top-3


# ── LLM routine tests ────────────────────────────────────────────────────


def test_llm_routine_registered() -> None:
    boot_registry()
    reg = get_registry()
    routine = reg.get("llm.advisor")
    assert routine is not None
    assert routine.kind == "llm"


def test_llm_returns_pass_when_no_provider_configured() -> None:
    """The default test environment has no LLM credentials — must not crash."""
    routine = LLMAdvisor()
    result = routine.run(_ctx_active(), {})
    assert result.verdict == "PASS"
    assert result.reason.startswith("llm.")
    assert result.payload.get("available") is False


def test_llm_skip_if_high_confidence() -> None:
    routine = LLMAdvisor()
    ctx = _ctx_active()
    ctx.ml_outcome_probs = {"KEEP": 0.95, "RETIRE": 0.02, "MERGE_MAP": 0.02, "REDESIGN": 0.01}
    result = routine.run(ctx, {"skip_if_high_confidence": 0.9})
    assert result.verdict == "PASS"
    assert "skipped_high_ml_confidence" in result.reason
    assert result.payload.get("skipped") is True


def test_llm_response_parser_strict_json() -> None:
    text = '{"verdict": "RETIRE", "confidence": 0.85, "reason": "Inactive for 3 years."}'
    parsed = _parse_response(text)
    assert parsed == {"verdict": "RETIRE", "confidence": 0.85, "reason": "Inactive for 3 years."}


def test_llm_response_parser_strips_code_fences() -> None:
    text = '```json\n{"verdict": "KEEP", "confidence": 0.7, "reason": "Active and material"}\n```'
    parsed = _parse_response(text)
    assert parsed is not None
    assert parsed["verdict"] == "KEEP"


def test_llm_response_parser_extracts_from_surrounding_text() -> None:
    text = (
        "Sure, here is my analysis: "
        '{"verdict": "MERGE_MAP", "confidence": 0.6, "reason": "Looks like a duplicate"} '
        "Hope this helps."
    )
    parsed = _parse_response(text)
    assert parsed is not None
    assert parsed["verdict"] == "MERGE_MAP"


def test_llm_response_parser_rejects_invalid_verdict() -> None:
    assert _parse_response('{"verdict": "MAYBE", "confidence": 0.5, "reason": "..."}') is None


def test_llm_response_parser_handles_invalid_json() -> None:
    assert _parse_response("this is not json at all") is None
    assert _parse_response("") is None
    assert _parse_response(None) is None


def test_llm_response_parser_clamps_confidence() -> None:
    # Out-of-range confidence values should be clamped to [0, 1].
    parsed = _parse_response('{"verdict": "KEEP", "confidence": 5.0, "reason": "..."}')
    assert parsed is not None
    assert 0 <= parsed["confidence"] <= 1
    parsed = _parse_response('{"verdict": "KEEP", "confidence": -1, "reason": "..."}')
    assert parsed is not None
    assert 0 <= parsed["confidence"] <= 1
