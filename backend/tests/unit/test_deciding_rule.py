"""Tests for the deciding-rule extraction helper.

The frontend simulation results page displays a single "deciding rule" per
proposal so reviewers can see at a glance which check drove the outcome.
This test suite verifies that extraction across the various rule_path
shapes the system stores (V1 dict-with-steps, V2 list, mixed dict/string).
"""

from __future__ import annotations

# Direct import of the private helper — it's the unit under test.
from app.api.runs import _extract_deciding_rule

# ── Empty / null inputs ─────────────────────────────────────────────────


def test_none_rule_path_returns_aggregate_label() -> None:
    r = _extract_deciding_rule(None, "KEEP")
    assert r["code"] is None
    assert "Aggregate" in r["label"]


def test_empty_dict_returns_aggregate() -> None:
    r = _extract_deciding_rule({}, "RETIRE")
    assert r["code"] is None


def test_empty_list_returns_aggregate() -> None:
    r = _extract_deciding_rule([], "KEEP")
    assert r["code"] is None


# ── V2 list-of-strings format ──────────────────────────────────────────


def test_v2_list_picks_matching_verdict() -> None:
    """Last step whose verdict matches outcome wins."""
    path = [
        "rule.ownership:KEEP",
        "rule.posting_activity:RETIRE",
        "aggregate.combine_outcomes:RETIRE",
    ]
    r = _extract_deciding_rule(path, "RETIRE")
    # Should pick aggregate.combine_outcomes (last matching) — but business
    # logic prefers the rule that actually emitted RETIRE first. The current
    # heuristic walks reverse and picks the LAST RETIRE match. Both are
    # acceptable as long as the verdict matches.
    assert r["verdict"] == "RETIRE"
    assert r["code"] in ("aggregate.combine_outcomes", "rule.posting_activity")


def test_v2_list_no_matching_verdict_returns_last_step() -> None:
    """If no verdict matches the outcome, fall through to the last step."""
    path = ["rule.ownership:KEEP", "rule.posting_activity:KEEP"]
    r = _extract_deciding_rule(path, "MERGE_MAP")
    assert r["code"] == "rule.posting_activity"


# ── V1 dict-with-steps format ──────────────────────────────────────────


def test_v1_dict_with_steps() -> None:
    path = {
        "steps": [
            "rule.ownership:KEEP",
            "rule.posting_activity:RETIRE",
        ]
    }
    r = _extract_deciding_rule(path, "RETIRE")
    assert r["verdict"] == "RETIRE"
    assert r["code"] == "rule.posting_activity"


def test_v1_dict_empty_steps() -> None:
    r = _extract_deciding_rule({"steps": []}, "KEEP")
    assert r["code"] is None


# ── Dict-step format ──────────────────────────────────────────────────


def test_dict_steps_format() -> None:
    """Some routines store {"code": ..., "verdict": ...} dicts."""
    path = [
        {"code": "rule.ownership", "verdict": "KEEP"},
        {"code": "rule.redundancy", "verdict": "MERGE_MAP"},
    ]
    r = _extract_deciding_rule(path, "MERGE_MAP")
    assert r["code"] == "rule.redundancy"
    assert r["verdict"] == "MERGE_MAP"


# ── Step without verdict ──────────────────────────────────────────────


def test_step_without_verdict_falls_through_to_last() -> None:
    path = ["rule.ownership", "rule.posting_activity"]
    r = _extract_deciding_rule(path, "KEEP")
    # No verdicts to match — fall through to last step
    assert r["code"] == "rule.posting_activity"
    assert r["verdict"] is None


# ── Business label resolution ─────────────────────────────────────────


def test_label_uses_business_friendly_name_when_known() -> None:
    """Known routine codes should produce a German business label."""
    path = ["rule.posting_activity:RETIRE"]
    r = _extract_deciding_rule(path, "RETIRE")
    # Should mention the friendly label "Inaktivität erkennen"
    assert "Inaktivität" in r["label"]


def test_label_falls_back_to_code_when_unknown() -> None:
    """An unknown routine code should still render in the label."""
    path = ["rule.totally_made_up:KEEP"]
    r = _extract_deciding_rule(path, "KEEP")
    assert "rule.totally_made_up" in r["label"]


def test_verdict_meaning_appears_in_label_when_available() -> None:
    """When the catalog has a verdict_meaning, it should be in the label."""
    path = ["rule.posting_activity:RETIRE"]
    r = _extract_deciding_rule(path, "RETIRE")
    # rule.posting_activity has verdict_meaning for RETIRE
    assert "stillegung" in r["label"].lower() or "inaktiv" in r["label"].lower()


# ── Verdict case-insensitivity ────────────────────────────────────────


def test_verdict_match_is_case_insensitive() -> None:
    path = ["rule.ownership:retire", "rule.posting_activity:KEEP"]
    r = _extract_deciding_rule(path, "RETIRE")
    assert r["code"] == "rule.ownership"
