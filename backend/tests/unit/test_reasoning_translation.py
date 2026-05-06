"""Tests for the business-friendly decision-reasoning translation layer."""

from __future__ import annotations

from app.services.reasoning_translation import (
    _split_step,
    translate_outcome,
    translate_rule_path,
    translate_step,
    translate_target,
)

# ── Step splitting ──────────────────────────────────────────────────────


def test_split_step_simple() -> None:
    assert _split_step("rule.posting_activity:RETIRE") == (
        "rule.posting_activity",
        "RETIRE",
    )


def test_split_step_with_colon_in_verdict() -> None:
    """The pc_approach routine emits verdicts like '1:1' that contain a colon."""
    assert _split_step("v2.pc_approach:1:1") == ("v2.pc_approach", "1:1")
    assert _split_step("v2.pc_approach:1:n") == ("v2.pc_approach", "1:n")


def test_split_step_no_verdict() -> None:
    assert _split_step("orphan_code") == ("orphan_code", "")


def test_split_step_non_string() -> None:
    assert _split_step(None) == ("", "")  # type: ignore[arg-type]
    assert _split_step(123) == ("", "")  # type: ignore[arg-type]


# ── Single-step translation ─────────────────────────────────────────────


def test_translate_step_known_routine_known_verdict() -> None:
    """v2.balance_migrate:MIGRATE_YES is in the catalog with both keys."""
    out = translate_step("v2.balance_migrate", "MIGRATE_YES")
    assert out["code"] == "v2.balance_migrate"
    assert out["verdict"] == "MIGRATE_YES"
    assert out["label"] == "Balance sheet migration check"
    # Must NOT be the technical code:
    assert "v2.balance_migrate" not in out["label"]
    # Must mention "balance" in plain language:
    assert "balance" in out["verdict_meaning"].lower()
    # Description present:
    assert len(out["description"]) > 20


def test_translate_step_pc_approach_with_colon_verdict() -> None:
    """The 1:1 / 1:n verdicts must translate, not show as raw."""
    out = translate_step("v2.pc_approach", "1:1")
    assert "Profit center" in out["label"] or "Profit Center" in out["label"]
    assert (
        "own profit center" in out["verdict_meaning"].lower()
        or "classic" in out["verdict_meaning"].lower()
    )


def test_translate_step_unknown_routine_falls_back() -> None:
    """No catalog entry → still produces something sane."""
    out = translate_step("rule.totally_made_up", "WHATEVER")
    assert out["code"] == "rule.totally_made_up"
    assert out["verdict"] == "WHATEVER"
    # Label falls back to the code (graceful), description is empty.
    assert out["label"] == "rule.totally_made_up"
    assert out["description"] == ""


def test_translate_step_known_routine_unknown_verdict_humanizes() -> None:
    out = translate_step("rule.posting_activity", "WAT_UP_DOC")
    # Humanized fallback: 'Wat up doc'
    assert out["verdict_meaning"]
    assert out["verdict_meaning"][0].isupper()


# ── Full rule-path translation ──────────────────────────────────────────


def test_translate_rule_path_v2_format() -> None:
    """V2 stores rule_path as ['code:verdict', ...] strings."""
    raw = ["v2.balance_migrate:MIGRATE_YES", "v2.pc_approach:1:1"]
    out = translate_rule_path(raw)
    assert len(out) == 2
    assert out[0]["code"] == "v2.balance_migrate"
    assert out[0]["verdict"] == "MIGRATE_YES"
    assert out[0]["label"] == "Balance sheet migration check"
    assert out[1]["code"] == "v2.pc_approach"
    assert out[1]["verdict"] == "1:1"


def test_translate_rule_path_v1_format() -> None:
    """V1 stores rule_path as {'steps': [{'routine':..., 'verdict':...}, ...]}."""
    raw = {
        "steps": [
            {"routine": "rule.posting_activity", "verdict": "RETIRE", "confidence": 0.92},
            {"routine": "rule.ownership", "verdict": "REDESIGN", "confidence": 0.71},
        ]
    }
    out = translate_rule_path(raw)
    assert len(out) == 2
    assert out[0]["code"] == "rule.posting_activity"
    assert out[0]["confidence"] == 0.92
    assert out[1]["confidence"] == 0.71


def test_translate_rule_path_empty_or_none() -> None:
    assert translate_rule_path(None) == []
    assert translate_rule_path([]) == []
    assert translate_rule_path({"steps": []}) == []


def test_translate_rule_path_handles_unknown_format() -> None:
    """Garbage in → graceful fallback, never an exception."""
    out = translate_rule_path("just a random string")
    assert len(out) == 1
    assert out[0]["description"]


# ── Outcome and target translation ──────────────────────────────────────


def test_translate_outcome_known() -> None:
    keep = translate_outcome("KEEP")
    assert keep["label"] == "Keep"
    assert "Keep" in keep["sentence"]
    retire = translate_outcome("RETIRE")
    assert retire["label"] == "Retire"
    assert "no longer" in retire["sentence"].lower()


def test_translate_outcome_case_insensitive() -> None:
    assert translate_outcome("keep")["label"] == "Keep"
    assert translate_outcome("Retire")["label"] == "Retire"


def test_translate_outcome_unknown_falls_back() -> None:
    out = translate_outcome("WEIRDO")
    assert out["label"] == "WEIRDO"
    assert "WEIRDO" in out["sentence"]


def test_translate_outcome_none() -> None:
    out = translate_outcome(None)
    assert out["label"] == "Undetermined"


def test_translate_target_known() -> None:
    assert "Cost Center" in translate_target("CC_AND_PC")
    assert "Profit Center" in translate_target("CC_AND_PC")
    assert "removed" in translate_target("NONE").lower() or "Nothing" in translate_target("NONE")


def test_translate_target_unknown_returns_input() -> None:
    assert translate_target("BIZARRE") == "BIZARRE"


def test_translate_target_empty() -> None:
    assert translate_target("") == ""
    assert translate_target(None) == ""
