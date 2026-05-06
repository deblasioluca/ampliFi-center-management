"""Tests for the LLM-powered Config Drafter and Configurator endpoints.

Calls the endpoint functions directly with mocked DB sessions and a
mocked LLM provider — no SQLite (avoids JSONB-on-sqlite incompatibility),
no network, no auth chain.

Coverage:
* Pipeline grounding helper filters by engine
* All three system prompts embed the grounding section
* _call_llm_json strips markdown fences
* _call_llm_json handles malformed JSON gracefully
* _validate_pipeline_config flags unknown routine codes
* Drafter happy path returns draft + validation
* Drafter rejects bogus engine values (HTTP 400)
* Configurator clarify step returns questions
* Configurator propose step folds clarifications into the user message
* Configurator refine step embeds the prior draft
* Configurator rejects bogus step values (HTTP 400)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api.admin import (
    ConfigConfiguratorRequest,
    ConfigDrafterRequest,
    _build_configurator_clarify_prompt,
    _build_configurator_refine_prompt,
    _build_drafter_system_prompt,
    _build_pipeline_grounding,
    _call_llm_json,
    _validate_pipeline_config,
    llm_configure_stepwise,
    llm_draft_config,
)


def _mock_db(llm_config_value: dict | None) -> MagicMock:
    db = MagicMock()
    if llm_config_value is None:
        db.execute.return_value.scalar_one_or_none.return_value = None
    else:
        cfg = MagicMock()
        cfg.value = llm_config_value
        db.execute.return_value.scalar_one_or_none.return_value = cfg
    return db


# ── Grounding ──────────────────────────────────────────────────────


def test_pipeline_grounding_v1_excludes_v2_routines() -> None:
    out = _build_pipeline_grounding("v1")
    # The grounding lists routines as "### <code> — <label>"
    assert "###" in out
    # V2-only routines (e.g. v2.retire_flag) should not appear
    assert "v2.retire_flag" not in out
    # V1 cleansing rules should appear
    assert "rule." in out  # at least one rule.* code


def test_pipeline_grounding_v2_includes_only_v2_routines() -> None:
    out = _build_pipeline_grounding("v2")
    # All headers must start with v2. since we filtered to engine=v2
    import re

    headers = re.findall(r"^### (\S+) ", out, re.MULTILINE)
    assert headers, "Expected at least one v2 routine in the catalog"
    for h in headers:
        assert h.startswith("v2."), f"Non-v2 routine leaked into v2 grounding: {h}"


def test_pipeline_grounding_includes_param_defaults() -> None:
    """The grounding must surface tunable parameters with defaults so the
    LLM can pick sensible values without inventing them."""
    out = _build_pipeline_grounding("v1")
    # At least one "default=" snippet should be present
    assert "default=" in out


# ── System prompts ─────────────────────────────────────────────────


def test_drafter_prompt_embeds_grounding_and_json_shape() -> None:
    grounding = "### foo.bar — Some rule\nNo description."
    prompt = _build_drafter_system_prompt("v1", grounding)
    assert grounding in prompt
    assert '"pipeline"' in prompt
    assert '"rationale"' in prompt
    assert "V1" in prompt


def test_clarify_prompt_includes_questions_shape() -> None:
    prompt = _build_configurator_clarify_prompt("v2", "GROUNDING_HERE")
    assert "GROUNDING_HERE" in prompt
    assert '"questions"' in prompt
    assert "V2" in prompt


def test_refine_prompt_signals_revision() -> None:
    prompt = _build_configurator_refine_prompt("v1", "GROUNDING")
    assert "GROUNDING" in prompt
    assert "revis" in prompt.lower() or "feedback" in prompt.lower()


# ── _call_llm_json helper ──────────────────────────────────────────


def test_call_llm_json_unavailable_when_no_config() -> None:
    db = _mock_db(None)
    out = _call_llm_json(db, "sys", "user", {})
    assert out["available"] is False


def test_call_llm_json_strips_markdown_fences() -> None:
    db = _mock_db({"provider": "azure", "model": "gpt-4o"})

    canned = MagicMock()
    canned.text = '```json\n{"key": "value"}\n```'
    canned.tokens_in = 10
    canned.tokens_out = 5

    class FakeProvider:
        def __init__(self, *_a, **_k) -> None: ...
        def complete(self, *_a, **_k):
            return canned

    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        out = _call_llm_json(db, "sys", "user", {})

    assert out["available"] is True
    assert out["parsed"] == {"key": "value"}


def test_call_llm_json_handles_malformed_json() -> None:
    db = _mock_db({"provider": "azure", "model": "gpt-4o"})

    canned = MagicMock()
    canned.text = "Sorry, I can't help with that."
    canned.tokens_in = 1
    canned.tokens_out = 1

    class FakeProvider:
        def __init__(self, *_a, **_k) -> None: ...
        def complete(self, *_a, **_k):
            return canned

    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        out = _call_llm_json(db, "sys", "user", {})

    assert out["available"] is False
    assert "non-JSON" in out["reason"]
    assert out["raw"]  # raw response preserved for debugging


def test_call_llm_json_handles_provider_failure() -> None:
    db = _mock_db({"provider": "azure", "model": "gpt-4o"})

    class ExplodingProvider:
        def __init__(self, *_a, **_k) -> None: ...
        def complete(self, *_a, **_k):
            raise ConnectionError("network")

    with patch("app.infra.llm.provider.AzureOpenAIProvider", ExplodingProvider):
        out = _call_llm_json(db, "sys", "user", {})

    assert out["available"] is False
    assert "ConnectionError" in out["reason"]


# ── Validation helper ─────────────────────────────────────────────


def test_validate_pipeline_config_flags_invalid_codes() -> None:
    parsed = {
        "config": {
            "pipeline": [
                {"routine": "rule.posting_activity"},  # real
                {"routine": "rule.absolutely_made_up"},  # not real
                {"routine": "aggregate.combine_outcomes"},  # real
            ]
        }
    }
    out = _validate_pipeline_config(parsed, "v1")
    assert out["ok"] is False
    assert "rule.absolutely_made_up" in out["invalid_codes"]
    assert "rule.posting_activity" in out["valid_codes"]


def test_validate_pipeline_config_ok_when_all_valid() -> None:
    from app.domain.decision_tree.rule_catalog import list_rule_catalog

    real_codes = [e["code"] for e in list_rule_catalog() if not e["code"].startswith("v2.")]
    assert real_codes
    parsed = {"config": {"pipeline": [{"routine": real_codes[0]}]}}
    out = _validate_pipeline_config(parsed, "v1")
    assert out["ok"] is True
    assert out["invalid_codes"] == []


# ── Drafter endpoint ──────────────────────────────────────────────


def test_drafter_rejects_bogus_engine() -> None:
    body = ConfigDrafterRequest(description="Something", engine="v3")
    with pytest.raises(HTTPException) as exc:
        llm_draft_config(body, db=_mock_db(None), _user=MagicMock())
    assert exc.value.status_code == 400


def test_drafter_unavailable_when_no_llm_config() -> None:
    body = ConfigDrafterRequest(description="Aggressive cleanup", engine="v1")
    out = llm_draft_config(body, db=_mock_db(None), _user=MagicMock())
    assert out["available"] is False
    assert out["draft"] is None


def test_drafter_happy_path_returns_draft_and_validation() -> None:
    db = _mock_db({"provider": "azure", "model": "gpt-4o"})

    canned = MagicMock()
    canned.text = (
        '{"rationale": "Aggressive defaults match the goal.",'
        '"config": {"pipeline": ['
        '{"routine": "rule.posting_activity", "enabled": true, "params": {}},'
        '{"routine": "aggregate.combine_outcomes", "enabled": true, "params": {}}'
        ']}, "warnings": ["thresholds tightened"]}'
    )
    canned.tokens_in = 100
    canned.tokens_out = 50

    captured: dict[str, Any] = {}

    class FakeProvider:
        def __init__(self, *_a, **_k) -> None: ...
        def complete(self, model, messages, **kwargs):
            captured["model"] = model
            captured["messages"] = messages
            return canned

    body = ConfigDrafterRequest(description="Aggressive cleanup", engine="v1")
    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        out = llm_draft_config(body, db=db, _user=MagicMock())

    assert out["available"] is True
    assert out["draft"]["pipeline"][0]["routine"] == "rule.posting_activity"
    assert out["rationale"].startswith("Aggressive")
    assert out["warnings"] == ["thresholds tightened"]
    assert out["validation"]["ok"] is True
    assert out["tokens_in"] == 100

    # System prompt must contain the V1 grounding (so the LLM can only
    # pick from V1 routines)
    assert "V1" in captured["messages"][0].content


# ── Configurator endpoint ─────────────────────────────────────────


def test_configurator_rejects_bogus_step() -> None:
    body = ConfigConfiguratorRequest(step="hallucinate", engine="v1", description="x")
    with pytest.raises(HTTPException) as exc:
        llm_configure_stepwise(body, db=_mock_db(None), _user=MagicMock())
    assert exc.value.status_code == 400


def test_configurator_clarify_returns_questions() -> None:
    db = _mock_db({"provider": "azure", "model": "gpt-4o"})
    canned = MagicMock()
    canned.text = (
        '{"questions": [{"key": "scope", "question": "Single entity or all?", '
        '"options": ["Single entity", "All entities"]}]}'
    )
    canned.tokens_in = 30
    canned.tokens_out = 20

    class FakeProvider:
        def __init__(self, *_a, **_k) -> None: ...
        def complete(self, *_a, **_k):
            return canned

    body = ConfigConfiguratorRequest(step="clarify", engine="v1", description="cleanup")
    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        out = llm_configure_stepwise(body, db=db, _user=MagicMock())

    assert out["available"] is True
    assert out["step"] == "clarify"
    assert len(out["questions"]) == 1
    assert out["questions"][0]["options"] == ["Single entity", "All entities"]


def test_configurator_propose_folds_clarifications_into_user_message() -> None:
    db = _mock_db({"provider": "azure", "model": "gpt-4o"})

    canned = MagicMock()
    canned.text = (
        '{"rationale": "Scoped to single entity.",'
        '"config": {"pipeline": [{"routine": "aggregate.combine_outcomes"}]},'
        '"warnings": []}'
    )
    canned.tokens_in = 100
    canned.tokens_out = 50

    captured: dict[str, Any] = {}

    class FakeProvider:
        def __init__(self, *_a, **_k) -> None: ...
        def complete(self, model, messages, **kwargs):
            captured["messages"] = messages
            return canned

    body = ConfigConfiguratorRequest(
        step="propose",
        engine="v1",
        description="initial goal",
        clarifications=[
            {"question": "Single entity or all?", "answer": "Single entity"},
        ],
    )
    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        out = llm_configure_stepwise(body, db=db, _user=MagicMock())

    assert out["available"] is True
    assert out["step"] == "propose"
    assert out["draft"]["pipeline"][0]["routine"] == "aggregate.combine_outcomes"

    # The user message should include both the original description and
    # the clarification Q/A
    user_msg = captured["messages"][-1].content
    assert "initial goal" in user_msg
    assert "Single entity or all?" in user_msg
    assert "Single entity" in user_msg


def test_configurator_refine_includes_prior_draft() -> None:
    db = _mock_db({"provider": "azure", "model": "gpt-4o"})

    canned = MagicMock()
    canned.text = (
        '{"rationale": "Removed redundancy rule per feedback.",'
        '"config": {"pipeline": [{"routine": "rule.posting_activity"}]},'
        '"warnings": []}'
    )
    canned.tokens_in = 200
    canned.tokens_out = 80

    captured: dict[str, Any] = {}

    class FakeProvider:
        def __init__(self, *_a, **_k) -> None: ...
        def complete(self, model, messages, **kwargs):
            captured["messages"] = messages
            return canned

    prior_draft = {
        "pipeline": [
            {"routine": "rule.posting_activity"},
            {"routine": "rule.redundancy"},
        ]
    }
    body = ConfigConfiguratorRequest(
        step="refine",
        engine="v1",
        draft=prior_draft,
        user_feedback="Drop the redundancy rule.",
    )
    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        out = llm_configure_stepwise(body, db=db, _user=MagicMock())

    assert out["available"] is True
    assert out["step"] == "refine"
    assert out["draft"]["pipeline"][0]["routine"] == "rule.posting_activity"
    assert len(out["draft"]["pipeline"]) == 1  # redundancy removed

    # Prior draft must have been serialized into the user message
    user_msg = captured["messages"][-1].content
    assert "rule.posting_activity" in user_msg
    assert "rule.redundancy" in user_msg
    assert "Drop the redundancy rule." in user_msg


def test_configurator_refine_requires_draft_and_feedback() -> None:
    body = ConfigConfiguratorRequest(step="refine", engine="v1")  # no draft, no feedback
    with pytest.raises(HTTPException) as exc:
        llm_configure_stepwise(body, db=_mock_db(None), _user=MagicMock())
    assert exc.value.status_code == 400


def test_configurator_clarify_requires_description() -> None:
    body = ConfigConfiguratorRequest(step="clarify", engine="v1")  # no description
    with pytest.raises(HTTPException) as exc:
        llm_configure_stepwise(body, db=_mock_db(None), _user=MagicMock())
    assert exc.value.status_code == 400
