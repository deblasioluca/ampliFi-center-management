"""Tests for the LLM-powered rule-catalog Q&A endpoint.

Verifies:
* Grounding helper builds short index when no rule_code is given
* Grounding helper includes detailed metadata when a rule_code is given
* System prompt contains the grounding section
* Endpoint logic returns ``available: false`` when no LLM config exists
* Endpoint short-circuits on bad provider type without calling out
* History is replayed into provider messages (cap respected)
* Endpoint surfaces LLM call failures as ``available: false``

Calls the endpoint function directly with a mocked DB session and a
mocked LLM provider — no SQLite (avoids JSONB-on-sqlite incompatibility),
no network, no auth chain.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from app.api.admin import (
    RuleCatalogQARequest,
    _build_qa_system_prompt,
    _build_rule_catalog_grounding,
    rule_catalog_qa,
)


# ── Grounding helper ────────────────────────────────────────────────


def test_build_grounding_no_rule_code_uses_short_index() -> None:
    out = _build_rule_catalog_grounding(rule_code=None)

    assert "## Rule catalog (index)" in out
    # Sanity: index format is "- code (tree/kind): label"
    assert "- " in out
    # Should not include detail-mode header
    assert "## Rule in focus" not in out


def test_build_grounding_with_rule_code_includes_detail_and_index() -> None:
    from app.domain.decision_tree.rule_catalog import list_rule_catalog

    catalog = list_rule_catalog()
    assert catalog, "Catalog must have at least one entry for this test"
    sample_code = catalog[0]["code"]

    out = _build_rule_catalog_grounding(rule_code=sample_code)

    assert f"## Rule in focus: {sample_code}" in out
    # The "other rules" section should still appear so the LLM can refer
    # to siblings if useful
    assert "## Other rules in the catalog" in out


def test_build_grounding_unknown_rule_code_does_not_crash() -> None:
    """If a stale rule_code reaches the helper, it should still return
    a useful prompt rather than blowing up."""
    out = _build_rule_catalog_grounding(rule_code="does.not.exist")
    assert "## Rule in focus: does.not.exist" in out
    assert "## Other rules in the catalog" in out


# ── System prompt builder ───────────────────────────────────────────


def test_system_prompt_embeds_grounding() -> None:
    grounding = "## Rule catalog (index)\n- foo.bar (cleansing/rule): Foo"
    prompt = _build_qa_system_prompt(grounding)

    assert grounding in prompt
    assert "decision-tree rule" in prompt.lower()
    assert "read-only" in prompt.lower()


# ── Endpoint logic — mocked DB and provider ─────────────────────────


def _mock_db(llm_config_value: dict | None) -> MagicMock:
    """Return a MagicMock Session whose ``execute(...).scalar_one_or_none()``
    returns a stub AppConfig (or None) for the LLM key."""
    db = MagicMock()
    if llm_config_value is None:
        db.execute.return_value.scalar_one_or_none.return_value = None
    else:
        cfg = MagicMock()
        cfg.value = llm_config_value
        db.execute.return_value.scalar_one_or_none.return_value = cfg
    return db


def test_qa_unavailable_when_no_llm_config() -> None:
    db = _mock_db(llm_config_value=None)
    body = RuleCatalogQARequest(question="What does the unused activity rule do?")

    result = rule_catalog_qa(body, db=db, _user=MagicMock())

    assert result["available"] is False
    assert result["answer"] is None
    assert "not configured" in result["reason"].lower()


def test_qa_unavailable_for_unknown_provider_type() -> None:
    db = _mock_db(llm_config_value={"provider": "definitely-not-real"})
    body = RuleCatalogQARequest(question="Hi")

    result = rule_catalog_qa(body, db=db, _user=MagicMock())

    assert result["available"] is False
    assert "definitely-not-real" in result["reason"]


def test_qa_happy_path_returns_answer_and_token_counts() -> None:
    db = _mock_db(
        llm_config_value={
            "provider": "azure",
            "endpoint": "https://example.openai.azure.com",
            "api_key": "test",
            "api_version": "2024-02-01",
            "deployment": "gpt-4o",
            "model": "gpt-4o",
        }
    )
    body = RuleCatalogQARequest(
        question="Which rule retires unused centers?",
        rule_code="cleansing.unused_activity",
        history=[
            {"role": "user", "content": "Earlier turn"},
            {"role": "assistant", "content": "Earlier reply"},
        ],
    )

    canned = MagicMock()
    canned.text = "The `cleansing.unused_activity` rule flags centers with no postings."
    canned.tokens_in = 312
    canned.tokens_out = 17

    captured: dict[str, Any] = {}

    class FakeProvider:
        def __init__(self, *_args, **_kwargs) -> None: ...

        def complete(self, model, messages, **kwargs):
            captured["model"] = model
            captured["messages"] = messages
            captured["kwargs"] = kwargs
            return canned

    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        result = rule_catalog_qa(body, db=db, _user=MagicMock())

    assert result["available"] is True
    assert result["answer"].startswith("The `cleansing.unused_activity`")
    assert result["scoped_to"] == "cleansing.unused_activity"
    assert result["model"] == "gpt-4o"
    assert result["tokens_in"] == 312
    assert result["tokens_out"] == 17

    msgs = captured["messages"]
    roles = [m.role for m in msgs]
    assert roles[0] == "system"
    assert "Rule in focus: cleansing.unused_activity" in msgs[0].content
    # Two history turns plus the new user message
    assert roles[-3:] == ["user", "assistant", "user"]
    assert msgs[-1].content == "Which rule retires unused centers?"


def test_qa_history_capped_at_ten_turns() -> None:
    """If the client passes more than 10 prior turns, server only replays
    the most recent 10 — keeps prompt size bounded."""
    db = _mock_db(llm_config_value={"provider": "azure", "model": "gpt-4o"})

    captured: dict[str, Any] = {}

    class FakeProvider:
        def __init__(self, *_args, **_kwargs) -> None: ...

        def complete(self, model, messages, **kwargs):
            captured["messages"] = messages
            r = MagicMock()
            r.text = "ok"
            r.tokens_in = 1
            r.tokens_out = 1
            return r

    long_history = [
        {"role": "user" if i % 2 == 0 else "assistant", "content": f"turn {i}"}
        for i in range(25)
    ]

    body = RuleCatalogQARequest(question="newest", history=long_history)
    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        result = rule_catalog_qa(body, db=db, _user=MagicMock())

    assert result["available"] is True
    msgs = captured["messages"]
    # 1 system + at most 10 history + 1 newest = 12
    assert len(msgs) <= 12
    # Newest user question must be the last message
    assert msgs[-1].content == "newest"


def test_qa_handles_provider_call_failure_gracefully() -> None:
    db = _mock_db(llm_config_value={"provider": "azure", "model": "gpt-4o"})

    class ExplodingProvider:
        def __init__(self, *_args, **_kwargs) -> None: ...

        def complete(self, *_args, **_kwargs):
            raise RuntimeError("network exploded")

    body = RuleCatalogQARequest(question="Anything")
    with patch("app.infra.llm.provider.AzureOpenAIProvider", ExplodingProvider):
        result = rule_catalog_qa(body, db=db, _user=MagicMock())

    assert result["available"] is False
    assert "RuntimeError" in result["reason"]
    assert result["answer"] is None


def test_qa_skips_malformed_history_entries() -> None:
    """Empty content, wrong role, or non-string content should be dropped
    silently — server stays robust to client-side bugs."""
    db = _mock_db(llm_config_value={"provider": "azure", "model": "gpt-4o"})

    captured: dict[str, Any] = {}

    class FakeProvider:
        def __init__(self, *_args, **_kwargs) -> None: ...

        def complete(self, model, messages, **kwargs):
            captured["messages"] = messages
            r = MagicMock()
            r.text = "ok"
            r.tokens_in = 1
            r.tokens_out = 1
            return r

    bad_history = [
        {"role": "user", "content": "real one"},
        {"role": "system", "content": "should be skipped (not user/assistant)"},
        {"role": "assistant", "content": ""},  # empty
        {"role": "user", "content": None},  # not a string
        {"content": "no role"},  # missing role
    ]

    body = RuleCatalogQARequest(question="newest", history=bad_history)
    with patch("app.infra.llm.provider.AzureOpenAIProvider", FakeProvider):
        rule_catalog_qa(body, db=db, _user=MagicMock())

    msgs = captured["messages"]
    contents = [m.content for m in msgs]
    # The single valid history turn must be in there
    assert "real one" in contents
    # The skipped entries must not be
    assert "should be skipped (not user/assistant)" not in contents
    assert "no role" not in contents
