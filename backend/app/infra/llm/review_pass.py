"""LLM review pass orchestrator (§13 / §05.9).

Implements SINGLE, SEQUENTIAL, and DEBATE modes using Jinja2 prompt templates.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import structlog
from jinja2 import Environment, FileSystemLoader

from app.infra.llm.provider import LLMProvider, Message

logger = structlog.get_logger()

PROMPTS_DIR = Path(__file__).parent / "prompts"
_jinja_env = Environment(
    loader=FileSystemLoader(str(PROMPTS_DIR)),
    autoescape=False,  # noqa: S701 — plain text templates, not HTML
)


_BLOCK_SEPARATOR = "---PROMPT_SPLIT---"


def _render(template_name: str, context: dict) -> tuple[str, str]:
    """Render a prompt template, returning (system, user) strings.

    Templates use {% block system %} and {% block user %} Jinja2 blocks.
    We inject a known separator between blocks during rendering, then split on it.
    """
    # Read template source, inject separator between endblock/block boundaries
    source = _jinja_env.loader.get_source(_jinja_env, template_name)[0]

    # Insert separator marker between {% endblock %} and {% block user %}
    source = re.sub(
        r"\{%\s*endblock\s*%\}\s*\{%\s*block\s+user\s*%\}",
        f"{{% endblock %}}\n{_BLOCK_SEPARATOR}\n{{% block user %}}",
        source,
    )
    tpl = _jinja_env.from_string(source)
    full = tpl.render(**context)

    if _BLOCK_SEPARATOR in full:
        system, user = full.split(_BLOCK_SEPARATOR, 1)
        return system.strip(), user.strip()
    return "", full.strip()


def _parse_json(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown fences."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        start = 1
        end = len(lines)
        for i in range(1, len(lines)):
            if lines[i].startswith("```"):
                end = i
                break
        text = "\n".join(lines[start:end])
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("llm.parse_json_failed", text=text[:200])
        return {"error": "Failed to parse LLM response", "raw": text[:500]}


def build_center_context(
    center: dict,
    features: dict,
    outcome: dict,
    ml: dict | None = None,
) -> dict:
    """Build the template context for a center review."""
    return {
        "center": center,
        "features": features,
        "outcome": outcome,
        "ml": ml or {"outcome_probs": {}, "target_probs": {}},
    }


def single_pass(
    provider: LLMProvider,
    model: str,
    center_context: dict,
    max_tokens: int = 2000,
) -> dict:
    """SINGLE mode — one LLM call per center (§13.2)."""
    system, user = _render("review.v3.j2", center_context)
    messages = [Message(role="system", content=system), Message(role="user", content=user)]
    completion = provider.complete(model, messages, temperature=0.0, max_tokens=max_tokens)
    result = _parse_json(completion.text)
    result["_llm_meta"] = {
        "mode": "SINGLE",
        "model": completion.model,
        "tokens_in": completion.tokens_in,
        "tokens_out": completion.tokens_out,
        "cost_usd": completion.cost_usd,
        "prompt_hash": completion.prompt_hash,
    }
    return result


def sequential_pass(
    provider: LLMProvider,
    model: str,
    center_context: dict,
    max_tokens: int = 2000,
) -> dict:
    """SEQUENTIAL mode — drafter → critic → finaliser (§13.3)."""
    # Step 1: Drafter
    system, user = _render("review_draft.v3.j2", center_context)
    draft_completion = provider.complete(
        model,
        [Message("system", system), Message("user", user)],
        temperature=0.0,
        max_tokens=max_tokens,
    )
    drafter_output = _parse_json(draft_completion.text)

    # Step 2: Critic
    critic_context = {**center_context, "drafter_output": drafter_output}
    system, user = _render("review_critic.v3.j2", critic_context)
    critic_completion = provider.complete(
        model,
        [Message("system", system), Message("user", user)],
        temperature=0.0,
        max_tokens=max_tokens,
    )
    critic_output = _parse_json(critic_completion.text)

    # Step 3: Finaliser
    final_context = {
        **center_context,
        "drafter_output": drafter_output,
        "critic_output": critic_output,
    }
    system, user = _render("review_final.v3.j2", final_context)
    final_completion = provider.complete(
        model,
        [Message("system", system), Message("user", user)],
        temperature=0.0,
        max_tokens=max_tokens,
    )
    result = _parse_json(final_completion.text)

    total_tokens_in = (
        draft_completion.tokens_in + critic_completion.tokens_in + final_completion.tokens_in
    )
    total_tokens_out = (
        draft_completion.tokens_out + critic_completion.tokens_out + final_completion.tokens_out
    )
    total_cost = draft_completion.cost_usd + critic_completion.cost_usd + final_completion.cost_usd

    result["_llm_meta"] = {
        "mode": "SEQUENTIAL",
        "model": final_completion.model,
        "tokens_in": total_tokens_in,
        "tokens_out": total_tokens_out,
        "cost_usd": total_cost,
        "drafter": drafter_output,
        "critic": critic_output,
    }
    return result


def debate_pass(
    provider: LLMProvider,
    model: str,
    center_context: dict,
    max_tokens: int = 2000,
    rounds: int = 1,
) -> dict:
    """DEBATE mode — advocate A / advocate B / rebuttals / judge (§13.4)."""
    outcome = center_context.get("outcome", {})
    det_outcome = outcome.get("cleansing", "KEEP")
    alt_outcome = "RETIRE" if det_outcome == "KEEP" else "KEEP"

    position_a = f"the deterministic outcome {det_outcome} is correct"
    position_b = f"the outcome should be {alt_outcome} instead"

    # Round 1: Advocate A
    ctx_a = {
        **center_context,
        "side": "A",
        "position": position_a,
        "opponent_arg": None,
        "prior_arg": None,
    }
    sys_a, usr_a = _render("debate_advocate.v1.j2", ctx_a)
    comp_a = provider.complete(
        model,
        [Message("system", sys_a), Message("user", usr_a)],
        temperature=0.0,
        max_tokens=max_tokens,
    )
    arg_a = _parse_json(comp_a.text)

    # Round 1: Advocate B
    ctx_b = {
        **center_context,
        "side": "B",
        "position": position_b,
        "opponent_arg": arg_a,
        "prior_arg": None,
    }
    sys_b, usr_b = _render("debate_advocate.v1.j2", ctx_b)
    comp_b = provider.complete(
        model,
        [Message("system", sys_b), Message("user", usr_b)],
        temperature=0.0,
        max_tokens=max_tokens,
    )
    arg_b = _parse_json(comp_b.text)

    total_tokens_in = comp_a.tokens_in + comp_b.tokens_in
    total_tokens_out = comp_a.tokens_out + comp_b.tokens_out
    total_cost = comp_a.cost_usd + comp_b.cost_usd

    # Additional rebuttal rounds
    for _r in range(rounds):
        # Rebuttal A
        ctx_a = {
            **center_context,
            "side": "A",
            "position": position_a,
            "opponent_arg": arg_b,
            "prior_arg": arg_a,
        }
        sys_a, usr_a = _render("debate_advocate.v1.j2", ctx_a)
        comp_a = provider.complete(
            model,
            [Message("system", sys_a), Message("user", usr_a)],
            temperature=0.0,
            max_tokens=max_tokens,
        )
        arg_a = _parse_json(comp_a.text)
        total_tokens_in += comp_a.tokens_in
        total_tokens_out += comp_a.tokens_out
        total_cost += comp_a.cost_usd

        # Rebuttal B
        ctx_b = {
            **center_context,
            "side": "B",
            "position": position_b,
            "opponent_arg": arg_a,
            "prior_arg": arg_b,
        }
        sys_b, usr_b = _render("debate_advocate.v1.j2", ctx_b)
        comp_b = provider.complete(
            model,
            [Message("system", sys_b), Message("user", usr_b)],
            temperature=0.0,
            max_tokens=max_tokens,
        )
        arg_b = _parse_json(comp_b.text)
        total_tokens_in += comp_b.tokens_in
        total_tokens_out += comp_b.tokens_out
        total_cost += comp_b.cost_usd

    # Judge
    judge_ctx = {**center_context, "advocate_a": arg_a, "advocate_b": arg_b}
    sys_j, usr_j = _render("debate_judge.v1.j2", judge_ctx)
    comp_j = provider.complete(
        model,
        [Message("system", sys_j), Message("user", usr_j)],
        temperature=0.0,
        max_tokens=max_tokens,
    )
    result = _parse_json(comp_j.text)
    total_tokens_in += comp_j.tokens_in
    total_tokens_out += comp_j.tokens_out
    total_cost += comp_j.cost_usd

    result["_llm_meta"] = {
        "mode": "DEBATE",
        "model": comp_j.model,
        "tokens_in": total_tokens_in,
        "tokens_out": total_tokens_out,
        "cost_usd": total_cost,
        "advocate_a": arg_a,
        "advocate_b": arg_b,
        "rounds": rounds + 1,
    }
    return result


def run_review_pass(
    provider: LLMProvider,
    model: str,
    mode: str,
    center_context: dict,
    max_tokens: int = 2000,
    debate_rounds: int = 1,
) -> dict:
    """Dispatcher — calls the appropriate mode handler."""
    if mode == "SINGLE":
        return single_pass(provider, model, center_context, max_tokens)
    elif mode == "SEQUENTIAL":
        return sequential_pass(provider, model, center_context, max_tokens)
    elif mode == "DEBATE":
        return debate_pass(provider, model, center_context, max_tokens, debate_rounds)
    else:
        raise ValueError(f"Unknown LLM review mode: {mode}")
