"""LLM review mode orchestrators (§05.9).

Three modes:
- SINGLE: One model, one prompt, one response per center
- SEQUENTIAL: Chain of models (drafter → critic → finalizer)
- DEBATE: Two advocates + judge
"""

from __future__ import annotations

import structlog

from app.infra.llm.provider import Completion, LLMProvider, Message

logger = structlog.get_logger()


def render_prompt(template: str, center: dict, det_outputs: dict, **extra: str) -> str:
    """Render a prompt template with center data and deterministic outputs."""
    context_lines = [
        f"Cost Center: {center.get('cctr', 'N/A')} ({center.get('txtsh', '')})",
        f"Company Code: {center.get('ccode', 'N/A')}",
        f"CO Area: {center.get('coarea', 'N/A')}",
        f"Category: {center.get('cctrcgy', 'N/A')}",
        f"Responsible: {center.get('responsible', 'N/A')}",
        f"Months Since Last Posting: {det_outputs.get('months_since_last_posting', 'N/A')}",
        f"Posting Count (window): {det_outputs.get('posting_count_window', 'N/A')}",
        f"Total Balance: {det_outputs.get('total_balance', 'N/A')}",
        f"Hierarchy Memberships: {det_outputs.get('hierarchy_membership_count', 'N/A')}",
        f"Deterministic Verdict: {det_outputs.get('cleansing_outcome', 'N/A')}",
        f"Target Object: {det_outputs.get('target_object', 'N/A')}",
        f"Rule Path: {det_outputs.get('rule_path', 'N/A')}",
    ]
    context_block = "\n".join(context_lines)

    replacements = {
        "{{center_context}}": context_block,
        "{{deterministic_verdict}}": str(det_outputs.get("cleansing_outcome", "")),
        "{{target_object}}": str(det_outputs.get("target_object", "")),
        "{{rule_path}}": str(det_outputs.get("rule_path", "")),
    }
    for k, v in extra.items():
        replacements[f"{{{{{k}}}}}"] = v

    result = template
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)
    return result


def review_single(
    provider: LLMProvider,
    model: str,
    prompt_template: str,
    center: dict,
    det_outputs: dict,
    temperature: float = 0.0,
) -> Completion:
    """SINGLE mode: one model, one prompt, one response (§05.9)."""
    prompt = render_prompt(prompt_template, center, det_outputs)
    messages = [
        Message(
            role="system",
            content="You are an expert SAP cost center analyst reviewing cleanup proposals.",
        ),
        Message(role="user", content=prompt),
    ]
    return provider.complete(model, messages, temperature=temperature)


def review_sequential(
    providers: list[tuple[LLMProvider, str, str]],  # [(provider, model, role)]
    prompt_templates: dict[str, str],  # role → template
    center: dict,
    det_outputs: dict,
    temperature: float = 0.0,
) -> list[Completion]:
    """SEQUENTIAL mode: drafter → critic → finalizer chain (§05.9)."""
    completions: list[Completion] = []
    draft_text = ""
    critique_text = ""

    for provider, model, role in providers:
        template = prompt_templates.get(role, "")
        if role == "drafter":
            prompt = render_prompt(template, center, det_outputs)
        elif role == "critic":
            prompt = render_prompt(template, center, det_outputs, draft=draft_text)
        elif role == "finalizer":
            prompt = render_prompt(
                template,
                center,
                det_outputs,
                draft=draft_text,
                critique=critique_text,
            )
        else:
            prompt = render_prompt(template, center, det_outputs)

        messages = [
            Message(
                role="system",
                content=f"You are acting as the {role} in a cost center review pipeline.",
            ),
            Message(role="user", content=prompt),
        ]
        completion = provider.complete(model, messages, temperature=temperature)
        completions.append(completion)

        if role == "drafter":
            draft_text = completion.text
        elif role == "critic":
            critique_text = completion.text

    return completions


def review_debate(
    advocate_a: tuple[LLMProvider, str],  # (provider, model)
    advocate_b: tuple[LLMProvider, str],
    judge: tuple[LLMProvider, str],
    prompt_templates: dict[str, str],
    center: dict,
    det_outputs: dict,
    rounds: int = 2,
    temperature: float = 0.0,
) -> list[Completion]:
    """DEBATE mode: two advocates + judge (§05.9)."""
    completions: list[Completion] = []

    prov_a, model_a = advocate_a
    prov_b, model_b = advocate_b
    prov_j, model_j = judge

    # Opening statements
    pos_a = ""
    pos_b = ""

    template_a = prompt_templates.get("advocate_a", "")
    prompt_a = render_prompt(template_a, center, det_outputs)
    messages_a = [
        Message(
            role="system",
            content="You are Advocate A arguing FOR keeping/mapping this cost center.",
        ),
        Message(role="user", content=prompt_a),
    ]
    comp_a = prov_a.complete(model_a, messages_a, temperature=temperature)
    completions.append(comp_a)
    pos_a = comp_a.text

    template_b = prompt_templates.get("advocate_b", "")
    prompt_b = render_prompt(template_b, center, det_outputs)
    messages_b = [
        Message(
            role="system",
            content="You are Advocate B arguing FOR retiring/merging this cost center.",
        ),
        Message(role="user", content=prompt_b),
    ]
    comp_b = prov_b.complete(model_b, messages_b, temperature=temperature)
    completions.append(comp_b)
    pos_b = comp_b.text

    # Rebuttal rounds
    for _round in range(rounds - 1):
        template_ra = prompt_templates.get("rebuttal_a", "")
        prompt_ra = render_prompt(
            template_ra,
            center,
            det_outputs,
            position_a=pos_a,
            position_b=pos_b,
        )
        messages_ra = [
            Message(
                role="system", content="You are Advocate A. Respond to Advocate B's arguments."
            ),
            Message(role="user", content=prompt_ra),
        ]
        comp_ra = prov_a.complete(model_a, messages_ra, temperature=temperature)
        completions.append(comp_ra)
        pos_a = comp_ra.text

        template_rb = prompt_templates.get("rebuttal_b", "")
        prompt_rb = render_prompt(
            template_rb,
            center,
            det_outputs,
            position_a=pos_a,
            position_b=pos_b,
        )
        messages_rb = [
            Message(
                role="system", content="You are Advocate B. Respond to Advocate A's arguments."
            ),
            Message(role="user", content=prompt_rb),
        ]
        comp_rb = prov_b.complete(model_b, messages_rb, temperature=temperature)
        completions.append(comp_rb)
        pos_b = comp_rb.text

    # Judge verdict
    template_j = prompt_templates.get("judge", "")
    prompt_j = render_prompt(
        template_j,
        center,
        det_outputs,
        position_a=pos_a,
        position_b=pos_b,
    )
    messages_j = [
        Message(
            role="system",
            content="You are the Judge. Review both advocates' arguments and render your verdict.",
        ),
        Message(role="user", content=prompt_j),
    ]
    comp_j = prov_j.complete(model_j, messages_j, temperature=temperature)
    completions.append(comp_j)

    return completions
