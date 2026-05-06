"""LLM routine: independent advisor.

Asks an LLM to look at the center and propose an outcome with reasoning,
without seeing the rule tree's verdict. The point is to use it in
*comparison mode* alongside the rule tree and the ML predictor: where all
three agree, we have high confidence; where they disagree, the case
deserves human review.

This is intentionally NOT a "review" routine that critiques another
verdict — that's what ``app.infra.llm.review`` is for. This is a fresh
opinion based only on the center's facts.

The routine is robust to an unavailable LLM (no credentials configured,
network failure, parsing failure): it returns a ``PASS`` verdict with a
``llm_unavailable`` reason so the comparison still works and the rest
of the pipeline isn't blocked.
"""

from __future__ import annotations

import json
import re
from typing import Any

import structlog

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine

logger = structlog.get_logger()


VERDICT_VOCAB = ("KEEP", "RETIRE", "MERGE_MAP", "REDESIGN")


def _build_system_prompt() -> str:
    return (
        "You are a senior SAP controlling expert advising on cost-center cleanup. "
        "You will look at a single cost center and propose ONE outcome from this exact "
        "vocabulary: KEEP, RETIRE, MERGE_MAP, REDESIGN. Give a short reason (max 2 sentences). "
        "You MUST respond with valid JSON of the form: "
        '{"verdict": "<one of KEEP|RETIRE|MERGE_MAP|REDESIGN>", '
        '"confidence": <0.0..1.0>, "reason": "<short text>"}. '
        "No markdown, no code fences, no extra prose around the JSON."
    )


def _build_user_prompt(ctx: CenterContext) -> str:
    months = (
        ctx.months_since_last_posting if ctx.months_since_last_posting is not None else "unknown"
    )
    pcount = ctx.posting_count_window if ctx.posting_count_window is not None else "unknown"
    ext_systems = (
        "+".join(
            s
            for s, v in [
                ("BW", ctx.in_bw_extractors),
                ("GRC", ctx.in_grc),
                ("IC", ctx.in_intercompany),
            ]
            if v
        )
        or "no"
    )

    facts = [
        f"Cost Center: {ctx.cctr} (company code {ctx.ccode})",
        f"Short text: {ctx.txtsh or '(none)'}",
        f"Long text: {ctx.txtmi or '(none)'}",
        f"Active: {'yes' if ctx.is_active else 'no'}",
        f"Responsible owner present: {'yes' if ctx.has_owner else 'no'}",
        f"Months since last posting: {months}",
        f"Posting count (recent window): {pcount}",
        f"Balance sheet amount: {ctx.bs_amt:,.0f}",
        f"Revenue amount: {ctx.rev_amt:,.0f}",
        f"Operational expense amount: {ctx.opex_amt:,.0f}",
        f"Hierarchy memberships: {ctx.hierarchy_membership_count}",
        f"Used as feeder: {'yes' if ctx.is_feeder else 'no'}",
        f"Used as allocation vehicle: {'yes' if ctx.is_allocation_vehicle else 'no'}",
        f"Project-related: {'yes' if ctx.is_project_related else 'no'}",
        f"Has direct revenue: {'yes' if ctx.has_direct_revenue else 'no'}",
        f"Has operational costs: {'yes' if ctx.has_operational_costs else 'no'}",
        f"Referenced by external systems (BW/GRC/IC): {ext_systems}",
        f"Duplicate cluster size: {ctx.duplicate_cluster_size}",
    ]
    return (
        "Recommend an outcome for this cost center.\n\n"
        + "\n".join(f"- {f}" for f in facts)
        + "\n\nRespond as JSON only."
    )


def _parse_response(text: str) -> dict[str, Any] | None:
    """Best-effort parse: try strict JSON first, then extract JSON object.

    Returns a dict with keys 'verdict', 'confidence', 'reason' on success,
    else None.
    """
    text = (text or "").strip()
    if not text:
        return None
    # Strip ```json fences if a model added them anyway.
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE).strip()

    parsed: dict[str, Any] | None = None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Try to find the first {...} block.
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group(0))
            except json.JSONDecodeError:
                return None
    if not isinstance(parsed, dict):
        return None

    verdict = str(parsed.get("verdict", "")).upper().strip()
    if verdict not in VERDICT_VOCAB:
        return None
    try:
        confidence = float(parsed.get("confidence", 0.5))
        confidence = max(0.0, min(1.0, confidence))
    except (TypeError, ValueError):
        confidence = 0.5
    reason = str(parsed.get("reason", "")).strip()[:500]
    return {"verdict": verdict, "confidence": confidence, "reason": reason}


def _get_llm_provider():
    """Resolve the configured LLM provider, or None if not available."""
    try:
        from app.config import get_settings
        from app.infra.llm.provider import AzureOpenAIProvider

        settings = get_settings()
        api_key = (
            settings.azure_openai_api_key.get_secret_value()
            if settings.azure_openai_api_key
            else ""
        )
        if not api_key:
            return None, None
        endpoint = getattr(settings, "azure_openai_endpoint", "")
        deployment = getattr(settings, "azure_openai_deployment", "")
        api_version = getattr(settings, "azure_openai_api_version", "2024-06-01")
        if not (endpoint and deployment):
            return None, None
        provider = AzureOpenAIProvider(
            {
                "endpoint": endpoint,
                "api_key": api_key,
                "deployment": deployment,
                "api_version": api_version,
            }
        )
        return provider, deployment
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("llm_advisor.provider_resolution_failed", error=str(exc))
        return None, None


@register_routine
class LLMAdvisor:
    """Independent LLM-based outcome advisor.

    When configured, calls the LLM provider with a minimal "what should
    happen with this center?" prompt and parses a structured JSON verdict.
    When the LLM is unreachable or the response cannot be parsed, the
    routine returns ``verdict='PASS'`` with reason ``llm_unavailable`` —
    the rest of the pipeline keeps running.
    """

    @property
    def code(self) -> str:
        return "llm.advisor"

    @property
    def name(self) -> str:
        return "LLM Outcome Advisor"

    @property
    def kind(self) -> str:
        return "llm"

    @property
    def tree(self) -> str | None:
        return None

    @property
    def params_schema(self) -> dict | None:
        return {
            "type": "object",
            "properties": {
                "model": {
                    "type": "string",
                    "default": "gpt-4o-mini",
                    "description": "LLM model identifier (provider-specific).",
                },
                "temperature": {
                    "type": "number",
                    "default": 0.0,
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "description": "0.0 = deterministic; raise to inject variability.",
                },
                "max_tokens": {
                    "type": "integer",
                    "default": 250,
                    "description": "Cap response length — verdicts are short.",
                },
                "skip_if_high_confidence": {
                    "type": "number",
                    "default": 0.0,
                    "description": (
                        "Skip the LLM call if the ML routine produced a verdict "
                        "with confidence ≥ this threshold. 0.0 = always call. "
                        "Useful to control LLM cost on large waves."
                    ),
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        # Optional cost-saving short-circuit: skip LLM when ML is already
        # confident enough.
        threshold = float(params.get("skip_if_high_confidence", 0.0))
        if threshold > 0 and ctx.ml_outcome_probs:
            top_prob = max(ctx.ml_outcome_probs.values())
            if top_prob >= threshold:
                return RoutineResult(
                    code=self.code,
                    verdict="PASS",
                    reason=f"llm.skipped_high_ml_confidence:{top_prob:.2f}",
                    payload={"engine": "llm", "skipped": True, "ml_confidence": top_prob},
                )

        provider, model_name = _get_llm_provider()
        configured_model = params.get("model") or model_name or "gpt-4o-mini"
        if provider is None:
            return RoutineResult(
                code=self.code,
                verdict="PASS",
                reason="llm.unavailable:no_provider_configured",
                comment=None,
                payload={"engine": "llm", "available": False},
            )

        try:
            from app.infra.llm.provider import Message

            messages = [
                Message(role="system", content=_build_system_prompt()),
                Message(role="user", content=_build_user_prompt(ctx)),
            ]
            completion = provider.complete(
                model=configured_model,
                messages=messages,
                temperature=float(params.get("temperature", 0.0)),
                max_tokens=int(params.get("max_tokens", 250)),
                metadata={"routine": self.code, "center_id": ctx.center_id},
            )
            parsed = _parse_response(completion.text)
        except Exception as exc:
            logger.warning(
                "llm_advisor.call_failed",
                center_id=ctx.center_id,
                error=str(exc),
            )
            return RoutineResult(
                code=self.code,
                verdict="PASS",
                reason=f"llm.call_failed:{type(exc).__name__}",
                payload={"engine": "llm", "available": True, "error": str(exc)},
            )

        if not parsed:
            return RoutineResult(
                code=self.code,
                verdict="PASS",
                reason="llm.parse_failed",
                comment=completion.text[:500] if completion else None,
                payload={"engine": "llm", "available": True, "raw": (completion.text or "")[:500]},
            )

        return RoutineResult(
            code=self.code,
            verdict=parsed["verdict"],
            score=parsed["confidence"],
            comment=parsed["reason"],
            reason=f"llm.advised:{parsed['verdict'].lower()}",
            payload={
                "engine": "llm",
                "available": True,
                "model": configured_model,
                "tokens_in": getattr(completion, "tokens_in", 0),
                "tokens_out": getattr(completion, "tokens_out", 0),
                "cost_usd": getattr(completion, "cost_usd", 0.0),
            },
        )
