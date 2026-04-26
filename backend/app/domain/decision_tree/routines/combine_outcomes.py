"""Aggregate: combine routine outputs into final outcome + target_object (§05.1).

Runs after all rule and ML routines. Reduces individual verdicts into the
canonical cleansing_outcome and target_object for the proposal.
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine

_CLEANSING_PRIORITY = {"RETIRE": 0, "MERGE_MAP": 1, "REDESIGN": 2, "KEEP": 3}
_TARGET_PRIORITY = {
    "PC_ONLY": 0,
    "PC": 0,
    "CC_AND_PC": 1,
    "WBS_REAL": 2,
    "CC": 3,
    "WBS_STAT": 4,
    "NONE": 5,
}


@register_routine
class CombineOutcomesRoutine:
    """Reduces all routine results into a final cleansing outcome + target object."""

    @property
    def code(self) -> str:
        return "aggregate.combine_outcomes"

    @property
    def name(self) -> str:
        return "Combine Outcomes"

    @property
    def kind(self) -> str:
        return "aggregate"

    @property
    def tree(self) -> str | None:
        return None

    @property
    def params_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "ml_override_when_confidence_above": {
                    "type": "number",
                    "default": 0.95,
                    "description": "ML can override rule verdict when confidence exceeds this",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        # This routine is special — it reads from ctx.flags which accumulates
        # prior routine results. The engine sets these before calling combine.
        prior_results: list[dict] = ctx.flags.get("_prior_results", [])

        cleansing_outcome = "KEEP"
        target_object = "NONE"
        rule_path: list[str] = []
        ml_confidence: float | None = None

        # Cleansing: first short-circuit wins (earliest in pipeline)
        for r in prior_results:
            verdict = r.get("verdict", "PASS")
            code = r.get("code", "")
            reason = r.get("reason", "")

            if verdict in _CLEANSING_PRIORITY and verdict != "KEEP":
                if _CLEANSING_PRIORITY.get(verdict, 99) < _CLEANSING_PRIORITY.get(
                    cleansing_outcome, 99
                ):
                    cleansing_outcome = verdict
                rule_path.append(f"{code}:{reason}")
            elif verdict == "PASS":
                rule_path.append(f"{code}:pass")

        # If no cleansing routine fired, it's KEEP
        if cleansing_outcome == "KEEP":
            rule_path.append("default:KEEP")

        # Mapping: only if KEEP or MERGE_MAP
        if cleansing_outcome in ("KEEP", "MERGE_MAP"):
            for r in prior_results:
                verdict = r.get("verdict", "PASS")
                code = r.get("code", "")
                if verdict in _TARGET_PRIORITY:
                    if _TARGET_PRIORITY.get(verdict, 99) < _TARGET_PRIORITY.get(target_object, 99):
                        target_object = verdict
                    rule_path.append(f"{code}→{verdict}")

        # RETIRE / REDESIGN → target NONE
        if cleansing_outcome in ("RETIRE", "REDESIGN"):
            target_object = "NONE"

        # Fallthrough for mapping: KEEP defaults to CC (ensures target creation)
        if cleansing_outcome == "KEEP" and target_object == "NONE":
            target_object = "CC"
            rule_path.append("mapping.fallthrough→CC")

        # ML confidence from context
        if ctx.ml_outcome_probs:
            ml_confidence = ctx.ml_outcome_probs.get(cleansing_outcome)

        return RoutineResult(
            code=self.code,
            verdict=cleansing_outcome,
            reason=f"combined.{cleansing_outcome.lower()}.{target_object.lower()}",
            payload={
                "cleansing_outcome": cleansing_outcome,
                "target_object": target_object,
                "rule_path": rule_path,
                "ml_confidence": ml_confidence,
            },
        )
