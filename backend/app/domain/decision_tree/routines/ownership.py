"""Rule: business ownership check (§04.1 criterion ②).

Centers without a valid owner are RETIRE candidates.
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class OwnershipRoutine:
    """Checks that the center has a valid, resolvable owner."""

    @property
    def code(self) -> str:
        return "rule.ownership"

    @property
    def name(self) -> str:
        return "Business Ownership Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "cleansing"

    @property
    def params_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "require_active_person": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, owner must resolve to an active person record",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        if not ctx.has_owner:
            return RoutineResult(
                code=self.code,
                verdict="RETIRE",
                reason="ownership.no_owner",
                short_circuit=True,
                payload={
                    "responsible": ctx.responsible,
                    "has_owner": False,
                },
            )

        return RoutineResult(
            code=self.code,
            verdict="PASS",
            reason="ownership.valid",
            payload={"responsible": ctx.responsible, "has_owner": True},
        )
