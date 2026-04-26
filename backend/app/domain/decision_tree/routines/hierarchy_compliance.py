"""Rule: hierarchy compliance (§04.1 criterion ④).

Centers must belong to exactly one hierarchy node.
Non-compliant centers are MERGE_MAP (or REDESIGN in strict mode).
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class HierarchyComplianceRoutine:
    """Checks single-node hierarchy membership."""

    @property
    def code(self) -> str:
        return "rule.hierarchy_compliance"

    @property
    def name(self) -> str:
        return "Hierarchy Compliance Check"

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
                "strict_hierarchy_mode": {
                    "type": "boolean",
                    "default": False,
                    "description": "When true, orphans (count=0) → REDESIGN instead of MERGE_MAP",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        strict = params.get("strict_hierarchy_mode", False)
        count = ctx.hierarchy_membership_count

        if count == 1:
            return RoutineResult(
                code=self.code,
                verdict="PASS",
                reason="hierarchy.compliant",
                payload={"membership_count": count},
            )

        if count == 0:
            verdict = "REDESIGN" if strict else "MERGE_MAP"
            sub_reason = "hierarchy.orphan" if strict else "hierarchy.non_compliant"
            return RoutineResult(
                code=self.code,
                verdict=verdict,
                reason=sub_reason,
                short_circuit=True,
                payload={"membership_count": 0, "strict_mode": strict},
            )

        # count > 1: multi-membership
        return RoutineResult(
            code=self.code,
            verdict="MERGE_MAP",
            reason="hierarchy.non_compliant",
            short_circuit=True,
            payload={"membership_count": count},
        )
