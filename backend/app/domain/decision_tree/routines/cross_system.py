"""Rule: cross-system dependency (§04.1 criterion ⑤).

Centers used in BW extractors, GRC, or intercompany processes
need careful migration and go through MERGE_MAP.
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class CrossSystemDependencyRoutine:
    """Flags centers with high cross-system dependencies."""

    @property
    def code(self) -> str:
        return "rule.cross_system_dependency"

    @property
    def name(self) -> str:
        return "Cross-System Dependency Check"

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
                "check_bw": {"type": "boolean", "default": True},
                "check_grc": {"type": "boolean", "default": True},
                "check_intercompany": {"type": "boolean", "default": True},
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        deps: list[str] = []
        if params.get("check_bw", True) and ctx.in_bw_extractors:
            deps.append("BW")
        if params.get("check_grc", True) and ctx.in_grc:
            deps.append("GRC")
        if params.get("check_intercompany", True) and ctx.in_intercompany:
            deps.append("intercompany")

        if deps:
            return RoutineResult(
                code=self.code,
                verdict="MERGE_MAP",
                reason="dependency.high",
                short_circuit=True,
                payload={"dependencies": deps},
            )

        return RoutineResult(
            code=self.code,
            verdict="PASS",
            reason="dependency.none",
            payload={"dependencies": []},
        )
