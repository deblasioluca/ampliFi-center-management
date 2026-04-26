"""Rule: balance threshold check (§04.6 example plugin).

Configurable rule that flags centers whose total balance falls below
or above user-defined thresholds, useful for identifying dormant or
anomalously high-balance centers.
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class BalanceThresholdRoutine:
    """Checks if center balance exceeds configured thresholds."""

    @property
    def code(self) -> str:
        return "rule.balance_threshold"

    @property
    def name(self) -> str:
        return "Balance Threshold Check"

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
                "min_balance_retire": {
                    "type": "number",
                    "default": 100,
                    "description": "Retire if total absolute balance below this (dormant center)",
                },
                "max_balance_redesign": {
                    "type": "number",
                    "default": 10000000,
                    "description": "Flag for redesign if total balance exceeds this",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        min_retire = params.get("min_balance_retire", 100)
        max_redesign = params.get("max_balance_redesign", 10_000_000)

        total = abs(ctx.bs_amt or 0) + abs(ctx.opex_amt or 0) + abs(ctx.rev_amt or 0)

        if total < min_retire:
            return RoutineResult(
                code=self.code,
                verdict="RETIRE",
                reason="balance.below_threshold",
                payload={"total_balance": float(total), "threshold": min_retire},
            )

        if total > max_redesign:
            return RoutineResult(
                code=self.code,
                verdict="REDESIGN",
                reason="balance.above_threshold",
                payload={"total_balance": float(total), "threshold": max_redesign},
            )

        return RoutineResult(
            code=self.code,
            verdict="PASS",
            reason="balance.within_range",
            payload={"total_balance": float(total)},
        )
