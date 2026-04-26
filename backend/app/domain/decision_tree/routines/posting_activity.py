"""Rule: posting activity check (§04.1 criterion ①).

Centers with no posting activity beyond the configured threshold are RETIRE candidates.
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class PostingActivityRoutine:
    """Checks months since last posting against inactivity threshold."""

    @property
    def code(self) -> str:
        return "rule.posting_activity"

    @property
    def name(self) -> str:
        return "Posting Activity Check"

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
                "posting_inactivity_threshold": {
                    "type": "integer",
                    "default": 12,
                    "description": "Months of inactivity before flagging RETIRE",
                },
                "posting_minimal_threshold": {
                    "type": "integer",
                    "default": 0,
                    "description": "Minimum posting count in window to be considered active",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        threshold_months = params.get("posting_inactivity_threshold", 12)
        min_postings = params.get("posting_minimal_threshold", 0)

        if not ctx.is_active:
            return RoutineResult(
                code=self.code,
                verdict="RETIRE",
                reason="posting.inactive_flag",
                short_circuit=True,
                payload={"is_active": False},
            )

        if ctx.months_since_last_posting is None:
            return RoutineResult(
                code=self.code,
                verdict="UNKNOWN",
                reason="posting.no_data",
                payload={"months_since_last_posting": None},
            )

        months = ctx.months_since_last_posting
        postings = ctx.posting_count_window or 0

        if months >= threshold_months and postings <= min_postings:
            return RoutineResult(
                code=self.code,
                verdict="RETIRE",
                reason="posting.inactive",
                short_circuit=True,
                payload={
                    "months_since_last_posting": months,
                    "threshold": threshold_months,
                    "posting_count_window": postings,
                },
            )

        return RoutineResult(
            code=self.code,
            verdict="PASS",
            reason="posting.active",
            payload={
                "months_since_last_posting": months,
                "posting_count_window": postings,
            },
        )
