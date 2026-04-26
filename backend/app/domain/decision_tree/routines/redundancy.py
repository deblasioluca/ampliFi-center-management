"""Rule: redundancy / duplicate check (§04.1 criterion ③).

Centers that belong to a duplicate cluster are MERGE_MAP candidates.
This rule works in tandem with ml.duplicate_cluster which sets the
cluster_id and cluster_size on the context.
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class RedundancyRoutine:
    """Checks for duplicate/overlapping centers via cluster membership."""

    @property
    def code(self) -> str:
        return "rule.redundancy"

    @property
    def name(self) -> str:
        return "Redundancy / Duplicate Check"

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
                "similarity_threshold": {
                    "type": "number",
                    "default": 0.92,
                    "description": "Cosine similarity threshold for duplicate detection",
                },
                "min_cluster_size": {
                    "type": "integer",
                    "default": 2,
                    "description": "Minimum cluster size to trigger MERGE_MAP",
                },
            },
        }

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        min_size = params.get("min_cluster_size", 2)

        if ctx.duplicate_cluster_id and ctx.duplicate_cluster_size >= min_size:
            return RoutineResult(
                code=self.code,
                verdict="MERGE_MAP",
                reason="redundancy.duplicate",
                short_circuit=True,
                payload={
                    "cluster_id": ctx.duplicate_cluster_id,
                    "cluster_size": ctx.duplicate_cluster_size,
                },
            )

        return RoutineResult(
            code=self.code,
            verdict="PASS",
            reason="redundancy.unique",
            payload={"duplicate_cluster_id": ctx.duplicate_cluster_id},
        )
