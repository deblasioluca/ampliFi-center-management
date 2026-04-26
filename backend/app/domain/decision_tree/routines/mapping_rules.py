"""Mapping tree routines (§04.2 — Tree B, slide 12).

These routines determine the target object type for centers that pass
the cleansing tree (KEEP or MERGE_MAP). Each routine checks one branch
of the mapping tree.
"""

from __future__ import annotations

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import register_routine


@register_routine
class BSRelevanceRoutine:
    """Tree B pre-step: balance sheet relevance check."""

    @property
    def code(self) -> str:
        return "rule.bs_relevance"

    @property
    def name(self) -> str:
        return "Balance Sheet Relevance"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "mapping"

    @property
    def params_schema(self) -> dict | None:
        return None

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        if ctx.bs_amt != 0:
            return RoutineResult(
                code=self.code,
                verdict="BS_RELEVANT",
                reason="bs.relevant",
                payload={"bs_amt": ctx.bs_amt},
            )
        return RoutineResult(code=self.code, verdict="PASS", reason="bs.not_relevant")


@register_routine
class HasDirectRevenueRoutine:
    """Tree B step ①: direct revenue booking."""

    @property
    def code(self) -> str:
        return "rule.has_direct_revenue"

    @property
    def name(self) -> str:
        return "Direct Revenue Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "mapping"

    @property
    def params_schema(self) -> dict | None:
        return None

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        has_rev = ctx.has_direct_revenue or ctx.rev_amt != 0
        if has_rev:
            if ctx.is_feeder:
                return RoutineResult(
                    code=self.code,
                    verdict="CC_AND_PC",
                    reason="revenue.with_feeder",
                    short_circuit=True,
                    payload={"rev_amt": ctx.rev_amt, "is_feeder": True},
                )
            return RoutineResult(
                code=self.code,
                verdict="PC_ONLY",
                reason="revenue.profitability_only",
                short_circuit=True,
                payload={"rev_amt": ctx.rev_amt},
            )
        return RoutineResult(code=self.code, verdict="PASS", reason="revenue.none")


@register_routine
class HasOperationalCostsRoutine:
    """Tree B step ③: direct operational costs."""

    @property
    def code(self) -> str:
        return "rule.has_operational_costs"

    @property
    def name(self) -> str:
        return "Operational Costs Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "mapping"

    @property
    def params_schema(self) -> dict | None:
        return None

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        has_ops = ctx.has_operational_costs or ctx.opex_amt != 0
        if has_ops:
            return RoutineResult(
                code=self.code,
                verdict="CC",
                reason="operational.costs",
                short_circuit=True,
                payload={"opex_amt": ctx.opex_amt},
            )
        return RoutineResult(code=self.code, verdict="PASS", reason="operational.none")


@register_routine
class CollectsProjectCostsRoutine:
    """Tree B step ②: project costs collection."""

    @property
    def code(self) -> str:
        return "rule.collects_project_costs"

    @property
    def name(self) -> str:
        return "Project Costs Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "mapping"

    @property
    def params_schema(self) -> dict | None:
        return None

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        if ctx.collects_project_costs or ctx.is_project_related:
            return RoutineResult(
                code=self.code,
                verdict="WBS_REAL",
                reason="project.costs",
                short_circuit=True,
                payload={"is_project_related": ctx.is_project_related},
            )
        return RoutineResult(code=self.code, verdict="PASS", reason="project.none")


@register_routine
class RevenueAllocationRoutine:
    """Tree B step ④: vehicle for revenue allocation."""

    @property
    def code(self) -> str:
        return "rule.revenue_allocation_vehicle"

    @property
    def name(self) -> str:
        return "Revenue Allocation Vehicle Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "mapping"

    @property
    def params_schema(self) -> dict | None:
        return None

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        if ctx.used_for_revenue_allocation:
            return RoutineResult(
                code=self.code,
                verdict="WBS_REAL",
                reason="allocation.revenue",
                short_circuit=True,
                payload={"used_for_revenue_allocation": True},
            )
        return RoutineResult(code=self.code, verdict="PASS", reason="allocation.revenue.none")


@register_routine
class CostAllocationRoutine:
    """Tree B step ⑤: vehicle for cost allocation."""

    @property
    def code(self) -> str:
        return "rule.cost_allocation_vehicle"

    @property
    def name(self) -> str:
        return "Cost Allocation Vehicle Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "mapping"

    @property
    def params_schema(self) -> dict | None:
        return None

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        if ctx.used_for_cost_allocation or ctx.is_allocation_vehicle:
            return RoutineResult(
                code=self.code,
                verdict="CC",
                reason="allocation.cost",
                short_circuit=True,
                payload={"is_allocation_vehicle": ctx.is_allocation_vehicle},
            )
        return RoutineResult(code=self.code, verdict="PASS", reason="allocation.cost.none")


@register_routine
class InfoOnlyRoutine:
    """Tree B step ⑥: information-only / statistical tracking."""

    @property
    def code(self) -> str:
        return "rule.info_only"

    @property
    def name(self) -> str:
        return "Information-Only Check"

    @property
    def kind(self) -> str:
        return "rule"

    @property
    def tree(self) -> str:
        return "mapping"

    @property
    def params_schema(self) -> dict | None:
        return None

    def run(self, ctx: CenterContext, params: dict) -> RoutineResult:
        if ctx.used_for_info_only:
            return RoutineResult(
                code=self.code,
                verdict="WBS_STAT",
                reason="info.statistical",
                short_circuit=True,
                payload={"used_for_info_only": True},
            )
        return RoutineResult(code=self.code, verdict="PASS", reason="info.not_statistical")
