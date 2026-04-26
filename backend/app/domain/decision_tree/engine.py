"""Decision tree engine — executes pipeline of registered routines (§04.5).

The engine runs a configured pipeline of routines against a center context.
Each routine produces a RoutineResult with a verdict. Short-circuiting
verdicts halt the pipeline early. The aggregate routine combines all
results into the final cleansing_outcome + target_object.

Determinism rule (§04): Re-running a pipeline on the same data_snapshot
with the same config MUST produce the same outcomes (LLM stochasticity
captured separately).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

import structlog

from app.domain.decision_tree.context import CenterContext, RoutineResult
from app.domain.decision_tree.registry import RoutineRegistry, get_registry

logger = structlog.get_logger()


class CleansingOutcome(StrEnum):
    KEEP = "KEEP"
    RETIRE = "RETIRE"
    MERGE_MAP = "MERGE_MAP"
    REDESIGN = "REDESIGN"


class TargetObject(StrEnum):
    CC = "CC"
    PC = "PC"
    CC_AND_PC = "CC_AND_PC"
    PC_ONLY = "PC_ONLY"
    WBS_REAL = "WBS_REAL"
    WBS_STAT = "WBS_STAT"
    NONE = "NONE"


@dataclass(frozen=True)
class CenterFeatures:
    """Legacy type for backward compatibility. Use CenterContext for new code."""

    coarea: str
    cctr: str
    ccode: str
    txtsh: str = ""
    txtmi: str = ""
    responsible: str = ""
    currency: str = ""
    cctrcgy: str = ""
    pctr: str = ""
    is_active: bool = True
    months_since_last_posting: int | None = None
    posting_count_window: int | None = None
    bs_amt: float = 0.0
    rev_amt: float = 0.0
    opex_amt: float = 0.0
    total_balance: float = 0.0
    hierarchy_membership_count: int = 0
    duplicate_cluster_id: str | None = None
    duplicate_cluster_size: int = 0
    has_owner: bool = True
    is_feeder: bool = False
    is_allocation_vehicle: bool = False
    is_project_related: bool = False
    attrs: dict = field(default_factory=dict)


@dataclass
class TreeResult:
    """Combined result of running both trees on a center."""

    cleansing: CleansingOutcome
    target_object: TargetObject | None = None
    merge_into: str | None = None
    rule_path: list[str] = field(default_factory=list)
    confidence: float = 1.0
    routine_results: list[RoutineResult] = field(default_factory=list)


class PipelineEngine:
    """Executes a configured pipeline of routines against a center (§04.5)."""

    def __init__(self, registry: RoutineRegistry | None = None) -> None:
        self._registry = registry or get_registry()

    def execute(self, config: dict, ctx: CenterContext) -> list[RoutineResult]:
        """Run pipeline steps in order, respecting short_circuit and enabled flags."""
        results: list[RoutineResult] = []
        pipeline = config.get("pipeline", [])

        for step in pipeline:
            if not step.get("enabled", True):
                continue

            routine_code = step.get("routine", "")
            routine = self._registry.get(routine_code)
            if routine is None:
                logger.warning("engine.routine_not_found", code=routine_code)
                results.append(
                    RoutineResult(
                        code=routine_code,
                        verdict="ERROR",
                        reason=f"routine.not_found:{routine_code}",
                    )
                )
                continue

            # Inject prior results into context for aggregate routines
            ctx.flags["_prior_results"] = [
                {
                    "code": r.code,
                    "verdict": r.verdict,
                    "reason": r.reason,
                    "payload": r.payload,
                    "short_circuit": r.short_circuit,
                }
                for r in results
            ]

            step_params = step.get("params", {})
            result = routine.run(ctx, step_params)
            results.append(result)

            if result.short_circuit:
                break

        return results


# ── Backward compatibility functions ─────────────────────────────────────
# These wrap the old hardcoded logic for existing callers.
# New code should use PipelineEngine directly.


def run_cleansing_tree(features: CenterFeatures, params: dict | None = None) -> TreeResult:
    """Deterministic cleansing tree — backward compatible wrapper."""
    path: list[str] = []
    p = params or {}
    inactivity_months = p.get("inactivity_threshold_months", 24)
    posting_threshold = p.get("posting_threshold", 0)
    strict_hierarchy = p.get("strict_hierarchy_compliance", False)

    if not features.is_active:
        path.append("inactive=true → RETIRE")
        return TreeResult(cleansing=CleansingOutcome.RETIRE, rule_path=path)
    path.append("inactive=false")

    if (
        features.months_since_last_posting is not None
        and features.months_since_last_posting >= inactivity_months
        and (
            features.posting_count_window is not None
            and features.posting_count_window <= posting_threshold
        )
    ):
        months = features.months_since_last_posting
        path.append(f"months_since_last_posting={months} >= {inactivity_months} → RETIRE")
        return TreeResult(cleansing=CleansingOutcome.RETIRE, rule_path=path)
    path.append("posting_activity=sufficient")

    if not features.has_owner:
        path.append("no_owner → RETIRE")
        return TreeResult(cleansing=CleansingOutcome.RETIRE, rule_path=path)
    path.append("owner=valid")

    if features.duplicate_cluster_id and features.duplicate_cluster_size > 1:
        cid = features.duplicate_cluster_id
        csz = features.duplicate_cluster_size
        path.append(f"duplicate_cluster={cid}, size={csz} → MERGE_MAP")
        return TreeResult(
            cleansing=CleansingOutcome.MERGE_MAP,
            merge_into=features.duplicate_cluster_id,
            rule_path=path,
        )
    path.append("no_duplicate")

    if features.hierarchy_membership_count != 1:
        count = features.hierarchy_membership_count
        if count == 0 and strict_hierarchy:
            path.append(f"hierarchy_membership={count} (strict) → REDESIGN")
            return TreeResult(cleansing=CleansingOutcome.REDESIGN, rule_path=path)
        elif count != 1:
            path.append(f"hierarchy_membership={count} → MERGE_MAP")
            return TreeResult(cleansing=CleansingOutcome.MERGE_MAP, rule_path=path)
    path.append("hierarchy_ok")

    in_bw = features.attrs.get("in_bw_extractors", False)
    in_grc = features.attrs.get("in_grc", False)
    in_ic = features.attrs.get("in_intercompany", False)
    if in_bw or in_grc or in_ic:
        deps = [d for d, v in [("BW", in_bw), ("GRC", in_grc), ("IC", in_ic)] if v]
        path.append(f"cross_system_deps={deps} → MERGE_MAP")
        return TreeResult(cleansing=CleansingOutcome.MERGE_MAP, rule_path=path)
    path.append("no_cross_deps")

    path.append("all_checks_passed → KEEP")
    return TreeResult(cleansing=CleansingOutcome.KEEP, rule_path=path)


def run_mapping_tree(
    features: CenterFeatures,
    cleansing: CleansingOutcome,
    params: dict | None = None,
) -> TargetObject | None:
    """Deterministic mapping tree (§04.2 slide 12) — backward compatible wrapper."""
    skip = (CleansingOutcome.RETIRE, CleansingOutcome.MERGE_MAP, CleansingOutcome.REDESIGN)
    if cleansing in skip:
        return TargetObject.NONE

    # Step ①: direct revenue
    has_rev = features.rev_amt != 0 or features.attrs.get("has_direct_revenue", False)
    if has_rev:
        if features.is_feeder:
            return TargetObject.CC_AND_PC
        return TargetObject.PC_ONLY

    # Step ②: project costs
    if features.is_project_related or features.attrs.get("collects_project_costs", False):
        return TargetObject.WBS_REAL

    # Step ③: operational costs
    if features.opex_amt != 0 or features.attrs.get("has_operational_costs", False):
        return TargetObject.CC

    # Step ④: revenue allocation vehicle
    if features.attrs.get("used_for_revenue_allocation", False):
        return TargetObject.WBS_REAL

    # Step ⑤: cost allocation vehicle
    if features.is_allocation_vehicle or features.attrs.get("used_for_cost_allocation", False):
        return TargetObject.CC

    # Step ⑥: information-only
    if features.attrs.get("used_for_info_only", False):
        return TargetObject.WBS_STAT

    # Fall-through: candidate for closing
    return TargetObject.NONE


def evaluate_center(
    features: CenterFeatures,
    params: dict | None = None,
) -> TreeResult:
    """Run both trees and return combined result (backward compatible)."""
    result = run_cleansing_tree(features, params)
    target = run_mapping_tree(features, result.cleansing, params)
    result.target_object = target
    return result


def evaluate_center_with_pipeline(
    ctx: CenterContext,
    config: dict,
    registry: RoutineRegistry | None = None,
) -> TreeResult:
    """Run the full pipeline engine and return a TreeResult.

    This is the preferred entry point for new code.
    """
    engine = PipelineEngine(registry)
    results = engine.execute(config, ctx)

    # Extract final outcome from results
    cleansing = CleansingOutcome.KEEP
    target = TargetObject.NONE
    rule_path: list[str] = []
    merge_into: str | None = None
    confidence = 1.0

    for r in results:
        rule_path.append(f"{r.code}:{r.verdict}:{r.reason}")

        if r.code == "aggregate.combine_outcomes":
            cleansing = CleansingOutcome(r.payload.get("cleansing_outcome", "KEEP"))
            target_str = r.payload.get("target_object", "NONE")
            target = TargetObject(target_str)
            if r.payload.get("ml_confidence") is not None:
                confidence = r.payload["ml_confidence"]
        elif r.verdict in ("RETIRE", "MERGE_MAP", "REDESIGN"):
            cleansing = CleansingOutcome(r.verdict)
            if r.verdict == "MERGE_MAP":
                merge_into = r.payload.get("cluster_id")
        elif r.short_circuit and r.verdict in (
            "CC",
            "PC",
            "PC_ONLY",
            "CC_AND_PC",
            "WBS_REAL",
            "WBS_STAT",
        ):
            target = TargetObject(r.verdict)

    return TreeResult(
        cleansing=cleansing,
        target_object=target,
        merge_into=merge_into,
        rule_path=rule_path,
        confidence=confidence,
        routine_results=results,
    )
