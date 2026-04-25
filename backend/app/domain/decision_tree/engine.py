"""Decision tree engine — deterministic cleansing & mapping trees (section 04)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol


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
    cleansing: CleansingOutcome
    target_object: TargetObject | None = None
    merge_into: str | None = None
    rule_path: list[str] = field(default_factory=list)
    confidence: float = 1.0


class RoutineProtocol(Protocol):
    code: str
    name: str
    kind: str
    tree: str

    def evaluate(self, features: CenterFeatures, params: dict) -> dict: ...


def run_cleansing_tree(features: CenterFeatures, params: dict | None = None) -> TreeResult:
    """Deterministic cleansing tree (section 04.1).

    Decision order:
    1. Inactive → RETIRE
    2. No postings in window → RETIRE
    3. Duplicate cluster member (not survivor) → MERGE_MAP
    4. Non-compliant hierarchy placement → REDESIGN (strict mode)
    5. Otherwise → KEEP
    """
    path: list[str] = []
    p = params or {}
    inactivity_months = p.get("inactivity_threshold_months", 24)
    posting_threshold = p.get("posting_threshold", 0)
    strict_hierarchy = p.get("strict_hierarchy_compliance", False)

    # Step 1: active check
    if not features.is_active:
        path.append("inactive=true → RETIRE")
        return TreeResult(cleansing=CleansingOutcome.RETIRE, rule_path=path)
    path.append("inactive=false")

    # Step 2: posting activity
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

    # Step 3: duplicate cluster
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

    # Step 4: hierarchy compliance
    if strict_hierarchy and features.hierarchy_membership_count == 0:
        path.append("hierarchy_membership=0 (strict) → REDESIGN")
        return TreeResult(cleansing=CleansingOutcome.REDESIGN, rule_path=path)
    path.append("hierarchy_ok")

    # Step 5: KEEP
    path.append("all_checks_passed → KEEP")
    return TreeResult(cleansing=CleansingOutcome.KEEP, rule_path=path)


def run_mapping_tree(
    features: CenterFeatures,
    cleansing: CleansingOutcome,
    params: dict | None = None,
) -> TargetObject | None:
    """Deterministic mapping tree (section 04.2).

    Assigns target object type based on financial profile.
    Only runs for KEEP and REDESIGN outcomes.
    """
    if cleansing in (CleansingOutcome.RETIRE, CleansingOutcome.MERGE_MAP):
        return TargetObject.NONE

    # Step 1: B/S relevance
    if features.bs_amt != 0:
        if features.rev_amt != 0 or features.opex_amt != 0:
            return TargetObject.CC_AND_PC
        return TargetObject.PC_ONLY

    # Step 2: project-related
    if features.is_project_related:
        if features.is_allocation_vehicle:
            return TargetObject.WBS_STAT
        return TargetObject.WBS_REAL

    # Step 3: feeder / allocation
    if features.is_feeder or features.is_allocation_vehicle:
        return TargetObject.CC

    # Step 4: operational with revenue
    if features.rev_amt != 0:
        return TargetObject.CC_AND_PC

    # Step 5: default
    return TargetObject.CC


def evaluate_center(
    features: CenterFeatures,
    params: dict | None = None,
) -> TreeResult:
    """Run both trees and return combined result."""
    result = run_cleansing_tree(features, params)
    target = run_mapping_tree(features, result.cleansing, params)
    result.target_object = target
    return result
