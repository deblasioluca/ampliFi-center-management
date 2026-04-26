"""Center context and routine result types for the pluggable engine (§04.5)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CenterContext:
    """All data about a single cost center available to routines."""

    center_id: int
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

    # Balance & posting features
    months_since_last_posting: int | None = None
    posting_count_window: int | None = None
    bs_amt: float = 0.0
    rev_amt: float = 0.0
    opex_amt: float = 0.0
    total_balance: float = 0.0

    # Hierarchy features
    hierarchy_membership_count: int = 0

    # Ownership
    has_owner: bool = True

    # Flags from source data or prior routines
    is_feeder: bool = False
    is_allocation_vehicle: bool = False
    is_project_related: bool = False
    in_bw_extractors: bool = False
    in_grc: bool = False
    in_intercompany: bool = False
    has_direct_revenue: bool = False
    has_operational_costs: bool = False
    collects_project_costs: bool = False
    used_for_revenue_allocation: bool = False
    used_for_cost_allocation: bool = False
    used_for_info_only: bool = False

    # Duplicate clustering (set by ml.duplicate_cluster)
    duplicate_cluster_id: str | None = None
    duplicate_cluster_size: int = 0

    # ML scores (injected by ML routines)
    ml_outcome_probs: dict[str, float] = field(default_factory=dict)
    ml_target_probs: dict[str, float] = field(default_factory=dict)
    ml_anomaly_score: float | None = None

    # Extension attributes
    attrs: dict = field(default_factory=dict)
    flags: dict = field(default_factory=dict)

    def as_feature_dict(self) -> dict:
        """Return a flat dict of all numeric/boolean features for ML models."""
        return {
            "months_since_last_posting": self.months_since_last_posting or 0,
            "posting_count_window": self.posting_count_window or 0,
            "bs_amt": self.bs_amt,
            "rev_amt": self.rev_amt,
            "opex_amt": self.opex_amt,
            "total_balance": self.total_balance,
            "hierarchy_membership_count": self.hierarchy_membership_count,
            "has_owner": int(self.has_owner),
            "is_feeder": int(self.is_feeder),
            "is_allocation_vehicle": int(self.is_allocation_vehicle),
            "is_project_related": int(self.is_project_related),
            "in_bw_extractors": int(self.in_bw_extractors),
            "in_grc": int(self.in_grc),
            "in_intercompany": int(self.in_intercompany),
            "is_active": int(self.is_active),
            "duplicate_cluster_size": self.duplicate_cluster_size,
        }


@dataclass
class RoutineResult:
    """Output of a single routine evaluation."""

    code: str
    verdict: str  # routine-specific: RETIRE, KEEP, MERGE_MAP, etc. or PASS/UNKNOWN
    score: float | None = None
    payload: dict = field(default_factory=dict)
    comment: str | None = None  # LLM-generated text
    short_circuit: bool = False  # if True, halts the tree for this center
    reason: str = ""  # human-readable reason code like "posting.inactive"
