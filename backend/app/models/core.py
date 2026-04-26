"""Core ORM models for the cleanup schema (section 03 of spec)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin

# ---------- reference / source data ----------


class Entity(TimestampMixin, Base):
    __tablename__ = "entity"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    ccode: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    country: Mapped[str | None] = mapped_column(String(3))
    region: Mapped[str | None] = mapped_column(String(50))
    currency: Mapped[str | None] = mapped_column(String(3))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    attrs: Mapped[dict | None] = mapped_column(JSONB)


class Employee(TimestampMixin, Base):
    """Employee master data (sourced from SAP HR or manual CSV upload)."""

    __tablename__ = "employee"
    __table_args__ = (
        UniqueConstraint("gpn", "refresh_batch"),
        Index("ix_emp_user_id", "user_id_pid"),
        Index("ix_emp_ou_cd", "ou_cd"),
        Index("ix_emp_cost_pc", "local_cc_cd"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    gpn: Mapped[str] = mapped_column(String(20), nullable=False)
    bs_name: Mapped[str | None] = mapped_column(String(200))
    bs_firstname: Mapped[str | None] = mapped_column(String(100))
    bs_lastname: Mapped[str | None] = mapped_column(String(100))
    legal_family_name: Mapped[str | None] = mapped_column(String(100))
    legal_first_name: Mapped[str | None] = mapped_column(String(100))
    email_address: Mapped[str | None] = mapped_column(String(200))
    emp_status: Mapped[str | None] = mapped_column(String(20))
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    gender_code: Mapped[str | None] = mapped_column(String(5))
    user_id_pid: Mapped[str | None] = mapped_column(String(30))
    user_id_tnumber: Mapped[str | None] = mapped_column(String(30))
    uuname: Mapped[str | None] = mapped_column(String(30))
    # Organizational
    ou_pk: Mapped[str | None] = mapped_column(String(20))
    ou_cd: Mapped[str | None] = mapped_column(String(20))
    ou_desc: Mapped[str | None] = mapped_column(String(200))
    wrk_in_ou_pk: Mapped[str | None] = mapped_column(String(20))
    wrk_in_ou_cd: Mapped[str | None] = mapped_column(String(20))
    wrk_in_ou_desc: Mapped[str | None] = mapped_column(String(200))
    # Cost center / company
    local_cc_cd: Mapped[str | None] = mapped_column(String(20))
    local_cc_desc: Mapped[str | None] = mapped_column(String(200))
    gcrs_comp_cd: Mapped[str | None] = mapped_column(String(20))
    gcrs_comp_desc: Mapped[str | None] = mapped_column(String(200))
    cost_pc_cd_e_ou: Mapped[str | None] = mapped_column(String(20))
    cost_pc_cd_w_ou: Mapped[str | None] = mapped_column(String(20))
    # Manager
    lm_gpn: Mapped[str | None] = mapped_column(String(20))
    lm_bs_firstname: Mapped[str | None] = mapped_column(String(100))
    lm_bs_lastname: Mapped[str | None] = mapped_column(String(100))
    supervisor_gpn: Mapped[str | None] = mapped_column(String(20))
    # Job / rank
    rank_cd: Mapped[str | None] = mapped_column(String(20))
    rank_desc: Mapped[str | None] = mapped_column(String(200))
    job_desc: Mapped[str | None] = mapped_column(String(200))
    empl_class: Mapped[str | None] = mapped_column(String(20))
    full_time_eq: Mapped[str | None] = mapped_column(String(10))
    head_of_own_ou: Mapped[str | None] = mapped_column(String(5))
    # Location
    reg_region: Mapped[str | None] = mapped_column(String(50))
    locn_city_name_1: Mapped[str | None] = mapped_column(String(100))
    locn_ctry_cd_1: Mapped[str | None] = mapped_column(String(5))
    building_cd_1: Mapped[str | None] = mapped_column(String(20))
    # All remaining fields stored as JSON
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )

    @property
    def display_name(self) -> str:
        """Format as 'GPN Name' for owner display."""
        name = self.bs_name or f"{self.bs_firstname or ''} {self.bs_lastname or ''}".strip()
        return f"{self.gpn} {name}".strip()


class LegacyCostCenter(TimestampMixin, Base):
    __tablename__ = "legacy_cost_center"
    __table_args__ = (
        UniqueConstraint("coarea", "cctr", "refresh_batch"),
        Index("ix_lcc_ccode", "ccode"),
        Index("ix_lcc_coarea_cctr", "coarea", "cctr"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    cctr: Mapped[str] = mapped_column(String(20), nullable=False)
    txtsh: Mapped[str | None] = mapped_column(String(40))
    txtmi: Mapped[str | None] = mapped_column(String(200))
    responsible: Mapped[str | None] = mapped_column(String(100))
    ccode: Mapped[str | None] = mapped_column(String(10))
    cctrcgy: Mapped[str | None] = mapped_column(String(4))
    currency: Mapped[str | None] = mapped_column(String(3))
    pctr: Mapped[str | None] = mapped_column(String(20))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )


class LegacyProfitCenter(TimestampMixin, Base):
    __tablename__ = "legacy_profit_center"
    __table_args__ = (
        UniqueConstraint("coarea", "pctr", "refresh_batch"),
        Index("ix_lpc_ccode", "ccode"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    pctr: Mapped[str] = mapped_column(String(20), nullable=False)
    txtsh: Mapped[str | None] = mapped_column(String(40))
    txtmi: Mapped[str | None] = mapped_column(String(200))
    responsible: Mapped[str | None] = mapped_column(String(100))
    ccode: Mapped[str | None] = mapped_column(String(10))
    department: Mapped[str | None] = mapped_column(String(20))
    currency: Mapped[str | None] = mapped_column(String(3))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )


class Balance(TimestampMixin, Base):
    __tablename__ = "balance"
    __table_args__ = (
        Index("ix_bal_coarea_cctr", "coarea", "cctr"),
        Index("ix_bal_period", "fiscal_year", "period"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    cctr: Mapped[str] = mapped_column(String(20), nullable=False)
    ccode: Mapped[str | None] = mapped_column(String(10))
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    account: Mapped[str | None] = mapped_column(String(20))
    account_class: Mapped[str | None] = mapped_column(String(20))
    tc_amt: Mapped[Decimal | None] = mapped_column(Numeric(23, 2))
    gc_amt: Mapped[Decimal | None] = mapped_column(Numeric(23, 2))
    gc2_amt: Mapped[Decimal | None] = mapped_column(Numeric(23, 2))
    currency_tc: Mapped[str | None] = mapped_column(String(3))
    currency_gc: Mapped[str | None] = mapped_column(String(3))
    currency_gc2: Mapped[str | None] = mapped_column(String(3))
    posting_count: Mapped[int] = mapped_column(Integer, default=0)
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )


class Hierarchy(TimestampMixin, Base):
    __tablename__ = "hierarchy"
    __table_args__ = (
        UniqueConstraint("setclass", "setname", "refresh_batch"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    setclass: Mapped[str] = mapped_column(String(10), nullable=False)
    setname: Mapped[str] = mapped_column(String(40), nullable=False)
    description: Mapped[str | None] = mapped_column(String(200))
    coarea: Mapped[str | None] = mapped_column(String(10))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    attrs: Mapped[dict | None] = mapped_column(JSONB)
    refresh_batch: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")
    )

    nodes: Mapped[list[HierarchyNode]] = relationship(back_populates="hierarchy")
    leaves: Mapped[list[HierarchyLeaf]] = relationship(back_populates="hierarchy")


class HierarchyNode(Base):
    __tablename__ = "hierarchy_node"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    hierarchy_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.hierarchy.id", ondelete="CASCADE"), nullable=False
    )
    parent_setname: Mapped[str] = mapped_column(String(40), nullable=False)
    child_setname: Mapped[str] = mapped_column(String(40), nullable=False)
    seq: Mapped[int] = mapped_column(Integer, default=0)

    hierarchy: Mapped[Hierarchy] = relationship(back_populates="nodes")


class HierarchyLeaf(Base):
    __tablename__ = "hierarchy_leaf"
    __table_args__ = (
        Index("ix_hleaf_cctr", "value"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    hierarchy_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.hierarchy.id", ondelete="CASCADE"), nullable=False
    )
    setname: Mapped[str] = mapped_column(String(40), nullable=False)
    value: Mapped[str] = mapped_column(String(20), nullable=False)
    seq: Mapped[int] = mapped_column(Integer, default=0)

    hierarchy: Mapped[Hierarchy] = relationship(back_populates="leaves")


# ---------- wave & analysis ----------


class Wave(TimestampMixin, Base):
    __tablename__ = "wave"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="draft"
    )  # draft|analysing|proposed|locked|in_review|signed_off|closed|cancelled
    is_full_scope: Mapped[bool] = mapped_column(Boolean, default=False)
    exclude_prior: Mapped[bool] = mapped_column(Boolean, default=True)
    config: Mapped[dict | None] = mapped_column(JSONB)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    signed_off_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    preferred_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="SET NULL")
    )
    created_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )

    entities: Mapped[list[WaveEntity]] = relationship(back_populates="wave")
    hierarchy_scopes: Mapped[list[WaveHierarchyScope]] = relationship(back_populates="wave")
    runs: Mapped[list[AnalysisRun]] = relationship(back_populates="wave")
    scopes: Mapped[list[ReviewScope]] = relationship(back_populates="wave")


class WaveEntity(Base):
    __tablename__ = "wave_entity"
    __table_args__ = (
        UniqueConstraint("wave_id", "entity_id"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    entity_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.entity.id", ondelete="CASCADE"), nullable=False
    )

    wave: Mapped[Wave] = relationship(back_populates="entities")
    entity: Mapped[Entity] = relationship()


class WaveHierarchyScope(Base):
    """Links a wave to specific hierarchy nodes for scoping."""

    __tablename__ = "wave_hierarchy_scope"
    __table_args__ = (
        UniqueConstraint("wave_id", "hierarchy_id", "node_setname"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    hierarchy_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.hierarchy.id", ondelete="CASCADE"), nullable=False
    )
    node_setname: Mapped[str] = mapped_column(String(40), nullable=False)

    wave: Mapped[Wave] = relationship(back_populates="hierarchy_scopes")
    hierarchy: Mapped[Hierarchy] = relationship()


class AnalysisConfig(TimestampMixin, Base):
    __tablename__ = "analysis_config"
    __table_args__ = (
        UniqueConstraint("code", "version"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    parent_code: Mapped[str | None] = mapped_column(String(60))
    status: Mapped[str] = mapped_column(String(20), default="active")
    config: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


class Routine(TimestampMixin, Base):
    __tablename__ = "routine"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)  # rule|ml|llm|aggregate
    tree: Mapped[str | None] = mapped_column(String(20))  # cleansing|mapping
    source: Mapped[str] = mapped_column(
        String(20), nullable=False, default="builtin"
    )  # builtin|plugin|dsl
    params_schema: Mapped[dict | None] = mapped_column(JSONB)
    default_params: Mapped[dict | None] = mapped_column(JSONB)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    order: Mapped[int] = mapped_column(Integer, default=100)


class AnalysisRun(TimestampMixin, Base):
    __tablename__ = "analysis_run"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    config_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.analysis_config.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )  # pending|running|completed|failed|cancelled
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    kpis: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    data_snapshot: Mapped[str | None] = mapped_column(String(64))
    triggered_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )

    wave: Mapped[Wave] = relationship(back_populates="runs")
    config: Mapped[AnalysisConfig] = relationship()
    outputs: Mapped[list[RoutineOutput]] = relationship(back_populates="run")
    proposals: Mapped[list[CenterProposal]] = relationship(back_populates="run")
    llm_passes: Mapped[list[LLMReviewPass]] = relationship(back_populates="run")


class RoutineOutput(Base):
    __tablename__ = "routine_output"
    __table_args__ = (
        Index("ix_ro_run_center", "run_id", "legacy_cc_id"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="CASCADE"), nullable=False
    )
    routine_code: Mapped[str] = mapped_column(String(60), nullable=False)
    legacy_cc_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.legacy_cost_center.id", ondelete="CASCADE"), nullable=False
    )
    verdict: Mapped[str | None] = mapped_column(String(30))
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    payload: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    run: Mapped[AnalysisRun] = relationship(back_populates="outputs")


class LLMReviewPass(TimestampMixin, Base):
    __tablename__ = "llm_review_pass"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="CASCADE"), nullable=False
    )
    mode: Mapped[str] = mapped_column(String(20), nullable=False)  # SINGLE|SEQUENTIAL|DEBATE
    stage: Mapped[str | None] = mapped_column(String(30))
    model: Mapped[str | None] = mapped_column(String(100))
    skill_id: Mapped[str | None] = mapped_column(String(64))
    skill_version: Mapped[str | None] = mapped_column(String(20))
    prompt_template: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20), default="pending")
    total_centers: Mapped[int] = mapped_column(Integer, default=0)
    completed_centers: Mapped[int] = mapped_column(Integer, default=0)
    tokens_in: Mapped[int] = mapped_column(Integer, default=0)
    tokens_out: Mapped[int] = mapped_column(Integer, default=0)
    cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    run: Mapped[AnalysisRun] = relationship(back_populates="llm_passes")


# ---------- proposals & review ----------


class CenterProposal(TimestampMixin, Base):
    __tablename__ = "center_proposal"
    __table_args__ = (
        UniqueConstraint("run_id", "legacy_cc_id"),
        Index("ix_cp_outcome", "cleansing_outcome"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="CASCADE"), nullable=False
    )
    legacy_cc_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.legacy_cost_center.id", ondelete="CASCADE"), nullable=False
    )
    entity_code: Mapped[str | None] = mapped_column(String(10))
    cleansing_outcome: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # KEEP|RETIRE|MERGE_MAP|REDESIGN
    target_object: Mapped[str | None] = mapped_column(
        String(20)
    )  # CC|PC|CC_AND_PC|PC_ONLY|WBS_REAL|WBS_STAT|NONE
    merge_into_cctr: Mapped[str | None] = mapped_column(String(20))
    rule_path: Mapped[dict | None] = mapped_column(JSONB)
    ml_scores: Mapped[dict | None] = mapped_column(JSONB)
    llm_commentary: Mapped[dict | None] = mapped_column(JSONB)
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))
    override_outcome: Mapped[str | None] = mapped_column(String(20))
    override_target: Mapped[str | None] = mapped_column(String(20))
    override_reason: Mapped[str | None] = mapped_column(Text)
    override_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    override_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    run: Mapped[AnalysisRun] = relationship(back_populates="proposals")
    legacy_cc: Mapped[LegacyCostCenter] = relationship()


class ReviewScope(TimestampMixin, Base):
    __tablename__ = "review_scope"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    scope_type: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # entity|hierarchy_node|list
    scope_filter: Mapped[dict] = mapped_column(JSONB, nullable=False)
    token: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    reviewer_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    token_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(
        String(20), default="pending"
    )  # pending|invited|in_progress|completed|expired|revoked
    total_items: Mapped[int] = mapped_column(Integer, default=0)
    signed_off_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    invited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reviewer_name: Mapped[str | None] = mapped_column(String(200))
    reviewer_email: Mapped[str | None] = mapped_column(String(320))

    wave: Mapped[Wave] = relationship(back_populates="scopes")
    items: Mapped[list[ReviewItem]] = relationship(back_populates="scope")


class ReviewItem(TimestampMixin, Base):
    __tablename__ = "review_item"
    __table_args__ = (
        UniqueConstraint("scope_id", "proposal_id"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    scope_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.review_scope.id", ondelete="CASCADE"), nullable=False
    )
    proposal_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.center_proposal.id", ondelete="CASCADE"), nullable=True
    )
    decision: Mapped[str] = mapped_column(
        String(20), default="PENDING"
    )  # PENDING|APPROVED|NOT_REQUIRED|COMMENTED
    comment: Mapped[str | None] = mapped_column(Text)
    decided_by: Mapped[str | None] = mapped_column(String(100))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    scope: Mapped[ReviewScope] = relationship(back_populates="items")
    proposal: Mapped[CenterProposal] = relationship()


# ---------- target objects ----------


class TargetCostCenter(TimestampMixin, Base):
    __tablename__ = "target_cost_center"
    __table_args__ = (
        UniqueConstraint("coarea", "cctr"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    cctr: Mapped[str] = mapped_column(String(20), nullable=False)
    txtsh: Mapped[str | None] = mapped_column(String(40))
    txtmi: Mapped[str | None] = mapped_column(String(200))
    responsible: Mapped[str | None] = mapped_column(String(100))
    ccode: Mapped[str | None] = mapped_column(String(10))
    cctrcgy: Mapped[str | None] = mapped_column(String(4))
    currency: Mapped[str | None] = mapped_column(String(3))
    pctr: Mapped[str | None] = mapped_column(String(20))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    source_proposal_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.center_proposal.id", ondelete="SET NULL")
    )
    approved_in_wave: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="SET NULL")
    )
    mdg_status: Mapped[str | None] = mapped_column(String(30))
    mdg_change_request_id: Mapped[str | None] = mapped_column(String(40))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TargetProfitCenter(TimestampMixin, Base):
    __tablename__ = "target_profit_center"
    __table_args__ = (
        UniqueConstraint("coarea", "pctr"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    pctr: Mapped[str] = mapped_column(String(20), nullable=False)
    txtsh: Mapped[str | None] = mapped_column(String(40))
    txtmi: Mapped[str | None] = mapped_column(String(200))
    responsible: Mapped[str | None] = mapped_column(String(100))
    ccode: Mapped[str | None] = mapped_column(String(10))
    department: Mapped[str | None] = mapped_column(String(20))
    currency: Mapped[str | None] = mapped_column(String(3))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    source_proposal_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.center_proposal.id", ondelete="SET NULL")
    )
    approved_in_wave: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="SET NULL")
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# ---------- housekeeping ----------


class HousekeepingCycle(TimestampMixin, Base):
    __tablename__ = "housekeeping_cycle"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    period: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), default="scheduled"
    )  # scheduled|running|review_open|closed
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    review_opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    config: Mapped[dict | None] = mapped_column(JSONB)
    kpis: Mapped[dict | None] = mapped_column(JSONB)


class HousekeepingItem(TimestampMixin, Base):
    __tablename__ = "housekeeping_item"
    __table_args__ = (
        UniqueConstraint("cycle_id", "target_cc_id", "flag"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    cycle_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.housekeeping_cycle.id", ondelete="CASCADE"), nullable=False
    )
    target_cc_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.target_cost_center.id", ondelete="CASCADE"), nullable=False
    )
    flag: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # UNUSED|LOW_VOLUME|NO_OWNER|ANOMALY
    owner_email: Mapped[str | None] = mapped_column(String(320))
    owner_token: Mapped[str | None] = mapped_column(String(64))
    decision: Mapped[str | None] = mapped_column(String(20))  # KEEP|CLOSE|DEFER
    decision_comment: Mapped[str | None] = mapped_column(Text)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reminders_sent: Mapped[int] = mapped_column(Integer, default=0)
    details: Mapped[dict | None] = mapped_column(JSONB)


# ---------- ingest ----------


class UploadBatch(TimestampMixin, Base):
    __tablename__ = "upload_batch"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # cost_center|profit_center|balance|hierarchy
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    storage_uri: Mapped[str | None] = mapped_column(String(500))
    status: Mapped[str] = mapped_column(
        String(20), default="uploaded"
    )  # uploaded|validating|validated|loading|loaded|failed|rolled_back
    rows_total: Mapped[int] = mapped_column(Integer, default=0)
    rows_valid: Mapped[int] = mapped_column(Integer, default=0)
    rows_error: Mapped[int] = mapped_column(Integer, default=0)
    rows_loaded: Mapped[int] = mapped_column(Integer, default=0)
    uploaded_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    loaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UploadError(Base):
    __tablename__ = "upload_error"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.upload_batch.id", ondelete="CASCADE"), nullable=False
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    column_name: Mapped[str | None] = mapped_column(String(100))
    error_code: Mapped[str] = mapped_column(String(40), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    raw_value: Mapped[str | None] = mapped_column(Text)


# ---------- SAP connections ----------


class SAPConnection(TimestampMixin, Base):
    __tablename__ = "sap_connection"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(500))
    system_type: Mapped[str] = mapped_column(String(20), nullable=False)
    landscape_type: Mapped[str | None] = mapped_column(String(10))
    base_url: Mapped[str] = mapped_column(String(500), nullable=False)
    client: Mapped[str] = mapped_column(String(3), nullable=False, default="100")
    language: Mapped[str] = mapped_column(String(2), nullable=False, default="EN")
    username: Mapped[str] = mapped_column(String(100), nullable=False)
    password_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    protocol: Mapped[str] = mapped_column(
        String(20), nullable=False, default="odata"
    )  # odata|adt|soap_rfc
    verify_ssl: Mapped[bool] = mapped_column(Boolean, default=True)
    use_proxy: Mapped[bool] = mapped_column(Boolean, default=False)
    saml2_disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    allowed_tables: Mapped[str | None] = mapped_column(Text)
    attrs: Mapped[dict | None] = mapped_column(JSONB)


class SAPObjectBinding(TimestampMixin, Base):
    __tablename__ = "sap_object_binding"
    __table_args__ = (
        UniqueConstraint("connection_id", "object_type"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.sap_connection.id", ondelete="CASCADE"), nullable=False
    )
    object_type: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # cost_center|profit_center|hierarchy|balance|gl_account
    entity_set: Mapped[str | None] = mapped_column(String(200))
    path: Mapped[str | None] = mapped_column(String(500))
    params: Mapped[dict | None] = mapped_column(JSONB)
    schedule_cron: Mapped[str | None] = mapped_column(String(100))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class SAPConnectionProbe(Base):
    __tablename__ = "sap_connection_probe"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    connection_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.sap_connection.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # ok|error
    protocol: Mapped[str] = mapped_column(String(20), nullable=False)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    details: Mapped[dict | None] = mapped_column(JSONB)
    probed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    probed_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


# ---------- auth & admin ----------


class AppUser(TimestampMixin, Base):
    __tablename__ = "app_user"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    password_hash: Mapped[str | None] = mapped_column(String(200))
    role: Mapped[str] = mapped_column(
        String(20), nullable=False, default="analyst"
    )  # admin|analyst|reviewer|auditor|owner
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    entraid_oid: Mapped[str | None] = mapped_column(String(64))
    last_login: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_logins: Mapped[int] = mapped_column(Integer, default=0)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    onboarding_done: Mapped[bool] = mapped_column(Boolean, default=False)
    attrs: Mapped[dict | None] = mapped_column(JSONB)


class AppConfig(Base):
    __tablename__ = "app_config"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    value: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
    updated_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


class AppConfigSecret(Base):
    __tablename__ = "app_config_secret"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    encrypted_value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class AuditLog(Base):
    __tablename__ = "audit_log"
    __table_args__ = (
        Index("ix_audit_action", "action"),
        Index("ix_audit_actor", "actor_id"),
        Index("ix_audit_ts", "created_at"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    action: Mapped[str] = mapped_column(String(60), nullable=False)
    entity_type: Mapped[str | None] = mapped_column(String(60))
    entity_id: Mapped[str | None] = mapped_column(String(60))
    actor_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    actor_email: Mapped[str | None] = mapped_column(String(320))
    before: Mapped[dict | None] = mapped_column(JSONB)
    after: Mapped[dict | None] = mapped_column(JSONB)
    ip_address: Mapped[str | None] = mapped_column(String(45))
    request_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class TaskRun(TimestampMixin, Base):
    __tablename__ = "task_run"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    task_name: Mapped[str] = mapped_column(String(100), nullable=False)
    task_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), default="pending"
    )  # pending|running|completed|failed|cancelled
    args_summary: Mapped[dict | None] = mapped_column(JSONB)
    result_summary: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.analysis_run.id", ondelete="SET NULL")
    )


class NamingSequence(Base):
    __tablename__ = "naming_sequence"
    __table_args__ = (
        UniqueConstraint("object_type", "coarea", "prefix"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    object_type: Mapped[str] = mapped_column(String(10), nullable=False)  # cc|pc|wbs
    coarea: Mapped[str] = mapped_column(String(10), nullable=False)
    prefix: Mapped[str] = mapped_column(String(20), nullable=False)
    next_value: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    reserved_by_wave: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="SET NULL")
    )
    reserved_range_start: Mapped[int | None] = mapped_column(Integer)
    reserved_range_end: Mapped[int | None] = mapped_column(Integer)


class ActivityFeedEntry(Base):
    """Activity feed for audit trail and notifications."""

    __tablename__ = "activity_feed"
    __table_args__ = (
        Index("ix_activity_feed_user", "user_id"),
        Index("ix_activity_feed_ts", "created_at"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )
    action: Mapped[str] = mapped_column(String(50), nullable=False)
    entity_type: Mapped[str | None] = mapped_column(String(30))
    entity_id: Mapped[int | None] = mapped_column(Integer)
    summary: Mapped[str] = mapped_column(String(500), nullable=False)
    detail: Mapped[dict | None] = mapped_column(JSONB)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class WaveTemplate(TimestampMixin, Base):
    """Reusable wave configuration template (§07.2)."""

    __tablename__ = "wave_template"
    __table_args__ = {"schema": "cleanup"}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    config: Mapped[dict | None] = mapped_column(JSONB)
    is_full_scope: Mapped[bool] = mapped_column(Boolean, default=False)
    exclude_prior: Mapped[bool] = mapped_column(Boolean, default=True)
    entity_ccodes: Mapped[list | None] = mapped_column(JSONB)
    created_by: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.app_user.id", ondelete="SET NULL")
    )


class GLAccountClassRange(TimestampMixin, Base):
    """GL account class ranges for balance classification (§03.5)."""

    __tablename__ = "gl_account_class_range"
    __table_args__ = (
        UniqueConstraint("class_code", "from_account"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    class_code: Mapped[str] = mapped_column(String(20), nullable=False)
    class_label: Mapped[str] = mapped_column(String(100), nullable=False)
    from_account: Mapped[str] = mapped_column(String(20), nullable=False)
    to_account: Mapped[str] = mapped_column(String(20), nullable=False)
    category: Mapped[str | None] = mapped_column(String(40))  # bs|rev|opex|other


class NamingPool(Base):
    """Pool of allocatable CC/PC IDs per wave (supports ID recycling)."""

    __tablename__ = "naming_pool"
    __table_args__ = (
        UniqueConstraint("wave_id", "pool_type"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    wave_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False
    )
    pool_type: Mapped[str] = mapped_column(String(10), nullable=False)  # CC or PC
    range_start: Mapped[int] = mapped_column(Integer, nullable=False)
    range_end: Mapped[int] = mapped_column(Integer, nullable=False)
    next_value: Mapped[int] = mapped_column(Integer, nullable=False)

    allocations: Mapped[list[NamingAllocation]] = relationship(back_populates="pool")


class NamingAllocation(Base):
    """Individual ID allocation from a naming pool, supports release/recycle."""

    __tablename__ = "naming_allocation"
    __table_args__ = (
        Index("ix_nalloc_pool", "pool_id"),
        Index("ix_nalloc_proposal", "proposal_id"),
        {"schema": "cleanup"},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    pool_id: Mapped[int] = mapped_column(
        ForeignKey("cleanup.naming_pool.id", ondelete="CASCADE"), nullable=False
    )
    proposal_id: Mapped[int | None] = mapped_column(
        ForeignKey("cleanup.center_proposal.id", ondelete="SET NULL")
    )
    allocated_value: Mapped[str] = mapped_column(String(20), nullable=False)
    is_released: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    pool: Mapped[NamingPool] = relationship(back_populates="allocations")
