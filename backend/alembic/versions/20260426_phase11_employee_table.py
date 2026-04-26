"""Add employee table for SAP HR master data.

Revision ID: phase11_employee
Revises: phase10_decided_by
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "phase11_employee"
down_revision = "phase10_decided_by"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "employee",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("gpn", sa.String(20), nullable=False),
        sa.Column("bs_name", sa.String(200)),
        sa.Column("bs_firstname", sa.String(100)),
        sa.Column("bs_lastname", sa.String(100)),
        sa.Column("legal_family_name", sa.String(100)),
        sa.Column("legal_first_name", sa.String(100)),
        sa.Column("email_address", sa.String(200)),
        sa.Column("emp_status", sa.String(20)),
        sa.Column("valid_from", sa.DateTime(timezone=True)),
        sa.Column("valid_to", sa.DateTime(timezone=True)),
        sa.Column("gender_code", sa.String(5)),
        sa.Column("user_id_pid", sa.String(30)),
        sa.Column("user_id_tnumber", sa.String(30)),
        sa.Column("uuname", sa.String(30)),
        sa.Column("ou_pk", sa.String(20)),
        sa.Column("ou_cd", sa.String(20)),
        sa.Column("ou_desc", sa.String(200)),
        sa.Column("wrk_in_ou_pk", sa.String(20)),
        sa.Column("wrk_in_ou_cd", sa.String(20)),
        sa.Column("wrk_in_ou_desc", sa.String(200)),
        sa.Column("local_cc_cd", sa.String(20)),
        sa.Column("local_cc_desc", sa.String(200)),
        sa.Column("gcrs_comp_cd", sa.String(20)),
        sa.Column("gcrs_comp_desc", sa.String(200)),
        sa.Column("cost_pc_cd_e_ou", sa.String(20)),
        sa.Column("cost_pc_cd_w_ou", sa.String(20)),
        sa.Column("lm_gpn", sa.String(20)),
        sa.Column("lm_bs_firstname", sa.String(100)),
        sa.Column("lm_bs_lastname", sa.String(100)),
        sa.Column("supervisor_gpn", sa.String(20)),
        sa.Column("rank_cd", sa.String(20)),
        sa.Column("rank_desc", sa.String(200)),
        sa.Column("job_desc", sa.String(200)),
        sa.Column("empl_class", sa.String(20)),
        sa.Column("full_time_eq", sa.String(10)),
        sa.Column("head_of_own_ou", sa.String(5)),
        sa.Column("reg_region", sa.String(50)),
        sa.Column("locn_city_name_1", sa.String(100)),
        sa.Column("locn_ctry_cd_1", sa.String(5)),
        sa.Column("building_cd_1", sa.String(20)),
        sa.Column("attrs", JSONB),
        sa.Column("refresh_batch", sa.Integer, sa.ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.UniqueConstraint("gpn", "refresh_batch"),
        sa.Index("ix_emp_user_id", "user_id_pid"),
        sa.Index("ix_emp_ou_cd", "ou_cd"),
        sa.Index("ix_emp_cost_pc", "local_cc_cd"),
        schema="cleanup",
    )
    # Add naming_pool table to track allocated CC/PC IDs
    op.create_table(
        "naming_pool",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("wave_id", sa.Integer, sa.ForeignKey("cleanup.wave.id", ondelete="CASCADE"), nullable=False),
        sa.Column("pool_type", sa.String(10), nullable=False),  # 'CC' or 'PC'
        sa.Column("range_start", sa.Integer, nullable=False),
        sa.Column("range_end", sa.Integer, nullable=False),
        sa.Column("next_value", sa.Integer, nullable=False),
        sa.UniqueConstraint("wave_id", "pool_type"),
        schema="cleanup",
    )
    op.create_table(
        "naming_allocation",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("pool_id", sa.Integer, sa.ForeignKey("cleanup.naming_pool.id", ondelete="CASCADE"), nullable=False),
        sa.Column("proposal_id", sa.Integer, sa.ForeignKey("cleanup.center_proposal.id", ondelete="SET NULL")),
        sa.Column("allocated_value", sa.String(20), nullable=False),
        sa.Column("is_released", sa.Boolean, default=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Index("ix_nalloc_pool", "pool_id"),
        sa.Index("ix_nalloc_proposal", "proposal_id"),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_table("naming_allocation", schema="cleanup")
    op.drop_table("naming_pool", schema="cleanup")
    op.drop_table("employee", schema="cleanup")
