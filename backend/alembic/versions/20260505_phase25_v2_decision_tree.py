"""V2 decision tree — add attrs to center_proposal, engine fields to analysis_run.

Revision ID: phase25_v2_decision_tree
Revises: phase24_scope_segregation
Create Date: 2026-05-05
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "phase25_v2_decision_tree"
down_revision = "phase24_scope_segregation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # CenterProposal.attrs (JSONB for V2 migrate/approach/pc_id/cc_id)
    op.add_column(
        "center_proposal",
        sa.Column("attrs", postgresql.JSONB(), nullable=True),
        schema="cleanup",
    )

    # AnalysisRun.engine_version, total_centers, completed_centers
    op.add_column(
        "analysis_run",
        sa.Column("engine_version", sa.String(30), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "analysis_run",
        sa.Column("total_centers", sa.Integer(), nullable=True, server_default="0"),
        schema="cleanup",
    )
    op.add_column(
        "analysis_run",
        sa.Column("completed_centers", sa.Integer(), nullable=True, server_default="0"),
        schema="cleanup",
    )
    op.add_column(
        "analysis_run",
        sa.Column("mode", sa.String(20), nullable=True, server_default="simulation"),
        schema="cleanup",
    )
    op.add_column(
        "analysis_run",
        sa.Column("label", sa.String(100), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "analysis_run",
        sa.Column("excluded_scopes", postgresql.JSONB(), nullable=True),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_column("center_proposal", "attrs", schema="cleanup")
    op.drop_column("analysis_run", "engine_version", schema="cleanup")
    op.drop_column("analysis_run", "total_centers", schema="cleanup")
    op.drop_column("analysis_run", "completed_centers", schema="cleanup")
    op.drop_column("analysis_run", "mode", schema="cleanup")
    op.drop_column("analysis_run", "label", schema="cleanup")
    op.drop_column("analysis_run", "excluded_scopes", schema="cleanup")
