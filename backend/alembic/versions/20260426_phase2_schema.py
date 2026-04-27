"""Phase 2 schema additions: preferred_run_id, entity_code, reviewer fields.

Revision ID: phase2_001
Revises: None
"""

import sqlalchemy as sa

from alembic import op

revision = "phase2_001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Wave: add preferred_run_id
    op.add_column(
        "wave",
        sa.Column("preferred_run_id", sa.Integer(), nullable=True),
        schema="cleanup",
    )
    op.create_foreign_key(
        "fk_wave_preferred_run",
        "wave",
        "analysis_run",
        ["preferred_run_id"],
        ["id"],
        source_schema="cleanup",
        referent_schema="cleanup",
        ondelete="SET NULL",
    )

    # CenterProposal: add entity_code
    op.add_column(
        "center_proposal",
        sa.Column("entity_code", sa.String(10), nullable=True),
        schema="cleanup",
    )

    # ReviewScope: add reviewer_user_id, total_items, signed_off_at
    op.add_column(
        "review_scope",
        sa.Column("reviewer_user_id", sa.Integer(), nullable=True),
        schema="cleanup",
    )
    op.create_foreign_key(
        "fk_review_scope_reviewer",
        "review_scope",
        "app_user",
        ["reviewer_user_id"],
        ["id"],
        source_schema="cleanup",
        referent_schema="cleanup",
        ondelete="SET NULL",
    )
    op.add_column(
        "review_scope",
        sa.Column("total_items", sa.Integer(), server_default="0", nullable=False),
        schema="cleanup",
    )
    op.add_column(
        "review_scope",
        sa.Column("signed_off_at", sa.DateTime(timezone=True), nullable=True),
        schema="cleanup",
    )
    # Make token_expires_at nullable
    op.alter_column(
        "review_scope",
        "token_expires_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=True,
        schema="cleanup",
    )


def downgrade() -> None:
    op.alter_column(
        "review_scope",
        "token_expires_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
        schema="cleanup",
    )
    op.drop_column("review_scope", "signed_off_at", schema="cleanup")
    op.drop_column("review_scope", "total_items", schema="cleanup")
    op.drop_constraint("fk_review_scope_reviewer", "review_scope", schema="cleanup")
    op.drop_column("review_scope", "reviewer_user_id", schema="cleanup")
    op.drop_column("center_proposal", "entity_code", schema="cleanup")
    op.drop_constraint("fk_wave_preferred_run", "wave", schema="cleanup")
    op.drop_column("wave", "preferred_run_id", schema="cleanup")
