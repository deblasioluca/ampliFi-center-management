"""Add refresh_batch to target tables + center_mapping table.

Revision ID: phase22_target_mapping
Revises: phase21_upload_progress
Create Date: 2026-04-28
"""

import sqlalchemy as sa

from alembic import op

revision = "phase22_target_mapping"
down_revision = "phase21_upload_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add refresh_batch FK to target_cost_center
    op.add_column(
        "target_cost_center",
        sa.Column(
            "refresh_batch",
            sa.Integer(),
            sa.ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL"),
            nullable=True,
        ),
        schema="cleanup",
    )
    # Add refresh_batch FK to target_profit_center
    op.add_column(
        "target_profit_center",
        sa.Column(
            "refresh_batch",
            sa.Integer(),
            sa.ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL"),
            nullable=True,
        ),
        schema="cleanup",
    )
    # Create center_mapping table
    op.create_table(
        "center_mapping",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("object_type", sa.String(20), nullable=False),
        sa.Column("legacy_coarea", sa.String(10), nullable=False),
        sa.Column("legacy_center", sa.String(20), nullable=False),
        sa.Column("legacy_name", sa.String(200), nullable=True),
        sa.Column("target_coarea", sa.String(10), nullable=False),
        sa.Column("target_center", sa.String(20), nullable=False),
        sa.Column("target_name", sa.String(200), nullable=True),
        sa.Column("mapping_type", sa.String(20), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "refresh_batch",
            sa.Integer(),
            sa.ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "object_type", "legacy_coarea", "legacy_center", "refresh_batch"
        ),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_table("center_mapping", schema="cleanup")
    op.drop_column("target_profit_center", "refresh_batch", schema="cleanup")
    op.drop_column("target_cost_center", "refresh_batch", schema="cleanup")
