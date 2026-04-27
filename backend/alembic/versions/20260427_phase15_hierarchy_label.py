"""Add label column to hierarchy table for editable custom names.

Revision ID: phase15_hierarchy_label
Revises: phase14_datasphere
Create Date: 2026-04-27
"""

from alembic import op
import sqlalchemy as sa

revision = "phase15_hierarchy_label"
down_revision = "phase14_datasphere"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "hierarchy",
        sa.Column("label", sa.String(200), nullable=True),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_column("hierarchy", "label", schema="cleanup")
