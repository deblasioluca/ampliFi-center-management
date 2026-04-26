"""Add decided_by column to review_item for audit trail.

Revision ID: phase10_decided_by
Revises: phase9_hk_flag
"""

import sqlalchemy as sa

from alembic import op

revision = "phase10_decided_by"
down_revision = "phase9_hk_flag"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "review_item",
        sa.Column("decided_by", sa.String(100), nullable=True),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_column("review_item", "decided_by", schema="cleanup")
