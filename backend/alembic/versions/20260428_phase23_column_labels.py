"""Add column_labels JSONB to explorer_display_config.

Revision ID: phase23_column_labels
Revises: phase22_target_mapping
Create Date: 2026-04-28
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "phase23_column_labels"
down_revision = "phase22_target_mapping"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "explorer_display_config",
        sa.Column("column_labels", JSONB, nullable=False, server_default="{}"),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_column("explorer_display_config", "column_labels", schema="cleanup")
