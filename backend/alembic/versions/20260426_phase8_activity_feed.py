"""Add activity_feed table.

Revision ID: phase8_activity
Revises: phase7_wave_tpl
Create Date: 2026-04-26
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "phase8_activity"
down_revision = "phase7_wave_tpl"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "activity_feed",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("cleanup.app_user.id", ondelete="SET NULL"),
        ),
        sa.Column("action", sa.String(50), nullable=False),
        sa.Column("entity_type", sa.String(30), nullable=True),
        sa.Column("entity_id", sa.Integer(), nullable=True),
        sa.Column("summary", sa.String(500), nullable=False),
        sa.Column("detail", JSONB(), nullable=True),
        sa.Column("is_read", sa.Boolean(), server_default="false"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        schema="cleanup",
    )
    op.create_index("ix_activity_feed_user", "activity_feed", ["user_id"], schema="cleanup")
    op.create_index("ix_activity_feed_ts", "activity_feed", ["created_at"], schema="cleanup")


def downgrade() -> None:
    op.drop_table("activity_feed", schema="cleanup")
