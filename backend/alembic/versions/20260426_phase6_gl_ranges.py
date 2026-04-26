"""Add gl_account_class_range table.

Revision ID: phase6_gl_ranges
Revises: phase5_mv_balance
Create Date: 2026-04-26
"""

from alembic import op
import sqlalchemy as sa

revision = "phase6_gl_ranges"
down_revision = "phase5_mv_balance"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "gl_account_class_range",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("class_code", sa.String(20), nullable=False),
        sa.Column("class_label", sa.String(100), nullable=False),
        sa.Column("from_account", sa.String(20), nullable=False),
        sa.Column("to_account", sa.String(20), nullable=False),
        sa.Column("category", sa.String(40), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("class_code", "from_account"),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_table("gl_account_class_range", schema="cleanup")
