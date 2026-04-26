"""Add wave_template table.

Revision ID: phase7_wave_tpl
Revises: phase6_gl_ranges
Create Date: 2026-04-26
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "phase7_wave_tpl"
down_revision = "phase6_gl_ranges"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "wave_template",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("config", JSONB(), nullable=True),
        sa.Column("is_full_scope", sa.Boolean(), server_default="false"),
        sa.Column("exclude_prior", sa.Boolean(), server_default="true"),
        sa.Column("entity_ccodes", JSONB(), nullable=True),
        sa.Column("created_by", sa.Integer(), sa.ForeignKey("cleanup.app_user.id", ondelete="SET NULL")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_table("wave_template", schema="cleanup")
