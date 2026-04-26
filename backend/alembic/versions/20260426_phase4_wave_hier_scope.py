"""Add wave_hierarchy_scope table.

Revision ID: phase4_wave_hier
Revises: phase3_llm_usage
Create Date: 2026-04-26
"""

from alembic import op
import sqlalchemy as sa

revision = "phase4_wave_hier"
down_revision = "phase3_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "wave_hierarchy_scope",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "wave_id",
            sa.Integer(),
            sa.ForeignKey("cleanup.wave.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "hierarchy_id",
            sa.Integer(),
            sa.ForeignKey("cleanup.hierarchy.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("node_setname", sa.String(40), nullable=False),
        sa.UniqueConstraint("wave_id", "hierarchy_id", "node_setname"),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_table("wave_hierarchy_scope", schema="cleanup")
