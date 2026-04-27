"""Make analysis_run.wave_id nullable for global (non-wave) analysis.

Revision ID: phase13_nullable_wave
Revises: phase12_username
Create Date: 2026-04-25
"""

from alembic import op

revision = "phase13_nullable_wave"
down_revision = "phase12_username"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "analysis_run",
        "wave_id",
        nullable=True,
        schema="cleanup",
    )


def downgrade() -> None:
    op.alter_column(
        "analysis_run",
        "wave_id",
        nullable=False,
        schema="cleanup",
    )
