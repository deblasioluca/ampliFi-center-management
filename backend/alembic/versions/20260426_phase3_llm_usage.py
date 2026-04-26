"""Phase 3: LLM usage log table for cost tracking.

Revision ID: phase3_001
Revises: phase2_001
"""

from alembic import op
import sqlalchemy as sa

revision = "phase3_001"
down_revision = "phase2_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "llm_usage_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("model", sa.String(100), nullable=False),
        sa.Column("provider", sa.String(50), nullable=False),
        sa.Column("tokens_in", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("tokens_out", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cost_usd", sa.Numeric(10, 6), nullable=False, server_default="0"),
        sa.Column("mode", sa.String(30), nullable=False, server_default="SINGLE"),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column("center_cctr", sa.String(20), nullable=True),
        sa.Column("prompt_hash", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        schema="cleanup",
    )
    op.create_index(
        "ix_llm_usage_log_created_at",
        "llm_usage_log",
        ["created_at"],
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_index("ix_llm_usage_log_created_at", table_name="llm_usage_log", schema="cleanup")
    op.drop_table("llm_usage_log", schema="cleanup")
