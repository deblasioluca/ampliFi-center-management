"""phase34: center exclusion rules table

Revision ID: phase34_excl
Revises: phase33_widen2
Create Date: 2026-05-10
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "phase34_excl"
down_revision = "phase33_widen_sap_columns2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "center_exclusion_rule",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scope", sa.String(20), nullable=True),
        sa.Column("object_type", sa.String(20), nullable=False, server_default="both"),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("condition", JSONB(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), server_default="true"),
        sa.Column("is_system", sa.Boolean(), server_default="false"),
        sa.Column("sort_order", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        schema="cleanup",
    )
    op.create_index(
        "ix_excl_rule_scope", "center_exclusion_rule", ["scope"], schema="cleanup"
    )
    op.create_index(
        "ix_excl_rule_enabled", "center_exclusion_rule", ["is_enabled"], schema="cleanup"
    )

    # Seed default rules
    op.execute("""
        INSERT INTO cleanup.center_exclusion_rule (scope, object_type, name, description, condition, is_enabled, is_system, sort_order)
        VALUES
        (NULL, 'both', 'Expired centers (Valid To ≠ 31.12.9999)', 'Centers with a finite validity end date are considered closed/expired and excluded from migration.', '{"field": "datbi", "operator": "!=", "value": "99991231"}', true, true, 1),
        (NULL, 'cost_center', 'Plan primary costs blocked (BKZKP)', 'Cost centers with blocked plan primary cost postings.', '{"field": "bkzkp", "operator": "==", "value": "X"}', false, true, 2),
        (NULL, 'cost_center', 'Plan secondary costs blocked (PKZKP)', 'Cost centers with blocked plan secondary cost postings.', '{"field": "pkzkp", "operator": "==", "value": "X"}', false, true, 3),
        (NULL, 'profit_center', 'Lock indicator set (LOCK_IND)', 'Profit centers with the lock indicator flag set.', '{"field": "lock_ind", "operator": "==", "value": "X"}', false, true, 4)
    """)


def downgrade() -> None:
    op.drop_index("ix_excl_rule_enabled", table_name="center_exclusion_rule", schema="cleanup")
    op.drop_index("ix_excl_rule_scope", table_name="center_exclusion_rule", schema="cleanup")
    op.drop_table("center_exclusion_rule", schema="cleanup")
