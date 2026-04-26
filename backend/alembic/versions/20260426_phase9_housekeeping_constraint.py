"""Update housekeeping_item unique constraint to include flag column.

Revision ID: phase9_hk_flag
Revises: phase8_activity
Create Date: 2026-04-26
"""

from alembic import op

revision = "phase9_hk_flag"
down_revision = "phase8_activity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "housekeeping_item_cycle_id_target_cc_id_key",
        "housekeeping_item",
        schema="cleanup",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_housekeeping_item_cycle_target_flag",
        "housekeeping_item",
        ["cycle_id", "target_cc_id", "flag"],
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_housekeeping_item_cycle_target_flag",
        "housekeeping_item",
        schema="cleanup",
        type_="unique",
    )
    op.create_unique_constraint(
        "housekeeping_item_cycle_id_target_cc_id_key",
        "housekeeping_item",
        ["cycle_id", "target_cc_id"],
        schema="cleanup",
    )
