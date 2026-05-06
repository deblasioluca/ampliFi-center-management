"""Housekeeping items support both Cost Centers and Profit Centers.

Adds:
- ``entity_type`` discriminator column (default 'CC' for backward compat)
- ``target_pc_id`` nullable FK to target_profit_center
- Makes ``target_cc_id`` nullable (PC-only items don't have a CC)
- Replaces the (cycle_id, target_cc_id, flag) unique constraint with
  (cycle_id, entity_type, target_cc_id, target_pc_id, flag) to allow both
  entity types to coexist in one cycle.

Datasphere note: NULL semantics in unique indexes vary across backends.
The application-layer logic in services/housekeeping.py guards against
duplicate inserts independently of the DB-level constraint.

Revision ID: phase26_housekeeping_pc
Revises: phase25_merge
Create Date: 2026-05-06
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "phase26_housekeeping_pc"
down_revision = "phase25_merge"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the old narrow unique constraint
    op.drop_constraint(
        "housekeeping_item_cycle_id_target_cc_id_flag_key",
        "housekeeping_item",
        type_="unique",
        schema="cleanup",
    )

    # entity_type discriminator (default 'CC' for existing rows)
    op.add_column(
        "housekeeping_item",
        sa.Column("entity_type", sa.String(8), nullable=False, server_default="CC"),
        schema="cleanup",
    )

    # Make target_cc_id nullable so PC-only items can exist
    op.alter_column(
        "housekeeping_item",
        "target_cc_id",
        existing_type=sa.Integer(),
        nullable=True,
        schema="cleanup",
    )

    # Add target_pc_id FK
    op.add_column(
        "housekeeping_item",
        sa.Column("target_pc_id", sa.Integer(), nullable=True),
        schema="cleanup",
    )
    op.create_foreign_key(
        "housekeeping_item_target_pc_id_fkey",
        "housekeeping_item",
        "target_profit_center",
        ["target_pc_id"],
        ["id"],
        ondelete="CASCADE",
        source_schema="cleanup",
        referent_schema="cleanup",
    )

    # New wider unique constraint
    op.create_unique_constraint(
        "housekeeping_item_cycle_entity_target_flag_key",
        "housekeeping_item",
        ["cycle_id", "entity_type", "target_cc_id", "target_pc_id", "flag"],
        schema="cleanup",
    )

    # Drop the server_default — the column is required, but defaulting at the
    # ORM layer is sufficient. Keeping a server_default would mask bugs where
    # the application forgot to set it.
    op.alter_column(
        "housekeeping_item",
        "entity_type",
        server_default=None,
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_constraint(
        "housekeeping_item_cycle_entity_target_flag_key",
        "housekeeping_item",
        type_="unique",
        schema="cleanup",
    )
    op.drop_constraint(
        "housekeeping_item_target_pc_id_fkey",
        "housekeeping_item",
        type_="foreignkey",
        schema="cleanup",
    )
    op.drop_column("housekeeping_item", "target_pc_id", schema="cleanup")
    op.drop_column("housekeeping_item", "entity_type", schema="cleanup")
    # Cannot reliably revert nullable to NOT NULL without orphan handling;
    # leave target_cc_id nullable in the downgrade path.
    op.create_unique_constraint(
        "housekeeping_item_cycle_id_target_cc_id_flag_key",
        "housekeeping_item",
        ["cycle_id", "target_cc_id", "flag"],
        schema="cleanup",
    )
