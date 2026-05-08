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

Constraint-name resilience
==========================
The (cycle_id, target_cc_id, flag) unique constraint that this migration
replaces was created in phase9_hk_flag with the explicit name
``uq_housekeeping_item_cycle_target_flag``. An earlier version of this
file hard-coded the auto-generated PostgreSQL name
``housekeeping_item_cycle_id_target_cc_id_flag_key`` and crashed any
deployment whose phase9 had used the explicit name (which is every
deployment that ran the migration as designed).

Rather than guess, this migration now looks up the actual constraint by
the columns it covers and drops it by whatever name PostgreSQL stored.
This makes the migration safe to run on:

* DBs where phase9 named the constraint explicitly (the documented path)
* DBs where the constraint was created via ``Base.metadata.create_all``
  with PG's auto-generated name
* DBs where the operator already manually renamed the constraint as a
  hot-fix workaround (idempotent re-runs)

Revision ID: phase26_housekeeping_pc
Revises: phase25_merge
Create Date: 2026-05-06
"""

from __future__ import annotations

import logging

import sqlalchemy as sa

from alembic import op

revision = "phase26_housekeeping_pc"
down_revision = "phase25_merge"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")


def _find_unique_constraint_name(
    bind: sa.engine.Connection,
    *,
    schema: str,
    table: str,
    columns: list[str],
) -> str | None:
    """Return the name of the unique constraint covering exactly ``columns``,
    or ``None`` if no such constraint exists. Order-insensitive.

    Uses SQLAlchemy's Inspector so it works against any DB backend Alembic
    supports — but in practice this project is PostgreSQL-only.
    """
    inspector = sa.inspect(bind)
    target = set(columns)
    for uc in inspector.get_unique_constraints(table, schema=schema):
        if set(uc.get("column_names") or []) == target:
            return uc["name"]
    return None


def upgrade() -> None:
    bind = op.get_bind()

    # Drop the old narrow unique constraint by looking up its actual name.
    # Hard-coding the name was unreliable (see module docstring).
    old_name = _find_unique_constraint_name(
        bind,
        schema="cleanup",
        table="housekeeping_item",
        columns=["cycle_id", "target_cc_id", "flag"],
    )
    if old_name:
        op.drop_constraint(
            old_name,
            "housekeeping_item",
            type_="unique",
            schema="cleanup",
        )
    else:
        # Idempotent re-run: the constraint was already dropped (e.g. by a
        # previous partial run that committed before failing, or a manual
        # hot-fix). Safe to skip.
        logger.warning(
            "phase26: no unique constraint on (cycle_id, target_cc_id, flag) "
            "found on cleanup.housekeeping_item — skipping drop"
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
    bind = op.get_bind()

    # Drop the wide constraint by looking it up rather than trusting the name.
    new_name = _find_unique_constraint_name(
        bind,
        schema="cleanup",
        table="housekeeping_item",
        columns=["cycle_id", "entity_type", "target_cc_id", "target_pc_id", "flag"],
    )
    if new_name:
        op.drop_constraint(
            new_name,
            "housekeeping_item",
            type_="unique",
            schema="cleanup",
        )
    else:
        logger.warning(
            "phase26 downgrade: no unique constraint on "
            "(cycle_id, entity_type, target_cc_id, target_pc_id, flag) "
            "found on cleanup.housekeeping_item — skipping drop"
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
    # Recreate the narrow unique constraint with the explicit name from
    # phase9 (consistent with how that migration named it).
    op.create_unique_constraint(
        "uq_housekeeping_item_cycle_target_flag",
        "housekeeping_item",
        ["cycle_id", "target_cc_id", "flag"],
        schema="cleanup",
    )
