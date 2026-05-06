"""Cost/profit center owner FK to employee.

Adds a real FK from ``cleanup.legacy_cost_center`` and
``cleanup.legacy_profit_center`` to ``cleanup.employee``, populated by
the sample-data generator and (eventually) by the SAP loader once the
HR feed is wired up.

Until now the link from a cost center to its responsible owner was a
free-text ``responsible`` (full name) plus ``verak_user`` (an SAP user
ID). That worked for read-only display but didn't let us:

  * Show the employee's full record (rank, department, manager) on the
    proposal page.
  * Detect when the owner has left the company (an employee row with a
    termination date).
  * Re-route proposals to the owner's manager automatically.

The FK is nullable. ``ON DELETE SET NULL`` so deleting an employee
doesn't blow up the cost center; it just goes back to "no owner".

Revision ID: phase27_cc_pc_owner_fk
Revises: phase26_housekeeping_pc
Create Date: 2026-05-06
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "phase27_cc_pc_owner_fk"
down_revision = "phase26_housekeeping_pc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Cost center → employee
    op.add_column(
        "legacy_cost_center",
        sa.Column("responsible_employee_id", sa.Integer(), nullable=True),
        schema="cleanup",
    )
    op.create_foreign_key(
        "fk_lcc_responsible_employee",
        "legacy_cost_center",
        "employee",
        ["responsible_employee_id"],
        ["id"],
        source_schema="cleanup",
        referent_schema="cleanup",
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_lcc_responsible_employee",
        "legacy_cost_center",
        ["responsible_employee_id"],
        schema="cleanup",
    )

    # Profit center → employee
    op.add_column(
        "legacy_profit_center",
        sa.Column("responsible_employee_id", sa.Integer(), nullable=True),
        schema="cleanup",
    )
    op.create_foreign_key(
        "fk_lpc_responsible_employee",
        "legacy_profit_center",
        "employee",
        ["responsible_employee_id"],
        ["id"],
        source_schema="cleanup",
        referent_schema="cleanup",
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_lpc_responsible_employee",
        "legacy_profit_center",
        ["responsible_employee_id"],
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_lpc_responsible_employee",
        table_name="legacy_profit_center",
        schema="cleanup",
    )
    op.drop_constraint(
        "fk_lpc_responsible_employee",
        "legacy_profit_center",
        type_="foreignkey",
        schema="cleanup",
    )
    op.drop_column(
        "legacy_profit_center",
        "responsible_employee_id",
        schema="cleanup",
    )
    op.drop_index(
        "ix_lcc_responsible_employee",
        table_name="legacy_cost_center",
        schema="cleanup",
    )
    op.drop_constraint(
        "fk_lcc_responsible_employee",
        "legacy_cost_center",
        type_="foreignkey",
        schema="cleanup",
    )
    op.drop_column(
        "legacy_cost_center",
        "responsible_employee_id",
        schema="cleanup",
    )
