"""phase36: add description column to CC and PC tables (legacy + target)

SAP CSKS/CEPC exports include human-readable text fields ('Name',
'Description', 'Cost ctr short text' / 'Long Text', 'Profit center
short text for matchcode').  txtsh and txtmi capture two; this adds a
dedicated 'description' column for the third on all four tables.

Revision ID: phase36_cc_desc
Revises: phase35_rb_idx
Create Date: 2026-05-11
"""

import sqlalchemy as sa

from alembic import op

revision = "phase36_cc_desc"
down_revision = "phase35_rb_idx"
branch_labels = None
depends_on = None

_TABLES = [
    "legacy_cost_center",
    "legacy_profit_center",
    "target_cost_center",
    "target_profit_center",
]


def upgrade() -> None:
    for table in _TABLES:
        op.add_column(
            table,
            sa.Column("description", sa.String(250), nullable=True),
            schema="cleanup",
        )


def downgrade() -> None:
    for table in reversed(_TABLES):
        op.drop_column(table, "description", schema="cleanup")
