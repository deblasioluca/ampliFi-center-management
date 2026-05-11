"""phase36: add description column to legacy_cost_center and legacy_profit_center

SAP CSKS/CEPC exports include human-readable text fields ('Name',
'Description', 'Cost ctr short text' / 'Long Text', 'Profit center
short text for matchcode').  txtsh and txtmi capture two; this adds a
dedicated 'description' column for the third on both tables.

Revision ID: phase36_cc_desc
Revises: phase35_rb_idx
Create Date: 2026-05-11
"""

from alembic import op
import sqlalchemy as sa

revision = "phase36_cc_desc"
down_revision = "phase35_rb_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "legacy_cost_center",
        sa.Column("description", sa.String(250), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "legacy_profit_center",
        sa.Column("description", sa.String(250), nullable=True),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_column("legacy_profit_center", "description", schema="cleanup")
    op.drop_column("legacy_cost_center", "description", schema="cleanup")
