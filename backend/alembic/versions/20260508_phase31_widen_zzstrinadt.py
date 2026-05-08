"""Widen zzstrinadt from 8 to 10 chars (DATS export = YYYY-MM-DD)."""

import sqlalchemy as sa

from alembic import op

revision = "phase31_widen_zzstrinadt"
down_revision = "phase30_widen_sap_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ("legacy_cost_center", "target_cost_center"):
        op.alter_column(
            table,
            "zzstrinadt",
            type_=sa.String(10),
            existing_type=sa.String(8),
            schema="cleanup",
        )


def downgrade() -> None:
    for table in ("target_cost_center", "legacy_cost_center"):
        op.alter_column(
            table,
            "zzstrinadt",
            type_=sa.String(8),
            existing_type=sa.String(10),
            schema="cleanup",
        )
