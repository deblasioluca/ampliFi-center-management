"""Widen SAP columns that truncate on real data exports.

datbi     10 → 20  (timestamp format YYYY-MM-DD HH:MM:SS)
datab     10 → 20
verak_user 12 → 20
funkt      3 → 10
xblnr     16 → 40
regio      3 → 10
zzstrgfd   3 → 10
zzstrfst   2 → 10
"""

import sqlalchemy as sa

from alembic import op

revision = "phase33_widen_sap_columns2"
down_revision = "phase32_employee_active_dq"
branch_labels = None
depends_on = None

# (table, column, old_size, new_size)
_CC_TABLES = ("legacy_cost_center", "target_cost_center")
_PC_TABLES = ("legacy_profit_center", "target_profit_center")
_ALL_TABLES = _CC_TABLES + _PC_TABLES

_CHANGES: list[tuple[tuple[str, ...], str, int, int]] = [
    # columns on ALL tables (CC + PC)
    (_ALL_TABLES, "datbi", 10, 20),
    (_ALL_TABLES, "datab", 10, 20),
    (_ALL_TABLES, "verak_user", 12, 20),
    (_ALL_TABLES, "regio", 3, 10),
    # columns on CC tables only
    (_CC_TABLES, "funkt", 3, 10),
    (_CC_TABLES, "xblnr", 16, 40),
    (_CC_TABLES, "zzstrgfd", 3, 10),
    (_CC_TABLES, "zzstrfst", 2, 10),
]


def upgrade() -> None:
    for tables, column, old_size, new_size in _CHANGES:
        for table in tables:
            op.alter_column(
                table,
                column,
                type_=sa.String(new_size),
                existing_type=sa.String(old_size),
                schema="cleanup",
            )


def downgrade() -> None:
    for tables, column, old_size, new_size in _CHANGES:
        for table in reversed(tables):
            op.alter_column(
                table,
                column,
                type_=sa.String(old_size),
                existing_type=sa.String(new_size),
                schema="cleanup",
            )
