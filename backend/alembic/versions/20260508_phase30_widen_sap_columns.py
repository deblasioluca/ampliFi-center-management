"""Widen SAP columns to match real export data sizes.

Several SAP fields were defined with sizes matching the internal SAP data
dictionary (e.g. DATS=8 for dates), but real SAP exports use different
formats (e.g. YYYY-MM-DD=10 for dates, MANDT='0100'=4 chars instead of 3).
This migration widens all affected columns across all 4 center tables
(legacy CC/PC, target CC/PC) plus entity to prevent data truncation.
"""

import sqlalchemy as sa

from alembic import op

revision = "phase30_widen_sap_columns"
down_revision = "phase29_multi_role"
branch_labels = None
depends_on = None

# (table_name, column_name, old_size, new_size)
_CHANGES: list[tuple[str, str, int, int]] = [
    # --- mandt: 3 → 4 (export format "0100") ---
    ("entity", "mandt", 3, 4),
    ("employee", "mandt", 3, 4),
    ("legacy_cost_center", "mandt", 3, 4),
    ("legacy_profit_center", "mandt", 3, 4),
    ("target_cost_center", "mandt", 3, 4),
    ("target_profit_center", "mandt", 3, 4),
    ("gl_account_ska1", "mandt", 3, 4),
    ("gl_account_skb1", "mandt", 3, 4),
    # --- datbi: 8 → 10 (YYYY-MM-DD) ---
    ("legacy_cost_center", "datbi", 8, 10),
    ("legacy_profit_center", "datbi", 8, 10),
    ("target_cost_center", "datbi", 8, 10),
    ("target_profit_center", "datbi", 8, 10),
    # --- datab: 8 → 10 ---
    ("legacy_cost_center", "datab", 8, 10),
    ("legacy_profit_center", "datab", 8, 10),
    ("target_cost_center", "datab", 8, 10),
    ("target_profit_center", "datab", 8, 10),
    # --- ersda: 8 → 14 (date + optional time) ---
    ("legacy_cost_center", "ersda", 8, 14),
    ("legacy_profit_center", "ersda", 8, 14),
    ("target_cost_center", "ersda", 8, 14),
    ("target_profit_center", "ersda", 8, 14),
    # --- pkzkp: 1 → 4 ---
    ("legacy_cost_center", "pkzkp", 1, 4),
    ("target_cost_center", "pkzkp", 1, 4),
    # --- land1: 3 → 4 ---
    ("legacy_cost_center", "land1", 3, 4),
    ("legacy_profit_center", "land1", 3, 4),
    ("target_cost_center", "land1", 3, 4),
    ("target_profit_center", "land1", 3, 4),
    # --- stakz: 1 → 12 ---
    ("legacy_cost_center", "stakz", 1, 12),
    ("target_cost_center", "stakz", 1, 12),
    # --- zzstrpctyp: 3 → 4 ---
    ("legacy_cost_center", "zzstrpctyp", 3, 4),
    ("target_cost_center", "zzstrpctyp", 3, 4),
    # --- zzstraagcd: 2 → 4 ---
    ("legacy_cost_center", "zzstraagcd", 2, 4),
    ("target_cost_center", "zzstraagcd", 2, 4),
    # --- zzstrkstyp: 1 → 20 ---
    ("legacy_cost_center", "zzstrkstyp", 1, 20),
    ("target_cost_center", "zzstrkstyp", 1, 20),
    # --- zzstrcurr2: 3 → 5 ---
    ("legacy_cost_center", "zzstrcurr2", 3, 5),
    ("target_cost_center", "zzstrcurr2", 3, 5),
    # --- entity.country: 3 → 4 (LAND1) ---
    ("entity", "country", 3, 4),
    # --- entity.currency: 3 → 5 (WAERS standard is 5) ---
    ("entity", "currency", 3, 5),
]


def upgrade() -> None:
    for table, col, old, new in _CHANGES:
        op.alter_column(
            table,
            col,
            type_=sa.String(new),
            existing_type=sa.String(old),
            schema="cleanup",
        )


def downgrade() -> None:
    for table, col, old, new in reversed(_CHANGES):
        op.alter_column(
            table,
            col,
            type_=sa.String(old),
            existing_type=sa.String(new),
            schema="cleanup",
        )
