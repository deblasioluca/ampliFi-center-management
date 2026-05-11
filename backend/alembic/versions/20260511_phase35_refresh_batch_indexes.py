"""phase35: add indexes on refresh_batch / batch_id FK columns

Without these indexes, DELETE FROM upload_batch triggers sequential scans
on every child table to check the SET NULL / CASCADE FK constraint.
With 200k+ rows in legacy_cost_center this causes multi-minute timeouts.

Revision ID: phase35_rb_idx
Revises: phase34_excl
Create Date: 2026-05-11
"""

from alembic import op

revision = "phase35_rb_idx"
down_revision = "phase34_excl"
branch_labels = None
depends_on = None

# (table, column, index_name)
_INDEXES = [
    ("employee", "refresh_batch", "ix_emp_refresh_batch"),
    ("legacy_cost_center", "refresh_batch", "ix_lcc_refresh_batch"),
    ("legacy_profit_center", "refresh_batch", "ix_lpc_refresh_batch"),
    ("balance", "refresh_batch", "ix_bal_refresh_batch"),
    ("hierarchy", "refresh_batch", "ix_hier_refresh_batch"),
    ("data_quality_issue", "batch_id", "ix_dqi_batch_id"),
    ("target_cost_center", "refresh_batch", "ix_tcc_refresh_batch"),
    ("target_profit_center", "refresh_batch", "ix_tpc_refresh_batch"),
    ("center_mapping", "refresh_batch", "ix_cmap_refresh_batch"),
    ("upload_error", "batch_id", "ix_uerr_batch_id"),
    ("gl_account_ska1", "refresh_batch", "ix_glska1_refresh_batch"),
    ("gl_account_skb1", "refresh_batch", "ix_glskb1_refresh_batch"),
]


def upgrade() -> None:
    for table, column, idx_name in _INDEXES:
        op.create_index(
            idx_name,
            table,
            [column],
            schema="cleanup",
            if_not_exists=True,
        )


def downgrade() -> None:
    for table, _column, idx_name in _INDEXES:
        op.drop_index(idx_name, table_name=table, schema="cleanup", if_exists=True)
