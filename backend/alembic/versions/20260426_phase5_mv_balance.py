"""Add materialized view mv_balance_per_center.

Revision ID: phase5_mv_balance
Revises: phase4_wave_hier
Create Date: 2026-04-26
"""

from alembic import op

revision = "phase5_mv_balance"
down_revision = "phase4_wave_hier"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS cleanup.mv_balance_per_center AS
        SELECT
            b.coarea,
            b.cctr,
            b.ccode,
            COUNT(*)                          AS row_count,
            SUM(b.posting_count)              AS total_postings,
            SUM(b.tc_amt)                     AS total_tc_amt,
            SUM(b.gc_amt)                     AS total_gc_amt,
            MIN(b.fiscal_year * 100 + b.period) AS min_period,
            MAX(b.fiscal_year * 100 + b.period) AS max_period,
            MAX(CASE WHEN b.posting_count > 0
                     THEN b.fiscal_year * 100 + b.period
                     ELSE NULL END)           AS last_posting_period
        FROM cleanup.balance b
        GROUP BY b.coarea, b.cctr, b.ccode
        WITH DATA
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ix_mv_bal_center
        ON cleanup.mv_balance_per_center (coarea, cctr, ccode)
    """)


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS cleanup.mv_balance_per_center")
