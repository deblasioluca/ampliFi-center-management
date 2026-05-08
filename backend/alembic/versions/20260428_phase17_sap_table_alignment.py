"""Align DB tables with SAP standard structures (CSKS, CEPC, T001).

Adds SAP-standard fields to cost centers, profit centers, and entities
so that data exported from SAP is preserved without information loss.

Revision ID: phase17_sap_align
Revises: phase16_explorer_src
Create Date: 2026-04-28
"""

import sqlalchemy as sa

from alembic import op

revision = "phase17_sap_align"
down_revision = "phase16_explorer_src"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- LegacyCostCenter (CSKS/CSKT) ---
    op.add_column("legacy_cost_center", sa.Column("mandt", sa.String(3)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("verak_user", sa.String(24)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("gsber", sa.String(4)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("werks", sa.String(4)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("abtei", sa.String(12)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("func_area", sa.String(16)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("land1", sa.String(3)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("nkost", sa.String(20)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("bkzkp", sa.String(1)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("bkzks", sa.String(1)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("pkzkp", sa.String(1)), schema="cleanup")
    op.add_column("legacy_cost_center", sa.Column("pkzks", sa.String(1)), schema="cleanup")

    # --- LegacyProfitCenter (CEPC/CEPCT) ---
    op.add_column("legacy_profit_center", sa.Column("mandt", sa.String(3)), schema="cleanup")
    op.add_column("legacy_profit_center", sa.Column("verak_user", sa.String(24)), schema="cleanup")
    op.add_column("legacy_profit_center", sa.Column("segment", sa.String(10)), schema="cleanup")
    op.add_column("legacy_profit_center", sa.Column("land1", sa.String(3)), schema="cleanup")
    op.add_column("legacy_profit_center", sa.Column("name1", sa.String(40)), schema="cleanup")
    op.add_column("legacy_profit_center", sa.Column("name2", sa.String(40)), schema="cleanup")
    op.add_column("legacy_profit_center", sa.Column("language", sa.String(2)), schema="cleanup")
    op.add_column("legacy_profit_center", sa.Column("nprctr", sa.String(10)), schema="cleanup")
    op.add_column("legacy_profit_center", sa.Column("lock_ind", sa.String(1)), schema="cleanup")

    # --- Entity (T001) ---
    op.add_column("entity", sa.Column("mandt", sa.String(3)), schema="cleanup")
    op.add_column("entity", sa.Column("city", sa.String(25)), schema="cleanup")
    op.add_column("entity", sa.Column("language", sa.String(2)), schema="cleanup")
    op.add_column("entity", sa.Column("chart_of_accounts", sa.String(4)), schema="cleanup")
    op.add_column("entity", sa.Column("fiscal_year_variant", sa.String(2)), schema="cleanup")
    op.add_column("entity", sa.Column("company", sa.String(6)), schema="cleanup")
    op.add_column("entity", sa.Column("credit_control_area", sa.String(4)), schema="cleanup")
    op.add_column("entity", sa.Column("fm_area", sa.String(4)), schema="cleanup")


def downgrade() -> None:
    # --- Entity ---
    op.drop_column("entity", "fm_area", schema="cleanup")
    op.drop_column("entity", "credit_control_area", schema="cleanup")
    op.drop_column("entity", "company", schema="cleanup")
    op.drop_column("entity", "fiscal_year_variant", schema="cleanup")
    op.drop_column("entity", "chart_of_accounts", schema="cleanup")
    op.drop_column("entity", "language", schema="cleanup")
    op.drop_column("entity", "city", schema="cleanup")
    op.drop_column("entity", "mandt", schema="cleanup")

    # --- LegacyProfitCenter ---
    op.drop_column("legacy_profit_center", "lock_ind", schema="cleanup")
    op.drop_column("legacy_profit_center", "nprctr", schema="cleanup")
    op.drop_column("legacy_profit_center", "language", schema="cleanup")
    op.drop_column("legacy_profit_center", "name2", schema="cleanup")
    op.drop_column("legacy_profit_center", "name1", schema="cleanup")
    op.drop_column("legacy_profit_center", "land1", schema="cleanup")
    op.drop_column("legacy_profit_center", "segment", schema="cleanup")
    op.drop_column("legacy_profit_center", "verak_user", schema="cleanup")
    op.drop_column("legacy_profit_center", "mandt", schema="cleanup")

    # --- LegacyCostCenter ---
    op.drop_column("legacy_cost_center", "pkzks", schema="cleanup")
    op.drop_column("legacy_cost_center", "pkzkp", schema="cleanup")
    op.drop_column("legacy_cost_center", "bkzks", schema="cleanup")
    op.drop_column("legacy_cost_center", "bkzkp", schema="cleanup")
    op.drop_column("legacy_cost_center", "nkost", schema="cleanup")
    op.drop_column("legacy_cost_center", "land1", schema="cleanup")
    op.drop_column("legacy_cost_center", "func_area", schema="cleanup")
    op.drop_column("legacy_cost_center", "abtei", schema="cleanup")
    op.drop_column("legacy_cost_center", "werks", schema="cleanup")
    op.drop_column("legacy_cost_center", "gsber", schema="cleanup")
    op.drop_column("legacy_cost_center", "verak_user", schema="cleanup")
    op.drop_column("legacy_cost_center", "mandt", schema="cleanup")
