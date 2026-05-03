"""GL Account master data tables (SKA1/SKB1) + Explorer display config.

Adds:
- cleanup.gl_account_ska1 — GL account chart-of-accounts level (SAP SKA1)
- cleanup.gl_account_skb1 — GL account company-code level (SAP SKB1)
- cleanup.explorer_display_config — global display column configuration

Revision ID: phase19_gl_display
Revises: phase18_sap_full
Create Date: 2026-04-28
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "phase19_gl_display"
down_revision = "phase18_sap_full"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- GL Account SKA1 ---
    op.create_table(
        "gl_account_ska1",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("mandt", sa.String(3)),
        sa.Column("ktopl", sa.String(4), nullable=False),
        sa.Column("saknr", sa.String(10), nullable=False),
        sa.Column("xbilk", sa.String(1)),
        sa.Column("sakan", sa.String(10)),
        sa.Column("bilkt", sa.String(10)),
        sa.Column("erdat", sa.String(8)),
        sa.Column("ernam", sa.String(12)),
        sa.Column("gvtyp", sa.String(2)),
        sa.Column("ktoks", sa.String(4)),
        sa.Column("mustr", sa.String(10)),
        sa.Column("vbund", sa.String(6)),
        sa.Column("xloev", sa.String(1)),
        sa.Column("xspea", sa.String(1)),
        sa.Column("xspeb", sa.String(1)),
        sa.Column("xspep", sa.String(1)),
        sa.Column("mcod1", sa.String(25)),
        sa.Column("func_area", sa.String(16)),
        sa.Column("glaccount_type", sa.String(1)),
        sa.Column("glaccount_subtype", sa.String(1)),
        sa.Column("main_saknr", sa.String(10)),
        sa.Column("last_changed_ts", sa.String(15)),
        sa.Column("txt20", sa.String(20)),
        sa.Column("txt50", sa.String(50)),
        sa.Column(
            "refresh_batch",
            sa.Integer(),
            sa.ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("ktopl", "saknr"),
        schema="cleanup",
    )
    op.create_index("ix_ska1_saknr", "gl_account_ska1", ["saknr"], schema="cleanup")

    # --- GL Account SKB1 ---
    op.create_table(
        "gl_account_skb1",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("mandt", sa.String(3)),
        sa.Column("bukrs", sa.String(4), nullable=False),
        sa.Column("saknr", sa.String(10), nullable=False),
        sa.Column("begru", sa.String(4)),
        sa.Column("busab", sa.String(2)),
        sa.Column("datlz", sa.String(8)),
        sa.Column("erdat", sa.String(8)),
        sa.Column("ernam", sa.String(12)),
        sa.Column("fdgrv", sa.String(10)),
        sa.Column("fdlev", sa.String(2)),
        sa.Column("fipls", sa.String(3)),
        sa.Column("fstag", sa.String(4)),
        sa.Column("hbkid", sa.String(5)),
        sa.Column("hktid", sa.String(5)),
        sa.Column("kdfsl", sa.String(4)),
        sa.Column("mitkz", sa.String(1)),
        sa.Column("mwskz", sa.String(2)),
        sa.Column("stext", sa.String(50)),
        sa.Column("vzskz", sa.String(2)),
        sa.Column("waers", sa.String(5)),
        sa.Column("wmeth", sa.String(2)),
        sa.Column("xgkon", sa.String(1)),
        sa.Column("xintb", sa.String(1)),
        sa.Column("xkres", sa.String(1)),
        sa.Column("xloeb", sa.String(1)),
        sa.Column("xnkon", sa.String(1)),
        sa.Column("xopvw", sa.String(1)),
        sa.Column("xspeb", sa.String(1)),
        sa.Column("zindt", sa.String(8)),
        sa.Column("zinrt", sa.String(2)),
        sa.Column("zuawa", sa.String(3)),
        sa.Column("altkt", sa.String(10)),
        sa.Column("xmitk", sa.String(1)),
        sa.Column("recid", sa.String(2)),
        sa.Column("fipos", sa.String(14)),
        sa.Column("xmwno", sa.String(1)),
        sa.Column("xsalh", sa.String(1)),
        sa.Column("bewgp", sa.String(10)),
        sa.Column("infky", sa.String(8)),
        sa.Column("togru", sa.String(4)),
        sa.Column("xlgclr", sa.String(1)),
        sa.Column("x_uj_clr", sa.String(1)),
        sa.Column("mcakey", sa.String(5)),
        sa.Column("cochanged", sa.String(1)),
        sa.Column("last_changed_ts", sa.String(15)),
        sa.Column(
            "refresh_batch",
            sa.Integer(),
            sa.ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("bukrs", "saknr"),
        schema="cleanup",
    )
    op.create_index("ix_skb1_saknr", "gl_account_skb1", ["saknr"], schema="cleanup")
    op.create_index("ix_skb1_bukrs", "gl_account_skb1", ["bukrs"], schema="cleanup")

    # --- Explorer Display Config ---
    op.create_table(
        "explorer_display_config",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("object_type", sa.String(30), nullable=False),
        sa.Column("table_columns", JSONB, nullable=False, server_default="[]"),
        sa.Column("detail_columns", JSONB, nullable=False, server_default="[]"),
        sa.Column("default_sort_column", sa.String(50)),
        sa.Column("default_sort_dir", sa.String(4), server_default="asc"),
        sa.Column(
            "updated_by",
            sa.Integer(),
            sa.ForeignKey("cleanup.app_user.id", ondelete="SET NULL"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("object_type"),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_table("explorer_display_config", schema="cleanup")
    op.drop_index("ix_skb1_bukrs", table_name="gl_account_skb1", schema="cleanup")
    op.drop_index("ix_skb1_saknr", table_name="gl_account_skb1", schema="cleanup")
    op.drop_table("gl_account_skb1", schema="cleanup")
    op.drop_index("ix_ska1_saknr", table_name="gl_account_ska1", schema="cleanup")
    op.drop_table("gl_account_ska1", schema="cleanup")
