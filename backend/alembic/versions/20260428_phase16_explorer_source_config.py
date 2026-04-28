"""Add explorer_source_config table for Data Explorer data source management.

Revision ID: phase16_explorer_src
Revises: phase15_hierarchy_label
Create Date: 2026-04-28
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "phase16_explorer_src"
down_revision = "phase15_hierarchy_label"
branch_labels = None
depends_on = None

# Default source configs seeded on first migration
_DEFAULTS = [
    # Legacy area
    ("cost_centers", "legacy", "Cost Centers", "local_db", "db_query", "replicated", 0),
    ("profit_centers", "legacy", "Profit Centers", "local_db", "db_query", "replicated", 1),
    ("entities", "legacy", "Entities", "local_db", "db_query", "replicated", 2),
    ("hierarchies", "legacy", "Hierarchies", "local_db", "db_query", "replicated", 3),
    ("balances", "legacy", "Balances", "local_db", "db_query", "replicated", 4),
    ("employees", "legacy", "Employees", "local_db", "db_query", "replicated", 5),
    ("gl_accounts", "legacy", "GL Accounts", "local_db", "db_query", "replicated", 6),
    # ampliFi area
    ("cost_centers", "amplifi", "Cost Centers (Target)", "local_db", "db_query", "replicated", 0),
    ("profit_centers", "amplifi", "Profit Centers (Target)", "local_db", "db_query", "replicated", 1),
    ("entities", "amplifi", "Entities (Target)", "local_db", "db_query", "replicated", 2),
    ("hierarchies", "amplifi", "Hierarchies (Target)", "local_db", "db_query", "replicated", 3),
    ("mapping", "amplifi", "Legacy → Target Mapping", "local_db", "db_query", "replicated", 10),
]


def upgrade() -> None:
    tbl = op.create_table(
        "explorer_source_config",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("object_type", sa.String(30), nullable=False),
        sa.Column("area", sa.String(10), nullable=False, server_default="legacy"),
        sa.Column("label", sa.String(100), nullable=False),
        sa.Column("source_system", sa.String(30), nullable=False, server_default="local_db"),
        sa.Column("protocol", sa.String(20), nullable=False, server_default="db_query"),
        sa.Column("mode", sa.String(15), nullable=False, server_default="replicated"),
        sa.Column("connection_ref", sa.String(200), nullable=True),
        sa.Column("endpoint", sa.String(500), nullable=True),
        sa.Column("replication_cron", sa.String(50), nullable=True),
        sa.Column("extra_config", JSONB, nullable=True),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("display_order", sa.Integer, nullable=False, server_default="0"),
        sa.Column("updated_by", sa.Integer, sa.ForeignKey("cleanup.app_user.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.UniqueConstraint("object_type", "area"),
        schema="cleanup",
    )
    op.bulk_insert(
        tbl,
        [
            {
                "object_type": d[0],
                "area": d[1],
                "label": d[2],
                "source_system": d[3],
                "protocol": d[4],
                "mode": d[5],
                "display_order": d[6],
            }
            for d in _DEFAULTS
        ],
    )


def downgrade() -> None:
    op.drop_table("explorer_source_config", schema="cleanup")
