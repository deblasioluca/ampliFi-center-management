"""phase37: add text column to hierarchy_node + node-items endpoint

Hierarchy nodes now carry an optional ``text`` column so the
frontend can display node descriptions alongside the technical
setname (e.g. "1000 — North America Region").

Revision ID: phase37_hier_text
Revises: phase36_cc_desc
Create Date: 2026-05-25
"""

import sqlalchemy as sa
from alembic import op

revision = "phase37_hier_text"
down_revision = "phase36_cc_desc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "hierarchy_node",
        sa.Column("text", sa.String(200), nullable=True),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_column("hierarchy_node", "text", schema="cleanup")
