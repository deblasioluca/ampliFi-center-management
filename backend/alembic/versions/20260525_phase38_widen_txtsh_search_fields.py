"""phase38: widen txtsh columns + add search_fields to display config

Widen ``txtsh`` from 40→100 chars on all four CC/PC tables
(legacy + target) so longer SAP descriptions are not truncated.

Add ``search_fields`` JSONB column to ``explorer_display_config``
so admins can configure which fields are searchable per object type.

Revision ID: phase38_txtsh_search
Revises: phase37_hier_text
Create Date: 2026-05-25
"""

import sqlalchemy as sa
from alembic import op

revision = "phase38_txtsh_search"
down_revision = "phase37_hier_text"
branch_labels = None
depends_on = None

_TABLES_WITH_TXTSH = [
    "legacy_cost_center",
    "legacy_profit_center",
    "target_cost_center",
    "target_profit_center",
]


def upgrade() -> None:
    for table in _TABLES_WITH_TXTSH:
        op.alter_column(
            table,
            "txtsh",
            type_=sa.String(100),
            existing_type=sa.String(40),
            schema="cleanup",
        )

    op.add_column(
        "explorer_display_config",
        sa.Column("search_fields", sa.JSON(), nullable=True),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_column("explorer_display_config", "search_fields", schema="cleanup")

    for table in _TABLES_WITH_TXTSH:
        op.alter_column(
            table,
            "txtsh",
            type_=sa.String(40),
            existing_type=sa.String(100),
            schema="cleanup",
        )
