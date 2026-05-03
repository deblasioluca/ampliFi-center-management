"""Add rows_processed column to upload_batch for progress tracking.

Revision ID: phase21_upload_progress
Revises: phase20_sap_conn_ext
Create Date: 2026-04-28
"""

import sqlalchemy as sa

from alembic import op

revision = "phase21_upload_progress"
down_revision = "phase20_sap_conn_ext"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "upload_batch",
        sa.Column("rows_processed", sa.Integer(), server_default="0", nullable=False),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_column("upload_batch", "rows_processed", schema="cleanup")
