"""Add datasphere_config table for SAP Datasphere integration.

Revision ID: phase14_datasphere
Revises: phase13_nullable_wave
Create Date: 2026-04-27
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "phase14_datasphere"
down_revision = "phase13_nullable_wave"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "datasphere_config",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ds_url", sa.String(500), nullable=True),
        sa.Column("ds_schema", sa.String(100), nullable=False, server_default="ACM"),
        sa.Column("ds_user", sa.String(200), nullable=True),
        sa.Column("ds_password_encrypted", sa.Text(), nullable=True),
        sa.Column("ds_use_ssl", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("domain_config", JSONB(), nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "updated_by",
            sa.Integer(),
            sa.ForeignKey("cleanup.app_user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_table("datasphere_config", schema="cleanup")
