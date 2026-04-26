"""Add username column to app_user; login via username instead of email.

Revision ID: phase12_username
Revises: phase11_employee
"""

import sqlalchemy as sa

from alembic import op

revision = "phase12_username"
down_revision = "phase11_employee"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add username column (nullable initially for backfill)
    op.add_column(
        "app_user",
        sa.Column("username", sa.String(100), nullable=True),
        schema="cleanup",
    )

    # Backfill existing users: derive username from email (part before @)
    op.execute(
        "UPDATE cleanup.app_user SET username = split_part(email, '@', 1) WHERE username IS NULL"
    )

    # Make username NOT NULL and UNIQUE
    op.alter_column(
        "app_user",
        "username",
        nullable=False,
        schema="cleanup",
    )
    op.create_unique_constraint(
        "uq_app_user_username",
        "app_user",
        ["username"],
        schema="cleanup",
    )

    # Make email nullable (no longer required for auth)
    op.alter_column(
        "app_user",
        "email",
        nullable=True,
        schema="cleanup",
    )


def downgrade() -> None:
    op.alter_column(
        "app_user",
        "email",
        nullable=False,
        schema="cleanup",
    )
    op.drop_constraint(
        "uq_app_user_username",
        "app_user",
        schema="cleanup",
    )
    op.drop_column("app_user", "username", schema="cleanup")
