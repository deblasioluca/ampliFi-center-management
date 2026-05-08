"""Multi-role support and analyst→data_manager rename.

Widens cleanup.app_user.role from VARCHAR(20) to VARCHAR(200) to allow
comma-separated roles (e.g. "admin,data_manager"). Renames "analyst"
to "data_manager" for all existing users.
"""

import sqlalchemy as sa

from alembic import op

revision = "phase29_multi_role"
down_revision = "phase28_wave_archive"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Widen column
    op.alter_column(
        "app_user",
        "role",
        type_=sa.String(200),
        existing_type=sa.String(20),
        schema="cleanup",
    )
    # Rename analyst → data_manager
    op.execute(
        "UPDATE cleanup.app_user SET role = REPLACE(role, 'analyst', 'data_manager') "
        "WHERE role LIKE '%analyst%'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE cleanup.app_user SET role = REPLACE(role, 'data_manager', 'analyst') "
        "WHERE role LIKE '%data_manager%'"
    )
    op.alter_column(
        "app_user",
        "role",
        type_=sa.String(20),
        existing_type=sa.String(200),
        schema="cleanup",
    )
