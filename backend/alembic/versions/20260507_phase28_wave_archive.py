"""Wave archival flag.

Adds ``cleanup.wave.is_archived`` (boolean, NOT NULL, default false).
The flag separates "completed but kept around for reference" from
"completed and put away to declutter the active wave list". Archive
is set explicitly via ``POST /api/waves/{id}/archive`` and is only
allowed on terminal waves (``signed_off``, ``closed``, ``cancelled``).

Why a separate flag and not a new status value: a wave's status
captures its position in the analysis lifecycle (draft → analysing
→ ... → signed_off). Archival is orthogonal — an admin should be
able to look at an archived signed_off wave and still see its real
analysis-lifecycle status. Conflating these into a single string
would lose information.

The default-false makes existing rows visible in the active list,
which is the conservative migration behaviour: nothing disappears
unexpectedly when this lands.

Revision ID: phase28_wave_archive
Revises: phase27_cc_pc_owner_fk
Create Date: 2026-05-07
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "phase28_wave_archive"
down_revision = "phase27_cc_pc_owner_fk"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "wave",
        sa.Column(
            "is_archived",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        schema="cleanup",
    )
    # Drop the server_default after the column exists so the model's
    # Python-side default (False) is the single source of truth going
    # forward.
    op.alter_column("wave", "is_archived", server_default=None, schema="cleanup")


def downgrade() -> None:
    op.drop_column("wave", "is_archived", schema="cleanup")
