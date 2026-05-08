"""Merge V2 decision tree + widen varchar heads

Revision ID: 41eabc1727d7
Revises: phase25_v2_decision_tree, phase25_widen_varchar4
Create Date: 2026-05-05 15:16:49.995826
"""

from __future__ import annotations

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "phase25_merge"
down_revision: str | None = ("phase25_v2_decision_tree", "phase25_widen_varchar4")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
