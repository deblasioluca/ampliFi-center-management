"""V2 ID assignment — sequential PC/CC IDs for CEMA migration.

After the V2 pipeline runs on all centers in a wave, this module assigns
sequential IDs:
  - PC IDs: P00001–PZZZZZ (one per 1:1 center, one per 1:n group)
  - CC IDs: C00001–CZZZZZ (one per migrated center)
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.core import CenterProposal

log = logging.getLogger(__name__)


def _format_id(prefix: str, number: int, width: int = 5) -> str:
    """Format an ID like P00137 or C00001."""
    return f"{prefix}{number:0{width}d}"


def assign_v2_ids(
    run_id: int,
    db: Session,
    pc_prefix: str = "P",
    cc_prefix: str = "C",
    pc_start: int = 137,
    cc_start: int = 1,
    id_width: int = 5,
) -> dict:
    """Assign sequential PC and CC IDs to proposals from a V2 analysis run.

    Returns summary with counts and ID ranges.
    """
    proposals = (
        db.execute(
            select(CenterProposal)
            .where(CenterProposal.run_id == run_id)
            .order_by(CenterProposal.id)
        )
        .scalars()
        .all()
    )

    # Separate migrating proposals and build sort keys from attrs
    migrating: list[CenterProposal] = []
    for p in proposals:
        attrs = p.attrs or {}
        if attrs.get("migrate") == "Y":
            migrating.append(p)

    if not migrating:
        return {"assigned": 0, "pc_ids": 0, "cc_ids": 0}

    # Sort by hierarchy path for deterministic ordering
    def _sort_key(p: CenterProposal) -> tuple:
        attrs = p.attrs or {}
        levels = attrs.get("ext_levels", {})
        return tuple(levels.get(f"ext_l{i}", "") for i in range(14))

    migrating.sort(key=_sort_key)

    # Assign CC IDs — one per migrated center
    cc_counter = cc_start
    for p in migrating:
        attrs = dict(p.attrs or {})
        attrs["cc_id"] = _format_id(cc_prefix, cc_counter, id_width)
        p.attrs = attrs
        cc_counter += 1

    # Assign PC IDs — grouped by approach
    # 1:1 centers each get a unique PC ID
    # 1:n centers sharing the same group_key share one PC ID
    pc_counter = pc_start
    group_pc_map: dict[str, str] = {}  # group_key → PC ID

    for p in migrating:
        attrs = dict(p.attrs or {})
        approach = attrs.get("approach", "1:1")
        group_key = attrs.get("group_key", "")

        if approach == "1:n" and group_key:
            if group_key not in group_pc_map:
                group_pc_map[group_key] = _format_id(pc_prefix, pc_counter, id_width)
                pc_counter += 1
            attrs["pc_id"] = group_pc_map[group_key]
        else:
            attrs["pc_id"] = _format_id(pc_prefix, pc_counter, id_width)
            pc_counter += 1

        p.attrs = attrs

    db.flush()

    result = {
        "assigned": len(migrating),
        "pc_ids": pc_counter - pc_start,
        "cc_ids": cc_counter - cc_start,
        "pc_range": (
            f"{_format_id(pc_prefix, pc_start, id_width)}"
            f"-{_format_id(pc_prefix, pc_counter - 1, id_width)}"
        ),
        "cc_range": (
            f"{_format_id(cc_prefix, cc_start, id_width)}"
            f"-{_format_id(cc_prefix, cc_counter - 1, id_width)}"
        ),
        "groups_1n": len(group_pc_map),
    }
    log.info("v2.ids_assigned: %s", result)
    return result
