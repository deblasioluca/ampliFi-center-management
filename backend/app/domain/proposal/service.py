"""Proposal management service (§06.5, §06.6).

Handles proposal override, locking, and target object draft creation.
When a wave is locked, proposals become immutable and target cost centers /
profit centers are created from the approved proposals.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.decision_tree.engine import CleansingOutcome, TargetObject
from app.models.core import (
    AnalysisRun,
    CenterProposal,
    Employee,
    LegacyCostCenter,
    NamingAllocation,
    NamingPool,
    TargetCostCenter,
    TargetProfitCenter,
    Wave,
)

logger = structlog.get_logger()


def override_proposal(
    proposal_id: int,
    new_outcome: str,
    new_target: str | None,
    reason: str,
    user_id: int,
    db: Session,
) -> CenterProposal:
    """Override a proposal's outcome (§04.4).

    The original deterministic verdict and rule_path are NOT modified —
    they remain for audit. Only override fields are set.
    """
    proposal = db.get(CenterProposal, proposal_id)
    if not proposal:
        raise ValueError(f"Proposal {proposal_id} not found")

    run = db.get(AnalysisRun, proposal.run_id)
    if not run:
        raise ValueError("Associated run not found")

    wave = db.get(Wave, run.wave_id)
    if wave and wave.status in ("locked", "in_review", "signed_off", "closed"):
        raise ValueError(f"Cannot override: wave is {wave.status}")

    if new_outcome not in [e.value for e in CleansingOutcome]:
        raise ValueError(f"Invalid outcome: {new_outcome}")

    if new_target and new_target not in [e.value for e in TargetObject]:
        raise ValueError(f"Invalid target object: {new_target}")

    if not reason or not reason.strip():
        raise ValueError("Override reason is required")

    proposal.override_outcome = new_outcome
    proposal.override_target = new_target
    proposal.override_reason = reason
    proposal.override_by = user_id
    proposal.override_at = datetime.now(UTC)

    db.commit()
    db.refresh(proposal)
    return proposal


def get_effective_outcome(proposal: CenterProposal) -> tuple[str, str | None]:
    """Get the effective outcome considering overrides."""
    outcome = proposal.override_outcome or proposal.cleansing_outcome
    target = proposal.override_target or proposal.target_object
    return outcome, target


def lock_proposals(wave_id: int, run_id: int, db: Session) -> dict:
    """Lock proposals and create target object drafts (§06.6).

    This is called when a wave transitions from 'proposed' to 'locked'.
    For each KEEP proposal, a target CC and/or PC is created.
    RETIRE, MERGE_MAP, and REDESIGN proposals are skipped (no target objects).
    """
    wave = db.get(Wave, wave_id)
    if not wave:
        raise ValueError(f"Wave {wave_id} not found")

    if wave.status not in ("proposed", "analysing"):
        raise ValueError(f"Cannot lock: wave is {wave.status}, expected proposed or analysing")

    run = db.get(AnalysisRun, run_id)
    if not run or run.wave_id != wave_id:
        raise ValueError(f"Run {run_id} does not belong to wave {wave_id}")

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == run_id)).scalars().all()
    )

    created_cc = 0
    created_pc = 0

    for proposal in proposals:
        outcome, target = get_effective_outcome(proposal)

        if outcome in ("RETIRE", "MERGE_MAP"):
            continue

        legacy = db.get(LegacyCostCenter, proposal.legacy_cc_id)
        if not legacy:
            continue

        # Resolve owner as "GPN Name" from Employee table
        owner = resolve_owner_display(legacy.responsible, db)

        # Create target cost center if target includes CC
        if target in ("CC", "CC_AND_PC"):
            existing = db.execute(
                select(TargetCostCenter).where(
                    TargetCostCenter.coarea == legacy.coarea,
                    TargetCostCenter.cctr == legacy.cctr,
                )
            ).scalar_one_or_none()

            if not existing:
                tcc = TargetCostCenter(
                    coarea=legacy.coarea,
                    cctr=legacy.cctr,
                    txtsh=legacy.txtsh,
                    txtmi=legacy.txtmi,
                    responsible=owner or legacy.responsible,
                    ccode=legacy.ccode,
                    cctrcgy=legacy.cctrcgy,
                    currency=legacy.currency,
                    pctr=legacy.pctr,
                    is_active=True,
                    source_proposal_id=proposal.id,
                    approved_in_wave=wave_id,
                )
                db.add(tcc)
                created_cc += 1

        # Create target profit center if target includes PC
        if target in ("PC", "PC_ONLY", "CC_AND_PC"):
            existing_pc = db.execute(
                select(TargetProfitCenter).where(
                    TargetProfitCenter.coarea == legacy.coarea,
                    TargetProfitCenter.pctr == (legacy.pctr or legacy.cctr),
                )
            ).scalar_one_or_none()

            if not existing_pc:
                tpc = TargetProfitCenter(
                    coarea=legacy.coarea,
                    pctr=legacy.pctr or legacy.cctr,
                    txtsh=legacy.txtsh,
                    txtmi=legacy.txtmi,
                    responsible=owner or legacy.responsible,
                    ccode=legacy.ccode,
                    currency=legacy.currency,
                    is_active=True,
                    source_proposal_id=proposal.id,
                    approved_in_wave=wave_id,
                )
                db.add(tpc)
                created_pc += 1

    # Update wave status
    wave.status = "locked"
    wave.locked_at = datetime.now(UTC)

    db.commit()

    result = {
        "wave_id": wave_id,
        "run_id": run_id,
        "proposals_processed": len(proposals),
        "target_cc_created": created_cc,
        "target_pc_created": created_pc,
    }

    logger.info("proposal.locked", **result)
    return result


# ---------------------------------------------------------------------------
# Owner resolution — format as "GPN Name" from Employee table
# ---------------------------------------------------------------------------


def resolve_owner_display(responsible: str | None, db: Session) -> str:
    """Resolve a cost center owner to 'GPN Name' format.

    Looks up the Employee table by GPN or user_id_pid.  Falls back to
    the raw responsible string if no employee record is found.
    """
    if not responsible:
        return ""

    gpn = responsible.strip()
    emp = db.execute(
        select(Employee).where((Employee.gpn == gpn) | (Employee.user_id_pid == gpn))
    ).scalar_one_or_none()

    if emp:
        return emp.display_name
    return gpn


# ---------------------------------------------------------------------------
# ID recycling — release allocated CC/PC IDs when proposals are deleted
# ---------------------------------------------------------------------------


def release_proposal_ids(proposal_id: int, db: Session) -> int:
    """Release naming allocations for a deleted/reset proposal.

    When a proposal is removed (e.g. before re-running analysis), any
    CC/PC IDs allocated from the NamingPool should be freed so they
    can be reused in subsequent runs.
    """
    allocations = (
        db.execute(
            select(NamingAllocation).where(
                NamingAllocation.proposal_id == proposal_id,
                NamingAllocation.is_released.is_(False),
            )
        )
        .scalars()
        .all()
    )

    released = 0
    for alloc in allocations:
        alloc.is_released = True
        alloc.proposal_id = None
        released += 1

    if released:
        db.flush()
        logger.info(
            "proposal.ids_released",
            proposal_id=proposal_id,
            released=released,
        )

    return released


def allocate_naming_id(wave_id: int, pool_type: str, proposal_id: int, db: Session) -> str | None:
    """Allocate the next available ID from the naming pool.

    Tries released IDs first (recycling), then increments next_value.
    Returns the allocated ID string, or None if no pool is configured.
    """
    pool = db.execute(
        select(NamingPool).where(
            NamingPool.wave_id == wave_id,
            NamingPool.pool_type == pool_type,
        )
    ).scalar_one_or_none()

    if not pool:
        return None

    # Try recycled IDs first
    recycled = (
        db.execute(
            select(NamingAllocation).where(
                NamingAllocation.pool_id == pool.id,
                NamingAllocation.is_released.is_(True),
            )
        )
        .scalars()
        .first()
    )

    if recycled:
        recycled.is_released = False
        recycled.proposal_id = proposal_id
        db.flush()
        return recycled.allocated_value

    # Allocate next sequential ID
    if pool.next_value > pool.range_end:
        logger.warning(
            "naming.pool_exhausted",
            wave_id=wave_id,
            pool_type=pool_type,
        )
        return None

    value = str(pool.next_value)
    alloc = NamingAllocation(
        pool_id=pool.id,
        proposal_id=proposal_id,
        allocated_value=value,
        is_released=False,
    )
    pool.next_value += 1
    db.add(alloc)
    db.flush()
    return value


# ---------------------------------------------------------------------------
# Attribute inheritance — copy owner, responsible, etc. from legacy to target
# ---------------------------------------------------------------------------


def inherit_attributes(legacy: LegacyCostCenter, target_cc: TargetCostCenter, db: Session) -> None:
    """Copy inheritable attributes from legacy CC to target CC.

    Transfers owner, responsible person, and other attributes that
    should carry over to the new center.
    """
    target_cc.responsible = legacy.responsible
    target_cc.txtsh = legacy.txtsh
    target_cc.txtmi = legacy.txtmi
    target_cc.currency = legacy.currency
    target_cc.cctrcgy = legacy.cctrcgy

    # Resolve owner for display
    if legacy.responsible:
        target_cc.responsible = resolve_owner_display(legacy.responsible, db)

    # Copy JSONB attrs if both have them
    if hasattr(legacy, "attrs") and legacy.attrs:
        if not target_cc.attrs:
            target_cc.attrs = {}
        inherited_keys = [
            "verak_user",
            "func_area",
            "bus_area",
            "profit_ctr",
            "company_code",
        ]
        for key in inherited_keys:
            if key in legacy.attrs:
                target_cc.attrs[key] = legacy.attrs[key]
