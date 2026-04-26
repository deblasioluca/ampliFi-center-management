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
    LegacyCostCenter,
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
    For each KEEP/MERGE_MAP proposal, a target CC and/or PC is created.
    """
    wave = db.get(Wave, wave_id)
    if not wave:
        raise ValueError(f"Wave {wave_id} not found")

    if wave.status not in ("proposed", "analysing"):
        raise ValueError(f"Cannot lock: wave is {wave.status}, expected proposed or analysing")

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == run_id)).scalars().all()
    )

    created_cc = 0
    created_pc = 0

    for proposal in proposals:
        outcome, target = get_effective_outcome(proposal)

        if outcome in ("RETIRE", "MERGE_MAP", "REDESIGN"):
            continue  # No target object for retired/merged/redesigned centers

        legacy = db.get(LegacyCostCenter, proposal.legacy_cc_id)
        if not legacy:
            continue

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
                    responsible=legacy.responsible,
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
        if target in ("PC_ONLY", "CC_AND_PC"):
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
                    responsible=legacy.responsible,
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
