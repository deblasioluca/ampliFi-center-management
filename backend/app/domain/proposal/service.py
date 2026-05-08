"""Proposal management service (§06.5, §06.6).

Handles proposal override, locking, and target object draft creation.
When a wave is locked, proposals become immutable and target cost centers /
profit centers are created from the approved proposals.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.domain.decision_tree.engine import CleansingOutcome, TargetObject
from app.models.core import (
    AnalysisRun,
    CenterMapping,
    CenterProposal,
    Employee,
    LegacyCostCenter,
    LegacyProfitCenter,
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


def _proposal_engine(proposal: CenterProposal) -> str:
    """Return the engine version that produced this proposal ("v1" or "v2").

    V2 stores ``engine_version=v2`` and grouping data (pc_id, cc_id, pc_name,
    cc_name, group_key, approach) in ``CenterProposal.attrs``. V1 has no attrs
    grouping data and historically left the field empty.
    """
    attrs = proposal.attrs or {}
    return str(attrs.get("engine_version", "v1")).lower()


def _resolve_target_ids(
    proposal: CenterProposal,
    legacy: LegacyCostCenter,
) -> tuple[str, str, str | None, str | None]:
    """Resolve (cctr, pctr, cc_name, pc_name) for the target objects.

    For V2 proposals, this uses the IDs assigned by ``assign_v2_ids`` which
    implement the canonical SAP m:1 model — a single ``pc_id`` is shared by
    all centers in the same ``group_key``. For V1 proposals (and any proposal
    without V2 attrs), the legacy 1:1 mapping is preserved as a fallback.
    """
    attrs = proposal.attrs or {}
    if _proposal_engine(proposal) == "v2":
        # V2: use the assigned grouping IDs/names. cc_id/pc_id are *required*
        # for V2 — if missing, the V2 ID assignment step did not run.
        cc_id = attrs.get("cc_id") or legacy.cctr
        pc_id = attrs.get("pc_id") or legacy.pctr or legacy.cctr
        cc_name = attrs.get("cc_name") or legacy.txtsh
        pc_name = attrs.get("pc_name") or legacy.txtsh
        return cc_id, pc_id, cc_name, pc_name

    # V1 fallback: legacy 1:1 behaviour
    return legacy.cctr, (legacy.pctr or legacy.cctr), legacy.txtsh, legacy.txtsh


def lock_proposals(wave_id: int, run_id: int, db: Session) -> dict:
    """Lock proposals and create target object drafts (§06.6).

    This is called when a wave transitions from 'proposed' to 'locked'.
    For each KEEP or REDESIGN proposal, a target CC and/or PC is created
    (REDESIGN targets are created when the user overrides target to CC/PC).
    RETIRE and MERGE_MAP proposals are skipped (no target objects).

    For V2 (CEMA migration) proposals, the assigned ``pc_id`` is shared by all
    centers in the same ``group_key`` — the existing-row check on
    ``(coarea, pctr)`` ensures only ONE TargetProfitCenter is created per
    group, implementing the canonical m:1 model.
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
    pc_groups_seen: set[tuple[str, str]] = set()  # (coarea, pc_id) for telemetry

    for proposal in proposals:
        outcome, target = get_effective_outcome(proposal)

        if outcome in ("RETIRE", "MERGE_MAP"):
            continue

        legacy = db.get(LegacyCostCenter, proposal.legacy_cc_id)
        if not legacy:
            continue

        cc_id, pc_id, cc_name, pc_name = _resolve_target_ids(proposal, legacy)

        # Resolve owner as "GPN Name" from Employee table
        owner = resolve_owner_display(legacy.responsible, db)

        # Create target cost center if target includes CC
        if target in ("CC", "CC_AND_PC"):
            existing = db.execute(
                select(TargetCostCenter).where(
                    TargetCostCenter.coarea == legacy.coarea,
                    TargetCostCenter.cctr == cc_id,
                )
            ).scalar_one_or_none()

            if not existing:
                tcc = TargetCostCenter(
                    coarea=legacy.coarea,
                    cctr=cc_id,
                    txtsh=cc_name or legacy.txtsh,
                    txtmi=legacy.txtmi,
                    responsible=owner or legacy.responsible,
                    ccode=legacy.ccode,
                    cctrcgy=legacy.cctrcgy,
                    currency=legacy.currency,
                    pctr=pc_id,
                    is_active=True,
                    source_proposal_id=proposal.id,
                    approved_in_wave=wave_id,
                )
                db.add(tcc)
                created_cc += 1

        # Create target profit center if target includes PC
        # For V2 m:1: same pc_id across multiple proposals → only first one
        # creates the row, the existing-check dedups the rest.
        if target in ("PC", "PC_ONLY", "CC_AND_PC"):
            pc_groups_seen.add((legacy.coarea, pc_id))
            existing_pc = db.execute(
                select(TargetProfitCenter).where(
                    TargetProfitCenter.coarea == legacy.coarea,
                    TargetProfitCenter.pctr == pc_id,
                )
            ).scalar_one_or_none()

            if not existing_pc:
                tpc = TargetProfitCenter(
                    coarea=legacy.coarea,
                    pctr=pc_id,
                    txtsh=pc_name or legacy.txtsh,
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
        "pc_groups": len(pc_groups_seen),
        "engine_version": run.engine_version or "v1",
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
    emp = (
        db.execute(
            select(Employee)
            .where((Employee.gpn == gpn) | (Employee.user_id_pid == gpn))
            .order_by(Employee.id.desc())
        )
        .scalars()
        .first()
    )

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
        select(NamingPool)
        .where(
            NamingPool.wave_id == wave_id,
            NamingPool.pool_type == pool_type,
        )
        .with_for_update()
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


# ---------------------------------------------------------------------------
# Common SAP fields to copy from legacy → target (attribute inheritance)
# ---------------------------------------------------------------------------

# CC fields shared between LegacyCostCenter and TargetCostCenter
_CC_INHERIT_FIELDS = [
    "mandt",
    "coarea",
    "txtsh",
    "txtmi",
    "datbi",
    "datab",
    "bkzkp",
    "pkzkp",
    "ccode",
    "gsber",
    "cctrcgy",
    "responsible",
    "verak_user",
    "currency",
    "kalsm",
    "txjcd",
    "pctr",
    "werks",
    "logsystem",
    "ersda",
    "usnam",
    "bkzks",
    "bkzer",
    "bkzob",
    "pkzks",
    "pkzer",
    "vmeth",
    "mgefl",
    "abtei",
    "nkost",
    "kvewe",
    "kappl",
    "koszschl",
    "land1",
    "anred",
    "name1",
    "name2",
    "name3",
    "name4",
    "ort01",
    "ort02",
    "stras",
    "pfach",
    "pstlz",
    "pstl2",
    "regio",
    "spras",
    "telbx",
    "telf1",
    "telf2",
    "telfx",
    "teltx",
    "telx1",
    "datlt",
    "drnam",
    "khinr",
    "cckey",
    "kompl",
    "stakz",
    "objnr",
    "funkt",
    "afunk",
    "cpi_templ",
    "cpd_templ",
    "func_area",
    "sci_templ",
    "scd_templ",
    "ski_templ",
    "skd_templ",
    # CI_CSKS customer fields
    "zzcuemncfu",
    "zzcueabacc",
    "zzcuegbcd",
    "zzcueubcd",
    "zzcuenkos",
    "zzstrpctyp",
    "zzstrkklas",
    "zzstraagcd",
    "zzstrgfd",
    "zzstrfst",
    "zzstrmacve",
    "zzstrabukr",
    "zzstrugcd",
    "zzstrinadt",
    "zzstrkstyp",
    "zzstrverik",
    "zzstrcurr2",
    "zzstrlccid",
    "zzstrmaloc",
    "zzstrtaxcd",
    "zzstrgrpid",
    "zzstrregcode",
    "zzstrtaxarea",
    "zzstrrepsit",
    "zzstrgsm",
    "zzcemapar",
    "zzledger",
    "zzhdstat",
    "zzhdtype",
    "zzfmd",
    "zzfmdcc",
    "zzfmdnode",
    "zzstate",
    "zztax",
    "zzstrentsa",
    "zzstrentzu",
    "xblnr",
    # JV fields
    "vname",
    "recid",
    "etype",
    "jv_otype",
    "jv_jibcl",
    "jv_jibsa",
    "ferc_ind",
]

# PC fields shared between LegacyProfitCenter and TargetProfitCenter
_PC_INHERIT_FIELDS = [
    "mandt",
    "coarea",
    "txtsh",
    "txtmi",
    "datbi",
    "datab",
    "ersda",
    "usnam",
    "merkmal",
    "department",
    "responsible",
    "verak_user",
    "currency",
    "nprctr",
    "land1",
    "anred",
    "name1",
    "name2",
    "name3",
    "name4",
    "ort01",
    "ort02",
    "stras",
    "pfach",
    "pstlz",
    "pstl2",
    "language",
    "telbx",
    "telf1",
    "telf2",
    "telfx",
    "teltx",
    "telx1",
    "datlt",
    "drnam",
    "khinr",
    "ccode",
    "vname",
    "recid",
    "etype",
    "txjcd",
    "regio",
    "kvewe",
    "kappl",
    "kalsm",
    "logsystem",
    "lock_ind",
    "pca_template",
    "segment",
]


def _copy_cc_fields(legacy: LegacyCostCenter, target: TargetCostCenter) -> None:
    """Copy all SAP fields from a legacy CC to a target CC."""
    for field in _CC_INHERIT_FIELDS:
        val = getattr(legacy, field, None)
        if val is not None and hasattr(target, field):
            setattr(target, field, val)
    if legacy.attrs:
        target.attrs = dict(legacy.attrs)


def _copy_pc_fields(legacy_pc: LegacyProfitCenter, target: TargetProfitCenter) -> None:
    """Copy all SAP fields from a legacy PC to a target PC."""
    for field in _PC_INHERIT_FIELDS:
        val = getattr(legacy_pc, field, None)
        if val is not None and hasattr(target, field):
            setattr(target, field, val)
    if legacy_pc.attrs:
        target.attrs = dict(legacy_pc.attrs)


# ---------------------------------------------------------------------------
# Generate wave targets — called from MDG export or explicitly
# ---------------------------------------------------------------------------


def generate_wave_targets(wave_id: int, db: Session) -> dict:
    """Generate target CC/PC records and mapping for all proposals in a wave.

    Every proposal (KEEP, MERGE_MAP, REDESIGN) gets a new target CC number.
    If the target type includes PC (CC_AND_PC), a new target PC number is also
    generated. All SAP attributes are inherited from the legacy source.

    For MERGE_MAP: multiple source CCs map to one target CC. The target
    inherits attributes from the first source CC in the group.

    For RETIRE: no target CC/PC is created; the legacy center is marked
    for deactivation in the mapping table.

    Returns a summary dict with counts and any owner conflicts detected.
    """
    wave = db.get(Wave, wave_id)
    if not wave:
        raise ValueError(f"Wave {wave_id} not found")

    preferred_run_id = (wave.config or {}).get("preferred_run_id")
    if not preferred_run_id:
        raise ValueError("No preferred run set for this wave")

    # Check if targets already exist for this wave
    existing_cc = (
        db.execute(select(TargetCostCenter).where(TargetCostCenter.approved_in_wave == wave_id))
        .scalars()
        .first()
    )
    if existing_cc:
        cc_count = (
            db.execute(
                select(func.count(TargetCostCenter.id)).where(
                    TargetCostCenter.approved_in_wave == wave_id
                )
            ).scalar()
            or 0
        )
        pc_count = (
            db.execute(
                select(func.count(TargetProfitCenter.id)).where(
                    TargetProfitCenter.approved_in_wave == wave_id
                )
            ).scalar()
            or 0
        )
        mapping_count = (
            db.execute(
                select(func.count(CenterMapping.id)).where(
                    CenterMapping.notes.like(f"wave:{wave_id} %")
                )
            ).scalar()
            or 0
        )
        return {
            "wave_id": wave_id,
            "already_generated": True,
            "target_cc_count": cc_count,
            "target_pc_count": pc_count,
            "mapping_count": mapping_count,
        }

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == preferred_run_id))
        .scalars()
        .all()
    )

    # --- Pre-load ALL legacy CCs and PCs in bulk (avoid N+1 queries) ---
    legacy_cc_ids = {p.legacy_cc_id for p in proposals if p.legacy_cc_id}
    legacy_cc_map: dict[int, LegacyCostCenter] = {}
    if legacy_cc_ids:
        rows = (
            db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(legacy_cc_ids)))
            .scalars()
            .all()
        )
        legacy_cc_map = {cc.id: cc for cc in rows}

    # Pre-load legacy PCs keyed by (coarea, pctr) for fast lookup
    legacy_pc_map: dict[tuple[str, str], LegacyProfitCenter] = {}
    pctr_keys = set()
    for cc in legacy_cc_map.values():
        if cc.pctr and cc.coarea:
            pctr_keys.add((cc.coarea, cc.pctr))
    if pctr_keys:
        all_pcs = db.execute(select(LegacyProfitCenter)).scalars().all()
        for pc in all_pcs:
            if pc.coarea and pc.pctr:
                legacy_pc_map[(pc.coarea, pc.pctr)] = pc

    # Load naming config from wave
    naming_cfg = (wave.config or {}).get("naming", {})
    cc_template = naming_cfg.get("cc_template", "{coarea}{seq:6}")
    pc_template = naming_cfg.get("pc_template", "{coarea}{seq:6}")
    seq_start = int(naming_cfg.get("seq_start", 1))

    cc_seq: dict[str, int] = {}
    pc_seq: dict[str, int] = {}

    def next_cc_id(coarea: str, template: str) -> str:
        if coarea not in cc_seq:
            cc_seq[coarea] = seq_start
        seq = cc_seq[coarea]
        cc_seq[coarea] = seq + 1
        return _format_naming(template, coarea, seq)

    def next_pc_id(coarea: str, template: str) -> str:
        if coarea not in pc_seq:
            pc_seq[coarea] = seq_start
        seq = pc_seq[coarea]
        pc_seq[coarea] = seq + 1
        return _format_naming(template, coarea, seq)

    created_cc = 0
    created_pc = 0
    created_mappings = 0
    owner_conflicts: list[dict] = []

    # Collect all objects to add in bulk
    pending_targets: list[TargetCostCenter | TargetProfitCenter] = []
    pending_mappings: list[CenterMapping] = []
    # Track CC→PC reference updates (applied after all CCs created)
    cc_pc_links: list[tuple[TargetCostCenter, str]] = []

    # For MERGE: group by merge_into_cctr to deduplicate target
    merge_groups: dict[str, list[CenterProposal]] = {}
    for proposal in proposals:
        outcome, _target = get_effective_outcome(proposal)
        if outcome == "MERGE_MAP" and proposal.merge_into_cctr:
            merge_groups.setdefault(str(proposal.merge_into_cctr), []).append(proposal)

    # --- Process MERGE groups first ---
    for _merge_key, group in merge_groups.items():
        primary = group[0]
        primary_legacy = legacy_cc_map.get(primary.legacy_cc_id)
        if not primary_legacy:
            continue

        _, target_type = get_effective_outcome(primary)
        coarea = primary_legacy.coarea
        new_cctr = next_cc_id(coarea, cc_template)

        tcc = TargetCostCenter(
            source_proposal_id=primary.id,
            approved_in_wave=wave_id,
            is_active=True,
        )
        _copy_cc_fields(primary_legacy, tcc)
        tcc.cctr = new_cctr
        pending_targets.append(tcc)
        created_cc += 1

        # Detect owner conflicts
        owners = set()
        for p in group:
            leg = legacy_cc_map.get(p.legacy_cc_id)
            if leg and leg.responsible:
                owners.add(leg.responsible)
        if len(owners) > 1:
            owner_conflicts.append(
                {
                    "target_cctr": new_cctr,
                    "coarea": coarea,
                    "owners": list(owners),
                    "source_count": len(group),
                }
            )

        # Create target PC if needed
        new_pctr = None
        if target_type in ("PC", "PC_ONLY", "CC_AND_PC"):
            new_pctr = next_pc_id(coarea, pc_template)
            pc_key = (primary_legacy.coarea, primary_legacy.pctr) if primary_legacy.pctr else None
            legacy_pc = legacy_pc_map.get(pc_key) if pc_key else None
            tpc = TargetProfitCenter(
                source_proposal_id=primary.id,
                approved_in_wave=wave_id,
                is_active=True,
            )
            if legacy_pc:
                _copy_pc_fields(legacy_pc, tpc)
            else:
                tpc.coarea = coarea
                tpc.txtsh = primary_legacy.txtsh
                tpc.txtmi = primary_legacy.txtmi
                tpc.ccode = primary_legacy.ccode
                tpc.responsible = primary_legacy.responsible
                tpc.currency = primary_legacy.currency
            tpc.pctr = new_pctr
            pending_targets.append(tpc)
            created_pc += 1
            cc_pc_links.append((tcc, new_pctr))

        # Create mappings for ALL source CCs in the group
        for p in group:
            leg = legacy_cc_map.get(p.legacy_cc_id)
            if not leg:
                continue
            pending_mappings.append(
                CenterMapping(
                    object_type="cost_center",
                    legacy_coarea=leg.coarea,
                    legacy_center=leg.cctr,
                    legacy_name=leg.txtsh,
                    target_coarea=coarea,
                    target_center=new_cctr,
                    target_name=primary_legacy.txtsh,
                    mapping_type="merge",
                    notes=f"wave:{wave_id} outcome:MERGE_MAP",
                )
            )
            created_mappings += 1

            if new_pctr and leg.pctr:
                pending_mappings.append(
                    CenterMapping(
                        object_type="profit_center",
                        legacy_coarea=leg.coarea,
                        legacy_center=leg.pctr,
                        legacy_name=leg.txtsh,
                        target_coarea=coarea,
                        target_center=new_pctr,
                        target_name=primary_legacy.txtsh,
                        mapping_type="merge",
                        notes=f"wave:{wave_id} outcome:MERGE_MAP",
                    )
                )
                created_mappings += 1

    # --- Process KEEP and REDESIGN proposals (1:1 mapping) ---
    for proposal in proposals:
        outcome, target_type = get_effective_outcome(proposal)

        if outcome == "MERGE_MAP":
            continue

        legacy = legacy_cc_map.get(proposal.legacy_cc_id)
        if not legacy:
            continue

        coarea = legacy.coarea

        if outcome == "RETIRE":
            pending_mappings.append(
                CenterMapping(
                    object_type="cost_center",
                    legacy_coarea=coarea,
                    legacy_center=legacy.cctr,
                    legacy_name=legacy.txtsh,
                    target_coarea=coarea,
                    target_center="RETIRED",
                    target_name="",
                    mapping_type="retire",
                    notes=f"wave:{wave_id} outcome:RETIRE",
                )
            )
            created_mappings += 1
            continue

        new_cctr = next_cc_id(coarea, cc_template)

        tcc = TargetCostCenter(
            source_proposal_id=proposal.id,
            approved_in_wave=wave_id,
            is_active=True,
        )
        _copy_cc_fields(legacy, tcc)
        tcc.cctr = new_cctr
        pending_targets.append(tcc)
        created_cc += 1

        pending_mappings.append(
            CenterMapping(
                object_type="cost_center",
                legacy_coarea=coarea,
                legacy_center=legacy.cctr,
                legacy_name=legacy.txtsh,
                target_coarea=coarea,
                target_center=new_cctr,
                target_name=legacy.txtsh,
                mapping_type="1:1" if outcome == "KEEP" else "redesign",
                notes=f"wave:{wave_id} outcome:{outcome}",
            )
        )
        created_mappings += 1

        if target_type in ("PC", "PC_ONLY", "CC_AND_PC"):
            new_pctr = next_pc_id(coarea, pc_template)
            legacy_pc = legacy_pc_map.get((legacy.coarea, legacy.pctr)) if legacy.pctr else None

            tpc = TargetProfitCenter(
                source_proposal_id=proposal.id,
                approved_in_wave=wave_id,
                is_active=True,
            )
            if legacy_pc:
                _copy_pc_fields(legacy_pc, tpc)
            else:
                tpc.coarea = coarea
                tpc.txtsh = legacy.txtsh
                tpc.txtmi = legacy.txtmi
                tpc.ccode = legacy.ccode
                tpc.responsible = legacy.responsible
                tpc.currency = legacy.currency
            tpc.pctr = new_pctr
            pending_targets.append(tpc)
            created_pc += 1
            cc_pc_links.append((tcc, new_pctr))

            pending_mappings.append(
                CenterMapping(
                    object_type="profit_center",
                    legacy_coarea=coarea,
                    legacy_center=legacy.pctr or legacy.cctr,
                    legacy_name=legacy.txtsh,
                    target_coarea=coarea,
                    target_center=new_pctr,
                    target_name=legacy.txtsh,
                    mapping_type="1:1" if outcome == "KEEP" else "redesign",
                    notes=f"wave:{wave_id} outcome:{outcome}",
                )
            )
            created_mappings += 1

    # --- Bulk insert: one flush for targets, one for mappings ---
    db.add_all(pending_targets)
    # Apply CC→PC cross-references before flush
    for tcc_obj, pctr_val in cc_pc_links:
        tcc_obj.pctr = pctr_val
    db.flush()
    db.add_all(pending_mappings)
    db.commit()

    result = {
        "wave_id": wave_id,
        "already_generated": False,
        "target_cc_count": created_cc,
        "target_pc_count": created_pc,
        "mapping_count": created_mappings,
        "owner_conflicts": owner_conflicts,
        "sequences": {
            "cc": dict(cc_seq),
            "pc": dict(pc_seq),
        },
    }
    logger.info("targets.generated", **result)
    return result


def _find_legacy_pc(legacy_cc: LegacyCostCenter, db: Session) -> LegacyProfitCenter | None:
    """Find the legacy profit center associated with a cost center."""
    if not legacy_cc.pctr:
        return None
    return (
        db.execute(
            select(LegacyProfitCenter).where(
                LegacyProfitCenter.coarea == legacy_cc.coarea,
                LegacyProfitCenter.pctr == legacy_cc.pctr,
            )
        )
        .scalars()
        .first()
    )


def _format_naming(template: str, coarea: str, seq: int) -> str:
    """Format a naming template with coarea and sequence number."""
    import re

    def replacer(match: re.Match) -> str:
        name = match.group(1)
        width = match.group(2)
        if name == "seq":
            w = int(width) if width else 6
            return str(seq).zfill(w)
        if name == "coarea":
            return coarea
        return match.group(0)

    return re.sub(r"\{(\w+)(?::(\d+))?\}", replacer, template)
