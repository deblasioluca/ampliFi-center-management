"""Analysis run API (section 11.6)."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination, require_role
from app.infra.db.session import get_db
from app.models.core import AnalysisRun, AppUser, CenterProposal

router = APIRouter()
log = logging.getLogger(__name__)

_HIER_CLS = {"0101": "Cost Center", "0104": "Profit Center", "0106": "Entity"}


def _hier_display_label(h: object) -> str:
    if getattr(h, "label", None):
        return h.label  # type: ignore[return-value]
    base = f"{_HIER_CLS.get(h.setclass, h.setclass)}: {h.setname}"  # type: ignore[attr-defined]
    if h.description:  # type: ignore[attr-defined]
        base += f" — {h.description}"
    return base


class RunOut(BaseModel):
    id: int
    wave_id: int | None = None
    config_id: int
    status: str
    kpis: dict | None = None
    started_at: str | None = None
    finished_at: str | None = None
    # Progress counters — populated as the engine processes each cost
    # center. The frontend polls /api/runs/{id} to drive a progress bar
    # without holding the original POST connection open.
    total_centers: int = 0
    completed_centers: int = 0

    model_config = {"from_attributes": True}


def _run_global_in_thread(run_id: int, config_id: int, user_id: int) -> None:
    """Daemon-thread runner for the V1 global analysis pipeline.

    Creates its own DB session (the request session is closed by the time
    we get here), drives the existing ``execute_analysis_for_run`` engine
    against the row identified by ``run_id``, and updates the row to
    ``completed`` or ``failed``.

    Why threading and not Celery: Celery is configured (see
    ``app.workers.tasks.run_analysis``) but isn't required by any
    deployment yet, and operators that don't run a Celery worker would
    otherwise see runs stuck in 'queued' forever. Threading keeps the
    same single-process deployment story while moving the work off the
    request handler.

    Trade-off: if the API process restarts mid-analysis, the thread dies
    and the run is left in 'running' status. This matches the previous
    behaviour of the synchronous endpoint (a request timeout would
    leave the same orphaned row), but operators who want survivability
    should switch to the Celery dispatch path.
    """
    from app.infra.db.session import SessionLocal
    from app.services.analysis import execute_analysis_for_run

    db = SessionLocal()
    try:
        run = db.get(AnalysisRun, run_id)
        if not run:
            log.warning("global_run_thread.not_found run_id=%s", run_id)
            return
        execute_analysis_for_run(
            run=run,
            wave_id=None,
            config_id=config_id,
            user_id=user_id,
            mode="simulation",
            label=None,
            excluded_scopes=None,
            db=db,
        )
        # execute_analysis_for_run commits internally on the success path.
    except Exception as e:  # noqa: BLE001 — log everything, mark run failed
        log.exception("global_run_thread.failed run_id=%s", run_id)
        try:
            run = db.get(AnalysisRun, run_id)
            if run:
                run.status = "failed"
                run.error = str(e)[:500]
                run.finished_at = datetime.now(UTC)
                db.commit()
        except Exception:
            log.exception("global_run_thread.failure_persist_failed run_id=%s", run_id)
    finally:
        db.close()


@router.post("/global")
def run_global_analysis(
    config_id: int | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Kick off a V1 analysis on ALL cost centers (no wave scope).

    The endpoint creates the AnalysisRun row in 'queued' status,
    dispatches the actual pipeline work to a background thread, and
    returns the run_id immediately. Clients poll ``GET /api/runs/{id}``
    to drive a progress UI; ``total_centers`` and ``completed_centers``
    on the response let them render a progress bar.

    Cancel via ``POST /api/runs/{id}/cancel`` — the engine checks every
    10 centers and exits cleanly if it sees ``status=cancelled``.
    """
    from app.services.analysis import get_or_create_default_config

    if config_id is None:
        config = get_or_create_default_config(db)
        config_id = config.id

    run = AnalysisRun(
        wave_id=None,
        config_id=config_id,
        status="queued",
        started_at=datetime.now(UTC),
        triggered_by=user.id,
        engine_version="v1",
        mode="simulation",
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    thread = threading.Thread(
        target=_run_global_in_thread,
        args=(run.id, config_id, user.id),
        daemon=True,
        name=f"global_run_{run.id}",
    )
    thread.start()

    return {
        "run_id": run.id,
        "status": run.status,
        "started_at": str(run.started_at) if run.started_at else None,
    }


@router.get("/global/list")
def list_global_runs(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    """List analysis runs not tied to any wave (global runs)."""
    query = select(AnalysisRun).where(AnalysisRun.wave_id.is_(None)).order_by(AnalysisRun.id.desc())
    total = (
        db.execute(select(func.count(AnalysisRun.id)).where(AnalysisRun.wave_id.is_(None))).scalar()
        or 0
    )
    items = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": r.id,
                "status": r.status,
                "kpis": r.kpis,
                "started_at": str(r.started_at) if r.started_at else None,
                "finished_at": str(r.finished_at) if r.finished_at else None,
            }
            for r in items
        ],
    }


@router.get("/{run_id}")
def get_run(run_id: int, db: Session = Depends(get_db)) -> RunOut:
    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return RunOut(
        id=run.id,
        wave_id=run.wave_id,
        config_id=run.config_id,
        status=run.status,
        kpis=run.kpis,
        started_at=str(run.started_at) if run.started_at else None,
        finished_at=str(run.finished_at) if run.finished_at else None,
        total_centers=run.total_centers or 0,
        completed_centers=run.completed_centers or 0,
    )


@router.post("/{run_id}/cancel")
def cancel_run(
    run_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status not in ("pending", "running"):
        raise HTTPException(status_code=409, detail="Run cannot be cancelled")
    run.status = "cancelled"
    db.commit()
    return {"status": "cancelled"}


@router.delete("/{run_id}")
def delete_run(
    run_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Delete an analysis run and all its proposals."""
    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status == "running":
        raise HTTPException(status_code=409, detail="Cannot delete a running analysis")
    from sqlalchemy import delete, select, update

    from app.models.core import (
        LLMReviewPass,
        NamingAllocation,
        RoutineOutput,
        Wave,
    )

    # Release naming allocations before deleting proposals
    proposal_ids = (
        db.execute(select(CenterProposal.id).where(CenterProposal.run_id == run_id)).scalars().all()
    )
    if proposal_ids:
        db.execute(
            update(NamingAllocation)
            .where(
                NamingAllocation.proposal_id.in_(proposal_ids),
                NamingAllocation.is_released.is_(False),
            )
            .values(is_released=True, proposal_id=None)
        )

    db.execute(delete(CenterProposal).where(CenterProposal.run_id == run_id))
    db.execute(delete(RoutineOutput).where(RoutineOutput.run_id == run_id))
    db.execute(delete(LLMReviewPass).where(LLMReviewPass.run_id == run_id))

    # Clear stale preferred_run_id from any wave referencing this run
    waves = db.execute(select(Wave).where(Wave.preferred_run_id == run_id)).scalars().all()
    for wave in waves:
        wave.preferred_run_id = None
        if wave.config and wave.config.get("preferred_run_id") == run_id:
            cfg = {**wave.config}
            del cfg["preferred_run_id"]
            wave.config = cfg

    db.delete(run)
    db.commit()
    return {"deleted": True}


@router.get("/{run_id}/proposals")
def list_proposals(
    run_id: int,
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    outcome: str | None = None,
    target: str | None = None,
) -> dict:
    query = select(CenterProposal).where(CenterProposal.run_id == run_id)
    if outcome:
        query = query.where(CenterProposal.cleansing_outcome == outcome)
    if target:
        query = query.where(CenterProposal.target_object == target)
    total_q = select(func.count(CenterProposal.id)).where(CenterProposal.run_id == run_id)
    if outcome:
        total_q = total_q.where(CenterProposal.cleansing_outcome == outcome)
    if target:
        total_q = total_q.where(CenterProposal.target_object == target)
    total = db.execute(total_q).scalar() or 0
    proposals = (
        db.execute(
            query.order_by(CenterProposal.id).offset((pag.page - 1) * pag.size).limit(pag.size)
        )
        .scalars()
        .all()
    )
    from app.models.core import Balance, LegacyCostCenter

    cc_ids = [p.legacy_cc_id for p in proposals]
    cc_map: dict[int, LegacyCostCenter] = {}
    if cc_ids:
        ccs = (
            db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids)))
            .scalars()
            .all()
        )
        cc_map = {c.id: c for c in ccs}

    # Fetch posting trends (last 12 periods) for sparklines
    trend_map: dict[str, list[int]] = {}
    cctrs = [cc_map[pid].cctr for pid in cc_ids if pid in cc_map]
    if cctrs:
        trend_rows = db.execute(
            select(
                Balance.cctr,
                (Balance.fiscal_year * 100 + Balance.period).label("ym"),
                func.coalesce(func.sum(Balance.posting_count), 0).label("cnt"),
            )
            .where(Balance.cctr.in_(cctrs))
            .group_by(Balance.cctr, "ym")
            .order_by(Balance.cctr, "ym")
        ).all()
        for cctr, _ym, cnt in trend_rows:
            trend_map.setdefault(cctr, []).append(int(cnt))
        for cctr in trend_map:
            trend_map[cctr] = trend_map[cctr][-12:]

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": p.id,
                "legacy_cc_id": p.legacy_cc_id,
                "cctr": cc_map[p.legacy_cc_id].cctr if p.legacy_cc_id in cc_map else None,
                "txtsh": cc_map[p.legacy_cc_id].txtsh if p.legacy_cc_id in cc_map else None,
                "ccode": cc_map[p.legacy_cc_id].ccode if p.legacy_cc_id in cc_map else None,
                "coarea": cc_map[p.legacy_cc_id].coarea if p.legacy_cc_id in cc_map else None,
                "responsible": cc_map[p.legacy_cc_id].responsible
                if p.legacy_cc_id in cc_map
                else None,
                "cleansing_outcome": p.cleansing_outcome,
                "target_object": p.target_object,
                "merge_into_cctr": p.merge_into_cctr,
                "confidence": str(p.confidence) if p.confidence else None,
                "override_outcome": p.override_outcome,
                "override_target": p.override_target,
                "override_reason": p.override_reason,
                "rule_path": p.rule_path,
                "llm_commentary": p.llm_commentary,
                "posting_trend": trend_map.get(
                    cc_map[p.legacy_cc_id].cctr if p.legacy_cc_id in cc_map else "", []
                ),
            }
            for p in proposals
        ],
    }


@router.get("/{run_id}/proposals/{proposal_id}/why")
def why_panel(run_id: int, proposal_id: int, db: Session = Depends(get_db)) -> dict:
    """Decision-reasoning data for a single proposal.

    Returns both the raw stored fields (rule_path, ml_scores, llm_commentary)
    AND a business-friendly translation layer that the frontend can render
    without knowing routine codes:

      outcome_friendly: {label, sentence}
      target_friendly:  short sentence
      rule_path_translated: list of {code, verdict, label, verdict_meaning, description}
    """
    from app.services.reasoning_translation import (
        translate_outcome,
        translate_rule_path,
        translate_target,
    )

    proposal = db.get(CenterProposal, proposal_id)
    if not proposal or proposal.run_id != run_id:
        raise HTTPException(status_code=404, detail="Proposal not found")

    outcome = proposal.override_outcome or proposal.cleansing_outcome
    target = proposal.override_target or proposal.target_object

    return {
        "proposal_id": proposal.id,
        "cleansing_outcome": proposal.cleansing_outcome,
        "target_object": proposal.target_object,
        "outcome_friendly": translate_outcome(outcome),
        "target_friendly": translate_target(target),
        "rule_path": proposal.rule_path,
        "rule_path_translated": translate_rule_path(proposal.rule_path),
        "ml_scores": proposal.ml_scores,
        "llm_commentary": proposal.llm_commentary,
        "confidence": float(proposal.confidence) if proposal.confidence is not None else None,
        "override": {
            "outcome": proposal.override_outcome,
            "target": proposal.override_target,
            "reason": proposal.override_reason,
        }
        if proposal.override_outcome
        else None,
    }


@router.get("/{run_id}/proposals/{proposal_id}/ml-opinion")
def proposal_ml_opinion(
    run_id: int,
    proposal_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst", "data_manager", "reviewer")),
) -> dict:
    """Run the ML predictor on this proposal's center and return its opinion.

    Read-only: does not persist anything. Lets reviewers see what the ML
    routine would say even when the active analysis pipeline didn't include
    it. Useful as a 'second opinion' next to the rule tree's verdict.
    """
    from app.domain.decision_tree.registry import boot_registry, get_registry
    from app.models.core import LegacyCostCenter
    from app.services.analysis import _build_context

    proposal = db.get(CenterProposal, proposal_id)
    if not proposal or proposal.run_id != run_id:
        raise HTTPException(status_code=404, detail="Proposal not found")
    cc = db.get(LegacyCostCenter, proposal.legacy_cc_id)
    if cc is None:
        raise HTTPException(status_code=404, detail="Cost center not found")

    registry = get_registry()
    if not registry.codes():
        boot_registry()
    routine = registry.get("ml.outcome_predictor")
    if routine is None:
        raise HTTPException(status_code=503, detail="ML routine not registered")

    ctx = _build_context(cc, db)
    result = routine.run(ctx, {})
    probs = result.payload.get("probs") or {}
    return {
        "proposal_id": proposal.id,
        "verdict": result.verdict,
        "confidence": float(result.score) if result.score is not None else None,
        "probs": probs,
        "anomaly_score": result.payload.get("anomaly"),
        "contributors": result.payload.get("contributors", []),
        "tree_verdict": proposal.cleansing_outcome,
        "agrees_with_tree": result.verdict == proposal.cleansing_outcome,
    }


@router.get("/{run_id}/proposals/{proposal_id}/llm-opinion")
def proposal_llm_opinion(
    run_id: int,
    proposal_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst", "data_manager", "reviewer")),
) -> dict:
    """Ask the LLM advisor for an independent opinion on this proposal.

    Read-only: does not persist anything. Can incur LLM cost — gated by
    role. Returns ``{available: false, reason: ...}`` when no LLM is
    configured so the frontend can show a clear message instead of a
    cryptic error.
    """
    from app.domain.decision_tree.registry import boot_registry, get_registry
    from app.models.core import LegacyCostCenter
    from app.services.analysis import _build_context

    proposal = db.get(CenterProposal, proposal_id)
    if not proposal or proposal.run_id != run_id:
        raise HTTPException(status_code=404, detail="Proposal not found")
    cc = db.get(LegacyCostCenter, proposal.legacy_cc_id)
    if cc is None:
        raise HTTPException(status_code=404, detail="Cost center not found")

    registry = get_registry()
    if not registry.codes():
        boot_registry()
    routine = registry.get("llm.advisor")
    if routine is None:
        raise HTTPException(status_code=503, detail="LLM routine not registered")

    ctx = _build_context(cc, db)
    result = routine.run(ctx, {})
    available = bool(result.payload.get("available"))
    return {
        "proposal_id": proposal.id,
        "available": available,
        "verdict": result.verdict if available else None,
        "confidence": float(result.score) if (available and result.score is not None) else None,
        "reasoning": result.comment,
        "reason": result.reason,
        "tree_verdict": proposal.cleansing_outcome,
        "agrees_with_tree": (available and result.verdict == proposal.cleansing_outcome),
        "model": result.payload.get("model"),
        "tokens_in": result.payload.get("tokens_in", 0),
        "tokens_out": result.payload.get("tokens_out", 0),
    }


@router.get("/{run_id}/data-browser")
def data_browser(
    run_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
) -> dict:
    """Combined data browser: centers + balances + results."""
    from app.models.core import (
        Balance,
        Hierarchy,
        HierarchyLeaf,
        HierarchyNode,
        LegacyCostCenter,
    )

    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == run_id)).scalars().all()
    )

    cc_ids = [p.legacy_cc_id for p in proposals]
    cc_map: dict[int, LegacyCostCenter] = {}
    if cc_ids:
        ccs = (
            db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids)))
            .scalars()
            .all()
        )
        cc_map = {c.id: c for c in ccs}

    # Monthly balances for all relevant cost centers
    cctrs = list({cc_map[pid].cctr for pid in cc_ids if pid in cc_map})
    balance_map: dict[str, list[dict]] = {}
    if cctrs:
        bal_rows = db.execute(
            select(
                Balance.cctr,
                Balance.fiscal_year,
                Balance.period,
                func.coalesce(func.sum(Balance.tc_amt), 0).label("total_amt"),
                func.coalesce(func.sum(Balance.posting_count), 0).label("total_postings"),
            )
            .where(Balance.cctr.in_(cctrs))
            .group_by(Balance.cctr, Balance.fiscal_year, Balance.period)
            .order_by(Balance.cctr, Balance.fiscal_year, Balance.period)
        ).all()
        for cctr, fy, per, amt, postings in bal_rows:
            balance_map.setdefault(cctr, []).append(
                {
                    "fiscal_year": fy,
                    "period": per,
                    "amount": float(amt),
                    "postings": int(postings),
                }
            )

    # CC→PC mapping (current 1:1 from legacy CC pctr field)
    # and proposed n:1 from merge_into_cctr
    pc_target_groups: dict[str, list[str]] = {}
    for p in proposals:
        target = p.merge_into_cctr
        if target and p.legacy_cc_id in cc_map:
            pc_target_groups.setdefault(target, []).append(cc_map[p.legacy_cc_id].cctr)

    # Hierarchy tree for hierarchical view
    hierarchies = db.execute(select(Hierarchy).where(Hierarchy.is_active.is_(True))).scalars().all()
    hier_trees = []
    for h in hierarchies:
        nodes = (
            db.execute(select(HierarchyNode).where(HierarchyNode.hierarchy_id == h.id))
            .scalars()
            .all()
        )
        leaves = (
            db.execute(select(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == h.id))
            .scalars()
            .all()
        )
        hier_trees.append(
            {
                "id": h.id,
                "setname": h.setname,
                "setclass": h.setclass,
                "label": _hier_display_label(h),
                "description": h.description,
                "coarea": h.coarea,
                "nodes": [
                    {"parent": n.parent_setname, "child": n.child_setname, "seq": n.seq}
                    for n in nodes
                ],
                "leaves": [
                    {"setname": lf.setname, "value": lf.value, "seq": lf.seq} for lf in leaves
                ],
            }
        )

    items = []
    for p in proposals:
        cc = cc_map.get(p.legacy_cc_id)
        cctr = cc.cctr if cc else None
        items.append(
            {
                "id": p.id,
                "legacy_cc_id": p.legacy_cc_id,
                "cctr": cctr,
                "txtsh": cc.txtsh if cc else None,
                "txtmi": cc.txtmi if cc else None,
                "ccode": cc.ccode if cc else None,
                "coarea": cc.coarea if cc else None,
                "responsible": cc.responsible if cc else None,
                "pctr": cc.pctr if cc else None,
                "is_active": cc.is_active if cc else None,
                "cleansing_outcome": p.cleansing_outcome,
                "target_object": p.target_object,
                "merge_into_cctr": p.merge_into_cctr,
                "confidence": str(p.confidence) if p.confidence else None,
                "override_outcome": p.override_outcome,
                "override_target": p.override_target,
                "rule_path": p.rule_path,
                "llm_commentary": p.llm_commentary,
                "monthly_balances": balance_map.get(cctr, []) if cctr else [],
            }
        )

    return {
        "run_id": run_id,
        "total": len(items),
        "items": items,
        "pc_target_groups": pc_target_groups,
        "hierarchies": hier_trees,
    }


@router.post("/{run_id}/proposals/{proposal_id}/override")
def override_proposal(
    run_id: int,
    proposal_id: int,
    outcome: str = Query(...),
    reason: str = Query(...),
    target: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    proposal = db.get(CenterProposal, proposal_id)
    if not proposal or proposal.run_id != run_id:
        raise HTTPException(status_code=404, detail="Proposal not found")
    from datetime import datetime

    proposal.override_outcome = outcome
    proposal.override_target = target
    proposal.override_reason = reason
    proposal.override_by = user.id
    proposal.override_at = datetime.now(UTC)
    db.commit()
    return {"status": "overridden", "outcome": outcome}


class LLMReviewRequest(BaseModel):
    mode: str = "SINGLE"  # SINGLE | SEQUENTIAL | DEBATE
    max_centers: int = 100
    outcomes: list[str] | None = None  # Filter to specific outcomes
    min_balance: float | None = None


@router.post("/{run_id}/llm-review")
def trigger_llm_review(
    run_id: int,
    body: LLMReviewRequest,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Trigger LLM review pass on proposals in a completed run."""
    from app.models.core import AppConfig, LegacyCostCenter

    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status != "completed":
        raise HTTPException(status_code=409, detail="Run must be completed")

    # Get LLM config
    cfg = db.execute(select(AppConfig).where(AppConfig.key == "llm")).scalar_one_or_none()
    if not cfg or not cfg.value:
        raise HTTPException(status_code=400, detail="LLM not configured")

    llm_config = cfg.value

    # Build provider
    from app.infra.llm.provider import AzureOpenAIProvider, SapBtpProvider

    provider_type = llm_config.get("provider", "azure")
    if provider_type == "azure":
        provider = AzureOpenAIProvider(llm_config)
    elif provider_type == "btp":
        provider = SapBtpProvider(llm_config)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown LLM provider: {provider_type}")

    model = llm_config.get("model", "gpt-4o")

    # Query proposals to review
    query = select(CenterProposal).where(CenterProposal.run_id == run_id)
    if body.outcomes:
        query = query.where(CenterProposal.cleansing_outcome.in_(body.outcomes))
    proposals = db.execute(query.limit(body.max_centers)).scalars().all()

    from app.infra.llm.review_pass import build_center_context, run_review_pass

    reviewed = 0
    total_cost = 0.0
    for proposal in proposals:
        cc = db.get(LegacyCostCenter, proposal.legacy_cc_id)
        if not cc:
            continue

        center = {
            "coarea": cc.coarea or "",
            "cctr": cc.cctr or "",
            "txtsh": cc.txtsh or "",
            "txtmi": cc.txtmi or "",
            "responsible": cc.responsible or "",
            "ccode": cc.ccode or "",
            "currency": cc.currency or "",
        }
        features = {
            "months_since_last_posting": 0,
            "posting_count_window": 0,
            "bs_amt": 0,
            "rev_amt": 0,
            "opex_amt": 0,
            "hierarchy_membership_count": 0,
        }
        outcome = {
            "cleansing": proposal.cleansing_outcome,
            "target_object": proposal.target_object,
            "rule_path": (proposal.rule_path or {}).get("steps", []),
        }
        ml = proposal.ml_scores or {}

        ctx = build_center_context(center, features, outcome, ml)

        try:
            result = run_review_pass(provider, model, body.mode, ctx)
            proposal.llm_commentary = result
            total_cost += result.get("_llm_meta", {}).get("cost_usd", 0.0)
            reviewed += 1
        except Exception as e:
            proposal.llm_commentary = {"error": str(e)}

    db.commit()
    return {
        "reviewed": reviewed,
        "total_proposals": len(proposals),
        "mode": body.mode,
        "total_cost_usd": round(total_cost, 4),
    }


@router.get("/{run_a}/diff/{run_b}")
def compare_runs(run_a: int, run_b: int, db: Session = Depends(get_db)) -> dict:
    a = db.get(AnalysisRun, run_a)
    b = db.get(AnalysisRun, run_b)
    if not a or not b:
        raise HTTPException(status_code=404, detail="Run not found")

    # Build outcome matrices
    props_a = db.execute(
        select(
            CenterProposal.legacy_cc_id,
            CenterProposal.cleansing_outcome,
            CenterProposal.target_object,
        ).where(CenterProposal.run_id == run_a)
    ).all()
    props_b = db.execute(
        select(
            CenterProposal.legacy_cc_id,
            CenterProposal.cleansing_outcome,
            CenterProposal.target_object,
        ).where(CenterProposal.run_id == run_b)
    ).all()

    map_a = {r[0]: (r[1], r[2]) for r in props_a}
    map_b = {r[0]: (r[1], r[2]) for r in props_b}

    # Outcome transition matrix
    outcome_matrix: dict[str, dict[str, int]] = {}
    target_matrix: dict[str, dict[str, int]] = {}
    changed_ids: list[int] = []

    all_ids = set(map_a.keys()) | set(map_b.keys())
    for cc_id in all_ids:
        oa = map_a.get(cc_id, ("N/A", "N/A"))
        ob = map_b.get(cc_id, ("N/A", "N/A"))
        # Outcome matrix
        outcome_matrix.setdefault(oa[0], {}).setdefault(ob[0], 0)
        outcome_matrix[oa[0]][ob[0]] += 1
        # Target matrix
        ta = oa[1] or "NONE"
        tb = ob[1] or "NONE"
        target_matrix.setdefault(ta, {}).setdefault(tb, 0)
        target_matrix[ta][tb] += 1
        if oa != ob:
            changed_ids.append(cc_id)

    return {
        "run_a": {"id": a.id, "status": a.status, "kpis": a.kpis},
        "run_b": {"id": b.id, "status": b.status, "kpis": b.kpis},
        "diff": {
            "outcome_matrix": outcome_matrix,
            "target_matrix": target_matrix,
            "changed_count": len(changed_ids),
            "total_a": len(map_a),
            "total_b": len(map_b),
        },
    }


@router.post("/{run_id}/batch-features")
def batch_compute_features(
    run_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Batch-compute features for all centers in a run using server-side aggregation.

    Instead of N+1 queries per center, this executes one SQL query for all centers
    and returns pre-computed feature vectors suitable for ML model training.
    """
    from app.models.core import Balance, LegacyCostCenter

    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == run_id)).scalars().all()
    )

    cc_ids = [p.legacy_cc_id for p in proposals]
    if not cc_ids:
        return {"features": [], "count": 0}

    # Batch query: all centers
    centers = (
        db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids))).scalars().all()
    )
    cc_map = {c.id: c for c in centers}

    # Batch aggregation: balance sums per center
    from sqlalchemy import case, literal_column

    balance_agg = db.execute(
        select(
            Balance.cctr,
            func.sum(
                case((Balance.account_class == "BS", Balance.tc_amt), else_=literal_column("0"))
            ).label("bs_amt"),
            func.sum(
                case((Balance.account_class == "OPEX", Balance.tc_amt), else_=literal_column("0"))
            ).label("opex_amt"),
            func.sum(
                case((Balance.account_class == "REV", Balance.tc_amt), else_=literal_column("0"))
            ).label("rev_amt"),
            func.count(Balance.id).label("posting_count"),
        )
        .where(Balance.cctr.in_([c.cctr for c in centers if c]))
        .group_by(Balance.cctr)
    ).all()
    bal_map = {row.cctr: row for row in balance_agg}

    features_list = []
    for p in proposals:
        cc = cc_map.get(p.legacy_cc_id)
        if not cc:
            continue
        bal = bal_map.get(cc.cctr)
        name = cc.txtsh or ""
        features_list.append(
            {
                "cc_id": cc.id,
                "cctr": cc.cctr,
                "ccode": cc.ccode,
                "verdict": p.cleansing_outcome,
                "features": {
                    "is_active": 1.0 if cc.is_active else 0.0,
                    "months_since_last_posting": 0.0,
                    "posting_count_window": float(bal.posting_count if bal else 0),
                    "bs_amt": float(bal.bs_amt if bal else 0),
                    "opex_amt": float(bal.opex_amt if bal else 0),
                    "rev_amt": float(bal.rev_amt if bal else 0),
                    "hierarchy_depth": 0.0,
                    "name_length": float(len(name)),
                    "has_responsible": 1.0 if cc.responsible else 0.0,
                },
            }
        )

    return {"count": len(features_list), "features": features_list}


# ── Decision visibility (which rule decided?) + Hierarchical view ────────


def _extract_deciding_rule(
    rule_path: dict | list | None,
    cleansing_outcome: str,
) -> dict[str, str | None]:
    """Extract the single rule that drove the final outcome.

    The decision tree fires multiple rules per center; ``rule_path`` records
    them in order. The "deciding" rule is the one whose verdict matched the
    final cleansing_outcome — typically the one that short-circuited the
    pipeline. Returns a dict with the rule code, its verdict, and a
    business-friendly label so the frontend can render a single line per
    proposal: "Inaktiv seit 18 Monaten — RETIRE".
    """
    if not rule_path:
        return {
            "code": None,
            "verdict": None,
            "label": "Aggregate (kein einzelner Auslöser)",
        }

    # Normalize: rule_path is sometimes {"steps": [...]} (V1) or a list (V2)
    steps_raw: list = []
    if isinstance(rule_path, dict):
        steps_raw = list(rule_path.get("steps") or [])
    elif isinstance(rule_path, list):
        steps_raw = rule_path
    else:
        return {"code": None, "verdict": None, "label": str(rule_path)[:80]}

    if not steps_raw:
        return {"code": None, "verdict": None, "label": "Aggregate"}

    # Normalize each step to (code, verdict). Steps come in various shapes:
    # - "rule.posting_activity:RETIRE" (V2 string format)
    # - {"code": "...", "verdict": "..."} (dict format)
    # - "rule.ownership" (just code, no verdict)
    parsed: list[tuple[str, str | None]] = []
    for step in steps_raw:
        if isinstance(step, str):
            if ":" in step:
                code, verdict = step.split(":", 1)
                parsed.append((code, verdict))
            else:
                parsed.append((step, None))
        elif isinstance(step, dict):
            parsed.append((str(step.get("code", "")), step.get("verdict")))

    # Heuristic: the deciding rule is the LAST step whose verdict matches
    # the cleansing_outcome. If none match, return the last step (it's
    # typically the aggregate that produced the final answer).
    deciding: tuple[str, str | None] | None = None
    for code, verdict in reversed(parsed):
        if verdict and verdict.upper() == cleansing_outcome.upper():
            deciding = (code, verdict)
            break
    if deciding is None:
        deciding = parsed[-1]

    code, verdict = deciding
    # Lookup business label from the catalog
    from app.domain.decision_tree.rule_catalog import get_rule_metadata

    meta = get_rule_metadata(code) or {}
    business_label = meta.get("business_label") or code
    verdict_meaning = (meta.get("verdict_meanings", {}) or {}).get(verdict or "", "")
    label = (
        f"{business_label} → {verdict_meaning}"
        if verdict_meaning
        else f"{business_label} → {verdict or cleansing_outcome}"
    )

    return {"code": code, "verdict": verdict, "label": label}


@router.get("/{run_id}/decisions")
def run_decisions_table(
    run_id: int,
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    outcome: str | None = None,
) -> dict:
    """Streamlined per-proposal decision table for the simulation results page.

    For each proposal returns a single row:
      - legacy CC identity (cctr, txtsh, ccode, coarea, responsible)
      - The deciding rule (code + verdict + business-friendly label)
      - Final outcome (cleansing_outcome + target_object)
      - Target identity if available (target_cctr, target_pctr) — produced by
        V2 group assignment or V1 1:1 mapping
      - Confidence

    This is the primary data source for the flat-table view of simulation
    results. Reviewers see "what decided?" at a glance.
    """
    from app.models.core import LegacyCostCenter

    base_q = select(CenterProposal).where(CenterProposal.run_id == run_id)
    if outcome:
        base_q = base_q.where(CenterProposal.cleansing_outcome == outcome)
    total_q = select(func.count(CenterProposal.id)).where(CenterProposal.run_id == run_id)
    if outcome:
        total_q = total_q.where(CenterProposal.cleansing_outcome == outcome)
    total = db.execute(total_q).scalar() or 0

    proposals = (
        db.execute(
            base_q.order_by(CenterProposal.id).offset((pag.page - 1) * pag.size).limit(pag.size)
        )
        .scalars()
        .all()
    )

    cc_ids = [p.legacy_cc_id for p in proposals]
    cc_map: dict[int, LegacyCostCenter] = {}
    if cc_ids:
        ccs = (
            db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids)))
            .scalars()
            .all()
        )
        cc_map = {c.id: c for c in ccs}

    items = []
    for p in proposals:
        cc = cc_map.get(p.legacy_cc_id)
        attrs = p.attrs or {}
        deciding = _extract_deciding_rule(p.rule_path, p.cleansing_outcome)
        # V2 attrs carry the assigned target IDs; V1 falls back to legacy IDs
        target_cctr = attrs.get("cc_id") or (cc.cctr if cc else None)
        target_pctr = attrs.get("pc_id") or (cc.pctr if cc else None)
        items.append(
            {
                "proposal_id": p.id,
                "legacy_cctr": cc.cctr if cc else None,
                "txtsh": cc.txtsh if cc else None,
                "ccode": cc.ccode if cc else None,
                "coarea": cc.coarea if cc else None,
                "responsible": cc.responsible if cc else None,
                "decision": {
                    "outcome": p.override_outcome or p.cleansing_outcome,
                    "target_object": p.override_target or p.target_object,
                    "is_overridden": bool(p.override_outcome),
                    "confidence": float(p.confidence) if p.confidence else None,
                },
                "deciding_rule": deciding,
                "target": {
                    "cctr": target_cctr,
                    "pctr": target_pctr,
                    "pc_name": attrs.get("pc_name"),
                    "approach": attrs.get("approach"),
                    "engine": "v2" if attrs.get("engine_version") == "v2" else "v1",
                },
            }
        )

    # Aggregate counters per outcome for the page header
    summary_rows = db.execute(
        select(CenterProposal.cleansing_outcome, func.count(CenterProposal.id))
        .where(CenterProposal.run_id == run_id)
        .group_by(CenterProposal.cleansing_outcome)
    ).all()
    summary = {row[0]: int(row[1]) for row in summary_rows}

    return {
        "run_id": run_id,
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "summary_by_outcome": summary,
        "items": items,
    }


@router.get("/{run_id}/decisions/by-hierarchy")
def run_decisions_by_hierarchy(
    run_id: int,
    db: Session = Depends(get_db),
    hierarchy_code: str | None = None,
    max_depth: int = 4,
) -> dict:
    """Hierarchical (tree) view of proposals.

    Groups proposals by their position in the configured hierarchy, so
    reviewers can drill down: ROOT → EUROPE → DACH → CH00 → individual CCs
    with their decisions.

    Each tree node carries:
      - label, code, depth
      - aggregate counts (children, total proposals, proposals_by_outcome)
      - direct child node IDs
      - direct proposals (only at leaf level — non-leaf nodes report counts only)

    Datasphere note: queries Hierarchy + HierarchyNode + HierarchyLeaf via
    SQLAlchemy. These are candidates for Datasphere migration but the
    queries here use only standard SQL.
    """
    from app.models.core import (
        Hierarchy,
        HierarchyLeaf,
        HierarchyNode,
        LegacyCostCenter,
    )

    # Resolve the hierarchy to traverse. If not specified, use the first
    # active "cema" or "standard" hierarchy.
    h_query = select(Hierarchy).where(Hierarchy.is_active.is_(True))
    if hierarchy_code:
        h_query = h_query.where(Hierarchy.code == hierarchy_code)
    hierarchy = db.execute(h_query.limit(1)).scalar_one_or_none()
    if not hierarchy:
        return {
            "run_id": run_id,
            "hierarchy": None,
            "message": "No active hierarchy found — falling back to flat view",
            "tree": [],
        }

    nodes = (
        db.execute(select(HierarchyNode).where(HierarchyNode.hierarchy_id == hierarchy.id))
        .scalars()
        .all()
    )
    children_of: dict[int | None, list[HierarchyNode]] = {}
    for n in nodes:
        children_of.setdefault(n.parent_id, []).append(n)

    # Leaves map a node to a set of legacy CC IDs
    leaves = (
        db.execute(
            select(HierarchyLeaf).where(
                HierarchyLeaf.hierarchy_id == hierarchy.id,
            )
        )
        .scalars()
        .all()
    )
    cctr_to_node_id: dict[str, int] = {ll.cctr: ll.node_id for ll in leaves if ll.cctr}

    # Load all proposals + their legacy CCs in two queries
    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == run_id)).scalars().all()
    )
    cc_ids = [p.legacy_cc_id for p in proposals]
    cc_map: dict[int, LegacyCostCenter] = {}
    if cc_ids:
        cc_map = {
            c.id: c
            for c in db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids)))
            .scalars()
            .all()
        }

    # Bucket proposals into their leaf node by legacy.cctr → node_id
    proposals_at_node: dict[int, list[CenterProposal]] = {}
    unassigned: list[CenterProposal] = []
    for p in proposals:
        cc = cc_map.get(p.legacy_cc_id)
        if not cc:
            unassigned.append(p)
            continue
        node_id = cctr_to_node_id.get(cc.cctr)
        if node_id is None:
            unassigned.append(p)
            continue
        proposals_at_node.setdefault(node_id, []).append(p)

    # Aggregate counts per node up the tree
    def _outcome_counts(props: list[CenterProposal]) -> dict[str, int]:
        c: dict[str, int] = {}
        for p in props:
            o = p.override_outcome or p.cleansing_outcome
            c[o] = c.get(o, 0) + 1
        return c

    # Build tree recursively
    def _serialize_node(node: HierarchyNode, depth: int = 0) -> dict[str, Any]:
        direct_props = proposals_at_node.get(node.id, [])
        child_nodes = children_of.get(node.id, [])
        # Recurse into children up to max_depth — beyond that, return only
        # aggregate counts (the UI loads deeper levels lazily on click)
        children_serialized: list[dict] = []
        descendant_props: list[CenterProposal] = list(direct_props)
        for child in child_nodes:
            child_data = _serialize_node(child, depth + 1)
            descendant_props.extend(child_data.get("_descendant_props", []))
            children_serialized.append(child_data)

        result = {
            "node_id": node.id,
            "code": node.node_code,
            "label": node.node_name or node.node_code,
            "depth": depth,
            "level": node.level,
            "child_count": len(child_nodes),
            "proposal_count_direct": len(direct_props),
            "proposal_count_total": len(descendant_props),
            "outcome_counts_total": _outcome_counts(descendant_props),
            "children": children_serialized if depth < max_depth else [],
            "has_more_children": depth >= max_depth and len(child_nodes) > 0,
            # Direct proposals at this node only (leaf level usually)
            "proposals": [
                {
                    "proposal_id": p.id,
                    "legacy_cctr": cc_map[p.legacy_cc_id].cctr
                    if p.legacy_cc_id in cc_map
                    else None,
                    "txtsh": cc_map[p.legacy_cc_id].txtsh if p.legacy_cc_id in cc_map else None,
                    "outcome": p.override_outcome or p.cleansing_outcome,
                    "target_object": p.override_target or p.target_object,
                    "deciding_rule": _extract_deciding_rule(p.rule_path, p.cleansing_outcome),
                }
                for p in direct_props
            ],
            # Internal: used during recursion to avoid double-walking
            "_descendant_props": descendant_props,
        }
        return result

    # Roots = nodes with parent_id == None
    roots = children_of.get(None, [])
    tree = [_serialize_node(r) for r in roots]

    # Strip the internal _descendant_props field from the serialized output
    def _clean(node: dict) -> dict:
        node.pop("_descendant_props", None)
        node["children"] = [_clean(c) for c in node["children"]]
        return node

    tree = [_clean(n) for n in tree]

    return {
        "run_id": run_id,
        "hierarchy": {
            "id": hierarchy.id,
            "code": hierarchy.code,
            "label": hierarchy.label or hierarchy.code,
        },
        "max_depth_loaded": max_depth,
        "tree": tree,
        "unassigned": [
            {
                "proposal_id": p.id,
                "legacy_cctr": cc_map.get(p.legacy_cc_id, type("x", (), {"cctr": None})()).cctr,
                "outcome": p.override_outcome or p.cleansing_outcome,
            }
            for p in unassigned[:100]  # cap so the response stays bounded
        ],
        "unassigned_total": len(unassigned),
    }


# ── Multi-engine comparison (rule tree vs ML vs LLM) ─────────────────────


@router.get("/compare/wave/{wave_id}")
def compare_engines(
    wave_id: int,
    engines: str = "tree,ml,llm",
    sample_size: int | None = 100,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
) -> dict:
    """Run all requested engines (rule tree, ML, LLM) on a wave's centers.

    Read-only diagnostic — does NOT persist proposals. Useful to see where
    the deterministic rule tree, the ML predictor and the LLM advisor agree
    or disagree before committing to a production engine for the wave.

    Query params:
    - engines: comma-separated subset of {tree,ml,llm}
    - sample_size: cap the population to keep the comparison fast.
                   Pass 0 (or omit) to use the default 100; pass -1 to run
                   against the whole wave.
    """
    from app.services.engine_comparison import compare_engines_on_wave

    engine_list = [e.strip() for e in engines.split(",") if e.strip()]
    if not engine_list:
        engine_list = ["tree", "ml", "llm"]
    invalid = [e for e in engine_list if e not in {"tree", "ml", "llm"}]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown engine(s): {invalid}. Pick from tree, ml, llm.",
        )

    effective_sample = None if sample_size == -1 else (sample_size or 100)
    if effective_sample is not None and effective_sample > 1000:
        # Hard cap: comparison is meant for diagnostics, not full reanalysis.
        effective_sample = 1000

    try:
        return compare_engines_on_wave(
            wave_id=wave_id,
            db=db,
            engines=engine_list,
            sample_size=effective_sample,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
