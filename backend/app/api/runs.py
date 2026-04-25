"""Analysis run API (section 11.6)."""

from __future__ import annotations

from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination, require_role
from app.infra.db.session import get_db
from app.models.core import AnalysisRun, AppUser, CenterProposal

router = APIRouter()


class RunOut(BaseModel):
    id: int
    wave_id: int
    config_id: int
    status: str
    kpis: dict | None = None
    started_at: str | None = None
    finished_at: str | None = None

    model_config = {"from_attributes": True}


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
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": p.id,
                "legacy_cc_id": p.legacy_cc_id,
                "cleansing_outcome": p.cleansing_outcome,
                "target_object": p.target_object,
                "confidence": str(p.confidence) if p.confidence else None,
            }
            for p in proposals
        ],
    }


@router.get("/{run_id}/proposals/{proposal_id}/why")
def why_panel(run_id: int, proposal_id: int, db: Session = Depends(get_db)) -> dict:
    proposal = db.get(CenterProposal, proposal_id)
    if not proposal or proposal.run_id != run_id:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return {
        "proposal_id": proposal.id,
        "cleansing_outcome": proposal.cleansing_outcome,
        "target_object": proposal.target_object,
        "rule_path": proposal.rule_path,
        "ml_scores": proposal.ml_scores,
        "llm_commentary": proposal.llm_commentary,
        "override": {
            "outcome": proposal.override_outcome,
            "target": proposal.override_target,
            "reason": proposal.override_reason,
        }
        if proposal.override_outcome
        else None,
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


@router.get("/{run_a}/diff/{run_b}")
def compare_runs(run_a: int, run_b: int, db: Session = Depends(get_db)) -> dict:
    a = db.get(AnalysisRun, run_a)
    b = db.get(AnalysisRun, run_b)
    if not a or not b:
        raise HTTPException(status_code=404, detail="Run not found")
    return {
        "run_a": {"id": a.id, "status": a.status, "kpis": a.kpis},
        "run_b": {"id": b.id, "status": b.status, "kpis": b.kpis},
        "diff": {},
    }
