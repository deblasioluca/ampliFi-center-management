"""Wave API endpoints (section 11.7)."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, get_current_user, pagination, require_role
from app.infra.db.session import get_db
from app.models.core import (
    AnalysisRun,
    AppUser,
    CenterProposal,
    Entity,
    LegacyCostCenter,
    ReviewItem,
    ReviewScope,
    Wave,
    WaveEntity,
)

router = APIRouter()

VALID_TRANSITIONS = {
    "draft": ["analysing", "cancelled"],
    "analysing": ["proposed", "draft", "cancelled"],
    "proposed": ["locked", "draft", "cancelled"],
    "locked": ["in_review", "proposed"],
    "in_review": ["signed_off"],
    "signed_off": ["closed"],
    "closed": [],
    "cancelled": [],
}


class WaveCreate(BaseModel):
    code: str
    name: str
    description: str | None = None
    is_full_scope: bool = False
    exclude_prior: bool = True
    entity_ccodes: list[str] = []


class WaveUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


class WaveOut(BaseModel):
    id: int
    code: str
    name: str
    description: str | None
    status: str
    is_full_scope: bool
    exclude_prior: bool
    entity_count: int = 0
    config: dict | None = None

    model_config = {"from_attributes": True}


@router.get("")
def list_waves(
    db: Session = Depends(get_db),
    user: AppUser = Depends(get_current_user),
    pag: PaginationParams = Depends(pagination),
    status: str | None = None,
) -> dict:
    query = select(Wave).order_by(Wave.created_at.desc())
    if status:
        query = query.where(Wave.status == status)
    total_q = select(func.count(Wave.id))
    if status:
        total_q = total_q.where(Wave.status == status)
    total = db.execute(total_q).scalar() or 0
    waves = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    items = []
    for w in waves:
        ec = (
            db.execute(select(func.count(WaveEntity.id)).where(WaveEntity.wave_id == w.id)).scalar()
            or 0
        )
        out = WaveOut(
            id=w.id,
            code=w.code,
            name=w.name,
            description=w.description,
            status=w.status,
            is_full_scope=w.is_full_scope,
            exclude_prior=w.exclude_prior,
            entity_count=ec,
            config=w.config,
        )
        items.append(out.model_dump())
    return {"total": total, "page": pag.page, "size": pag.size, "items": items}


@router.post("")
def create_wave(
    body: WaveCreate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> WaveOut:
    existing = db.execute(select(Wave).where(Wave.code == body.code)).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Wave code already exists")
    wave = Wave(
        code=body.code,
        name=body.name,
        description=body.description,
        is_full_scope=body.is_full_scope,
        exclude_prior=body.exclude_prior,
        created_by=user.id,
    )
    db.add(wave)
    db.flush()
    for ccode in body.entity_ccodes:
        entity = db.execute(select(Entity).where(Entity.ccode == ccode)).scalar_one_or_none()
        if entity:
            db.add(WaveEntity(wave_id=wave.id, entity_id=entity.id))
    db.commit()
    db.refresh(wave)
    ec = (
        db.execute(select(func.count(WaveEntity.id)).where(WaveEntity.wave_id == wave.id)).scalar()
        or 0
    )
    return WaveOut(
        id=wave.id,
        code=wave.code,
        name=wave.name,
        description=wave.description,
        status=wave.status,
        is_full_scope=wave.is_full_scope,
        exclude_prior=wave.exclude_prior,
        entity_count=ec,
        config=wave.config,
    )


@router.get("/{wave_id}")
def get_wave(wave_id: int, db: Session = Depends(get_db)) -> WaveOut:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    ec = (
        db.execute(select(func.count(WaveEntity.id)).where(WaveEntity.wave_id == wave.id)).scalar()
        or 0
    )
    return WaveOut(
        id=wave.id,
        code=wave.code,
        name=wave.name,
        description=wave.description,
        status=wave.status,
        is_full_scope=wave.is_full_scope,
        exclude_prior=wave.exclude_prior,
        entity_count=ec,
        config=wave.config,
    )


@router.patch("/{wave_id}")
def update_wave(
    wave_id: int,
    body: WaveUpdate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> WaveOut:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if body.name is not None:
        wave.name = body.name
    if body.description is not None:
        wave.description = body.description
    db.commit()
    db.refresh(wave)
    ec = (
        db.execute(select(func.count(WaveEntity.id)).where(WaveEntity.wave_id == wave.id)).scalar()
        or 0
    )
    return WaveOut(
        id=wave.id,
        code=wave.code,
        name=wave.name,
        description=wave.description,
        status=wave.status,
        is_full_scope=wave.is_full_scope,
        exclude_prior=wave.exclude_prior,
        entity_count=ec,
        config=wave.config,
    )


@router.post("/{wave_id}/cancel")
def cancel_wave(
    wave_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if "cancelled" not in VALID_TRANSITIONS.get(wave.status, []):
        raise HTTPException(status_code=409, detail=f"Cannot cancel wave in status {wave.status}")
    wave.status = "cancelled"
    db.commit()
    return {"status": "cancelled"}


@router.post("/{wave_id}/proposal/lock")
def lock_proposal(
    wave_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status != "proposed":
        raise HTTPException(status_code=409, detail="Wave must be in proposed status to lock")
    wave.status = "locked"

    wave.locked_at = datetime.now(UTC)
    db.commit()
    return {"status": "locked"}


@router.post("/{wave_id}/proposal/unlock")
def unlock_proposal(
    wave_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status != "locked":
        raise HTTPException(status_code=409, detail="Wave must be locked to unlock")
    scope_count = (
        db.execute(
            select(func.count(ReviewScope.id)).where(
                ReviewScope.wave_id == wave.id,
                ReviewScope.status == "invited",
            )
        ).scalar()
        or 0
    )
    if scope_count > 0:
        raise HTTPException(status_code=409, detail="Cannot unlock: review scopes already invited")
    wave.status = "proposed"
    wave.locked_at = None
    db.commit()
    return {"status": "proposed"}


@router.post("/{wave_id}/signoff")
def signoff_wave(
    wave_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status != "in_review":
        raise HTTPException(status_code=409, detail="Wave must be in_review to sign off")

    wave.status = "signed_off"
    wave.signed_off_at = datetime.now(UTC)
    db.commit()
    return {"status": "signed_off"}


@router.post("/{wave_id}/close")
def close_wave(
    wave_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status != "signed_off":
        raise HTTPException(status_code=409, detail="Wave must be signed_off to close")

    wave.status = "closed"
    wave.closed_at = datetime.now(UTC)
    db.commit()
    return {"status": "closed"}


@router.get("/{wave_id}/entities")
def list_wave_entities(
    wave_id: int,
    db: Session = Depends(get_db),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    we_rows = db.execute(select(WaveEntity).where(WaveEntity.wave_id == wave_id)).scalars().all()
    items = []
    for we in we_rows:
        entity = db.get(Entity, we.entity_id)
        if entity:
            cc_count = (
                db.execute(
                    select(func.count(LegacyCostCenter.id)).where(
                        LegacyCostCenter.ccode == entity.ccode
                    )
                ).scalar()
                or 0
            )
            items.append(
                {
                    "entity_id": entity.id,
                    "ccode": entity.ccode,
                    "name": entity.name,
                    "region": entity.region,
                    "cost_centers": cc_count,
                }
            )
    return {"wave_id": wave_id, "items": items}


@router.post("/{wave_id}/entities")
def add_wave_entities(
    wave_id: int,
    body: dict,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status not in ("draft", "analysing"):
        raise HTTPException(status_code=409, detail="Cannot modify entities in this state")
    ccodes = body.get("ccodes", [])
    added = 0
    for ccode in ccodes:
        entity = db.execute(select(Entity).where(Entity.ccode == ccode)).scalar_one_or_none()
        if not entity:
            continue
        existing = db.execute(
            select(WaveEntity).where(
                WaveEntity.wave_id == wave_id,
                WaveEntity.entity_id == entity.id,
            )
        ).scalar_one_or_none()
        if not existing:
            db.add(WaveEntity(wave_id=wave_id, entity_id=entity.id))
            added += 1
    db.commit()
    return {"added": added}


@router.get("/{wave_id}/runs")
def list_wave_runs(
    wave_id: int,
    db: Session = Depends(get_db),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    runs = (
        db.execute(
            select(AnalysisRun)
            .where(AnalysisRun.wave_id == wave_id)
            .order_by(AnalysisRun.created_at.desc())
        )
        .scalars()
        .all()
    )
    return {
        "wave_id": wave_id,
        "items": [
            {
                "id": r.id,
                "config_id": r.config_id,
                "status": r.status,
                "kpis": r.kpis,
                "started_at": str(r.started_at) if r.started_at else None,
                "finished_at": str(r.finished_at) if r.finished_at else None,
            }
            for r in runs
        ],
    }


@router.post("/{wave_id}/analyse")
def run_analysis(
    wave_id: int,
    config_id: int | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Execute decision tree analysis on wave's cost centers."""
    from app.services.analysis import execute_analysis, get_or_create_default_config

    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status not in ("draft", "analysing", "proposed"):
        raise HTTPException(status_code=409, detail=f"Cannot analyse wave in status {wave.status}")

    if config_id is None:
        config = get_or_create_default_config(db)
        config_id = config.id

    try:
        run = execute_analysis(wave_id, config_id, user.id, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {e}") from None

    return {
        "run_id": run.id,
        "status": run.status,
        "kpis": run.kpis,
        "started_at": str(run.started_at) if run.started_at else None,
        "finished_at": str(run.finished_at) if run.finished_at else None,
    }


@router.post("/{wave_id}/propose/{run_id}")
def set_preferred_run(
    wave_id: int,
    run_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    run = db.get(AnalysisRun, run_id)
    if not run or run.wave_id != wave_id:
        raise HTTPException(status_code=404, detail="Run not found in this wave")
    if run.status != "completed":
        raise HTTPException(status_code=409, detail="Run must be completed")
    if wave.status not in ("analysing", "proposed"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot propose from status {wave.status}",
        )
    wave.status = "proposed"
    wave.config = {"preferred_run_id": run_id}
    db.commit()
    return {"status": "proposed", "preferred_run_id": run_id}


class ReviewScopeCreate(BaseModel):
    name: str
    scope_type: str = "entity"
    scope_filter: dict = {}
    reviewer_name: str | None = None
    reviewer_email: str | None = None


@router.post("/{wave_id}/scopes")
def create_review_scope(
    wave_id: int,
    body: ReviewScopeCreate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status not in ("locked", "in_review"):
        raise HTTPException(
            status_code=409,
            detail="Wave must be locked or in_review to create scopes",
        )

    token = secrets.token_urlsafe(48)
    expires = datetime.now(UTC) + timedelta(days=30)

    scope = ReviewScope(
        wave_id=wave_id,
        name=body.name,
        scope_type=body.scope_type,
        scope_filter=body.scope_filter,
        token=token,
        token_expires_at=expires,
        reviewer_name=body.reviewer_name,
        reviewer_email=body.reviewer_email,
        status="pending",
    )
    db.add(scope)
    db.flush()

    # Find proposals for this scope and create review items
    preferred_run_id = (wave.config or {}).get("preferred_run_id")
    if preferred_run_id:
        proposals = (
            db.execute(select(CenterProposal).where(CenterProposal.run_id == preferred_run_id))
            .scalars()
            .all()
        )

        # Filter by scope filter
        scope_ccodes = body.scope_filter.get("entity_ccodes", [])
        for p in proposals:
            if scope_ccodes:
                cc = db.get(LegacyCostCenter, p.legacy_cc_id)
                if cc and cc.ccode not in scope_ccodes:
                    continue
            db.add(
                ReviewItem(
                    scope_id=scope.id,
                    proposal_id=p.id,
                    decision="PENDING",
                )
            )

    # Transition wave to in_review if first scope
    if wave.status == "locked":
        wave.status = "in_review"

    db.commit()
    db.refresh(scope)
    return {
        "id": scope.id,
        "name": scope.name,
        "token": token,
        "review_url": f"/review/{token}",
        "status": scope.status,
    }


@router.get("/{wave_id}/scopes")
def list_review_scopes(
    wave_id: int,
    db: Session = Depends(get_db),
) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    scopes = db.execute(select(ReviewScope).where(ReviewScope.wave_id == wave_id)).scalars().all()
    items = []
    for s in scopes:
        total_items = (
            db.execute(
                select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == s.id)
            ).scalar()
            or 0
        )
        decided_items = (
            db.execute(
                select(func.count(ReviewItem.id)).where(
                    ReviewItem.scope_id == s.id,
                    ReviewItem.decision != "PENDING",
                )
            ).scalar()
            or 0
        )
        items.append(
            {
                "id": s.id,
                "name": s.name,
                "scope_type": s.scope_type,
                "status": s.status,
                "reviewer_name": s.reviewer_name,
                "reviewer_email": s.reviewer_email,
                "total_items": total_items,
                "decided_items": decided_items,
                "token": s.token,
            }
        )
    return {"wave_id": wave_id, "items": items}


@router.get("/{wave_id}/progress")
def wave_progress(wave_id: int, db: Session = Depends(get_db)) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    scopes = db.execute(select(ReviewScope).where(ReviewScope.wave_id == wave.id)).scalars().all()
    total_review_items = 0
    decided_items = 0
    for s in scopes:
        t = (
            db.execute(
                select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == s.id)
            ).scalar()
            or 0
        )
        d = (
            db.execute(
                select(func.count(ReviewItem.id)).where(
                    ReviewItem.scope_id == s.id, ReviewItem.decision != "PENDING"
                )
            ).scalar()
            or 0
        )
        total_review_items += t
        decided_items += d
    return {
        "wave_id": wave.id,
        "status": wave.status,
        "total_review_items": total_review_items,
        "decided_items": decided_items,
        "completion_pct": (
            round(decided_items / total_review_items * 100, 1) if total_review_items else 0
        ),
        "scopes": [
            {"id": s.id, "name": s.name, "status": s.status, "scope_type": s.scope_type}
            for s in scopes
        ],
    }
