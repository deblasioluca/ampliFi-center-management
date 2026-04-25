"""Wave API endpoints (section 11.7)."""

from __future__ import annotations

from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, get_current_user, pagination, require_role
from app.infra.db.session import get_db
from app.models.core import AppUser, Entity, ReviewScope, Wave, WaveEntity

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
    from datetime import datetime

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
    from datetime import datetime

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
    from datetime import datetime

    wave.status = "closed"
    wave.closed_at = datetime.now(UTC)
    db.commit()
    return {"status": "closed"}


@router.get("/{wave_id}/progress")
def wave_progress(wave_id: int, db: Session = Depends(get_db)) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    scopes = db.execute(select(ReviewScope).where(ReviewScope.wave_id == wave.id)).scalars().all()
    return {
        "wave_id": wave.id,
        "status": wave.status,
        "scopes": [
            {"id": s.id, "name": s.name, "status": s.status, "scope_type": s.scope_type}
            for s in scopes
        ],
    }
