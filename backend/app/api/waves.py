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
    WaveHierarchyScope,
    WaveTemplate,
)

router = APIRouter()

logger = __import__("structlog").get_logger()


def _send_scope_invitation(scope: ReviewScope, wave: Wave, db: Session) -> None:
    """Best-effort email invitation to reviewer."""
    from app.models.core import AppConfig

    try:
        cfg = db.execute(select(AppConfig).where(AppConfig.key == "email")).scalar_one_or_none()
        if not cfg or not cfg.value:
            return
        email_cfg = cfg.value
        from app.infra.email.engine import EmailEngine

        engine = EmailEngine(
            host=email_cfg.get("host", "localhost"),
            port=int(email_cfg.get("port", 1025)),
            username=email_cfg.get("username", ""),
            password=email_cfg.get("password", ""),
            use_tls=email_cfg.get("tls", "none") != "none",
            from_address=email_cfg.get("from_address", "noreply@amplifi.dev"),
        )
        item_count = (
            db.execute(
                select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == scope.id)
            ).scalar()
            or 0
        )
        engine.send(
            to=scope.reviewer_email,
            template_name="review_invitation",
            context={
                "reviewer_name": scope.reviewer_name or scope.reviewer_email,
                "wave_name": wave.name,
                "review_url": f"/review/{scope.token}",
                "expires_at": str(scope.token_expires_at)[:10] if scope.token_expires_at else "N/A",
                "scope_name": scope.name,
                "item_count": item_count,
            },
        )
        logger.info("email.invitation_sent", scope_id=scope.id, to=scope.reviewer_email)
    except Exception:
        logger.warning("email.invitation_failed", scope_id=scope.id, exc_info=True)


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


class HierarchyScopeIn(BaseModel):
    hierarchy_id: int
    node_setname: str


class WaveCreate(BaseModel):
    code: str
    name: str
    description: str | None = None
    is_full_scope: bool = False
    exclude_prior: bool = True
    entity_ccodes: list[str] = []
    hierarchy_scopes: list[HierarchyScopeIn] = []


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
    for hs in body.hierarchy_scopes:
        db.add(
            WaveHierarchyScope(
                wave_id=wave.id,
                hierarchy_id=hs.hierarchy_id,
                node_setname=hs.node_setname,
            )
        )
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


# --- Wave Templates (must be defined before /{wave_id} to avoid shadowing) ---


class TemplateCreate(BaseModel):
    name: str
    description: str | None = None
    config: dict | None = None
    is_full_scope: bool = False
    exclude_prior: bool = True
    entity_ccodes: list[str] | None = None


class TemplateOut(BaseModel):
    id: int
    name: str
    description: str | None
    config: dict | None
    is_full_scope: bool
    exclude_prior: bool
    entity_ccodes: list | None

    model_config = {"from_attributes": True}


@router.get("/templates")
def list_templates(db: Session = Depends(get_db)) -> list[TemplateOut]:
    rows = db.execute(select(WaveTemplate).order_by(WaveTemplate.name)).scalars().all()
    return [TemplateOut.model_validate(r) for r in rows]


@router.post("/templates")
def create_template(
    body: TemplateCreate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> TemplateOut:
    t = WaveTemplate(
        name=body.name,
        description=body.description,
        config=body.config,
        is_full_scope=body.is_full_scope,
        exclude_prior=body.exclude_prior,
        entity_ccodes=body.entity_ccodes,
        created_by=user.id,
    )
    db.add(t)
    db.commit()
    db.refresh(t)
    return TemplateOut.model_validate(t)


@router.delete("/templates/{template_id}")
def delete_template(
    template_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    t = db.get(WaveTemplate, template_id)
    if not t:
        raise HTTPException(status_code=404, detail="Template not found")
    db.delete(t)
    db.commit()
    return {"deleted": True}


@router.post("/templates/{template_id}/create-wave")
def create_wave_from_template(
    template_id: int,
    code: str,
    name: str,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Create a new wave from an existing template."""
    tpl = db.get(WaveTemplate, template_id)
    if not tpl:
        raise HTTPException(status_code=404, detail="Template not found")
    wave = Wave(
        code=code,
        name=name,
        description=tpl.description,
        status="draft",
        is_full_scope=tpl.is_full_scope,
        exclude_prior=tpl.exclude_prior,
        config=tpl.config,
        created_by=user.id,
    )
    db.add(wave)
    db.flush()
    if tpl.entity_ccodes:
        entities = (
            db.execute(select(Entity).where(Entity.ccode.in_(tpl.entity_ccodes))).scalars().all()
        )
        for ent in entities:
            db.add(WaveEntity(wave_id=wave.id, entity_id=ent.id))
    db.commit()
    db.refresh(wave)
    return {"id": wave.id, "code": wave.code, "status": wave.status}


@router.get("/review-scopes")
def my_review_scopes(
    db: Session = Depends(get_db),
    user: AppUser = Depends(get_current_user),
) -> list[dict]:
    """List review scopes assigned to the current user."""
    scopes = (
        db.execute(
            select(ReviewScope)
            .where(ReviewScope.reviewer_user_id == user.id)
            .order_by(ReviewScope.id.desc())
        )
        .scalars()
        .all()
    )
    result = []
    for s in scopes:
        total = (
            db.execute(
                select(func.count(ReviewItem.id)).where(
                    ReviewItem.scope_id == s.id
                )
            ).scalar()
            or 0
        )
        decided = (
            db.execute(
                select(func.count(ReviewItem.id)).where(
                    ReviewItem.scope_id == s.id,
                    ReviewItem.decision != "PENDING",
                )
            ).scalar()
            or 0
        )
        wave = s.wave
        result.append(
            {
                "id": s.id,
                "name": s.name,
                "scope_type": s.scope_type,
                "status": s.status,
                "token": s.token,
                "total_items": total,
                "decided_items": decided,
                "wave_name": wave.name if wave else None,
            }
        )
    return result


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
    old_status = wave.status
    wave.status = "cancelled"
    from app.domain.audit import write_audit

    write_audit(
        db,
        action="wave.cancel",
        entity_type="wave",
        entity_id=wave.id,
        actor_id=user.id,
        actor_email=user.email or user.username,
        before={"status": old_status},
        after={"status": "cancelled"},
    )
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
    from app.domain.audit import write_audit

    write_audit(
        db,
        action="wave.lock",
        entity_type="wave",
        entity_id=wave.id,
        actor_id=user.id,
        actor_email=user.email or user.username,
        before={"status": "proposed"},
        after={"status": "locked"},
    )
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

    # Completeness check: block if any review items are still PENDING
    pending_count = (
        db.execute(
            select(func.count(ReviewItem.id)).where(
                ReviewItem.scope_id.in_(
                    select(ReviewScope.id).where(ReviewScope.wave_id == wave.id)
                ),
                ReviewItem.decision == "PENDING",
            )
        ).scalar()
        or 0
    )
    if pending_count > 0:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot sign off: {pending_count} review items still PENDING",
        )

    wave.status = "signed_off"
    wave.signed_off_at = datetime.now(UTC)
    from app.domain.audit import write_audit

    write_audit(
        db,
        action="wave.signoff",
        entity_type="wave",
        entity_id=wave.id,
        actor_id=user.id,
        actor_email=user.email or user.username,
        before={"status": "in_review"},
        after={"status": "signed_off"},
    )
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
    wave.preferred_run_id = run_id
    wave.config = {**(wave.config or {}), "preferred_run_id": run_id}
    db.commit()
    return {"status": "proposed", "preferred_run_id": run_id}


@router.post("/{wave_id}/reset-proposals")
def reset_proposals(
    wave_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Delete all proposals for a wave and release allocated IDs.

    Used when re-running analysis — clears old proposals and recycles
    any CC/PC IDs allocated from the NamingPool back to the pool.
    """
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status in ("locked", "in_review", "signed_off", "closed", "cancelled"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot reset proposals: wave is {wave.status}",
        )

    from app.domain.proposal.service import release_proposal_ids

    proposals = (
        db.execute(select(CenterProposal).join(AnalysisRun).where(AnalysisRun.wave_id == wave_id))
        .scalars()
        .all()
    )

    released_ids = 0
    for p in proposals:
        released_ids += release_proposal_ids(p.id, db)
        db.delete(p)

    if wave.status == "proposed":
        wave.status = "analysing"

    from app.domain.audit import write_audit

    write_audit(
        db,
        action="wave.proposals_reset",
        entity_type="wave",
        entity_id=wave.id,
        actor_id=user.id,
        actor_email=user.email or user.username,
        after={
            "proposals_deleted": len(proposals),
            "ids_released": released_ids,
        },
    )
    db.commit()
    return {
        "proposals_deleted": len(proposals),
        "ids_released": released_ids,
    }


@router.delete("/{wave_id}")
def delete_wave(
    wave_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Delete a wave and all associated data (proposals, scopes, runs)."""
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status in ("in_review", "signed_off"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete wave in status {wave.status}",
        )

    from app.domain.proposal.service import release_proposal_ids

    proposals = (
        db.execute(select(CenterProposal).join(AnalysisRun).where(AnalysisRun.wave_id == wave_id))
        .scalars()
        .all()
    )
    for p in proposals:
        release_proposal_ids(p.id, db)

    db.delete(wave)
    from app.domain.audit import write_audit

    write_audit(
        db,
        action="wave.deleted",
        entity_type="wave",
        entity_id=wave_id,
        actor_id=user.id,
        actor_email=user.email or user.username,
        after={"wave_code": wave.code},
    )
    db.commit()
    return {"status": "deleted"}


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

    token = secrets.token_urlsafe(32)
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
                if not cc or cc.ccode not in scope_ccodes:
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

    # Send invitation email if reviewer_email is provided
    if body.reviewer_email:
        _send_scope_invitation(scope, wave, db)

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
    user: AppUser = Depends(require_role("admin", "analyst")),
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
                "token_hint": s.token[:8] + "..." if s.token else None,
            }
        )
    return {"wave_id": wave_id, "items": items}


@router.post("/{wave_id}/proposals/{proposal_id}/override")
def override_proposal(
    wave_id: int,
    proposal_id: int,
    body: dict,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Override a proposal's cleansing outcome and/or target object."""
    from app.domain.proposal.service import override_proposal as do_override

    try:
        proposal = do_override(
            proposal_id=proposal_id,
            new_outcome=body.get("outcome", ""),
            new_target=body.get("target_object"),
            reason=body.get("reason", ""),
            user_id=user.id,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None

    return {
        "id": proposal.id,
        "cleansing_outcome": proposal.cleansing_outcome,
        "target_object": proposal.target_object,
        "override_outcome": proposal.override_outcome,
        "override_target": proposal.override_target,
        "override_reason": proposal.override_reason,
    }


@router.post("/{wave_id}/lock-and-create-targets")
def lock_and_create_targets(
    wave_id: int,
    body: dict,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Lock proposals and create target CC/PC objects from approved proposals."""
    from app.domain.proposal.service import lock_proposals

    run_id = body.get("run_id")
    if not run_id:
        # Try to use preferred run from wave config
        wave = db.get(Wave, wave_id)
        if not wave:
            raise HTTPException(status_code=404, detail="Wave not found")
        run_id = (wave.config or {}).get("preferred_run_id")
        if not run_id:
            raise HTTPException(status_code=400, detail="No run_id specified and no preferred run")

    try:
        result = lock_proposals(wave_id, run_id, db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None

    return result


@router.get("/{wave_id}/mdg-export")
def mdg_export(
    wave_id: int,
    export_type: str = "cost_center",
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Generate MDG export file for the wave's target objects."""
    from app.infra.mdg.export import export_cost_centers, export_profit_centers, export_retire_list
    from app.models.core import TargetCostCenter, TargetProfitCenter

    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status not in ("signed_off", "closed"):
        raise HTTPException(status_code=409, detail="Wave must be signed_off or closed for export")

    if export_type == "cost_center":
        targets = (
            db.execute(select(TargetCostCenter).where(TargetCostCenter.approved_in_wave == wave_id))
            .scalars()
            .all()
        )
        centers = [
            {
                "cctr": t.cctr,
                "coarea": t.coarea,
                "txtsh": t.txtsh,
                "txtmi": t.txtmi,
                "responsible": t.responsible,
                "ccode": t.ccode,
                "cctrcgy": t.cctrcgy,
                "currency": t.currency,
                "pctr": t.pctr,
            }
            for t in targets
        ]
        result = export_cost_centers(centers, wave_id)
    elif export_type == "profit_center":
        targets = (
            db.execute(
                select(TargetProfitCenter).where(TargetProfitCenter.approved_in_wave == wave_id)
            )
            .scalars()
            .all()
        )
        centers = [
            {
                "pctr": t.pctr,
                "coarea": t.coarea,
                "txtsh": t.txtsh,
                "txtmi": t.txtmi,
                "responsible": t.responsible,
                "ccode": t.ccode,
                "currency": t.currency,
            }
            for t in targets
        ]
        result = export_profit_centers(centers, wave_id)
    elif export_type == "retire":
        # Get proposals with RETIRE outcome for the preferred run
        preferred_run_id = (wave.config or {}).get("preferred_run_id")
        if not preferred_run_id:
            raise HTTPException(status_code=400, detail="No preferred run set")
        proposals = (
            db.execute(
                select(CenterProposal).where(
                    CenterProposal.run_id == preferred_run_id,
                    CenterProposal.cleansing_outcome == "RETIRE",
                )
            )
            .scalars()
            .all()
        )
        centers = []
        for p in proposals:
            cc = db.get(LegacyCostCenter, p.legacy_cc_id)
            if cc:
                centers.append(
                    {
                        "cctr": cc.cctr,
                        "coarea": cc.coarea,
                        "txtsh": cc.txtsh,
                        "txtmi": cc.txtmi,
                        "responsible": cc.responsible,
                        "ccode": cc.ccode,
                        "cctrcgy": cc.cctrcgy,
                        "currency": cc.currency,
                        "pctr": cc.pctr,
                    }
                )
        result = export_retire_list(centers, wave_id)
    else:
        raise HTTPException(status_code=400, detail=f"Invalid export_type: {export_type}")

    return {
        "filename": result.filename,
        "content": result.content,
        "record_count": result.record_count,
        "export_type": result.export_type,
    }


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


# --- Reviewer Workload Balancer ---


@router.get("/{wave_id}/workload")
def reviewer_workload(
    wave_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Get reviewer workload distribution for a wave."""
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")

    scopes = db.execute(select(ReviewScope).where(ReviewScope.wave_id == wave_id)).scalars().all()

    workload = []
    for s in scopes:
        total_items = (
            db.execute(
                select(func.count(ReviewItem.id)).where(ReviewItem.scope_id == s.id)
            ).scalar()
            or 0
        )
        decided = (
            db.execute(
                select(func.count(ReviewItem.id)).where(
                    ReviewItem.scope_id == s.id,
                    ReviewItem.decision != "PENDING",
                )
            ).scalar()
            or 0
        )
        pending = total_items - decided
        workload.append(
            {
                "scope_id": s.id,
                "name": s.name,
                "reviewer": s.reviewer_email,
                "status": s.status,
                "total_items": total_items,
                "decided": decided,
                "pending": pending,
                "completion_pct": round(decided / total_items * 100, 1) if total_items else 0,
            }
        )

    total_all = sum(w["total_items"] for w in workload)
    avg_per_reviewer = round(total_all / len(workload), 1) if workload else 0
    max_load = max((w["total_items"] for w in workload), default=0)
    min_load = min((w["total_items"] for w in workload), default=0)
    imbalance = round((max_load - min_load) / avg_per_reviewer * 100, 1) if avg_per_reviewer else 0

    return {
        "wave_id": wave_id,
        "reviewers": workload,
        "summary": {
            "total_items": total_all,
            "total_reviewers": len(workload),
            "avg_per_reviewer": avg_per_reviewer,
            "max_load": max_load,
            "min_load": min_load,
            "imbalance_pct": imbalance,
        },
    }


# --- Auto-Approve ---


class AutoApproveParams(BaseModel):
    confidence_threshold: float = 0.85
    max_items: int | None = None
    verdicts: list[str] = ["KEEP"]


@router.post("/{wave_id}/auto-approve")
def auto_approve_obvious(
    wave_id: int,
    params: AutoApproveParams,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Auto-approve review items where the analysis confidence is above threshold."""
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status != "in_review":
        raise HTTPException(status_code=409, detail="Wave must be in_review for auto-approve")

    preferred_run_id = (wave.config or {}).get("preferred_run_id")
    if not preferred_run_id:
        raise HTTPException(status_code=409, detail="No preferred run set")

    # Get pending review items with their proposals
    pending = db.execute(
        select(ReviewItem, CenterProposal)
        .join(CenterProposal, ReviewItem.proposal_id == CenterProposal.id)
        .join(ReviewScope, ReviewItem.scope_id == ReviewScope.id)
        .where(
            ReviewScope.wave_id == wave_id,
            ReviewItem.decision == "PENDING",
            CenterProposal.run_id == preferred_run_id,
        )
    ).all()

    approved_count = 0
    for item, proposal in pending:
        confidence = float(proposal.confidence or 0)
        verdict_match = proposal.cleansing_outcome in params.verdicts
        if verdict_match and confidence >= params.confidence_threshold:
            item.decision = "APPROVED"
            item.decided_by = f"auto:{user.id}"
            item.decided_at = datetime.now(UTC)
            approved_count += 1
            if params.max_items and approved_count >= params.max_items:
                break

    db.commit()
    return {
        "approved": approved_count,
        "remaining_pending": len(pending) - approved_count,
        "threshold": params.confidence_threshold,
        "verdicts": params.verdicts,
    }


# --- Workload-Aware Scope Assignment ---


class AutoAssignParams(BaseModel):
    reviewer_emails: list[str]
    scope_type: str = "entity"
    strategy: str = "round_robin"  # round_robin | balanced | entity_group


@router.post("/{wave_id}/auto-assign")
def auto_assign_scopes(
    wave_id: int,
    params: AutoAssignParams,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Automatically create review scopes and distribute proposals to reviewers."""
    wave = db.get(Wave, wave_id)
    if not wave:
        raise HTTPException(status_code=404, detail="Wave not found")
    if wave.status not in ("locked", "in_review"):
        raise HTTPException(status_code=409, detail="Wave must be locked or in_review")

    preferred_run_id = (wave.config or {}).get("preferred_run_id")
    if not preferred_run_id:
        raise HTTPException(status_code=409, detail="No preferred run set")

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == preferred_run_id))
        .scalars()
        .all()
    )
    if not proposals:
        raise HTTPException(status_code=409, detail="No proposals found for preferred run")

    reviewers = list(dict.fromkeys(params.reviewer_emails))  # deduplicate preserving order
    if not reviewers:
        raise HTTPException(status_code=400, detail="At least one reviewer required")

    # Group proposals by entity if strategy is entity_group
    if params.strategy == "entity_group":
        entity_groups: dict[str, list] = {}
        for p in proposals:
            cc = db.get(LegacyCostCenter, p.legacy_cc_id)
            ccode = cc.ccode if cc else "UNKNOWN"
            entity_groups.setdefault(ccode, []).append(p)
        # Distribute entity groups to reviewers round-robin
        sorted_groups = sorted(entity_groups.items(), key=lambda x: -len(x[1]))
        reviewer_loads: dict[str, list] = {r: [] for r in reviewers}
        for _ccode, group_proposals in sorted_groups:
            min_reviewer = min(reviewer_loads, key=lambda r: len(reviewer_loads[r]))
            reviewer_loads[min_reviewer].extend(group_proposals)
    else:
        # round_robin or balanced
        reviewer_loads = {r: [] for r in reviewers}
        for i, p in enumerate(proposals):
            target = reviewers[i % len(reviewers)]
            reviewer_loads[target].append(p)

    created_scopes = []
    for email, assigned_proposals in reviewer_loads.items():
        if not assigned_proposals:
            continue
        token = secrets.token_urlsafe(32)
        expires = datetime.now(UTC) + timedelta(days=30)
        scope = ReviewScope(
            wave_id=wave_id,
            name=f"Auto-assigned: {email}",
            scope_type=params.scope_type,
            scope_filter={"auto_assigned": True, "reviewer": email},
            token=token,
            token_expires_at=expires,
            reviewer_email=email,
            status="pending",
        )
        db.add(scope)
        db.flush()
        for p in assigned_proposals:
            db.add(ReviewItem(scope_id=scope.id, proposal_id=p.id, decision="PENDING"))
        created_scopes.append(
            {
                "scope_id": scope.id,
                "reviewer": email,
                "items": len(assigned_proposals),
                "review_url": f"/review/{token}",
            }
        )

    if wave.status == "locked":
        wave.status = "in_review"

    db.commit()
    return {
        "scopes_created": len(created_scopes),
        "scopes": created_scopes,
        "total_items": len(proposals),
        "strategy": params.strategy,
    }
