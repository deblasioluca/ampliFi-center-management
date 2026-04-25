"""Statistics endpoints (section 20.8)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.infra.db.session import get_db
from app.models.core import (
    Entity,
    LegacyCostCenter,
    LegacyProfitCenter,
    TargetCostCenter,
    TargetProfitCenter,
    Wave,
)

router = APIRouter()


@router.get("/global")
def global_stats(db: Session = Depends(get_db)) -> dict:
    entities_total = db.execute(select(func.count(Entity.id))).scalar() or 0
    legacy_cc_total = (
        db.execute(
            select(func.count(LegacyCostCenter.id)).where(LegacyCostCenter.is_active.is_(True))
        ).scalar()
        or 0
    )
    legacy_pc_total = (
        db.execute(
            select(func.count(LegacyProfitCenter.id)).where(LegacyProfitCenter.is_active.is_(True))
        ).scalar()
        or 0
    )
    target_cc = db.execute(select(func.count(TargetCostCenter.id))).scalar() or 0
    target_pc = db.execute(select(func.count(TargetProfitCenter.id))).scalar() or 0
    return {
        "universe": {
            "entities_total": entities_total,
            "legacy_cc_total": legacy_cc_total,
            "legacy_pc_total": legacy_pc_total,
            "target_cc_total": target_cc,
            "target_pc_total": target_pc,
        },
    }


@router.get("/wave/{wave_id}")
def wave_stats(wave_id: int, db: Session = Depends(get_db)) -> dict:
    wave = db.get(Wave, wave_id)
    if not wave:
        return {"error": "Wave not found"}
    return {
        "wave_id": wave.id,
        "code": wave.code,
        "status": wave.status,
    }
