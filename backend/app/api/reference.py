"""Reference data endpoints (section 11.10)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination
from app.infra.db.session import get_db
from app.models.core import Entity, Hierarchy, LegacyCostCenter, LegacyProfitCenter

router = APIRouter()


@router.get("/entities")
def list_entities(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    country: str | None = None,
) -> dict:
    query = select(Entity).order_by(Entity.ccode)
    if country:
        query = query.where(Entity.country == country)
    total = db.execute(select(func.count(Entity.id))).scalar() or 0
    entities = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": e.id,
                "ccode": e.ccode,
                "name": e.name,
                "country": e.country,
                "region": e.region,
                "is_active": e.is_active,
            }
            for e in entities
        ],
    }


@router.get("/legacy/cost-centers")
def list_legacy_ccs(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    ccode: str | None = None,
    coarea: str | None = None,
    cctr: str | None = None,
) -> dict:
    query = select(LegacyCostCenter)
    if ccode:
        query = query.where(LegacyCostCenter.ccode == ccode)
    if coarea:
        query = query.where(LegacyCostCenter.coarea == coarea)
    if cctr:
        query = query.where(LegacyCostCenter.cctr.ilike(f"{cctr}%"))
    total_q = select(func.count(LegacyCostCenter.id))
    total = db.execute(total_q).scalar() or 0
    ccs = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": c.id,
                "coarea": c.coarea,
                "cctr": c.cctr,
                "txtsh": c.txtsh,
                "ccode": c.ccode,
                "is_active": c.is_active,
            }
            for c in ccs
        ],
    }


@router.get("/legacy/profit-centers")
def list_legacy_pcs(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    ccode: str | None = None,
) -> dict:
    query = select(LegacyProfitCenter)
    if ccode:
        query = query.where(LegacyProfitCenter.ccode == ccode)
    total = db.execute(select(func.count(LegacyProfitCenter.id))).scalar() or 0
    pcs = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": p.id,
                "coarea": p.coarea,
                "pctr": p.pctr,
                "txtsh": p.txtsh,
                "ccode": p.ccode,
                "is_active": p.is_active,
            }
            for p in pcs
        ],
    }


@router.get("/legacy/hierarchies")
def list_hierarchies(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    total = db.execute(select(func.count(Hierarchy.id))).scalar() or 0
    hiers = (
        db.execute(select(Hierarchy).offset((pag.page - 1) * pag.size).limit(pag.size))
        .scalars()
        .all()
    )
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": h.id,
                "setclass": h.setclass,
                "setname": h.setname,
                "description": h.description,
                "coarea": h.coarea,
            }
            for h in hiers
        ],
    }
