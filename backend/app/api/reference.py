"""Reference data endpoints (section 11.10) — browse all data types."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination
from app.infra.db.session import get_db
from app.models.core import (
    Balance,
    Entity,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
    UploadBatch,
)

router = APIRouter()


@router.get("/entities")
def list_entities(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    country: str | None = None,
    search: str | None = None,
) -> dict:
    query = select(Entity).order_by(Entity.ccode)
    count_q = select(func.count(Entity.id))
    if country:
        query = query.where(Entity.country == country)
        count_q = count_q.where(Entity.country == country)
    if search:
        pattern = f"%{search}%"
        query = query.where(Entity.ccode.ilike(pattern) | Entity.name.ilike(pattern))
        count_q = count_q.where(Entity.ccode.ilike(pattern) | Entity.name.ilike(pattern))
    total = db.execute(count_q).scalar() or 0
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
                "currency": e.currency,
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
    search: str | None = None,
) -> dict:
    query = select(LegacyCostCenter).order_by(LegacyCostCenter.cctr)
    count_q = select(func.count(LegacyCostCenter.id))
    if ccode:
        query = query.where(LegacyCostCenter.ccode == ccode)
        count_q = count_q.where(LegacyCostCenter.ccode == ccode)
    if coarea:
        query = query.where(LegacyCostCenter.coarea == coarea)
        count_q = count_q.where(LegacyCostCenter.coarea == coarea)
    if cctr:
        query = query.where(LegacyCostCenter.cctr.ilike(f"{cctr}%"))
        count_q = count_q.where(LegacyCostCenter.cctr.ilike(f"{cctr}%"))
    if search:
        pattern = f"%{search}%"
        query = query.where(
            LegacyCostCenter.cctr.ilike(pattern)
            | LegacyCostCenter.txtsh.ilike(pattern)
            | LegacyCostCenter.txtmi.ilike(pattern)
        )
        count_q = count_q.where(
            LegacyCostCenter.cctr.ilike(pattern)
            | LegacyCostCenter.txtsh.ilike(pattern)
            | LegacyCostCenter.txtmi.ilike(pattern)
        )
    total = db.execute(count_q).scalar() or 0
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
                "txtmi": c.txtmi,
                "responsible": c.responsible,
                "cctrcgy": c.cctrcgy,
                "ccode": c.ccode,
                "currency": c.currency,
                "pctr": c.pctr,
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
    coarea: str | None = None,
    search: str | None = None,
) -> dict:
    query = select(LegacyProfitCenter).order_by(LegacyProfitCenter.pctr)
    count_q = select(func.count(LegacyProfitCenter.id))
    if ccode:
        query = query.where(LegacyProfitCenter.ccode == ccode)
        count_q = count_q.where(LegacyProfitCenter.ccode == ccode)
    if coarea:
        query = query.where(LegacyProfitCenter.coarea == coarea)
        count_q = count_q.where(LegacyProfitCenter.coarea == coarea)
    if search:
        pattern = f"%{search}%"
        query = query.where(
            LegacyProfitCenter.pctr.ilike(pattern)
            | LegacyProfitCenter.txtsh.ilike(pattern)
            | LegacyProfitCenter.txtmi.ilike(pattern)
        )
        count_q = count_q.where(
            LegacyProfitCenter.pctr.ilike(pattern)
            | LegacyProfitCenter.txtsh.ilike(pattern)
            | LegacyProfitCenter.txtmi.ilike(pattern)
        )
    total = db.execute(count_q).scalar() or 0
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
                "txtmi": p.txtmi,
                "responsible": p.responsible,
                "department": p.department,
                "ccode": p.ccode,
                "currency": p.currency,
                "is_active": p.is_active,
            }
            for p in pcs
        ],
    }


@router.get("/legacy/balances")
def list_balances(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    ccode: str | None = None,
    coarea: str | None = None,
    cctr: str | None = None,
    fiscal_year: int | None = None,
) -> dict:
    query = select(Balance).order_by(Balance.fiscal_year.desc(), Balance.period.desc())
    count_q = select(func.count(Balance.id))
    if ccode:
        query = query.where(Balance.ccode == ccode)
        count_q = count_q.where(Balance.ccode == ccode)
    if coarea:
        query = query.where(Balance.coarea == coarea)
        count_q = count_q.where(Balance.coarea == coarea)
    if cctr:
        query = query.where(Balance.cctr == cctr)
        count_q = count_q.where(Balance.cctr == cctr)
    if fiscal_year:
        query = query.where(Balance.fiscal_year == fiscal_year)
        count_q = count_q.where(Balance.fiscal_year == fiscal_year)
    total = db.execute(count_q).scalar() or 0
    rows = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": b.id,
                "coarea": b.coarea,
                "cctr": b.cctr,
                "ccode": b.ccode,
                "fiscal_year": b.fiscal_year,
                "period": b.period,
                "account": b.account,
                "account_class": b.account_class,
                "tc_amt": str(b.tc_amt) if b.tc_amt is not None else "0",
                "gc_amt": str(b.gc_amt) if b.gc_amt is not None else "0",
                "gc2_amt": str(b.gc2_amt) if b.gc2_amt is not None else "0",
                "currency_tc": b.currency_tc,
                "posting_count": b.posting_count,
            }
            for b in rows
        ],
    }


@router.get("/legacy/hierarchies")
def list_hierarchies(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    setclass: str | None = None,
) -> dict:
    query = select(Hierarchy)
    count_q = select(func.count(Hierarchy.id))
    if setclass:
        query = query.where(Hierarchy.setclass == setclass)
        count_q = count_q.where(Hierarchy.setclass == setclass)
    total = db.execute(count_q).scalar() or 0
    hiers = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
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
                "is_active": h.is_active,
            }
            for h in hiers
        ],
    }


@router.get("/legacy/hierarchies/{hier_id}/nodes")
def list_hierarchy_nodes(
    hier_id: int,
    db: Session = Depends(get_db),
) -> dict:
    nodes = (
        db.execute(
            select(HierarchyNode)
            .where(HierarchyNode.hierarchy_id == hier_id)
            .order_by(HierarchyNode.seq)
        )
        .scalars()
        .all()
    )
    return {
        "hierarchy_id": hier_id,
        "items": [
            {"id": n.id, "parent": n.parent_setname, "child": n.child_setname, "seq": n.seq}
            for n in nodes
        ],
    }


@router.get("/legacy/hierarchies/{hier_id}/leaves")
def list_hierarchy_leaves(
    hier_id: int,
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    total = (
        db.execute(
            select(func.count(HierarchyLeaf.id)).where(HierarchyLeaf.hierarchy_id == hier_id)
        ).scalar()
        or 0
    )
    leaves = (
        db.execute(
            select(HierarchyLeaf)
            .where(HierarchyLeaf.hierarchy_id == hier_id)
            .order_by(HierarchyLeaf.seq)
            .offset((pag.page - 1) * pag.size)
            .limit(pag.size)
        )
        .scalars()
        .all()
    )
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {"id": lf.id, "setname": lf.setname, "value": lf.value, "seq": lf.seq} for lf in leaves
        ],
    }


@router.get("/data/counts")
def data_counts(db: Session = Depends(get_db)) -> dict:
    """Aggregate counts for the data management dashboard."""
    return {
        "entities": db.execute(select(func.count(Entity.id))).scalar() or 0,
        "cost_centers": db.execute(select(func.count(LegacyCostCenter.id))).scalar() or 0,
        "profit_centers": db.execute(select(func.count(LegacyProfitCenter.id))).scalar() or 0,
        "balances": db.execute(select(func.count(Balance.id))).scalar() or 0,
        "hierarchies": db.execute(select(func.count(Hierarchy.id))).scalar() or 0,
        "upload_batches": db.execute(select(func.count(UploadBatch.id))).scalar() or 0,
    }


@router.post("/data/duplicate-check")
def check_duplicates(
    coarea: str | None = None,
    threshold: float = 0.85,
    limit: int = 100,
    db: Session = Depends(get_db),
) -> dict:
    """Find near-duplicate cost center names using embeddings."""
    from app.domain.ml.embeddings import find_duplicates

    query = select(LegacyCostCenter).where(LegacyCostCenter.is_active.is_(True))
    if coarea:
        query = query.where(LegacyCostCenter.coarea == coarea)
    ccs = db.execute(query).scalars().all()
    names = [cc.txtsh or cc.txtmi or cc.cctr for cc in ccs]
    ids = [cc.id for cc in ccs]
    pairs = find_duplicates(names, ids, threshold=threshold)
    return {"total": len(pairs), "pairs": pairs[:limit]}


@router.post("/data/naming-suggestions")
def naming_suggestions(
    cctr: str,
    coarea: str = "",
    top_k: int = 5,
    db: Session = Depends(get_db),
) -> dict:
    """Suggest standardized names for a cost center."""
    from app.domain.ml.embeddings import suggest_names

    query = select(LegacyCostCenter).where(LegacyCostCenter.cctr == cctr)
    if coarea:
        query = query.where(LegacyCostCenter.coarea == coarea)
    cc = db.execute(query).scalars().first()
    if not cc:
        return {"suggestions": [], "error": "Cost center not found"}
    current = cc.txtsh or cc.txtmi or cc.cctr

    ref_query = (
        select(LegacyCostCenter.txtsh)
        .where(
            LegacyCostCenter.is_active.is_(True),
            LegacyCostCenter.txtsh.isnot(None),
            LegacyCostCenter.id != cc.id,
        )
        .limit(2000)
    )
    refs = [r[0] for r in db.execute(ref_query).all() if r[0]]
    suggestions = suggest_names(current, refs, top_k=top_k)
    return {"current_name": current, "suggestions": suggestions}
