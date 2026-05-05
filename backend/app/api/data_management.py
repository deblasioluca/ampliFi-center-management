"""Data management endpoints — selective and bulk delete for imported data tables."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.api.deps import require_role
from app.infra.db.session import get_db
from app.models.core import (
    ALL_CATEGORIES,
    ALL_SCOPES,
    SCOPE_CLEANUP,
    AppUser,
    Balance,
    CenterMapping,
    Employee,
    Entity,
    GLAccountSKA1,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
    TargetCostCenter,
    TargetProfitCenter,
    UploadBatch,
    UploadError,
)

router = APIRouter()


class DeleteByIds(BaseModel):
    ids: list[int]


class DeleteByFilter(BaseModel):
    ccode: str | None = None
    coarea: str | None = None


class DeleteResult(BaseModel):
    table: str
    deleted: int


# ── Entity data ─────────────────────────────────────────────────────────


@router.delete("/entities")
def delete_entities(
    body: DeleteByIds | None = None,
    ccode: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if body and body.ids:
        stmt = delete(Entity).where(Entity.id.in_(body.ids))
    elif ccode:
        stmt = delete(Entity).where(Entity.ccode == ccode)
    else:
        raise HTTPException(status_code=400, detail="Provide ids in body or ccode as query param")
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="entity", deleted=result.rowcount)


@router.delete("/entities/all")
def delete_all_entities(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    result = db.execute(delete(Entity))
    db.commit()
    return DeleteResult(table="entity", deleted=result.rowcount)


# ── Legacy cost centers ─────────────────────────────────────────────────


@router.delete("/legacy/cost-centers")
def delete_cost_centers(
    body: DeleteByIds | None = None,
    ccode: str | None = None,
    coarea: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if body and body.ids:
        stmt = delete(LegacyCostCenter).where(LegacyCostCenter.id.in_(body.ids))
    elif ccode:
        stmt = delete(LegacyCostCenter).where(LegacyCostCenter.ccode == ccode)
    elif coarea:
        stmt = delete(LegacyCostCenter).where(LegacyCostCenter.coarea == coarea)
    else:
        raise HTTPException(
            status_code=400, detail="Provide ids in body, or ccode/coarea as query param"
        )
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="legacy_cost_center", deleted=result.rowcount)


@router.delete("/legacy/cost-centers/all")
def delete_all_cost_centers(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    result = db.execute(delete(LegacyCostCenter))
    db.commit()
    return DeleteResult(table="legacy_cost_center", deleted=result.rowcount)


# ── Legacy profit centers ───────────────────────────────────────────────


@router.delete("/legacy/profit-centers")
def delete_profit_centers(
    body: DeleteByIds | None = None,
    ccode: str | None = None,
    coarea: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if body and body.ids:
        stmt = delete(LegacyProfitCenter).where(LegacyProfitCenter.id.in_(body.ids))
    elif ccode:
        stmt = delete(LegacyProfitCenter).where(LegacyProfitCenter.ccode == ccode)
    elif coarea:
        stmt = delete(LegacyProfitCenter).where(LegacyProfitCenter.coarea == coarea)
    else:
        raise HTTPException(
            status_code=400, detail="Provide ids in body, or ccode/coarea as query param"
        )
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="legacy_profit_center", deleted=result.rowcount)


@router.delete("/legacy/profit-centers/all")
def delete_all_profit_centers(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    result = db.execute(delete(LegacyProfitCenter))
    db.commit()
    return DeleteResult(table="legacy_profit_center", deleted=result.rowcount)


# ── Target cost centers ─────────────────────────────────────────────────


@router.delete("/target/cost-centers")
def delete_target_cost_centers(
    body: DeleteByIds | None = None,
    ccode: str | None = None,
    coarea: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if body and body.ids:
        stmt = delete(TargetCostCenter).where(TargetCostCenter.id.in_(body.ids))
    elif ccode:
        stmt = delete(TargetCostCenter).where(TargetCostCenter.ccode == ccode)
    elif coarea:
        stmt = delete(TargetCostCenter).where(TargetCostCenter.coarea == coarea)
    else:
        raise HTTPException(
            status_code=400, detail="Provide ids in body, or ccode/coarea as query param"
        )
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="target_cost_center", deleted=result.rowcount)


@router.delete("/target/cost-centers/all")
def delete_all_target_cost_centers(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    result = db.execute(delete(TargetCostCenter))
    db.commit()
    return DeleteResult(table="target_cost_center", deleted=result.rowcount)


# ── Target profit centers ───────────────────────────────────────────────


@router.delete("/target/profit-centers")
def delete_target_profit_centers(
    body: DeleteByIds | None = None,
    ccode: str | None = None,
    coarea: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if body and body.ids:
        stmt = delete(TargetProfitCenter).where(TargetProfitCenter.id.in_(body.ids))
    elif ccode:
        stmt = delete(TargetProfitCenter).where(TargetProfitCenter.ccode == ccode)
    elif coarea:
        stmt = delete(TargetProfitCenter).where(TargetProfitCenter.coarea == coarea)
    else:
        raise HTTPException(
            status_code=400, detail="Provide ids in body, or ccode/coarea as query param"
        )
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="target_profit_center", deleted=result.rowcount)


@router.delete("/target/profit-centers/all")
def delete_all_target_profit_centers(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    result = db.execute(delete(TargetProfitCenter))
    db.commit()
    return DeleteResult(table="target_profit_center", deleted=result.rowcount)


# ── Center mappings ─────────────────────────────────────────────────────


@router.delete("/center-mappings")
def delete_center_mappings(
    body: DeleteByIds | None = None,
    object_type: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if body and body.ids:
        stmt = delete(CenterMapping).where(CenterMapping.id.in_(body.ids))
    elif object_type:
        stmt = delete(CenterMapping).where(CenterMapping.object_type == object_type)
    else:
        raise HTTPException(
            status_code=400, detail="Provide ids in body or object_type as query param"
        )
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="center_mapping", deleted=result.rowcount)


@router.delete("/center-mappings/all")
def delete_all_center_mappings(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    result = db.execute(delete(CenterMapping))
    db.commit()
    return DeleteResult(table="center_mapping", deleted=result.rowcount)


# ── Balances ────────────────────────────────────────────────────────────


@router.delete("/balances")
def delete_balances(
    body: DeleteByIds | None = None,
    ccode: str | None = None,
    coarea: str | None = None,
    fiscal_year: int | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if body and body.ids:
        stmt = delete(Balance).where(Balance.id.in_(body.ids))
    elif ccode:
        stmt = delete(Balance).where(Balance.ccode == ccode)
        if fiscal_year:
            stmt = stmt.where(Balance.fiscal_year == fiscal_year)
    elif coarea:
        stmt = delete(Balance).where(Balance.coarea == coarea)
        if fiscal_year:
            stmt = stmt.where(Balance.fiscal_year == fiscal_year)
    elif fiscal_year:
        stmt = delete(Balance).where(Balance.fiscal_year == fiscal_year)
    else:
        raise HTTPException(
            status_code=400,
            detail="Provide ids in body, or ccode/coarea/fiscal_year as query params",
        )
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="balance", deleted=result.rowcount)


@router.delete("/balances/all")
def delete_all_balances(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    result = db.execute(delete(Balance))
    db.commit()
    return DeleteResult(table="balance", deleted=result.rowcount)


# ── Hierarchies ─────────────────────────────────────────────────────────


@router.delete("/hierarchies")
def delete_hierarchies(
    body: DeleteByIds | None = None,
    coarea: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if body and body.ids:
        db.execute(delete(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id.in_(body.ids)))
        db.execute(delete(HierarchyNode).where(HierarchyNode.hierarchy_id.in_(body.ids)))
        stmt = delete(Hierarchy).where(Hierarchy.id.in_(body.ids))
    elif coarea:
        hier_ids = [
            h.id for h in db.execute(select(Hierarchy.id).where(Hierarchy.coarea == coarea)).all()
        ]
        if hier_ids:
            db.execute(delete(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id.in_(hier_ids)))
            db.execute(delete(HierarchyNode).where(HierarchyNode.hierarchy_id.in_(hier_ids)))
        stmt = delete(Hierarchy).where(Hierarchy.coarea == coarea)
    else:
        raise HTTPException(status_code=400, detail="Provide ids in body or coarea as query param")
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="hierarchy", deleted=result.rowcount)


@router.delete("/hierarchies/all")
def delete_all_hierarchies(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    db.execute(delete(HierarchyLeaf))
    db.execute(delete(HierarchyNode))
    result = db.execute(delete(Hierarchy))
    db.commit()
    return DeleteResult(table="hierarchy", deleted=result.rowcount)


# ── Employees ───────────────────────────────────────────────────────────


@router.delete("/employees")
def delete_employees(
    body: DeleteByIds | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if not body or not body.ids:
        raise HTTPException(status_code=400, detail="Provide ids in body")
    stmt = delete(Employee).where(Employee.id.in_(body.ids))
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="employee", deleted=result.rowcount)


@router.delete("/employees/all")
def delete_all_employees(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    result = db.execute(delete(Employee))
    db.commit()
    return DeleteResult(table="employee", deleted=result.rowcount)


# ── Upload batches ──────────────────────────────────────────────────────


@router.delete("/uploads")
def delete_uploads(
    body: DeleteByIds | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    if not body or not body.ids:
        raise HTTPException(status_code=400, detail="Provide ids in body")
    db.execute(delete(UploadError).where(UploadError.batch_id.in_(body.ids)))
    stmt = delete(UploadBatch).where(UploadBatch.id.in_(body.ids))
    result = db.execute(stmt)
    db.commit()
    return DeleteResult(table="upload_batch", deleted=result.rowcount)


@router.delete("/uploads/all")
def delete_all_uploads(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> DeleteResult:
    db.execute(delete(UploadError))
    result = db.execute(delete(UploadBatch))
    db.commit()
    return DeleteResult(table="upload_batch", deleted=result.rowcount)


# ── Purge all sample/imported data ──────────────────────────────────────


@router.delete("/purge-all")
def purge_all_data(
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    counts: dict[str, int] = {}
    r = db.execute(delete(CenterMapping))
    counts["center_mappings"] = r.rowcount
    r = db.execute(delete(TargetCostCenter))
    counts["target_cost_centers"] = r.rowcount
    r = db.execute(delete(TargetProfitCenter))
    counts["target_profit_centers"] = r.rowcount
    r = db.execute(delete(UploadError))
    counts["upload_errors"] = r.rowcount
    r = db.execute(delete(UploadBatch))
    counts["upload_batches"] = r.rowcount
    r = db.execute(delete(Balance))
    counts["balances"] = r.rowcount
    db.execute(delete(HierarchyLeaf))
    db.execute(delete(HierarchyNode))
    r = db.execute(delete(Hierarchy))
    counts["hierarchies"] = r.rowcount
    r = db.execute(delete(LegacyCostCenter))
    counts["cost_centers"] = r.rowcount
    r = db.execute(delete(LegacyProfitCenter))
    counts["profit_centers"] = r.rowcount
    r = db.execute(delete(Entity))
    counts["entities"] = r.rowcount
    r = db.execute(delete(Employee))
    counts["employees"] = r.rowcount
    db.commit()
    return {"status": "purged", "counts": counts}


# ── Stats for data management ───────────────────────────────────────────


@router.get("/counts")
def data_counts(
    scope: str | None = Query(default=None),
    data_category: str | None = Query(default=None),
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    def _count(model: type, scope_col: str = "scope") -> int:
        stmt = select(func.count(model.id))
        if scope:
            stmt = stmt.where(getattr(model, scope_col) == scope)
        if data_category and hasattr(model, "data_category"):
            stmt = stmt.where(model.data_category == data_category)
        return db.execute(stmt).scalar() or 0

    ub_stmt = select(func.count(UploadBatch.id))
    if scope:
        ub_stmt = ub_stmt.where(UploadBatch.scope == scope)
    if data_category:
        ub_stmt = ub_stmt.where(UploadBatch.data_category == data_category)

    return {
        "entities": _count(Entity),
        "cost_centers": _count(LegacyCostCenter),
        "profit_centers": _count(LegacyProfitCenter),
        "balances": _count(Balance),
        "hierarchies": _count(Hierarchy),
        "employees": _count(Employee),
        "target_cost_centers": _count(TargetCostCenter),
        "target_profit_centers": _count(TargetProfitCenter),
        "center_mappings": _count(CenterMapping),
        "gl_accounts": _count(GLAccountSKA1),
        "upload_batches": db.execute(ub_stmt).scalar() or 0,
    }


# --- SAP OData Extraction ---


class SAPExtractionRequest(BaseModel):
    connection_id: int
    kind: str
    scope: str = SCOPE_CLEANUP
    data_category: str = "legacy"
    odata_params: dict | None = None


@router.post("/sap-extract")
def trigger_sap_extraction(
    body: SAPExtractionRequest,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Trigger SAP OData extraction for a given data kind."""
    if body.scope not in ALL_SCOPES:
        raise HTTPException(status_code=400, detail=f"Invalid scope: {body.scope}")
    if body.data_category not in ALL_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"Invalid data_category: {body.data_category}")
    from app.services.sap_extraction import extract_from_sap

    try:
        result = extract_from_sap(
            db,
            body.connection_id,
            body.kind,
            body.odata_params,
            scope=body.scope,
            data_category=body.data_category,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/sap-extract/available")
def available_extractions(
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> list[dict]:
    """List SAP connections available for OData extraction."""
    from app.services.sap_extraction import list_available_extractions

    return list_available_extractions(db)


@router.get("/browser")
def data_browser(
    scope: str = Query(default=SCOPE_CLEANUP),
    data_category: str | None = Query(default=None),
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
) -> dict:
    """Unified data browser: all centers + balances + hierarchies filtered by scope."""
    cc_q = select(LegacyCostCenter).where(LegacyCostCenter.scope == scope)
    pc_q = select(LegacyProfitCenter).where(LegacyProfitCenter.scope == scope)
    if data_category:
        cc_q = cc_q.where(LegacyCostCenter.data_category == data_category)
        pc_q = pc_q.where(LegacyProfitCenter.data_category == data_category)
    ccs = db.execute(cc_q).scalars().all()
    pcs = db.execute(pc_q).scalars().all()
    pc_map = {(p.coarea, p.pctr): p for p in pcs}

    # Monthly balances grouped by (coarea, cctr)
    bal_q = (
        select(
            Balance.coarea,
            Balance.cctr,
            Balance.fiscal_year,
            Balance.period,
            func.coalesce(func.sum(Balance.tc_amt), 0).label("amt"),
            func.coalesce(func.sum(Balance.posting_count), 0).label("post"),
            func.max(Balance.currency_tc).label("currency"),
        )
        .where(Balance.scope == scope)
        .group_by(Balance.coarea, Balance.cctr, Balance.fiscal_year, Balance.period)
        .order_by(Balance.coarea, Balance.cctr, Balance.fiscal_year, Balance.period)
    )
    if data_category:
        bal_q = bal_q.where(Balance.data_category == data_category)
    bal_rows = db.execute(bal_q).all()
    balance_map: dict[tuple[str, str], list[dict]] = {}
    for coarea, cctr, fy, per, amt, post, curr in bal_rows:
        balance_map.setdefault((coarea, cctr), []).append(
            {
                "fiscal_year": fy,
                "period": per,
                "amount": float(amt),
                "postings": int(post),
                "currency": curr or "",
            }
        )

    items = []
    for c in ccs:
        pc = pc_map.get((c.coarea, c.pctr)) if c.pctr else None
        items.append(
            {
                "id": c.id,
                "cctr": c.cctr,
                "txtsh": c.txtsh,
                "txtmi": c.txtmi,
                "ccode": c.ccode,
                "coarea": c.coarea,
                "pctr": c.pctr,
                "pc_txtsh": pc.txtsh if pc else None,
                "responsible": c.responsible,
                "cctrcgy": c.cctrcgy,
                "is_active": c.is_active,
                "monthly_balances": balance_map.get((c.coarea, c.cctr), []),
            }
        )

    # Hierarchies with tree structure
    hier_q = (
        select(Hierarchy)
        .where(Hierarchy.scope == scope, Hierarchy.is_active.is_(True))
        .order_by(Hierarchy.setclass, Hierarchy.setname)
    )
    if data_category:
        hier_q = hier_q.where(Hierarchy.data_category == data_category)
    hiers = db.execute(hier_q).scalars().all()
    cls_labels = {
        "0101": "Cost Center",
        "0104": "Profit Center",
        "0106": "Entity",
    }
    hier_trees = []
    for h in hiers:
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
        base = f"{cls_labels.get(h.setclass, h.setclass)}: {h.setname}"
        if h.description:
            base += f" — {h.description}"
        hier_trees.append(
            {
                "id": h.id,
                "setname": h.setname,
                "setclass": h.setclass,
                "label": h.label or base,
                "coarea": h.coarea,
                "nodes": [
                    {
                        "parent": n.parent_setname,
                        "child": n.child_setname,
                        "seq": n.seq,
                    }
                    for n in nodes
                ],
                "leaves": [
                    {"setname": lf.setname, "value": lf.value, "seq": lf.seq} for lf in leaves
                ],
            }
        )

    return {
        "total": len(items),
        "items": items,
        "hierarchies": hier_trees,
    }
