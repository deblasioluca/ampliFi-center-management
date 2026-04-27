"""Data management endpoints — selective and bulk delete for imported data tables."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.api.deps import require_role
from app.infra.db.session import get_db
from app.models.core import (
    AppUser,
    Balance,
    Employee,
    Entity,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
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
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin")),
) -> dict:
    return {
        "entities": db.execute(select(func.count(Entity.id))).scalar() or 0,
        "cost_centers": db.execute(select(func.count(LegacyCostCenter.id))).scalar() or 0,
        "profit_centers": db.execute(select(func.count(LegacyProfitCenter.id))).scalar() or 0,
        "balances": db.execute(select(func.count(Balance.id))).scalar() or 0,
        "hierarchies": db.execute(select(func.count(Hierarchy.id))).scalar() or 0,
        "employees": db.execute(select(func.count(Employee.id))).scalar() or 0,
        "upload_batches": db.execute(select(func.count(UploadBatch.id))).scalar() or 0,
    }


# --- SAP OData Extraction ---


class SAPExtractionRequest(BaseModel):
    connection_id: int
    kind: str
    odata_params: dict | None = None


@router.post("/sap-extract")
def trigger_sap_extraction(
    body: SAPExtractionRequest,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Trigger SAP OData extraction for a given data kind."""
    from app.services.sap_extraction import extract_from_sap

    try:
        result = extract_from_sap(db, body.connection_id, body.kind, body.odata_params)
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
