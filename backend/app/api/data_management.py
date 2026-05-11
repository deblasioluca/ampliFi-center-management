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


class AutoDeriveResult(BaseModel):
    """Outcome of an auto-derive run.

    ``created`` and ``updated`` are exclusive — a row that already
    matched (legacy_center, target_center) is updated in place rather
    than duplicated, so re-running the auto-derive is safe (idempotent
    on the unique-constraint columns).
    """

    created: int
    updated: int
    skipped: int
    runs_consulted: int
    source_runs: list[int]


@router.post("/center-mappings/auto-derive")
def auto_derive_center_mappings(
    run_id: int | None = None,
    overwrite: bool = False,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager")),
) -> AutoDeriveResult:
    """Populate ``center_mapping`` from completed analysis runs (PR #89, A13).

    Operator question: 'mapping is leer, wie kann diese befüllen?' The
    canonical answer is "upload a CSV via the Upload page" — but in a
    sizable share of operator workflows the merge decisions already
    exist on the proposals table (output of a global or wave analysis
    run) and the operator has no separate mapping CSV to upload. This
    endpoint closes that gap.

    Every ``CenterProposal`` row with ``cleansing_outcome == 'MERGE_MAP'``
    and a non-null ``merge_into_cctr`` becomes a CenterMapping row:

    * ``object_type``        = ``"cost_center"``
    * ``legacy_center``      = legacy CC's ``cctr``
    * ``legacy_name``        = legacy CC's ``txtsh``
    * ``target_center``      = ``merge_into_cctr`` (from the proposal)
    * ``target_name``        = best-effort lookup from
      ``LegacyCostCenter`` (the merge target is itself a known CC in
      99% of real cases) or ``None`` if it's not in the table
    * ``mapping_type``       = ``"merge"``
    * ``notes``              = ``"Auto-derived from run #N"``

    Behaviour:
    * ``run_id`` optional. If provided, derive only from that run; if
      omitted, derive from ALL completed runs (most recent overrides
      older when the same legacy → target pair appears twice).
    * ``overwrite`` defaults to False. Existing rows on the unique
      constraint (scope/object_type/legacy_coarea/legacy_center/
      target_coarea/target_center) are skipped unless ``overwrite=True``,
      in which case ``legacy_name``, ``target_name``, ``mapping_type``,
      and ``notes`` are refreshed. We never delete pre-existing manual
      mappings — operators put work into those.

    The endpoint is idempotent and safe to re-run.
    """
    from app.models.core import AnalysisRun, CenterProposal

    # Resolve which runs to consult. Defaulting to all "completed" runs
    # (the success path) avoids picking up half-finished work.
    runs_q = select(AnalysisRun).where(AnalysisRun.status == "completed")
    if run_id is not None:
        runs_q = runs_q.where(AnalysisRun.id == run_id)
    runs = db.execute(runs_q).scalars().all()
    if not runs:
        # If a specific run_id was given but it's not completed, that's
        # a 4xx not a 5xx — give the operator a clear message.
        if run_id is not None:
            raise HTTPException(
                status_code=404,
                detail=f"No completed run with id={run_id}",
            )
        return AutoDeriveResult(created=0, updated=0, skipped=0, runs_consulted=0, source_runs=[])

    run_ids = [r.id for r in runs]

    # Collect all MERGE_MAP proposals from those runs in a single
    # query, joined to the legacy CC table for the names we need on
    # the mapping rows.
    rows = db.execute(
        select(
            CenterProposal.legacy_cc_id,
            CenterProposal.merge_into_cctr,
            LegacyCostCenter.cctr,
            LegacyCostCenter.coarea,
            LegacyCostCenter.txtsh,
        )
        .join(LegacyCostCenter, LegacyCostCenter.id == CenterProposal.legacy_cc_id)
        .where(
            CenterProposal.run_id.in_(run_ids),
            CenterProposal.cleansing_outcome == "MERGE_MAP",
            CenterProposal.merge_into_cctr.isnot(None),
        )
    ).all()

    # Pre-fetch target names (the merge_into_cctr should match a known
    # legacy CC in the same coarea — usually the surviving "winner" of
    # a merge). One IN-bounded query rather than N row-by-row lookups.
    # We use tuple unpacking on the row rather than attribute access so
    # the endpoint stays test-friendly (mocked rows are plain tuples).
    target_cctrs = list({m for _l, m, _c, _co, _t in rows if m})
    target_name_map: dict[tuple[str, str], str] = {}
    if target_cctrs:
        tn_rows = db.execute(
            select(LegacyCostCenter.coarea, LegacyCostCenter.cctr, LegacyCostCenter.txtsh).where(
                LegacyCostCenter.cctr.in_(target_cctrs)
            )
        ).all()
        for coarea, cctr, txtsh in tn_rows:
            target_name_map[(coarea, cctr)] = txtsh or ""

    created = 0
    updated = 0
    skipped = 0

    for _legacy_cc_id, target_cctr, legacy_cctr, coarea, legacy_txtsh in rows:
        if not target_cctr or not legacy_cctr:
            skipped += 1
            continue

        # Look for an existing mapping on the unique-constraint key.
        existing = db.execute(
            select(CenterMapping).where(
                CenterMapping.scope == SCOPE_CLEANUP,
                CenterMapping.object_type == "cost_center",
                CenterMapping.legacy_coarea == coarea,
                CenterMapping.legacy_center == legacy_cctr,
                CenterMapping.target_coarea == coarea,
                CenterMapping.target_center == target_cctr,
            )
        ).scalar_one_or_none()

        target_name = target_name_map.get((coarea, target_cctr))
        notes = (
            f"Auto-derived from analysis run #{run_id}"
            if run_id
            else "Auto-derived from analysis runs"
        )

        if existing is not None:
            if not overwrite:
                skipped += 1
                continue
            existing.legacy_name = legacy_txtsh
            existing.target_name = target_name
            existing.mapping_type = "merge"
            existing.notes = notes
            updated += 1
        else:
            db.add(
                CenterMapping(
                    scope=SCOPE_CLEANUP,
                    object_type="cost_center",
                    legacy_coarea=coarea,
                    legacy_center=legacy_cctr,
                    legacy_name=legacy_txtsh,
                    target_coarea=coarea,
                    target_center=target_cctr,
                    target_name=target_name,
                    mapping_type="merge",
                    notes=notes,
                )
            )
            created += 1

    db.commit()
    return AutoDeriveResult(
        created=created,
        updated=updated,
        skipped=skipped,
        runs_consulted=len(runs),
        source_runs=run_ids,
    )


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


@router.delete("/uploads/{batch_id}")
def delete_upload_by_id(
    batch_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager")),
) -> DeleteResult:
    """Delete a single upload batch by path param (avoids DELETE-with-body issues)."""
    return _delete_upload_ids([batch_id], db)


@router.delete("/uploads")
def delete_uploads(
    body: DeleteByIds | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager")),
) -> DeleteResult:
    if not body or not body.ids:
        raise HTTPException(status_code=400, detail="Provide ids in body")
    return _delete_upload_ids(body.ids, db)


def _delete_upload_ids(ids: list[int], db: Session) -> DeleteResult:
    """Delete upload batches and any data they loaded.

    Only runs cascade cleanup for batches whose status indicates data
    was actually written to the DB ('loaded', 'rolled_back').  Batches
    that are still 'validated', 'validating', or 'failed' never loaded
    data, so we skip cascade and just delete the batch record.
    """
    import logging

    from sqlalchemy.exc import OperationalError

    from app.models.core import DataQualityIssue

    _log = logging.getLogger(__name__)

    # Cascade-delete loaded data only for batches that actually loaded
    for bid in ids:
        batch = db.get(UploadBatch, bid)
        if not batch:
            continue
        if batch.status in ("loaded", "rolled_back"):
            try:
                with db.begin_nested():
                    _cascade_delete_batch_data(batch, db)
            except Exception as exc:
                _log.warning(
                    "delete_uploads: cascade failed for batch %s: %s",
                    bid,
                    exc,
                )

    # Delete upload errors and DQ issues (safe for any status)
    db.execute(delete(UploadError).where(UploadError.batch_id.in_(ids)))
    db.execute(delete(DataQualityIssue).where(DataQualityIssue.batch_id.in_(ids)))

    # Delete the batch records themselves
    try:
        stmt = delete(UploadBatch).where(UploadBatch.id.in_(ids))
        result = db.execute(stmt)
        db.commit()
    except OperationalError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete upload batch(es): {exc}",
        ) from None
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete upload batch(es): {exc}",
        ) from None
    return DeleteResult(table="upload_batch", deleted=result.rowcount)


def _cascade_delete_batch_data(batch: UploadBatch, db: Session) -> int:
    """Delete all data records created by this upload batch."""
    from app.models.core import (
        Balance,
        CenterMapping,
        Employee,
        GLAccountSKA1,
        GLAccountSKB1,
        Hierarchy,
        HierarchyLeaf,
        HierarchyNode,
        LegacyCostCenter,
        LegacyProfitCenter,
        TargetCostCenter,
        TargetProfitCenter,
    )

    deleted = 0
    kind = batch.kind or ""

    # Cost centers
    if kind in ("cost_center", "cost_centers", "cc_with_hierarchy"):
        r = db.execute(delete(LegacyCostCenter).where(LegacyCostCenter.refresh_batch == batch.id))
        deleted += r.rowcount

    # Profit centers
    if kind in ("profit_center", "profit_centers"):
        r = db.execute(
            delete(LegacyProfitCenter).where(LegacyProfitCenter.refresh_batch == batch.id)
        )
        deleted += r.rowcount

    # Balances
    if kind in ("balance", "balances", "balances_gcr"):
        r = db.execute(delete(Balance).where(Balance.refresh_batch == batch.id))
        deleted += r.rowcount

    # Employees
    if kind in ("employee", "employees"):
        r = db.execute(delete(Employee).where(Employee.refresh_batch == batch.id))
        deleted += r.rowcount

    # Hierarchies (standalone or from cc_with_hierarchy)
    if kind in (
        "hierarchy",
        "hierarchies",
        "hierarchies_flat",
        "entity_hierarchy",
        "cc_with_hierarchy",
    ):
        from sqlalchemy import select as sa_select

        hier_ids = [
            h.id
            for h in db.execute(sa_select(Hierarchy).where(Hierarchy.refresh_batch == batch.id))
            .scalars()
            .all()
        ]
        for hid in hier_ids:
            db.execute(delete(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == hid))
            db.execute(delete(HierarchyNode).where(HierarchyNode.hierarchy_id == hid))
        r = db.execute(delete(Hierarchy).where(Hierarchy.refresh_batch == batch.id))
        deleted += r.rowcount

    # GL Accounts
    if kind in ("gl_accounts_ska1", "gl_accounts_group"):
        r = db.execute(delete(GLAccountSKA1).where(GLAccountSKA1.refresh_batch == batch.id))
        deleted += r.rowcount
    if kind == "gl_accounts_skb1":
        r = db.execute(delete(GLAccountSKB1).where(GLAccountSKB1.refresh_batch == batch.id))
        deleted += r.rowcount

    # Target objects
    if kind == "target_cost_centers":
        r = db.execute(delete(TargetCostCenter).where(TargetCostCenter.refresh_batch == batch.id))
        deleted += r.rowcount
    if kind == "target_profit_centers":
        r = db.execute(
            delete(TargetProfitCenter).where(TargetProfitCenter.refresh_batch == batch.id)
        )
        deleted += r.rowcount

    # Center mappings
    if kind == "center_mapping":
        r = db.execute(delete(CenterMapping).where(CenterMapping.refresh_batch == batch.id))
        deleted += r.rowcount

    return deleted


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
    _user: AppUser = Depends(require_role("admin", "data_manager")),
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
    _user: AppUser = Depends(require_role("admin", "data_manager")),
) -> list[dict]:
    """List SAP connections available for OData extraction."""
    from app.services.sap_extraction import list_available_extractions

    return list_available_extractions(db)


@router.get("/browser")
def data_browser(
    scope: str = Query(default=SCOPE_CLEANUP),
    data_category: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    # Cap was 500 (PR #87, perf-driven). PR #91 raises to 200_000 so
    # the hierarchical view can load every CC in one fetch — without
    # this the operator gets HTTP 422 when switching to the
    # hierarchical view because the leaf-count + click-to-detail logic
    # needs the complete set, not just the current page. Tabular still
    # uses small pages by default; only hierarchy mode opts up to the
    # higher limit.
    size: int = Query(default=200, ge=1, le=200_000),
    search: str | None = Query(default=None),
    include_balances: bool = Query(default=False),
    include_hierarchies: bool = Query(default=False),
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "data_manager", "data_manager")),
) -> dict:
    """Unified data browser: paginated centers + optional balances + hierarchies.

    PR #87 — perf rework. The previous version of this endpoint loaded
    every CC, every PC, every monthly balance row (grouped by year+month
    in SQL but still N rows per CC), and every hierarchy with all its
    nodes and leaves, then returned the whole lot as a single JSON
    payload. On a real SAP dataset (10k+ CCs, multi-year balance
    history, several hierarchies with thousands of nodes) the response
    could be tens of megabytes and the frontend then synchronously
    rendered a single table with hundreds of thousands of DOM nodes —
    locking up the browser for many seconds.

    Three knobs to keep the default response small:

    * **page / size** — server-side pagination, default ``size=200``
      capped at ``500``. Initial page paints in well under a second
      regardless of dataset size. ``total`` is returned so the UI can
      build a pager.
    * **include_balances** — default ``False``. When the caller asks,
      monthly balance summaries are attached to each item *on the
      current page only*, not for the whole table. The frontend uses
      this for an expand-on-click pattern.
    * **include_hierarchies** — default ``False``. Hierarchies are
      cheap to enumerate (a handful of rows from ``cleanup.hierarchy``)
      but expensive to expand (thousands of nodes + leaves per
      hierarchy). When omitted the response carries hierarchy *metadata*
      only — id, setname, setclass, label, leaf count — and the
      frontend fetches full structure via the existing
      ``/api/legacy/hierarchies/{id}/nodes`` + ``/leaves`` endpoints
      when the operator drills in. Setting this flag restores the old
      "everything inline" shape for callers that need it.

    Optional ``search`` filter pushes a single LIKE query down to the
    DB so the operator can narrow results without paying client-side
    filtering on every keystroke. Matches against ``cctr``, ``txtsh``,
    ``txtmi``, ``ccode``, ``responsible``.
    """
    cc_q = select(LegacyCostCenter).where(LegacyCostCenter.scope == scope)
    cc_count_q = select(func.count(LegacyCostCenter.id)).where(LegacyCostCenter.scope == scope)
    if data_category:
        cc_q = cc_q.where(LegacyCostCenter.data_category == data_category)
        cc_count_q = cc_count_q.where(LegacyCostCenter.data_category == data_category)
    if search:
        # ILIKE on the searchable text columns. Wrap once at the boundary
        # so the param is parameterised by SQLAlchemy (not f-string'd).
        pattern = f"%{search}%"
        text_filter = (
            LegacyCostCenter.cctr.ilike(pattern)
            | LegacyCostCenter.txtsh.ilike(pattern)
            | LegacyCostCenter.txtmi.ilike(pattern)
            | LegacyCostCenter.ccode.ilike(pattern)
            | LegacyCostCenter.responsible.ilike(pattern)
        )
        cc_q = cc_q.where(text_filter)
        cc_count_q = cc_count_q.where(text_filter)

    total = db.execute(cc_count_q).scalar() or 0
    cc_q = cc_q.order_by(LegacyCostCenter.cctr).offset((page - 1) * size).limit(size)
    ccs = db.execute(cc_q).scalars().all()

    # Profit-center lookup is page-bounded: only fetch PCs referenced by
    # the CCs on this page. For 200 CCs this is a tiny IN-list query
    # rather than the whole PC table.
    pc_keys = {(c.coarea, c.pctr) for c in ccs if c.pctr}
    pc_map: dict[tuple[str, str], LegacyProfitCenter] = {}
    if pc_keys:
        coareas = list({k[0] for k in pc_keys})
        pctrs = list({k[1] for k in pc_keys})
        pc_q = (
            select(LegacyProfitCenter)
            .where(LegacyProfitCenter.scope == scope)
            .where(LegacyProfitCenter.coarea.in_(coareas))
            .where(LegacyProfitCenter.pctr.in_(pctrs))
        )
        if data_category:
            pc_q = pc_q.where(LegacyProfitCenter.data_category == data_category)
        for p in db.execute(pc_q).scalars().all():
            pc_map[(p.coarea, p.pctr)] = p

    # Balances — also page-bounded when requested. Same rationale: the
    # query returns one row per (coarea, cctr, year, period) so even a
    # bounded set can grow with history depth, but capping by the
    # current page's CCs keeps this from being the limiting factor.
    balance_map: dict[tuple[str, str], list[dict]] = {}
    if include_balances and ccs:
        cctrs_on_page = list({c.cctr for c in ccs})
        coareas_on_page = list({c.coarea for c in ccs})
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
            .where(Balance.cctr.in_(cctrs_on_page))
            .where(Balance.coarea.in_(coareas_on_page))
            .group_by(Balance.coarea, Balance.cctr, Balance.fiscal_year, Balance.period)
            .order_by(Balance.coarea, Balance.cctr, Balance.fiscal_year, Balance.period)
        )
        if data_category:
            bal_q = bal_q.where(Balance.data_category == data_category)
        for coarea, cctr, fy, per, amt, post, curr in db.execute(bal_q).all():
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
        item = {
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
        }
        if include_balances:
            item["monthly_balances"] = balance_map.get((c.coarea, c.cctr), [])
        items.append(item)

    # Hierarchies — metadata always, full structure only when asked.
    # The metadata fits in well under a kilobyte even with several
    # hierarchies; the full structure can be megabytes.
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
    hier_payload: list[dict] = []
    for h in hiers:
        base = f"{cls_labels.get(h.setclass, h.setclass)}: {h.setname}"
        if h.description:
            base += f" — {h.description}"
        meta: dict = {
            "id": h.id,
            "setname": h.setname,
            "setclass": h.setclass,
            "description": h.description,
            "label": h.label or base,
            "coarea": h.coarea,
        }
        if include_hierarchies:
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
            meta["nodes"] = [
                {"parent": n.parent_setname, "child": n.child_setname, "seq": n.seq} for n in nodes
            ]
            meta["leaves"] = [
                {"setname": lf.setname, "value": lf.value, "seq": lf.seq} for lf in leaves
            ]
        hier_payload.append(meta)

    return {
        "total": total,
        "page": page,
        "size": size,
        "items": items,
        "hierarchies": hier_payload,
        # Hint to the frontend: did we return full structure or just
        # metadata? Lets the UI know to fetch nodes/leaves on demand.
        "hierarchies_inlined": include_hierarchies,
    }
