"""Public explore / visualization endpoints — no authentication required.

Provides read-only access to legacy data, analysis results (ampliFi), and
mapping views for the public data visualization page.

Now supports:
- Dynamic column selection via ExplorerDisplayConfig
- GL Accounts (SKA1/SKB1)
- Excel/CSV export
- Detail view data
"""

from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import func, inspect, select
from sqlalchemy.orm import Session

from app.infra.db.session import get_db
from app.models.core import (
    SCOPE_EXPLORER,
    AnalysisRun,
    Balance,
    CenterMapping,
    CenterProposal,
    Employee,
    Entity,
    ExplorerDisplayConfig,
    GLAccountClassRange,
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

router = APIRouter()

# ── Object type → model mapping ──────────────────────────────────────────

_OBJECT_MODELS: dict[str, Any] = {
    "cost-centers": LegacyCostCenter,
    "profit-centers": LegacyProfitCenter,
    "entities": Entity,
    "employees": Employee,
    "gl-accounts-ska1": GLAccountSKA1,
    "gl-accounts-skb1": GLAccountSKB1,
    "balances": Balance,
    "hierarchies": Hierarchy,
    "gl-accounts": GLAccountClassRange,
}

# Default columns per object type (when no ExplorerDisplayConfig exists)
_DEFAULT_TABLE_COLUMNS: dict[str, list[str]] = {
    "cost-centers": [
        "cctr",
        "txtsh",
        "ccode",
        "coarea",
        "pctr",
        "responsible",
        "cctrcgy",
        "currency",
        "is_active",
    ],
    "profit-centers": [
        "pctr",
        "txtsh",
        "ccode",
        "coarea",
        "responsible",
        "department",
        "currency",
        "is_active",
    ],
    "entities": [
        "ccode",
        "name",
        "city",
        "country",
        "region",
        "currency",
        "language",
        "is_active",
    ],
    "employees": [
        "gpn",
        "bs_name",
        "bs_firstname",
        "bs_lastname",
        "ou_cd",
        "ou_desc",
        "local_cc_cd",
        "job_desc",
        "email_address",
    ],
    "gl-accounts-ska1": [
        "ktopl",
        "saknr",
        "txt20",
        "txt50",
        "xbilk",
        "gvtyp",
        "ktoks",
        "bilkt",
        "func_area",
        "glaccount_type",
    ],
    "gl-accounts-skb1": [
        "bukrs",
        "saknr",
        "stext",
        "waers",
        "mitkz",
        "mwskz",
        "fstag",
        "xopvw",
        "xkres",
        "xintb",
    ],
    "balances": [
        "coarea",
        "cctr",
        "ccode",
        "fiscal_year",
        "period",
        "account",
        "tc_amt",
        "currency_tc",
    ],
    "gl-accounts": [
        "class_code",
        "class_label",
        "from_account",
        "to_account",
        "category",
    ],
}

# Search fields per object
_SEARCH_FIELDS: dict[str, list[str]] = {
    "cost-centers": ["cctr", "txtsh", "ccode", "responsible"],
    "profit-centers": ["pctr", "txtsh", "ccode", "responsible"],
    "entities": ["ccode", "name", "country"],
    "employees": ["gpn", "bs_name", "email_address", "ou_cd"],
    "gl-accounts-ska1": ["saknr", "txt20", "txt50", "ktopl", "ktoks"],
    "gl-accounts-skb1": ["saknr", "stext", "bukrs", "waers"],
    "balances": ["cctr", "ccode", "coarea"],
    "gl-accounts": ["class_code", "class_label"],
}

# Sort key per object
_DEFAULT_SORT: dict[str, str] = {
    "cost-centers": "cctr",
    "profit-centers": "pctr",
    "entities": "ccode",
    "employees": "gpn",
    "gl-accounts-ska1": "saknr",
    "gl-accounts-skb1": "saknr",
    "balances": "cctr",
    "gl-accounts": "class_code",
}


# Human-readable default labels for technical field names
_DEFAULT_COLUMN_LABELS: dict[str, str] = {
    # Cost Centers (CSKS)
    "cctr": "Cost Center",
    "txtsh": "Short Text",
    "txtmd": "Medium Text",
    "txtlg": "Long Text",
    "ccode": "Company Code",
    "coarea": "Controlling Area",
    "pctr": "Profit Center",
    "responsible": "Responsible Person",
    "cctrcgy": "Category",
    "currency": "Currency",
    "is_active": "Active",
    "valid_from": "Valid From",
    "valid_to": "Valid To",
    "func_area": "Functional Area",
    "department": "Department",
    # Profit Centers (CEPC)
    "segment": "Segment",
    # Entities (T001)
    "name": "Name",
    "city": "City",
    "country": "Country",
    "region": "Region",
    "language": "Language",
    "fiscal_year_variant": "Fiscal Year Variant",
    "chart_of_accounts": "Chart of Accounts",
    # Employees
    "gpn": "GPN",
    "bs_name": "Full Name",
    "bs_firstname": "First Name",
    "bs_lastname": "Last Name",
    "ou_cd": "Org Unit Code",
    "ou_desc": "Org Unit",
    "local_cc_cd": "Local Cost Center",
    "job_desc": "Job Title",
    "email_address": "Email",
    # GL Accounts (SKA1)
    "ktopl": "Chart of Accounts",
    "saknr": "Account Number",
    "txt20": "Short Text",
    "txt50": "Long Text",
    "xbilk": "BS Indicator",
    "gvtyp": "P&L Type",
    "ktoks": "Account Group",
    "bilkt": "Alt Account",
    "glaccount_type": "Account Type",
    # GL Accounts (SKB1)
    "bukrs": "Company Code",
    "stext": "Description",
    "waers": "Currency",
    "mitkz": "Reconciliation",
    "mwskz": "Tax Category",
    "fstag": "Field Status Group",
    "xopvw": "Open Item Mgmt",
    "xkres": "Line Item Display",
    "xintb": "Post Automatically",
    # Balances
    "fiscal_year": "Fiscal Year",
    "period": "Period",
    "account": "Account",
    "tc_amt": "Amount (TC)",
    "currency_tc": "Currency (TC)",
    "gc_amt": "Amount (GC)",
    "currency_gc": "Currency (GC)",
    # GL Account Ranges
    "class_code": "Class Code",
    "class_label": "Class Label",
    "from_account": "From Account",
    "to_account": "To Account",
    "category": "Category",
    # Hierarchies
    "setname": "Hierarchy Name",
    "setclass": "Set Class",
    "type_label": "Type",
    "label": "Label",
    "parent_set": "Parent",
    "level": "Level",
    # Common
    "id": "ID",
    "created_at": "Created",
    "updated_at": "Updated",
    "source_system": "Source System",
    "refresh_batch": "Refresh Batch",
}


def _get_model_columns(model: Any) -> list[str]:
    """Get all column names from a SQLAlchemy model."""
    mapper = inspect(model)
    return [c.key for c in mapper.column_attrs]


def _get_display_config(db: Session, object_type: str) -> dict:
    """Get display configuration for an object type."""
    cfg = db.execute(
        select(ExplorerDisplayConfig).where(ExplorerDisplayConfig.object_type == object_type)
    ).scalar_one_or_none()
    if cfg:
        return {
            "table_columns": cfg.table_columns or [],
            "detail_columns": cfg.detail_columns or [],
            "column_labels": cfg.column_labels or {},
            "default_sort_column": cfg.default_sort_column,
            "default_sort_dir": cfg.default_sort_dir or "asc",
        }
    return {
        "table_columns": _DEFAULT_TABLE_COLUMNS.get(object_type, []),
        "detail_columns": [],
        "column_labels": {},
        "default_sort_column": _DEFAULT_SORT.get(object_type),
        "default_sort_dir": "asc",
    }


def _row_to_dict(row: Any, columns: list[str]) -> dict:
    """Convert a model instance to a dict with only specified columns."""
    result: dict = {"id": row.id}
    for col in columns:
        val = getattr(row, col, None)
        if val is not None:
            result[col] = val
        else:
            result[col] = None
    return result


def _row_to_full_dict(row: Any) -> dict:
    """Convert a model instance to a dict with all columns."""
    mapper = inspect(type(row))
    result: dict = {}
    for c in mapper.column_attrs:
        val = getattr(row, c.key, None)
        if val is not None:
            result[c.key] = val
        else:
            result[c.key] = None
    return result


# ── Counts / overview ────────────────────────────────────────────────────


@router.get("/counts")
def explore_counts(db: Session = Depends(get_db)) -> dict:
    """Object counts for the explore dashboard — only explorer-scoped data."""

    def _cnt(model: type) -> int:
        return (
            db.execute(select(func.count(model.id)).where(model.scope == SCOPE_EXPLORER)).scalar()
            or 0
        )

    return {
        "entities": _cnt(Entity),
        "cost_centers": _cnt(LegacyCostCenter),
        "profit_centers": _cnt(LegacyProfitCenter),
        "balances": _cnt(Balance),
        "hierarchies": _cnt(Hierarchy),
        "employees": _cnt(Employee),
        "gl_accounts_ska1": _cnt(GLAccountSKA1),
        "gl_accounts_skb1": _cnt(GLAccountSKB1),
        "gl_ranges": db.execute(select(func.count(GLAccountClassRange.id))).scalar() or 0,
        "target_cost_centers": _cnt(TargetCostCenter),
        "target_profit_centers": _cnt(TargetProfitCenter),
        "center_mappings": db.execute(
            select(func.count(CenterMapping.id)).where(CenterMapping.scope == SCOPE_EXPLORER)
        ).scalar()
        or 0,
        "proposals": db.execute(select(func.count(CenterProposal.id))).scalar() or 0,
    }


# ── Display config (public) ─────────────────────────────────────────────


@router.get("/display-config/{object_type}")
def get_display_config(object_type: str, db: Session = Depends(get_db)) -> dict:
    """Public: get display configuration for an object type."""
    model = _OBJECT_MODELS.get(object_type)
    all_columns = _get_model_columns(model) if model else []
    config = _get_display_config(db, object_type)

    # Build full label mapping: defaults + custom overrides (for all available columns)
    all_labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in all_columns}
    all_labels.update(config.get("column_labels", {}))

    return {
        "object_type": object_type,
        "all_columns": all_columns,
        "default_labels": dict(_DEFAULT_COLUMN_LABELS),
        **config,
        "column_labels": all_labels,
    }


# ── Legacy: Balances (aggregated) — must be before generic {object_type} ──


@router.get("/legacy/balances-agg")
def explore_balances_agg(
    db: Session = Depends(get_db),
    coarea: str | None = None,
    cctr: str | None = None,
    max_rows: int = Query(50000, ge=1, le=200000),
) -> dict:
    query = (
        select(
            Balance.coarea,
            Balance.cctr,
            Balance.fiscal_year,
            Balance.period,
            func.coalesce(func.sum(Balance.tc_amt), 0).label("amt"),
            func.coalesce(func.sum(Balance.posting_count), 0).label("post"),
            func.max(Balance.currency_tc).label("currency"),
        )
        .where(Balance.scope == SCOPE_EXPLORER)
        .group_by(Balance.coarea, Balance.cctr, Balance.fiscal_year, Balance.period)
        .order_by(Balance.coarea, Balance.cctr, Balance.fiscal_year, Balance.period)
    )
    if coarea:
        query = query.where(Balance.coarea == coarea)
    if cctr:
        query = query.where(Balance.cctr == cctr)
    rows = db.execute(query.limit(max_rows)).all()
    items: dict[str, list[dict]] = {}
    for ca, cc, fy, per, amt, post, curr in rows:
        key = f"{ca}:{cc}"
        items.setdefault(key, []).append(
            {
                "fiscal_year": fy,
                "period": per,
                "amount": float(amt),
                "postings": int(post),
                "currency": curr or "",
            }
        )
    return {"total_keys": len(items), "balances": items}


# ── Generic object data endpoint ─────────────────────────────────────────


@router.get("/legacy/{object_type}")
def explore_object(
    object_type: str,
    db: Session = Depends(get_db),
    search: str | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=10000),
    sort: str | None = None,
    sort_dir: str | None = None,
) -> dict:
    """Generic endpoint: fetch data for any object type with dynamic columns."""
    # Special case: hierarchies have their own endpoint
    if object_type == "hierarchies":
        return _explore_hierarchies(db, search)

    model = _OBJECT_MODELS.get(object_type)
    if not model:
        return {"error": f"Unknown object type: {object_type}", "items": [], "total": 0}

    config = _get_display_config(db, object_type)
    table_cols = config["table_columns"]

    # Build query — only explorer-scoped data
    query = select(model)
    count_q = select(func.count(model.id))
    if hasattr(model, "scope"):
        query = query.where(model.scope == SCOPE_EXPLORER)
        count_q = count_q.where(model.scope == SCOPE_EXPLORER)

    # Search
    if search:
        pat = f"%{search}%"
        search_fields = _SEARCH_FIELDS.get(object_type, [])
        if search_fields:
            conditions = []
            for field_name in search_fields:
                col = getattr(model, field_name, None)
                if col is not None:
                    conditions.append(col.ilike(pat))
            if conditions:
                from sqlalchemy import or_

                flt = or_(*conditions)
                query = query.where(flt)
                count_q = count_q.where(flt)

    # Sort (validate against actual column names to prevent 500 on non-column attrs)
    sort_col_name = sort or config.get("default_sort_column") or _DEFAULT_SORT.get(object_type)
    if sort_col_name:
        from sqlalchemy import inspect as sa_inspect

        valid_columns = {c.key for c in sa_inspect(model).column_attrs}
        if sort_col_name in valid_columns:
            sort_attr = getattr(model, sort_col_name)
            direction = sort_dir or config.get("default_sort_dir", "asc")
            if direction == "desc":
                query = query.order_by(sort_attr.desc().nulls_last())
            else:
                query = query.order_by(sort_attr.asc().nulls_last())

    total = db.execute(count_q).scalar() or 0
    rows = db.execute(query.offset((page - 1) * size).limit(size)).scalars().all()

    # Merge default labels with custom overrides
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in table_cols}
    labels.update({k: v for k, v in config.get("column_labels", {}).items() if k in table_cols})

    return {
        "total": total,
        "page": page,
        "size": size,
        "columns": table_cols,
        "column_labels": labels,
        "items": [_row_to_dict(r, table_cols) for r in rows],
    }


# ── Detail view ──────────────────────────────────────────────────────────


@router.get("/legacy/{object_type}/{item_id}")
def explore_object_detail(
    object_type: str,
    item_id: int,
    db: Session = Depends(get_db),
) -> dict:
    """Get full detail for a single item."""
    model = _OBJECT_MODELS.get(object_type)
    if not model:
        return {"error": f"Unknown object type: {object_type}"}

    row = db.get(model, item_id)
    if not row:
        return {"error": "Not found"}

    config = _get_display_config(db, object_type)
    detail_cols = config["detail_columns"]

    data = _row_to_dict(row, detail_cols) if detail_cols else _row_to_full_dict(row)

    # Merge default labels with custom overrides
    cols_for_labels = detail_cols if detail_cols else list(data.keys())
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in cols_for_labels}
    custom = config.get("column_labels", {})
    labels.update({k: v for k, v in custom.items() if k in cols_for_labels})

    return {"item": data, "detail_columns": detail_cols, "column_labels": labels}


# ── Hierarchies (special) ───────────────────────────────────────────────


def _explore_hierarchies(db: Session, search: str | None = None) -> dict:
    """Hierarchies endpoint with node/leaf data."""
    cls_labels = {"0101": "Cost Center", "0104": "Profit Center", "0106": "Entity"}
    query = select(Hierarchy).where(
        Hierarchy.scope == SCOPE_EXPLORER, Hierarchy.is_active.is_(True)
    )
    if search:
        pat = f"%{search}%"
        from sqlalchemy import or_

        query = query.where(
            or_(
                Hierarchy.setname.ilike(pat),
                Hierarchy.label.ilike(pat),
                Hierarchy.description.ilike(pat),
                Hierarchy.coarea.ilike(pat),
            )
        )
    hiers = db.execute(query.order_by(Hierarchy.setclass, Hierarchy.setname)).scalars().all()
    result = []
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
        result.append(
            {
                "id": h.id,
                "setname": h.setname,
                "setclass": h.setclass,
                "type_label": cls_labels.get(h.setclass, h.setclass),
                "label": h.label or base,
                "coarea": h.coarea,
                "nodes": [
                    {"parent": n.parent_setname, "child": n.child_setname, "seq": n.seq}
                    for n in nodes
                ],
                "leaves": [
                    {"setname": lf.setname, "value": lf.value, "seq": lf.seq} for lf in leaves
                ],
            }
        )
    cols = ["setname", "setclass", "type_label", "label", "coarea"]
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in cols}
    return {"total": len(result), "hierarchies": result, "columns": cols, "column_labels": labels}


# ── Export (CSV/Excel) ───────────────────────────────────────────────────


@router.get("/export/{object_type}")
def export_object(
    object_type: str,
    db: Session = Depends(get_db),
    export_format: str = Query("csv", alias="format"),
    search: str | None = None,
) -> StreamingResponse:
    """Export object data as CSV or Excel."""
    model = _OBJECT_MODELS.get(object_type)
    if not model:
        return StreamingResponse(
            io.BytesIO(b"Unknown object type"),
            media_type="text/plain",
            status_code=400,
        )

    config = _get_display_config(db, object_type)
    table_cols = config["table_columns"]
    if not table_cols:
        table_cols = _get_model_columns(model)
        # Remove internal columns
        for skip in ("id", "created_at", "updated_at"):
            if skip in table_cols:
                table_cols.remove(skip)

    # Build query — only explorer-scoped data
    query = select(model)
    if hasattr(model, "scope"):
        query = query.where(model.scope == SCOPE_EXPLORER)
    if search:
        pat = f"%{search}%"
        search_fields = _SEARCH_FIELDS.get(object_type, [])
        if search_fields:
            conditions = []
            for field_name in search_fields:
                col = getattr(model, field_name, None)
                if col is not None:
                    conditions.append(col.ilike(pat))
            if conditions:
                from sqlalchemy import or_

                query = query.where(or_(*conditions))

    sort_col_name = _DEFAULT_SORT.get(object_type)
    if sort_col_name:
        sort_attr = getattr(model, sort_col_name, None)
        if sort_attr is not None:
            query = query.order_by(sort_attr.asc().nulls_last())

    rows = db.execute(query.limit(50000)).scalars().all()

    # Build display labels for export headers
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in table_cols}
    labels.update({k: v for k, v in config.get("column_labels", {}).items() if k in table_cols})
    header_row = [labels.get(c, c) for c in table_cols]

    if export_format == "excel":
        try:
            import openpyxl

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = object_type
            ws.append(header_row)
            for row in rows:
                ws.append(
                    ["" if (v := getattr(row, c, None)) is None else str(v) for c in table_cols]
                )
            buf = io.BytesIO()
            wb.save(buf)
            buf.seek(0)
            return StreamingResponse(
                buf,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={object_type}.xlsx"},
            )
        except ImportError:
            pass  # Fall back to CSV

    # CSV export
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header_row)
    for row in rows:
        writer.writerow(
            ["" if (v := getattr(row, c, None)) is None else str(v) for c in table_cols]
        )
    content = buf.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={object_type}.csv"},
    )


# ── ampliFi: Mapping (Legacy → Target) ──────────────────────────────────


@router.get("/amplifi/mapping")
def explore_mapping(
    db: Session = Depends(get_db),
    run_id: int | None = None,
) -> dict:
    """Latest analysis results: legacy CC → proposed target mapping."""
    if run_id is not None:
        run = db.execute(select(AnalysisRun).where(AnalysisRun.id == run_id)).scalars().first()
    else:
        run = (
            db.execute(
                select(AnalysisRun)
                .where(AnalysisRun.status == "completed")
                .order_by(AnalysisRun.finished_at.desc())
            )
            .scalars()
            .first()
        )
    if not run:
        return {"run_id": None, "items": [], "summary": {}}

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == run.id)).scalars().all()
    )
    cc_ids = [p.legacy_cc_id for p in proposals]
    ccs = (
        {
            c.id: c
            for c in db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids)))
            .scalars()
            .all()
        }
        if cc_ids
        else {}
    )

    items = []
    summary: dict[str, int] = {}
    for p in proposals:
        cc = ccs.get(p.legacy_cc_id)
        outcome = p.override_outcome or p.cleansing_outcome
        summary[outcome] = summary.get(outcome, 0) + 1
        items.append(
            {
                "id": p.id,
                "legacy_cctr": cc.cctr if cc else None,
                "legacy_txtsh": cc.txtsh if cc else None,
                "legacy_ccode": cc.ccode if cc else None,
                "legacy_coarea": cc.coarea if cc else None,
                "legacy_pctr": cc.pctr if cc else None,
                "outcome": outcome,
                "target_object": p.target_object,
                "merge_into_cctr": p.merge_into_cctr,
                "confidence": str(p.confidence) if p.confidence else None,
                "rule_path": p.rule_path,
            }
        )

    return {
        "run_id": run.id,
        "run_label": f"Run #{run.id}" + (f" (Wave {run.wave_id})" if run.wave_id else " (Global)"),
        "run_status": run.status,
        "total": len(items),
        "summary": summary,
        "items": items,
    }


# ── ampliFi: Available Runs ─────────────────────────────────────────────


@router.get("/amplifi/runs")
def explore_runs(db: Session = Depends(get_db)) -> dict:
    runs = (
        db.execute(
            select(AnalysisRun)
            .where(AnalysisRun.status == "completed")
            .order_by(AnalysisRun.finished_at.desc())
        )
        .scalars()
        .all()
    )
    return {
        "total": len(runs),
        "runs": [
            {
                "id": r.id,
                "label": f"Run #{r.id}" + (f" (Wave {r.wave_id})" if r.wave_id else " (Global)"),
                "status": r.status,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            }
            for r in runs
        ],
    }


# ── Data Source Configuration ────────────────────────────────────────────


@router.get("/data-sources")
def list_data_sources(db: Session = Depends(get_db)) -> dict:
    return {
        "sources": [
            {
                "object": "cost_centers",
                "label": "Cost Centers",
                "source": "Local DB",
                "count": db.execute(select(func.count(LegacyCostCenter.id))).scalar() or 0,
            },
            {
                "object": "profit_centers",
                "label": "Profit Centers",
                "source": "Local DB",
                "count": db.execute(select(func.count(LegacyProfitCenter.id))).scalar() or 0,
            },
            {
                "object": "entities",
                "label": "Entities",
                "source": "Local DB",
                "count": db.execute(select(func.count(Entity.id))).scalar() or 0,
            },
            {
                "object": "balances",
                "label": "Balances",
                "source": "Local DB",
                "count": db.execute(select(func.count(Balance.id))).scalar() or 0,
            },
            {
                "object": "hierarchies",
                "label": "Hierarchies",
                "source": "Local DB",
                "count": db.execute(select(func.count(Hierarchy.id))).scalar() or 0,
            },
            {
                "object": "employees",
                "label": "Employees",
                "source": "Local DB",
                "count": db.execute(select(func.count(Employee.id))).scalar() or 0,
            },
            {
                "object": "gl_accounts_ska1",
                "label": "GL Accounts (Chart of Accounts)",
                "source": "Local DB",
                "count": db.execute(select(func.count(GLAccountSKA1.id))).scalar() or 0,
            },
            {
                "object": "gl_accounts_skb1",
                "label": "GL Accounts (Company Code)",
                "source": "Local DB",
                "count": db.execute(select(func.count(GLAccountSKB1.id))).scalar() or 0,
            },
            {
                "object": "gl_ranges",
                "label": "GL Account Ranges",
                "source": "Local DB",
                "count": db.execute(select(func.count(GLAccountClassRange.id))).scalar() or 0,
            },
        ]
    }


@router.get("/source-config")
def explore_source_config(db: Session = Depends(get_db)) -> dict:
    """Public endpoint: returns enabled explorer source configs per area."""
    from app.models.core import ExplorerSourceConfig

    stmt = (
        select(ExplorerSourceConfig)
        .where(ExplorerSourceConfig.enabled.is_(True))
        .order_by(ExplorerSourceConfig.area, ExplorerSourceConfig.display_order)
    )
    rows = db.execute(stmt).scalars().all()
    legacy = []
    amplifi = []
    for r in rows:
        item = {
            "object_type": r.object_type,
            "label": r.label,
            "source_system": r.source_system,
            "protocol": r.protocol,
            "mode": r.mode,
        }
        if r.area == "amplifi":
            amplifi.append(item)
        else:
            legacy.append(item)
    return {"legacy": legacy, "amplifi": amplifi}
