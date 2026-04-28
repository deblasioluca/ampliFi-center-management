"""Public explore / visualization endpoints — no authentication required.

Provides read-only access to legacy data, analysis results (ampliFi), and
mapping views for the public data visualization page.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.infra.db.session import get_db
from app.models.core import (
    AnalysisRun,
    Balance,
    CenterProposal,
    Employee,
    Entity,
    GLAccountClassRange,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
)

router = APIRouter()


# ── Counts / overview ────────────────────────────────────────────────────


@router.get("/counts")
def explore_counts(db: Session = Depends(get_db)) -> dict:
    """Object counts for the explore dashboard."""
    return {
        "entities": db.execute(select(func.count(Entity.id))).scalar() or 0,
        "cost_centers": db.execute(select(func.count(LegacyCostCenter.id))).scalar() or 0,
        "profit_centers": db.execute(select(func.count(LegacyProfitCenter.id))).scalar() or 0,
        "balances": db.execute(select(func.count(Balance.id))).scalar() or 0,
        "hierarchies": db.execute(select(func.count(Hierarchy.id))).scalar() or 0,
        "employees": db.execute(select(func.count(Employee.id))).scalar() or 0,
        "gl_ranges": db.execute(select(func.count(GLAccountClassRange.id))).scalar() or 0,
        "proposals": db.execute(select(func.count(CenterProposal.id))).scalar() or 0,
    }


# ── Legacy: Cost Centers ─────────────────────────────────────────────────


@router.get("/legacy/cost-centers")
def explore_cost_centers(
    db: Session = Depends(get_db),
    ccode: str | None = None,
    coarea: str | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=5000),
) -> dict:
    query = select(LegacyCostCenter).order_by(LegacyCostCenter.cctr)
    count_q = select(func.count(LegacyCostCenter.id))
    if ccode:
        query = query.where(LegacyCostCenter.ccode == ccode)
        count_q = count_q.where(LegacyCostCenter.ccode == ccode)
    if coarea:
        query = query.where(LegacyCostCenter.coarea == coarea)
        count_q = count_q.where(LegacyCostCenter.coarea == coarea)
    if search:
        pat = f"%{search}%"
        flt = LegacyCostCenter.cctr.ilike(pat) | LegacyCostCenter.txtsh.ilike(pat)
        query = query.where(flt)
        count_q = count_q.where(flt)
    total = db.execute(count_q).scalar() or 0
    rows = db.execute(query.offset((page - 1) * size).limit(size)).scalars().all()
    return {
        "total": total,
        "page": page,
        "size": size,
        "items": [
            {
                "id": c.id,
                "cctr": c.cctr,
                "txtsh": c.txtsh,
                "txtmi": c.txtmi,
                "ccode": c.ccode,
                "coarea": c.coarea,
                "pctr": c.pctr,
                "responsible": c.responsible,
                "cctrcgy": c.cctrcgy,
                "currency": c.currency,
                "is_active": c.is_active,
            }
            for c in rows
        ],
    }


# ── Legacy: Profit Centers ───────────────────────────────────────────────


@router.get("/legacy/profit-centers")
def explore_profit_centers(
    db: Session = Depends(get_db),
    ccode: str | None = None,
    coarea: str | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=5000),
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
        pat = f"%{search}%"
        flt = LegacyProfitCenter.pctr.ilike(pat) | LegacyProfitCenter.txtsh.ilike(pat)
        query = query.where(flt)
        count_q = count_q.where(flt)
    total = db.execute(count_q).scalar() or 0
    rows = db.execute(query.offset((page - 1) * size).limit(size)).scalars().all()
    return {
        "total": total,
        "page": page,
        "size": size,
        "items": [
            {
                "id": p.id,
                "pctr": p.pctr,
                "txtsh": p.txtsh,
                "txtmi": p.txtmi,
                "ccode": p.ccode,
                "coarea": p.coarea,
                "responsible": p.responsible,
                "department": p.department,
                "currency": p.currency,
                "is_active": p.is_active,
            }
            for p in rows
        ],
    }


# ── Legacy: Entities ─────────────────────────────────────────────────────


@router.get("/legacy/entities")
def explore_entities(db: Session = Depends(get_db)) -> dict:
    rows = db.execute(select(Entity).order_by(Entity.ccode)).scalars().all()
    return {
        "total": len(rows),
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
            for e in rows
        ],
    }


# ── Legacy: GL Account Ranges ────────────────────────────────────────────


@router.get("/legacy/gl-accounts")
def explore_gl_accounts(db: Session = Depends(get_db)) -> dict:
    rows = (
        db.execute(
            select(GLAccountClassRange).order_by(
                GLAccountClassRange.class_code, GLAccountClassRange.from_account
            )
        )
        .scalars()
        .all()
    )
    return {
        "total": len(rows),
        "items": [
            {
                "id": r.id,
                "class_code": r.class_code,
                "class_label": r.class_label,
                "from_account": r.from_account,
                "to_account": r.to_account,
                "category": r.category,
            }
            for r in rows
        ],
    }


# ── Legacy: Balances (aggregated) ────────────────────────────────────────


@router.get("/legacy/balances")
def explore_balances(
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


# ── Legacy: Hierarchies ─────────────────────────────────────────────────


@router.get("/legacy/hierarchies")
def explore_hierarchies(db: Session = Depends(get_db)) -> dict:
    cls_labels = {"0101": "Cost Center", "0104": "Profit Center", "0106": "Entity"}
    hiers = (
        db.execute(
            select(Hierarchy)
            .where(Hierarchy.is_active.is_(True))
            .order_by(Hierarchy.setclass, Hierarchy.setname)
        )
        .scalars()
        .all()
    )
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
    return {"total": len(result), "hierarchies": result}


# ── Legacy: Employees ────────────────────────────────────────────────────


@router.get("/legacy/employees")
def explore_employees(
    db: Session = Depends(get_db),
    search: str | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(200, ge=1, le=2000),
) -> dict:
    query = select(Employee).order_by(Employee.gpn)
    count_q = select(func.count(Employee.id))
    if search:
        pat = f"%{search}%"
        flt = Employee.gpn.ilike(pat) | Employee.bs_name.ilike(pat) | Employee.ou_cd.ilike(pat)
        query = query.where(flt)
        count_q = count_q.where(flt)
    total = db.execute(count_q).scalar() or 0
    rows = db.execute(query.offset((page - 1) * size).limit(size)).scalars().all()
    return {
        "total": total,
        "page": page,
        "size": size,
        "items": [
            {
                "id": e.id,
                "gpn": e.gpn,
                "bs_name": e.bs_name,
                "bs_firstname": e.bs_firstname,
                "bs_lastname": e.bs_lastname,
                "ou_cd": e.ou_cd,
                "ou_desc": e.ou_desc,
                "local_cc_cd": e.local_cc_cd,
                "local_cc_desc": e.local_cc_desc,
                "job_desc": e.job_desc,
                "rank_desc": e.rank_desc,
                "email_address": e.email_address,
                "emp_status": e.emp_status,
            }
            for e in rows
        ],
    }


# ── ampliFi: Mapping (Legacy → Target) ──────────────────────────────────


@router.get("/amplifi/mapping")
def explore_mapping(
    db: Session = Depends(get_db),
    run_id: int | None = None,
) -> dict:
    """Latest analysis results: legacy CC → proposed target mapping.

    If run_id is not given, picks the most recent completed run.
    """
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
    """Return available data sources per object type."""
    # For now, return DB counts as indicator of data availability
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
                "object": "gl_accounts",
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

    stmt = select(ExplorerSourceConfig).where(
        ExplorerSourceConfig.enabled.is_(True)
    ).order_by(ExplorerSourceConfig.area, ExplorerSourceConfig.display_order)
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
