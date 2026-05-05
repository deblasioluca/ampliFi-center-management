"""Reference data endpoints (section 11.10) — browse all data types."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination
from app.infra.db.session import get_db
from app.models.core import (
    Balance,
    CenterMapping,
    Employee,
    Entity,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
    TargetCostCenter,
    TargetProfitCenter,
    UploadBatch,
)

router = APIRouter()


@router.get("/entities")
def list_entities(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    country: str | None = None,
    search: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(Entity).order_by(Entity.ccode)
    count_q = select(func.count(Entity.id))
    if scope:
        query = query.where(Entity.scope == scope)
        count_q = count_q.where(Entity.scope == scope)
    if data_category:
        query = query.where(Entity.data_category == data_category)
        count_q = count_q.where(Entity.data_category == data_category)
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
                "mandt": e.mandt,
                "ccode": e.ccode,
                "name": e.name,
                "country": e.country,
                "region": e.region,
                "currency": e.currency,
                "city": e.city,
                "language": e.language,
                "chart_of_accounts": e.chart_of_accounts,
                "fiscal_year_variant": e.fiscal_year_variant,
                "company": e.company,
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
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(LegacyCostCenter).order_by(LegacyCostCenter.cctr)
    count_q = select(func.count(LegacyCostCenter.id))
    if scope:
        query = query.where(LegacyCostCenter.scope == scope)
        count_q = count_q.where(LegacyCostCenter.scope == scope)
    if data_category:
        query = query.where(LegacyCostCenter.data_category == data_category)
        count_q = count_q.where(LegacyCostCenter.data_category == data_category)
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
                "mandt": c.mandt,
                "coarea": c.coarea,
                "cctr": c.cctr,
                "txtsh": c.txtsh,
                "txtmi": c.txtmi,
                "responsible": c.responsible,
                "verak_user": c.verak_user,
                "cctrcgy": c.cctrcgy,
                "ccode": c.ccode,
                "currency": c.currency,
                "pctr": c.pctr,
                "gsber": c.gsber,
                "werks": c.werks,
                "abtei": c.abtei,
                "func_area": c.func_area,
                "land1": c.land1,
                "nkost": c.nkost,
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
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(LegacyProfitCenter).order_by(LegacyProfitCenter.pctr)
    count_q = select(func.count(LegacyProfitCenter.id))
    if scope:
        query = query.where(LegacyProfitCenter.scope == scope)
        count_q = count_q.where(LegacyProfitCenter.scope == scope)
    if data_category:
        query = query.where(LegacyProfitCenter.data_category == data_category)
        count_q = count_q.where(LegacyProfitCenter.data_category == data_category)
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
                "mandt": p.mandt,
                "coarea": p.coarea,
                "pctr": p.pctr,
                "txtsh": p.txtsh,
                "txtmi": p.txtmi,
                "responsible": p.responsible,
                "verak_user": p.verak_user,
                "department": p.department,
                "ccode": p.ccode,
                "currency": p.currency,
                "segment": p.segment,
                "land1": p.land1,
                "name1": p.name1,
                "name2": p.name2,
                "is_active": p.is_active,
            }
            for p in pcs
        ],
    }


@router.get("/target/cost-centers")
def list_target_ccs(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    ccode: str | None = None,
    coarea: str | None = None,
    search: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(TargetCostCenter).order_by(TargetCostCenter.cctr)
    count_q = select(func.count(TargetCostCenter.id))
    if scope:
        query = query.where(TargetCostCenter.scope == scope)
        count_q = count_q.where(TargetCostCenter.scope == scope)
    if data_category:
        query = query.where(TargetCostCenter.data_category == data_category)
        count_q = count_q.where(TargetCostCenter.data_category == data_category)
    if ccode:
        query = query.where(TargetCostCenter.ccode == ccode)
        count_q = count_q.where(TargetCostCenter.ccode == ccode)
    if coarea:
        query = query.where(TargetCostCenter.coarea == coarea)
        count_q = count_q.where(TargetCostCenter.coarea == coarea)
    if search:
        pattern = f"%{search}%"
        query = query.where(
            TargetCostCenter.cctr.ilike(pattern)
            | TargetCostCenter.txtsh.ilike(pattern)
            | TargetCostCenter.txtmi.ilike(pattern)
        )
        count_q = count_q.where(
            TargetCostCenter.cctr.ilike(pattern)
            | TargetCostCenter.txtsh.ilike(pattern)
            | TargetCostCenter.txtmi.ilike(pattern)
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
                "mdg_status": c.mdg_status,
                "mdg_change_request_id": c.mdg_change_request_id,
            }
            for c in ccs
        ],
    }


@router.get("/target/profit-centers")
def list_target_pcs(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    ccode: str | None = None,
    coarea: str | None = None,
    search: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(TargetProfitCenter).order_by(TargetProfitCenter.pctr)
    count_q = select(func.count(TargetProfitCenter.id))
    if scope:
        query = query.where(TargetProfitCenter.scope == scope)
        count_q = count_q.where(TargetProfitCenter.scope == scope)
    if data_category:
        query = query.where(TargetProfitCenter.data_category == data_category)
        count_q = count_q.where(TargetProfitCenter.data_category == data_category)
    if ccode:
        query = query.where(TargetProfitCenter.ccode == ccode)
        count_q = count_q.where(TargetProfitCenter.ccode == ccode)
    if coarea:
        query = query.where(TargetProfitCenter.coarea == coarea)
        count_q = count_q.where(TargetProfitCenter.coarea == coarea)
    if search:
        pattern = f"%{search}%"
        query = query.where(
            TargetProfitCenter.pctr.ilike(pattern)
            | TargetProfitCenter.txtsh.ilike(pattern)
            | TargetProfitCenter.txtmi.ilike(pattern)
        )
        count_q = count_q.where(
            TargetProfitCenter.pctr.ilike(pattern)
            | TargetProfitCenter.txtsh.ilike(pattern)
            | TargetProfitCenter.txtmi.ilike(pattern)
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


@router.get("/center-mappings")
def list_center_mappings(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    object_type: str | None = None,
    search: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(CenterMapping).order_by(CenterMapping.legacy_center)
    count_q = select(func.count(CenterMapping.id))
    if scope:
        query = query.where(CenterMapping.scope == scope)
        count_q = count_q.where(CenterMapping.scope == scope)
    if data_category:
        query = query.where(CenterMapping.data_category == data_category)
        count_q = count_q.where(CenterMapping.data_category == data_category)
    if object_type:
        query = query.where(CenterMapping.object_type == object_type)
        count_q = count_q.where(CenterMapping.object_type == object_type)
    if search:
        pattern = f"%{search}%"
        query = query.where(
            CenterMapping.legacy_center.ilike(pattern)
            | CenterMapping.target_center.ilike(pattern)
            | CenterMapping.legacy_name.ilike(pattern)
            | CenterMapping.target_name.ilike(pattern)
        )
        count_q = count_q.where(
            CenterMapping.legacy_center.ilike(pattern)
            | CenterMapping.target_center.ilike(pattern)
            | CenterMapping.legacy_name.ilike(pattern)
            | CenterMapping.target_name.ilike(pattern)
        )
    total = db.execute(count_q).scalar() or 0
    rows = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": m.id,
                "object_type": m.object_type,
                "legacy_coarea": m.legacy_coarea,
                "legacy_center": m.legacy_center,
                "legacy_name": m.legacy_name,
                "target_coarea": m.target_coarea,
                "target_center": m.target_center,
                "target_name": m.target_name,
                "mapping_type": m.mapping_type,
                "notes": m.notes,
            }
            for m in rows
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
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(Balance).order_by(Balance.fiscal_year.desc(), Balance.period.desc())
    count_q = select(func.count(Balance.id))
    if scope:
        query = query.where(Balance.scope == scope)
        count_q = count_q.where(Balance.scope == scope)
    if data_category:
        query = query.where(Balance.data_category == data_category)
        count_q = count_q.where(Balance.data_category == data_category)
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
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(Hierarchy)
    count_q = select(func.count(Hierarchy.id))
    if scope:
        query = query.where(Hierarchy.scope == scope)
        count_q = count_q.where(Hierarchy.scope == scope)
    if data_category:
        query = query.where(Hierarchy.data_category == data_category)
        count_q = count_q.where(Hierarchy.data_category == data_category)
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


@router.get("/legacy/hierarchies/{hier_id}/tree")
def hierarchy_tree(
    hier_id: int,
    db: Session = Depends(get_db),
) -> dict:
    """Build a full tree structure for a hierarchy: root → nodes → leaves with CC details."""
    hier = db.get(Hierarchy, hier_id)
    if not hier:
        raise HTTPException(status_code=404, detail="Hierarchy not found")

    nodes = (
        db.execute(select(HierarchyNode).where(HierarchyNode.hierarchy_id == hier_id))
        .scalars()
        .all()
    )
    leaves = (
        db.execute(select(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == hier_id))
        .scalars()
        .all()
    )

    # Build children map: parent_setname → list of child_setnames
    children_map: dict[str, list[str]] = {}
    all_children: set[str] = set()
    for n in nodes:
        children_map.setdefault(n.parent_setname, []).append(n.child_setname)
        all_children.add(n.child_setname)

    # Build leaves map: setname → list of cost center values
    leaves_map: dict[str, list[str]] = {}
    for lf in leaves:
        leaves_map.setdefault(lf.setname, []).append(lf.value)

    # Collect all CC values for lookup
    all_cc_values = {lf.value for lf in leaves}
    cc_lookup: dict[str, LegacyCostCenter] = {}
    if all_cc_values:
        ccs = (
            db.execute(select(LegacyCostCenter).where(LegacyCostCenter.cctr.in_(all_cc_values)))
            .scalars()
            .all()
        )
        cc_lookup = {cc.cctr: cc for cc in ccs}

    # Find root nodes (parents that are not children of anything, or the hierarchy setname itself)
    all_parents = set(children_map.keys())
    roots = all_parents - all_children
    if not roots:
        roots = {hier.setname}

    def build_node(setname: str, depth: int = 0) -> dict:
        node_children = children_map.get(setname, [])
        node_leaves = leaves_map.get(setname, [])
        cc_items = []
        for val in node_leaves:
            cc = cc_lookup.get(val)
            cc_items.append(
                {
                    "cctr": val,
                    "name": cc.txtsh if cc else None,
                    "ccode": cc.ccode if cc else None,
                    "responsible": cc.responsible if cc else None,
                    "pctr": cc.pctr if cc else None,
                }
            )
        return {
            "setname": setname,
            "type": "leaf" if (not node_children and node_leaves) else "node",
            "children": [build_node(c, depth + 1) for c in sorted(node_children)],
            "cost_centers": cc_items,
            "depth": depth,
        }

    tree = [build_node(r) for r in sorted(roots)]

    return {
        "hierarchy_id": hier_id,
        "setclass": hier.setclass,
        "setname": hier.setname,
        "description": hier.description,
        "tree": tree,
    }


@router.get("/employees")
def list_employees(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    search: str | None = None,
    ou_cd: str | None = None,
    cc_cd: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    query = select(Employee)
    count_q = select(func.count(Employee.id))
    if scope:
        query = query.where(Employee.scope == scope)
        count_q = count_q.where(Employee.scope == scope)
    if data_category:
        query = query.where(Employee.data_category == data_category)
        count_q = count_q.where(Employee.data_category == data_category)
    if search:
        like = f"%{search}%"
        flt = (
            Employee.gpn.ilike(like)
            | Employee.bs_name.ilike(like)
            | Employee.email_address.ilike(like)
        )
        query = query.where(flt)
        count_q = count_q.where(flt)
    if ou_cd:
        query = query.where(Employee.ou_cd == ou_cd)
        count_q = count_q.where(Employee.ou_cd == ou_cd)
    if cc_cd:
        query = query.where(Employee.local_cc_cd == cc_cd)
        count_q = count_q.where(Employee.local_cc_cd == cc_cd)
    total = db.execute(count_q).scalar() or 0
    emps = (
        db.execute(query.order_by(Employee.gpn).offset((pag.page - 1) * pag.size).limit(pag.size))
        .scalars()
        .all()
    )
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": e.id,
                "gpn": e.gpn,
                "display_name": e.display_name,
                "bs_name": e.bs_name,
                "bs_firstname": e.bs_firstname,
                "bs_lastname": e.bs_lastname,
                "email_address": e.email_address,
                "emp_status": e.emp_status,
                "ou_cd": e.ou_cd,
                "ou_desc": e.ou_desc,
                "local_cc_cd": e.local_cc_cd,
                "local_cc_desc": e.local_cc_desc,
                "gcrs_comp_cd": e.gcrs_comp_cd,
                "rank_desc": e.rank_desc,
                "job_desc": e.job_desc,
                "reg_region": e.reg_region,
                "locn_city_name_1": e.locn_city_name_1,
                "lm_gpn": e.lm_gpn,
                "lm_bs_firstname": e.lm_bs_firstname,
                "lm_bs_lastname": e.lm_bs_lastname,
            }
            for e in emps
        ],
    }


@router.get("/employees/{gpn}")
def get_employee(gpn: str, db: Session = Depends(get_db)) -> dict:
    """Lookup an employee by GPN — used for owner display (GPN + Name)."""
    emp = db.execute(select(Employee).where(Employee.gpn == gpn)).scalars().first()
    if not emp:
        return {"found": False, "gpn": gpn}
    return {
        "found": True,
        "gpn": emp.gpn,
        "display_name": emp.display_name,
        "bs_name": emp.bs_name,
        "bs_firstname": emp.bs_firstname,
        "bs_lastname": emp.bs_lastname,
        "email_address": emp.email_address,
        "emp_status": emp.emp_status,
        "ou_cd": emp.ou_cd,
        "ou_desc": emp.ou_desc,
        "local_cc_cd": emp.local_cc_cd,
        "job_desc": emp.job_desc,
        "rank_desc": emp.rank_desc,
    }


@router.get("/data/counts")
def data_counts(
    db: Session = Depends(get_db),
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    """Aggregate counts for the data management dashboard."""

    def _cnt(model: type, scope: str | None, data_category: str | None) -> int:
        q = select(func.count(model.id))  # type: ignore[attr-defined]
        if scope and hasattr(model, "scope"):
            q = q.where(model.scope == scope)  # type: ignore[attr-defined]
        if data_category and hasattr(model, "data_category"):
            q = q.where(model.data_category == data_category)  # type: ignore[attr-defined]
        return db.execute(q).scalar() or 0

    return {
        "entities": _cnt(Entity, scope, data_category),
        "cost_centers": _cnt(LegacyCostCenter, scope, data_category),
        "profit_centers": _cnt(LegacyProfitCenter, scope, data_category),
        "balances": _cnt(Balance, scope, data_category),
        "hierarchies": _cnt(Hierarchy, scope, data_category),
        "employees": _cnt(Employee, scope, data_category),
        "upload_batches": _cnt(UploadBatch, scope, data_category),
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


@router.post("/ml/predict")
def ml_predict(
    coarea: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
) -> dict:
    """Run ML prediction on cost centers (heuristic fallback if sklearn unavailable)."""
    from app.domain.ml.classifier import predict

    query = select(LegacyCostCenter).where(LegacyCostCenter.is_active.is_(True))
    if coarea:
        query = query.where(LegacyCostCenter.coarea == coarea)
    ccs = db.execute(query.limit(limit)).scalars().all()

    contexts = []
    for cc in ccs:
        contexts.append(
            {
                "cctr": cc.cctr,
                "ccode": cc.ccode,
                "txtsh": cc.txtsh,
                "is_active": cc.is_active,
                "responsible": cc.responsible,
                "months_since_last_posting": None,
                "posting_count_window": None,
                "bs_amt": 0,
                "opex_amt": 0,
                "rev_amt": 0,
                "hierarchy_depth": 0,
            }
        )

    predictions = predict(contexts)
    results = []
    for cc, pred_result in zip(ccs, predictions, strict=False):
        results.append(
            {
                "id": cc.id,
                "cctr": cc.cctr,
                "txtsh": cc.txtsh,
                "ccode": cc.ccode,
                **pred_result,
            }
        )

    return {"total": len(results), "items": results}


# ---------------------------------------------------------------------------
# Upload templates (public — static CSV column definitions, no user data)
# ---------------------------------------------------------------------------

from app.api.admin import UPLOAD_TEMPLATES  # noqa: E402


@router.get("/data/upload-templates")
def list_upload_templates_public() -> dict:
    """List available upload templates (public, no auth)."""
    return {
        "templates": [
            {"kind": k, "filename": v["filename"], "description": v["description"]}
            for k, v in UPLOAD_TEMPLATES.items()
        ]
    }


@router.get("/data/upload-templates/{kind}")
def download_upload_template_public(kind: str) -> dict:
    """Get CSV content for an upload template (public, no auth)."""
    tmpl = UPLOAD_TEMPLATES.get(kind)
    if not tmpl:
        raise HTTPException(status_code=404, detail=f"No template for kind: {kind}")

    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(tmpl["columns"])

    if kind == "hierarchies":
        if "sample_row_header" in tmpl:
            writer.writerow(tmpl["sample_row_header"])
        if "sample_row_node" in tmpl:
            writer.writerow(tmpl["sample_row_node"])
        if "sample_row_leaf" in tmpl:
            writer.writerow(tmpl["sample_row_leaf"])
    elif "sample_row" in tmpl:
        writer.writerow(tmpl["sample_row"])

    return {
        "kind": kind,
        "filename": tmpl["filename"],
        "content": output.getvalue(),
        "content_type": "text/csv",
    }
