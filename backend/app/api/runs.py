"""Analysis run API (section 11.6)."""

from __future__ import annotations

from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination, require_role
from app.infra.db.session import get_db
from app.models.core import AnalysisRun, AppUser, CenterProposal

router = APIRouter()


class RunOut(BaseModel):
    id: int
    wave_id: int | None = None
    config_id: int
    status: str
    kpis: dict | None = None
    started_at: str | None = None
    finished_at: str | None = None

    model_config = {"from_attributes": True}


@router.post("/global")
def run_global_analysis(
    config_id: int | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Run analysis on ALL cost centers (not scoped to any wave)."""
    from app.services.analysis import execute_analysis, get_or_create_default_config

    if config_id is None:
        config = get_or_create_default_config(db)
        config_id = config.id

    try:
        run = execute_analysis(None, config_id, user.id, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {e}") from None

    return {
        "run_id": run.id,
        "status": run.status,
        "kpis": run.kpis,
        "started_at": str(run.started_at) if run.started_at else None,
        "finished_at": str(run.finished_at) if run.finished_at else None,
    }


@router.get("/global/list")
def list_global_runs(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
) -> dict:
    """List analysis runs not tied to any wave (global runs)."""
    query = select(AnalysisRun).where(AnalysisRun.wave_id.is_(None)).order_by(AnalysisRun.id.desc())
    total = (
        db.execute(select(func.count(AnalysisRun.id)).where(AnalysisRun.wave_id.is_(None))).scalar()
        or 0
    )
    items = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()
    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": r.id,
                "status": r.status,
                "kpis": r.kpis,
                "started_at": str(r.started_at) if r.started_at else None,
                "finished_at": str(r.finished_at) if r.finished_at else None,
            }
            for r in items
        ],
    }


@router.get("/{run_id}")
def get_run(run_id: int, db: Session = Depends(get_db)) -> RunOut:
    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return RunOut(
        id=run.id,
        wave_id=run.wave_id,
        config_id=run.config_id,
        status=run.status,
        kpis=run.kpis,
        started_at=str(run.started_at) if run.started_at else None,
        finished_at=str(run.finished_at) if run.finished_at else None,
    )


@router.post("/{run_id}/cancel")
def cancel_run(
    run_id: int,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status not in ("pending", "running"):
        raise HTTPException(status_code=409, detail="Run cannot be cancelled")
    run.status = "cancelled"
    db.commit()
    return {"status": "cancelled"}


@router.delete("/{run_id}")
def delete_run(
    run_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Delete an analysis run and all its proposals."""
    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status == "running":
        raise HTTPException(status_code=409, detail="Cannot delete a running analysis")
    from sqlalchemy import delete, select, update

    from app.models.core import (
        LLMReviewPass,
        NamingAllocation,
        RoutineOutput,
        Wave,
    )

    # Release naming allocations before deleting proposals
    proposal_ids = (
        db.execute(select(CenterProposal.id).where(CenterProposal.run_id == run_id)).scalars().all()
    )
    if proposal_ids:
        db.execute(
            update(NamingAllocation)
            .where(
                NamingAllocation.proposal_id.in_(proposal_ids),
                NamingAllocation.is_released.is_(False),
            )
            .values(is_released=True, proposal_id=None)
        )

    db.execute(delete(CenterProposal).where(CenterProposal.run_id == run_id))
    db.execute(delete(RoutineOutput).where(RoutineOutput.run_id == run_id))
    db.execute(delete(LLMReviewPass).where(LLMReviewPass.run_id == run_id))

    # Clear stale preferred_run_id from any wave referencing this run
    waves = db.execute(select(Wave).where(Wave.preferred_run_id == run_id)).scalars().all()
    for wave in waves:
        wave.preferred_run_id = None
        if wave.config and wave.config.get("preferred_run_id") == run_id:
            cfg = {**wave.config}
            del cfg["preferred_run_id"]
            wave.config = cfg

    db.delete(run)
    db.commit()
    return {"deleted": True}


@router.get("/{run_id}/proposals")
def list_proposals(
    run_id: int,
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    outcome: str | None = None,
    target: str | None = None,
) -> dict:
    query = select(CenterProposal).where(CenterProposal.run_id == run_id)
    if outcome:
        query = query.where(CenterProposal.cleansing_outcome == outcome)
    if target:
        query = query.where(CenterProposal.target_object == target)
    total_q = select(func.count(CenterProposal.id)).where(CenterProposal.run_id == run_id)
    if outcome:
        total_q = total_q.where(CenterProposal.cleansing_outcome == outcome)
    if target:
        total_q = total_q.where(CenterProposal.target_object == target)
    total = db.execute(total_q).scalar() or 0
    proposals = (
        db.execute(
            query.order_by(CenterProposal.id).offset((pag.page - 1) * pag.size).limit(pag.size)
        )
        .scalars()
        .all()
    )
    from app.models.core import Balance, LegacyCostCenter

    cc_ids = [p.legacy_cc_id for p in proposals]
    cc_map: dict[int, LegacyCostCenter] = {}
    if cc_ids:
        ccs = (
            db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids)))
            .scalars()
            .all()
        )
        cc_map = {c.id: c for c in ccs}

    # Fetch posting trends (last 12 periods) for sparklines
    trend_map: dict[str, list[int]] = {}
    cctrs = [cc_map[pid].cctr for pid in cc_ids if pid in cc_map]
    if cctrs:
        trend_rows = db.execute(
            select(
                Balance.cctr,
                (Balance.fiscal_year * 100 + Balance.period).label("ym"),
                func.coalesce(func.sum(Balance.posting_count), 0).label("cnt"),
            )
            .where(Balance.cctr.in_(cctrs))
            .group_by(Balance.cctr, "ym")
            .order_by(Balance.cctr, "ym")
        ).all()
        for cctr, _ym, cnt in trend_rows:
            trend_map.setdefault(cctr, []).append(int(cnt))
        for cctr in trend_map:
            trend_map[cctr] = trend_map[cctr][-12:]

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": p.id,
                "legacy_cc_id": p.legacy_cc_id,
                "cctr": cc_map[p.legacy_cc_id].cctr if p.legacy_cc_id in cc_map else None,
                "txtsh": cc_map[p.legacy_cc_id].txtsh if p.legacy_cc_id in cc_map else None,
                "ccode": cc_map[p.legacy_cc_id].ccode if p.legacy_cc_id in cc_map else None,
                "coarea": cc_map[p.legacy_cc_id].coarea if p.legacy_cc_id in cc_map else None,
                "responsible": cc_map[p.legacy_cc_id].responsible
                if p.legacy_cc_id in cc_map
                else None,
                "cleansing_outcome": p.cleansing_outcome,
                "target_object": p.target_object,
                "merge_into_cctr": p.merge_into_cctr,
                "confidence": str(p.confidence) if p.confidence else None,
                "override_outcome": p.override_outcome,
                "override_target": p.override_target,
                "override_reason": p.override_reason,
                "posting_trend": trend_map.get(
                    cc_map[p.legacy_cc_id].cctr if p.legacy_cc_id in cc_map else "", []
                ),
            }
            for p in proposals
        ],
    }


@router.get("/{run_id}/proposals/{proposal_id}/why")
def why_panel(run_id: int, proposal_id: int, db: Session = Depends(get_db)) -> dict:
    proposal = db.get(CenterProposal, proposal_id)
    if not proposal or proposal.run_id != run_id:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return {
        "proposal_id": proposal.id,
        "cleansing_outcome": proposal.cleansing_outcome,
        "target_object": proposal.target_object,
        "rule_path": proposal.rule_path,
        "ml_scores": proposal.ml_scores,
        "llm_commentary": proposal.llm_commentary,
        "override": {
            "outcome": proposal.override_outcome,
            "target": proposal.override_target,
            "reason": proposal.override_reason,
        }
        if proposal.override_outcome
        else None,
    }


@router.get("/{run_id}/data-browser")
def data_browser(
    run_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst", "data_manager")),
) -> dict:
    """Combined data browser: legacy centers + monthly balances + analysis results + CC→PC mapping."""
    from app.models.core import Balance, Hierarchy, HierarchyLeaf, HierarchyNode, LegacyCostCenter

    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    proposals = db.execute(
        select(CenterProposal).where(CenterProposal.run_id == run_id)
    ).scalars().all()

    cc_ids = [p.legacy_cc_id for p in proposals]
    cc_map: dict[int, LegacyCostCenter] = {}
    if cc_ids:
        ccs = db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids))).scalars().all()
        cc_map = {c.id: c for c in ccs}

    # Monthly balances for all relevant cost centers
    cctrs = list({cc_map[pid].cctr for pid in cc_ids if pid in cc_map})
    balance_map: dict[str, list[dict]] = {}
    if cctrs:
        bal_rows = db.execute(
            select(
                Balance.cctr,
                Balance.fiscal_year,
                Balance.period,
                func.coalesce(func.sum(Balance.tc_amt), 0).label("total_amt"),
                func.coalesce(func.sum(Balance.posting_count), 0).label("total_postings"),
            )
            .where(Balance.cctr.in_(cctrs))
            .group_by(Balance.cctr, Balance.fiscal_year, Balance.period)
            .order_by(Balance.cctr, Balance.fiscal_year, Balance.period)
        ).all()
        for cctr, fy, per, amt, postings in bal_rows:
            balance_map.setdefault(cctr, []).append({
                "fiscal_year": fy,
                "period": per,
                "amount": float(amt),
                "postings": int(postings),
            })

    # CC→PC mapping (current 1:1 from legacy CC pctr field)
    # and proposed n:1 from merge_into_cctr
    pc_target_groups: dict[str, list[str]] = {}
    for p in proposals:
        target = p.merge_into_cctr
        if target and p.legacy_cc_id in cc_map:
            pc_target_groups.setdefault(target, []).append(cc_map[p.legacy_cc_id].cctr)

    # Hierarchy tree for hierarchical view
    hierarchies = db.execute(select(Hierarchy).where(Hierarchy.is_active.is_(True))).scalars().all()
    hier_trees = []
    for h in hierarchies:
        nodes = db.execute(
            select(HierarchyNode).where(HierarchyNode.hierarchy_id == h.id)
        ).scalars().all()
        leaves = db.execute(
            select(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == h.id)
        ).scalars().all()
        class_labels = {"0101": "Cost Center", "0104": "Profit Center", "0106": "Entity"}
        hier_trees.append({
            "id": h.id,
            "setname": h.setname,
            "setclass": h.setclass,
            "label": h.label or f"{class_labels.get(h.setclass, h.setclass)}: {h.setname}" + (f" — {h.description}" if h.description else ""),
            "description": h.description,
            "coarea": h.coarea,
            "nodes": [{"parent": n.parent_setname, "child": n.child_setname, "seq": n.seq} for n in nodes],
            "leaves": [{"setname": lf.setname, "value": lf.value, "seq": lf.seq} for lf in leaves],
        })

    items = []
    for p in proposals:
        cc = cc_map.get(p.legacy_cc_id)
        cctr = cc.cctr if cc else None
        items.append({
            "id": p.id,
            "legacy_cc_id": p.legacy_cc_id,
            "cctr": cctr,
            "txtsh": cc.txtsh if cc else None,
            "txtmi": cc.txtmi if cc else None,
            "ccode": cc.ccode if cc else None,
            "coarea": cc.coarea if cc else None,
            "responsible": cc.responsible if cc else None,
            "pctr": cc.pctr if cc else None,
            "is_active": cc.is_active if cc else None,
            "cleansing_outcome": p.cleansing_outcome,
            "target_object": p.target_object,
            "merge_into_cctr": p.merge_into_cctr,
            "confidence": str(p.confidence) if p.confidence else None,
            "override_outcome": p.override_outcome,
            "override_target": p.override_target,
            "monthly_balances": balance_map.get(cctr, []) if cctr else [],
        })

    return {
        "run_id": run_id,
        "total": len(items),
        "items": items,
        "pc_target_groups": pc_target_groups,
        "hierarchies": hier_trees,
    }


@router.post("/{run_id}/proposals/{proposal_id}/override")
def override_proposal(
    run_id: int,
    proposal_id: int,
    outcome: str = Query(...),
    reason: str = Query(...),
    target: str | None = None,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    proposal = db.get(CenterProposal, proposal_id)
    if not proposal or proposal.run_id != run_id:
        raise HTTPException(status_code=404, detail="Proposal not found")
    from datetime import datetime

    proposal.override_outcome = outcome
    proposal.override_target = target
    proposal.override_reason = reason
    proposal.override_by = user.id
    proposal.override_at = datetime.now(UTC)
    db.commit()
    return {"status": "overridden", "outcome": outcome}


class LLMReviewRequest(BaseModel):
    mode: str = "SINGLE"  # SINGLE | SEQUENTIAL | DEBATE
    max_centers: int = 100
    outcomes: list[str] | None = None  # Filter to specific outcomes
    min_balance: float | None = None


@router.post("/{run_id}/llm-review")
def trigger_llm_review(
    run_id: int,
    body: LLMReviewRequest,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Trigger LLM review pass on proposals in a completed run."""
    from app.models.core import AppConfig, LegacyCostCenter

    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status != "completed":
        raise HTTPException(status_code=409, detail="Run must be completed")

    # Get LLM config
    cfg = db.execute(select(AppConfig).where(AppConfig.key == "llm")).scalar_one_or_none()
    if not cfg or not cfg.value:
        raise HTTPException(status_code=400, detail="LLM not configured")

    llm_config = cfg.value

    # Build provider
    from app.infra.llm.provider import AzureOpenAIProvider, SapBtpProvider

    provider_type = llm_config.get("provider", "azure")
    if provider_type == "azure":
        provider = AzureOpenAIProvider(llm_config)
    elif provider_type == "btp":
        provider = SapBtpProvider(llm_config)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown LLM provider: {provider_type}")

    model = llm_config.get("model", "gpt-4o")

    # Query proposals to review
    query = select(CenterProposal).where(CenterProposal.run_id == run_id)
    if body.outcomes:
        query = query.where(CenterProposal.cleansing_outcome.in_(body.outcomes))
    proposals = db.execute(query.limit(body.max_centers)).scalars().all()

    from app.infra.llm.review_pass import build_center_context, run_review_pass

    reviewed = 0
    total_cost = 0.0
    for proposal in proposals:
        cc = db.get(LegacyCostCenter, proposal.legacy_cc_id)
        if not cc:
            continue

        center = {
            "coarea": cc.coarea or "",
            "cctr": cc.cctr or "",
            "txtsh": cc.txtsh or "",
            "txtmi": cc.txtmi or "",
            "responsible": cc.responsible or "",
            "ccode": cc.ccode or "",
            "currency": cc.currency or "",
        }
        features = {
            "months_since_last_posting": 0,
            "posting_count_window": 0,
            "bs_amt": 0,
            "rev_amt": 0,
            "opex_amt": 0,
            "hierarchy_membership_count": 0,
        }
        outcome = {
            "cleansing": proposal.cleansing_outcome,
            "target_object": proposal.target_object,
            "rule_path": (proposal.rule_path or {}).get("steps", []),
        }
        ml = proposal.ml_scores or {}

        ctx = build_center_context(center, features, outcome, ml)

        try:
            result = run_review_pass(provider, model, body.mode, ctx)
            proposal.llm_commentary = result
            total_cost += result.get("_llm_meta", {}).get("cost_usd", 0.0)
            reviewed += 1
        except Exception as e:
            proposal.llm_commentary = {"error": str(e)}

    db.commit()
    return {
        "reviewed": reviewed,
        "total_proposals": len(proposals),
        "mode": body.mode,
        "total_cost_usd": round(total_cost, 4),
    }


@router.get("/{run_a}/diff/{run_b}")
def compare_runs(run_a: int, run_b: int, db: Session = Depends(get_db)) -> dict:
    a = db.get(AnalysisRun, run_a)
    b = db.get(AnalysisRun, run_b)
    if not a or not b:
        raise HTTPException(status_code=404, detail="Run not found")

    # Build outcome matrices
    props_a = db.execute(
        select(
            CenterProposal.legacy_cc_id,
            CenterProposal.cleansing_outcome,
            CenterProposal.target_object,
        ).where(CenterProposal.run_id == run_a)
    ).all()
    props_b = db.execute(
        select(
            CenterProposal.legacy_cc_id,
            CenterProposal.cleansing_outcome,
            CenterProposal.target_object,
        ).where(CenterProposal.run_id == run_b)
    ).all()

    map_a = {r[0]: (r[1], r[2]) for r in props_a}
    map_b = {r[0]: (r[1], r[2]) for r in props_b}

    # Outcome transition matrix
    outcome_matrix: dict[str, dict[str, int]] = {}
    target_matrix: dict[str, dict[str, int]] = {}
    changed_ids: list[int] = []

    all_ids = set(map_a.keys()) | set(map_b.keys())
    for cc_id in all_ids:
        oa = map_a.get(cc_id, ("N/A", "N/A"))
        ob = map_b.get(cc_id, ("N/A", "N/A"))
        # Outcome matrix
        outcome_matrix.setdefault(oa[0], {}).setdefault(ob[0], 0)
        outcome_matrix[oa[0]][ob[0]] += 1
        # Target matrix
        ta = oa[1] or "NONE"
        tb = ob[1] or "NONE"
        target_matrix.setdefault(ta, {}).setdefault(tb, 0)
        target_matrix[ta][tb] += 1
        if oa != ob:
            changed_ids.append(cc_id)

    return {
        "run_a": {"id": a.id, "status": a.status, "kpis": a.kpis},
        "run_b": {"id": b.id, "status": b.status, "kpis": b.kpis},
        "diff": {
            "outcome_matrix": outcome_matrix,
            "target_matrix": target_matrix,
            "changed_count": len(changed_ids),
            "total_a": len(map_a),
            "total_b": len(map_b),
        },
    }


@router.post("/{run_id}/batch-features")
def batch_compute_features(
    run_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin", "analyst")),
) -> dict:
    """Batch-compute features for all centers in a run using server-side aggregation.

    Instead of N+1 queries per center, this executes one SQL query for all centers
    and returns pre-computed feature vectors suitable for ML model training.
    """
    from app.models.core import Balance, LegacyCostCenter

    run = db.get(AnalysisRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == run_id)).scalars().all()
    )

    cc_ids = [p.legacy_cc_id for p in proposals]
    if not cc_ids:
        return {"features": [], "count": 0}

    # Batch query: all centers
    centers = (
        db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids))).scalars().all()
    )
    cc_map = {c.id: c for c in centers}

    # Batch aggregation: balance sums per center
    from sqlalchemy import case, literal_column

    balance_agg = db.execute(
        select(
            Balance.cctr,
            func.sum(
                case((Balance.account_class == "BS", Balance.tc_amt), else_=literal_column("0"))
            ).label("bs_amt"),
            func.sum(
                case((Balance.account_class == "OPEX", Balance.tc_amt), else_=literal_column("0"))
            ).label("opex_amt"),
            func.sum(
                case((Balance.account_class == "REV", Balance.tc_amt), else_=literal_column("0"))
            ).label("rev_amt"),
            func.count(Balance.id).label("posting_count"),
        )
        .where(Balance.cctr.in_([c.cctr for c in centers if c]))
        .group_by(Balance.cctr)
    ).all()
    bal_map = {row.cctr: row for row in balance_agg}

    features_list = []
    for p in proposals:
        cc = cc_map.get(p.legacy_cc_id)
        if not cc:
            continue
        bal = bal_map.get(cc.cctr)
        name = cc.txtsh or ""
        features_list.append(
            {
                "cc_id": cc.id,
                "cctr": cc.cctr,
                "ccode": cc.ccode,
                "verdict": p.cleansing_outcome,
                "features": {
                    "is_active": 1.0 if cc.is_active else 0.0,
                    "months_since_last_posting": 0.0,
                    "posting_count_window": float(bal.posting_count if bal else 0),
                    "bs_amt": float(bal.bs_amt if bal else 0),
                    "opex_amt": float(bal.opex_amt if bal else 0),
                    "rev_amt": float(bal.rev_amt if bal else 0),
                    "hierarchy_depth": 0.0,
                    "name_length": float(len(name)),
                    "has_responsible": 1.0 if cc.responsible else 0.0,
                },
            }
        )

    return {"count": len(features_list), "features": features_list}
