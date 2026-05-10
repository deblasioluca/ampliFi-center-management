"""Reference data endpoints (section 11.10) — browse all data types."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination
from app.infra.db.session import get_db
from app.models.core import (
    Balance,
    CenterExclusionRule,
    CenterMapping,
    Employee,
    Entity,
    GLAccountSKA1,
    GLAccountSKB1,
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

log = logging.getLogger(__name__)


def _get_excluded_ids_for_page(db: Session, centers, scope: str | None, object_type: str) -> set[int]:
    """Evaluate exclusion rules against a page of centers. Returns set of IDs to mark as excluded."""
    q = select(CenterExclusionRule).where(
        CenterExclusionRule.is_enabled == True,  # noqa: E712
        (CenterExclusionRule.scope == None) | (CenterExclusionRule.scope == scope),  # noqa: E711
        (CenterExclusionRule.object_type == "both") | (CenterExclusionRule.object_type == object_type),
    )
    rules = db.scalars(q).all()
    if not rules:
        return set()

    from app.api.exclusion_rules import _matches_condition

    excluded = set()
    for center in centers:
        for rule in rules:
            if _matches_condition(center, rule.condition):
                excluded.add(center.id)
                break
    return excluded


# ── Cluster duplicate-check job registry ─────────────────────────────
# In-memory registry of duplicate-check jobs, keyed by uuid hex. See
# the docstring on ``check_duplicates`` for why this is in-memory rather
# than a real DB table. Don't access this dict from request handlers
# without the lock — the runner thread mutates the same entries.
_cluster_jobs: dict[str, dict] = {}
_cluster_jobs_lock = __import__("threading").Lock()


def _run_duplicate_check_in_thread(
    job_id: str,
    coarea: str | None,
    threshold: float,
    limit: int,
) -> None:
    """Daemon-thread runner for the duplicate-check job.

    Drives the same ``find_duplicates`` ML routine as the previous
    synchronous endpoint, but updates the in-memory job entry in three
    stages so the frontend's progress bar has something to show:

      ``loading`` → SQL query for cost centers in scope
      ``embedding`` → batch embed names through the model
      ``pairing`` → pairwise cosine + threshold filter

    On any exception the job flips to ``status='failed'`` with the
    message set on ``error``; the frontend renders that as a red
    notice rather than a stuck spinner. Even on success we trim the
    pairs list to ``limit`` before storing — operators rarely need
    more than the top 100 and shipping 50k pairs through the polling
    endpoint defeats the point of being async.
    """
    from app.domain.ml.embeddings import find_duplicates
    from app.infra.db.session import SessionLocal
    from app.models.core import LegacyCostCenter

    def _set(**kwargs):
        with _cluster_jobs_lock:
            job = _cluster_jobs.get(job_id)
            if job is not None:
                job.update(kwargs)

    db = SessionLocal()
    try:
        _set(status="running", stage="loading", progress=0)
        query = select(LegacyCostCenter).where(LegacyCostCenter.is_active.is_(True))
        if coarea:
            query = query.where(LegacyCostCenter.coarea == coarea)
        ccs = db.execute(query).scalars().all()
        names = [cc.txtsh or cc.txtmi or cc.cctr for cc in ccs]
        ids = [cc.id for cc in ccs]
        n = len(names)
        _set(stage="embedding", total=n, progress=0)
        if n < 2:
            _set(
                status="done",
                stage="done",
                progress=n,
                total=n,
                result={"total": 0, "pairs": []},
            )
            return
        # find_duplicates is a single call so we can't get fine-grained
        # progress out of it without rewriting the embedding routine.
        # Mark progress as "halfway" while it runs — better than a
        # frozen 0% bar that makes operators think something hung.
        _set(progress=max(1, n // 2))
        pairs = find_duplicates(names, ids, threshold=threshold)
        _set(stage="pairing", progress=n)
        _set(
            status="done",
            stage="done",
            progress=n,
            total=n,
            result={"total": len(pairs), "pairs": pairs[:limit]},
        )
    except Exception as e:  # noqa: BLE001 — we want to surface the message
        log.exception("duplicate_check_thread.failed job=%s", job_id)
        _set(status="failed", stage="failed", error=str(e))
    finally:
        db.close()


# Setclass codes used by SAP for the two hierarchy types we surface in
# the data browser. Documented here so the frontend code paths and the
# backend filters stay consistent.
SETCLASS_COST_CENTER = "0101"
SETCLASS_PROFIT_CENTER = "0104"
SETCLASS_ENTITY = "0106"

# Uploaded data may use short aliases (CC, PC, ENT) instead of the
# standard 4-digit SAP setclass codes. Normalise so all downstream
# comparisons can use the canonical codes.
_SETCLASS_ALIASES: dict[str, str] = {
    "CC": SETCLASS_COST_CENTER,
    "PC": SETCLASS_PROFIT_CENTER,
    "ENT": SETCLASS_ENTITY,
    "ENTITY": SETCLASS_ENTITY,
}


def normalise_setclass(raw: str | None) -> str:
    """Return the canonical SAP setclass code for a given raw value."""
    if not raw:
        return ""
    upper = raw.strip().upper()
    return _SETCLASS_ALIASES.get(upper, upper)


def _resolve_hierarchy_paths(
    db: Session, hierarchy_id: int, leaf_values: list[str]
) -> tuple[dict[str, list[str]], int]:
    """For each ``leaf_value`` (a CC or PC code), return the list of
    setnames from the root of the given hierarchy down to the leaf.

    Returns ``(paths, max_depth)``:

    * ``paths`` — ``{leaf_value: [L0_setname, L1_setname, ..., leaf_setname]}``
      Leaves with no entry in ``hierarchy_leaf`` (i.e. not part of this
      hierarchy at all) are NOT included in the dict — callers can fill
      them in with empty levels.
    * ``max_depth`` — the deepest path length seen across all leaves,
      which the API uses to size the L0..Lx column header list. Always
      at least 0 even if no leaves resolved.

    The walk is done in Python after pulling all edges + leaves for the
    hierarchy in two queries, so the cost is O(edges + leaves) per
    request rather than per-leaf. For a typical SAP hierarchy with a
    few thousand nodes this is negligible.
    """
    if not leaf_values:
        return {}, 0

    # Pull the entire hierarchy structure in two queries
    edges = (
        db.execute(select(HierarchyNode).where(HierarchyNode.hierarchy_id == hierarchy_id))
        .scalars()
        .all()
    )
    leaves = (
        db.execute(
            select(HierarchyLeaf)
            .where(HierarchyLeaf.hierarchy_id == hierarchy_id)
            .where(HierarchyLeaf.value.in_(leaf_values))
        )
        .scalars()
        .all()
    )

    # parent_of[child_setname] = parent_setname (first one wins if
    # duplicated — same convention as the /nodes endpoint)
    parent_of: dict[str, str] = {}
    for e in edges:
        parent_of.setdefault(e.child_setname, e.parent_setname)

    # leaf_value → setname it hangs under (same first-wins convention).
    # SAP hierarchies in practice have one leaf-to-setname mapping but
    # the schema doesn't enforce uniqueness.
    leaf_setname: dict[str, str] = {}
    for lf in leaves:
        leaf_setname.setdefault(lf.value, lf.setname)

    paths: dict[str, list[str]] = {}
    max_depth = 0
    for value, setname in leaf_setname.items():
        # Walk from the leaf-bound setname up to the root, collecting
        # ancestors. Cycle-guarded to avoid infinite loops on malformed
        # data (shouldn't happen, but cheap to be safe).
        chain: list[str] = []
        seen: set[str] = set()
        cur = setname
        while cur and cur not in seen:
            seen.add(cur)
            chain.append(cur)
            cur = parent_of.get(cur, "")
        # chain currently goes leaf-to-root; reverse so [0] is the root
        chain.reverse()
        paths[value] = chain
        if len(chain) > max_depth:
            max_depth = len(chain)

    return paths, max_depth


def _resolve_paths_for_ccs(
    db: Session,
    hierarchy_id: int,
    ccs: list,  # list[LegacyCostCenter]
) -> tuple[dict[str, list[str]], int]:
    """Resolve hierarchy paths for a list of cost centers, picking the
    right lookup key based on the hierarchy's setclass.

    Bug report (post-PR-#88): operator selected an Entity hierarchy
    (``UBS_GROUP_ENT — Group → Region → Country → Type → Entity``)
    in the Cost Centers tab and saw no L0..LX columns despite paths
    existing. Root cause: the underlying ``_resolve_hierarchy_paths``
    helper looked up leaves by the cctr value, but Entity-hierarchy
    leaves store ccodes, not cctrs — so every lookup missed and
    ``max_depth`` stayed at 0.

    The fix is to pick the right field from each CC based on what the
    hierarchy's leaves actually contain:

    * ``setclass='0101'`` (CC hierarchy)     → leaves = cctrs   → use ``cc.cctr``
    * ``setclass='0104'`` (PC hierarchy)     → leaves = pctrs   → use ``cc.pctr``
    * ``setclass='0106'`` (Entity hierarchy) → leaves = ccodes  → use ``cc.ccode``

    Returns paths keyed by ``cc.cctr`` regardless of which field was
    used to look it up — callers always index by cctr, this helper hides
    the indirection. CCs whose lookup field doesn't match a leaf get
    no entry in the dict (callers fill in empty levels).

    For unknown setclasses we fall back to cctr lookup, which matches
    the previous behaviour and still works for any CC-style hierarchy
    even if it's tagged with a custom setclass.
    """
    if not ccs:
        return {}, 0

    hier = db.get(Hierarchy, hierarchy_id)
    if hier is None:
        return {}, 0

    setclass = normalise_setclass(hier.setclass)

    # Pick the field on the CC row that matches what the hierarchy's
    # leaves contain. Tested on real data: SAP loaders write all three
    # fields onto LegacyCostCenter, so this is just an attribute read.
    if setclass == SETCLASS_ENTITY:
        # Entity hierarchy — leaves are ccodes
        key_fn = lambda cc: cc.ccode  # noqa: E731
    elif setclass == SETCLASS_PROFIT_CENTER:
        # PC hierarchy — leaves are pctrs. CCs without a pctr (or where
        # pctr is null) won't resolve, which is the right behaviour
        # (they're not part of any PC tree).
        key_fn = lambda cc: cc.pctr  # noqa: E731
    else:
        # 0101 (CC hierarchy) or unknown — leaves are cctrs.
        key_fn = lambda cc: cc.cctr  # noqa: E731

    # Build cc.cctr → key (e.g. cctr → ccode) map so we can flip back at
    # the end. We also de-dup the lookup keys — for entity hierarchies
    # in particular we'd otherwise pass thousands of duplicate ccodes
    # (every CC under the same entity).
    cctr_to_key: dict[str, str] = {}
    unique_keys: set[str] = set()
    for cc in ccs:
        k = key_fn(cc)
        if k:
            cctr_to_key[cc.cctr] = k
            unique_keys.add(k)

    if not unique_keys:
        return {}, 0

    # Resolve once for the unique key set, then map back to cctr-keyed
    # response.
    paths_by_key, max_depth = _resolve_hierarchy_paths(db, hierarchy_id, list(unique_keys))
    paths_by_cctr: dict[str, list[str]] = {}
    for cctr, k in cctr_to_key.items():
        if k in paths_by_key:
            paths_by_cctr[cctr] = paths_by_key[k]
    return paths_by_cctr, max_depth


def _resolve_paths_for_pcs(
    db: Session,
    hierarchy_id: int,
    pcs: list,  # list[LegacyProfitCenter]
) -> tuple[dict[str, list[str]], int]:
    """PC counterpart of ``_resolve_paths_for_ccs`` (PR #90).

    For a profit-center listing, the same setclass-driven indirection
    applies:

    * ``setclass='0104'`` (PC hierarchy)     → leaves = pctrs   → use ``pc.pctr``
    * ``setclass='0106'`` (Entity hierarchy) → leaves = ccodes  → use ``pc.ccode``
    * ``setclass='0101'`` (CC hierarchy)     → no useful resolution
      possible — PCs don't appear in CC-tree leaves. Returns empty.

    Returns paths keyed by ``pc.pctr``.
    """
    if not pcs:
        return {}, 0

    hier = db.get(Hierarchy, hierarchy_id)
    if hier is None:
        return {}, 0

    setclass = normalise_setclass(hier.setclass)

    if setclass == SETCLASS_ENTITY:
        key_fn = lambda pc: pc.ccode  # noqa: E731
    elif setclass == SETCLASS_COST_CENTER:
        # CC hierarchy doesn't apply to PCs — return empty rather than
        # silently giving wrong results
        return {}, 0
    else:
        # 0104 (PC) or unknown — leaves are pctrs
        key_fn = lambda pc: pc.pctr  # noqa: E731

    pctr_to_key: dict[str, str] = {}
    unique_keys: set[str] = set()
    for pc in pcs:
        k = key_fn(pc)
        if k:
            pctr_to_key[pc.pctr] = k
            unique_keys.add(k)

    if not unique_keys:
        return {}, 0

    paths_by_key, max_depth = _resolve_hierarchy_paths(db, hierarchy_id, list(unique_keys))
    paths_by_pctr: dict[str, list[str]] = {}
    for pctr, k in pctr_to_key.items():
        if k in paths_by_key:
            paths_by_pctr[pctr] = paths_by_key[k]
    return paths_by_pctr, max_depth


@router.get("/entities")
def list_entities(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    country: str | None = None,
    search: str | None = None,
    search_values: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
    hierarchy_id: int | None = None,
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
    if search_values:
        vals = [v.strip() for v in search_values.split(",") if v.strip()]
        if vals:
            query = query.where(Entity.ccode.in_(vals))
            count_q = count_q.where(Entity.ccode.in_(vals))
    total = db.execute(count_q).scalar() or 0
    entities = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()

    paths: dict[str, list[str]] = {}
    max_depth = 0
    if hierarchy_id is not None and entities:
        leaf_values = [e.ccode for e in entities if e.ccode]
        paths, max_depth = _resolve_hierarchy_paths(db, hierarchy_id, leaf_values)

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "hierarchy_id": hierarchy_id,
        "hierarchy_max_depth": max_depth,
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
                "levels": paths.get(e.ccode, []),
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
    search_values: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
    hierarchy_id: int | None = None,
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
    if search_values:
        vals = [v.strip() for v in search_values.split(",") if v.strip()]
        if vals:
            query = query.where(LegacyCostCenter.cctr.in_(vals))
            count_q = count_q.where(LegacyCostCenter.cctr.in_(vals))
    total = db.execute(count_q).scalar() or 0
    ccs = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()

    # Optional: enrich each row with its path through the requested
    # hierarchy. PR #90 — uses the setclass-aware resolver, so picking
    # an Entity hierarchy (setclass=0106) correctly resolves via
    # cc.ccode, a PC hierarchy via cc.pctr, etc. The previous code
    # passed cctrs straight in and silently came up empty for non-CC
    # hierarchies.
    paths: dict[str, list[str]] = {}
    max_depth = 0
    if hierarchy_id is not None and ccs:
        paths, max_depth = _resolve_paths_for_ccs(db, hierarchy_id, ccs)

    # Evaluate exclusion rules for this page
    excluded_ids = _get_excluded_ids_for_page(db, ccs, scope, "cost_center")

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "hierarchy_id": hierarchy_id,
        "hierarchy_max_depth": max_depth,
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
                "is_excluded": c.id in excluded_ids,
                "levels": paths.get(c.cctr, []),
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
    search_values: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
    hierarchy_id: int | None = None,
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
    if search_values:
        vals = [v.strip() for v in search_values.split(",") if v.strip()]
        if vals:
            query = query.where(LegacyProfitCenter.pctr.in_(vals))
            count_q = count_q.where(LegacyProfitCenter.pctr.in_(vals))
    total = db.execute(count_q).scalar() or 0
    pcs = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()

    # PR #90 — setclass-aware path resolution. Picking an Entity
    # hierarchy on the PC tab now resolves via pc.ccode; a CC hierarchy
    # returns empty (PCs don't appear in CC trees).
    paths: dict[str, list[str]] = {}
    max_depth = 0
    if hierarchy_id is not None and pcs:
        paths, max_depth = _resolve_paths_for_pcs(db, hierarchy_id, pcs)

    # Evaluate exclusion rules for this page
    excluded_ids = _get_excluded_ids_for_page(db, pcs, scope, "profit_center")

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "hierarchy_id": hierarchy_id,
        "hierarchy_max_depth": max_depth,
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
                "is_excluded": p.id in excluded_ids,
                "levels": paths.get(p.pctr, []),
            }
            for p in pcs
        ],
    }


@router.get("/legacy/gl-accounts")
def list_legacy_gl_accounts(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    ktopl: str | None = None,
    bukrs: str | None = None,
    saknr: str | None = None,
    search: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
) -> dict:
    """List GL accounts (chart-of-accounts level — SAP SKA1).

    Returns the master record from SKA1 with optional company-code-level
    description from SKB1 joined in (as ``stext_skb1`` and ``bukrs``) when
    available. The hierarchical view in the frontend derives buckets from
    the leading characters of ``saknr`` (1-char and 5-char prefixes).
    """
    query = select(GLAccountSKA1).order_by(GLAccountSKA1.saknr)
    count_q = select(func.count(GLAccountSKA1.id))
    if scope:
        query = query.where(GLAccountSKA1.scope == scope)
        count_q = count_q.where(GLAccountSKA1.scope == scope)
    if data_category:
        query = query.where(GLAccountSKA1.data_category == data_category)
        count_q = count_q.where(GLAccountSKA1.data_category == data_category)
    if ktopl:
        query = query.where(GLAccountSKA1.ktopl == ktopl)
        count_q = count_q.where(GLAccountSKA1.ktopl == ktopl)
    if saknr:
        query = query.where(GLAccountSKA1.saknr.ilike(f"{saknr}%"))
        count_q = count_q.where(GLAccountSKA1.saknr.ilike(f"{saknr}%"))
    if search:
        pattern = f"%{search}%"
        query = query.where(
            GLAccountSKA1.saknr.ilike(pattern)
            | GLAccountSKA1.txt20.ilike(pattern)
            | GLAccountSKA1.txt50.ilike(pattern)
        )
        count_q = count_q.where(
            GLAccountSKA1.saknr.ilike(pattern)
            | GLAccountSKA1.txt20.ilike(pattern)
            | GLAccountSKA1.txt50.ilike(pattern)
        )

    total = db.execute(count_q).scalar() or 0
    accounts = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()

    # Optional SKB1 description lookup, scoped consistently with SKA1 query.
    skb1_q = select(GLAccountSKB1)
    if scope:
        skb1_q = skb1_q.where(GLAccountSKB1.scope == scope)
    if data_category:
        skb1_q = skb1_q.where(GLAccountSKB1.data_category == data_category)
    if bukrs:
        skb1_q = skb1_q.where(GLAccountSKB1.bukrs == bukrs)
    skb1_rows = db.execute(skb1_q).scalars().all()
    # Index by saknr — first hit wins (frontend doesn't need every company-code)
    skb1_by_saknr: dict[str, GLAccountSKB1] = {}
    for r in skb1_rows:
        if r.saknr not in skb1_by_saknr:
            skb1_by_saknr[r.saknr] = r

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": [
            {
                "id": a.id,
                "mandt": a.mandt,
                "ktopl": a.ktopl,
                "saknr": a.saknr,
                "txt20": a.txt20,
                "txt50": a.txt50,
                "glaccount_type": a.glaccount_type,
                "glaccount_subtype": a.glaccount_subtype,
                "func_area": a.func_area,
                "ktoks": a.ktoks,
                "xbilk": a.xbilk,
                "xloev": a.xloev,
                "main_saknr": a.main_saknr,
                # SKB1-derived (best-effort, may be None)
                "bukrs": (skb1_by_saknr.get(a.saknr).bukrs if skb1_by_saknr.get(a.saknr) else None),
                "stext_skb1": (
                    skb1_by_saknr.get(a.saknr).stext if skb1_by_saknr.get(a.saknr) else None
                ),
                "waers": (skb1_by_saknr.get(a.saknr).waers if skb1_by_saknr.get(a.saknr) else None),
            }
            for a in accounts
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


@router.get("/center-mappings/overview")
def center_mapping_overview(
    db: Session = Depends(get_db),
    scope: str | None = None,
    search: str | None = None,
) -> dict:
    """Return the 4-column center mapping view:
    Legacy CC, Legacy PC, Target CC, Target PC.

    Groups Target CCs that share a Target PC for rowspan display.
    """
    # Get CC mappings
    cc_q = select(CenterMapping).where(CenterMapping.object_type == "cost_center")
    pc_q = select(CenterMapping).where(CenterMapping.object_type == "profit_center")
    if scope:
        cc_q = cc_q.where(CenterMapping.scope == scope)
        pc_q = pc_q.where(CenterMapping.scope == scope)

    cc_mappings = db.execute(cc_q.order_by(CenterMapping.target_center)).scalars().all()
    pc_mappings = db.execute(pc_q.order_by(CenterMapping.target_center)).scalars().all()

    # Build PC lookup: legacy_center → target_center
    pc_map = {}  # legacy_pc → {target_pc, target_pc_name}
    for pm in pc_mappings:
        pc_map[pm.legacy_center] = {
            "target_center": pm.target_center,
            "target_name": pm.target_name,
        }

    # We need to connect CCs to their PCs.
    # Look up the legacy CC's profit center assignment from the cost center table
    from app.models.core import LegacyCostCenter, TargetCostCenter, CATEGORY_LEGACY, CATEGORY_TARGET

    # Build legacy CC → legacy PC lookup (from CC's profit center field)
    legacy_cc_pc = {}  # cctr → profit_center
    legacy_ccs_q = select(LegacyCostCenter.cctr, LegacyCostCenter.pctr).where(
        LegacyCostCenter.data_category == CATEGORY_LEGACY
    )
    if scope:
        legacy_ccs_q = legacy_ccs_q.where(LegacyCostCenter.scope == scope)
    for row in db.execute(legacy_ccs_q):
        if row.pctr:
            legacy_cc_pc[row.cctr] = row.pctr

    # Build target CC → target PC lookup
    target_cc_pc = {}
    target_ccs_q = select(TargetCostCenter.cctr, TargetCostCenter.pctr).where(
        TargetCostCenter.data_category == CATEGORY_TARGET
    )
    if scope:
        target_ccs_q = target_ccs_q.where(TargetCostCenter.scope == scope)
    for row in db.execute(target_ccs_q):
        if row.pctr:
            target_cc_pc[row.cctr] = row.pctr

    # Build the overview rows
    rows = []
    for cm in cc_mappings:
        legacy_pc = legacy_cc_pc.get(cm.legacy_center, "")
        target_pc = target_cc_pc.get(cm.target_center, "")
        # If no direct target PC from CC, try the PC mapping
        if not target_pc and legacy_pc and legacy_pc in pc_map:
            target_pc = pc_map[legacy_pc].get("target_center", "")

        row = {
            "legacy_cc": cm.legacy_center,
            "legacy_cc_name": cm.legacy_name or "",
            "legacy_pc": legacy_pc,
            "target_cc": cm.target_center,
            "target_cc_name": cm.target_name or "",
            "target_pc": target_pc,
        }
        if search:
            pattern = search.lower()
            searchable = " ".join([
                row["legacy_cc"], row["legacy_cc_name"],
                row["legacy_pc"], row["target_cc"],
                row["target_cc_name"], row["target_pc"],
            ]).lower()
            if pattern not in searchable:
                continue
        rows.append(row)

    # Sort by target_pc then target_cc for grouping
    rows.sort(key=lambda r: (r["target_pc"] or "zzz", r["target_cc"]))

    return {"items": rows, "total": len(rows)}


@router.get("/legacy/balances")
def list_balances(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    ccode: str | None = None,
    coarea: str | None = None,
    cctr: str | None = None,
    fiscal_year: int | None = None,
    search_values: str | None = None,
    scope: str | None = None,
    data_category: str | None = None,
    hierarchy_id: int | None = None,
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
    if search_values:
        vals = [v.strip() for v in search_values.split(",") if v.strip()]
        if vals:
            query = query.where(Balance.cctr.in_(vals))
            count_q = count_q.where(Balance.cctr.in_(vals))
    total = db.execute(count_q).scalar() or 0
    rows = db.execute(query.offset((pag.page - 1) * pag.size).limit(pag.size)).scalars().all()

    paths: dict[str, list[str]] = {}
    max_depth = 0
    use_ccode_key = False
    if hierarchy_id is not None and rows:
        hier = db.get(Hierarchy, hierarchy_id)
        if hier:
            sc = normalise_setclass(hier.setclass)
            use_ccode_key = sc == SETCLASS_ENTITY
            if use_ccode_key:
                leaf_values = list({b.ccode for b in rows if b.ccode})
            else:
                leaf_values = list({b.cctr for b in rows if b.cctr})
            paths, max_depth = _resolve_hierarchy_paths(db, hierarchy_id, leaf_values)

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "hierarchy_id": hierarchy_id,
        "hierarchy_max_depth": max_depth,
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
                "levels": paths.get(b.ccode if use_ccode_key else b.cctr, []),
            }
            for b in rows
        ],
    }


@router.get("/legacy/balances/by-hierarchy")
def balances_by_hierarchy(
    hierarchy_id: int,
    fiscal_year: int | None = None,
    scope: str | None = None,
    data_category: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    """Aggregated balances per cost center, grouped by hierarchy path
    for the Balances → Hierarchical view (PR #89, A14).

    Operator follow-up (PR #90): the previous version JOINed
    ``hierarchy_leaf`` on ``cctr`` only, which silently returned an
    empty result when the operator picked an Entity hierarchy
    (setclass=0106 — leaves are ccodes, not cctrs). The endpoint now
    picks the right JOIN column based on the hierarchy's setclass:

    * ``0101`` (CC)     → JOIN ``hierarchy_leaf.value = balance.cctr``
    * ``0104`` (PC)     → JOIN through ``legacy_cc.pctr`` so we still
      group by CC for the leaf table; the path tells the entity-team
      story
    * ``0106`` (Entity) → JOIN ``hierarchy_leaf.value = balance.ccode``

    The ``totals`` reflect ALL accounts and periods within the optional
    ``fiscal_year`` filter — operators in this view care about scale,
    not the per-account breakdown (the tabular view is for that).
    """
    # Look up the hierarchy first so we can choose the JOIN strategy
    # before issuing the aggregation query.
    hier = db.get(Hierarchy, hierarchy_id)
    if hier is None:
        return {
            "hierarchy_id": hierarchy_id,
            "fiscal_year": fiscal_year,
            "max_depth": 0,
            "total_items": 0,
            "items": [],
        }

    setclass = normalise_setclass(hier.setclass)

    # Build the aggregation query. We always GROUP BY (cctr, ccode) so
    # the leaf table at the bottom of the tree shows per-CC numbers
    # regardless of hierarchy type — only the JOIN to hierarchy_leaf
    # changes.
    if setclass == SETCLASS_ENTITY:
        # Entity hierarchy — join via ccode
        bal_q = (
            select(
                Balance.cctr,
                Balance.ccode,
                func.coalesce(func.sum(Balance.tc_amt), 0).label("tc_amt"),
                func.coalesce(func.sum(Balance.posting_count), 0).label("posting_count"),
                func.count(Balance.id).label("rows"),
            )
            .join(HierarchyLeaf, HierarchyLeaf.value == Balance.ccode)
            .where(HierarchyLeaf.hierarchy_id == hierarchy_id)
            .group_by(Balance.cctr, Balance.ccode)
            .order_by(Balance.cctr)
        )
    elif setclass == SETCLASS_PROFIT_CENTER:
        # PC hierarchy — leaves are pctrs. Join through legacy_cc to
        # get from cctr (the balance row) → pctr (the leaf key).
        bal_q = (
            select(
                Balance.cctr,
                Balance.ccode,
                func.coalesce(func.sum(Balance.tc_amt), 0).label("tc_amt"),
                func.coalesce(func.sum(Balance.posting_count), 0).label("posting_count"),
                func.count(Balance.id).label("rows"),
            )
            .join(LegacyCostCenter, LegacyCostCenter.cctr == Balance.cctr)
            .join(HierarchyLeaf, HierarchyLeaf.value == LegacyCostCenter.pctr)
            .where(HierarchyLeaf.hierarchy_id == hierarchy_id)
            .group_by(Balance.cctr, Balance.ccode)
            .order_by(Balance.cctr)
        )
    else:
        # CC hierarchy (0101) or unknown — leaves are cctrs (the
        # original behaviour).
        bal_q = (
            select(
                Balance.cctr,
                Balance.ccode,
                func.coalesce(func.sum(Balance.tc_amt), 0).label("tc_amt"),
                func.coalesce(func.sum(Balance.posting_count), 0).label("posting_count"),
                func.count(Balance.id).label("rows"),
            )
            .join(HierarchyLeaf, HierarchyLeaf.value == Balance.cctr)
            .where(HierarchyLeaf.hierarchy_id == hierarchy_id)
            .group_by(Balance.cctr, Balance.ccode)
            .order_by(Balance.cctr)
        )

    if scope:
        bal_q = bal_q.where(Balance.scope == scope)
    if data_category:
        bal_q = bal_q.where(Balance.data_category == data_category)
    if fiscal_year:
        bal_q = bal_q.where(Balance.fiscal_year == fiscal_year)

    rows = db.execute(bal_q).all()
    cctrs = [r.cctr for r in rows]

    # Resolve paths. For entity hierarchies we resolve by ccode; for
    # PC hierarchies via the CC's pctr. We need the cctr→ccode and
    # cctr→pctr maps from legacy_cc to do that — same approach as
    # _resolve_paths_for_ccs.
    paths: dict[str, list[str]] = {}
    max_depth = 0
    if cctrs:
        if setclass == SETCLASS_ENTITY:
            # Need cctr → ccode map (we already have ccode on each
            # balance row from the GROUP BY)
            ccode_by_cctr: dict[str, str] = {r.cctr: (r.ccode or "") for r in rows}
            unique_ccodes = list({c for c in ccode_by_cctr.values() if c})
            if unique_ccodes:
                paths_by_ccode, max_depth = _resolve_hierarchy_paths(
                    db, hierarchy_id, unique_ccodes
                )
                for cctr, ccode in ccode_by_cctr.items():
                    if ccode in paths_by_ccode:
                        paths[cctr] = paths_by_ccode[ccode]
        elif setclass == SETCLASS_PROFIT_CENTER:
            # Need cctr → pctr map (not on Balance — fetch from CC)
            pctr_rows = db.execute(
                select(LegacyCostCenter.cctr, LegacyCostCenter.pctr).where(
                    LegacyCostCenter.cctr.in_(cctrs)
                )
            ).all()
            pctr_by_cctr = dict(pctr_rows)
            unique_pctrs = list({p for p in pctr_by_cctr.values() if p})
            if unique_pctrs:
                paths_by_pctr, max_depth = _resolve_hierarchy_paths(db, hierarchy_id, unique_pctrs)
                for cctr, pctr in pctr_by_cctr.items():
                    if pctr in paths_by_pctr:
                        paths[cctr] = paths_by_pctr[pctr]
        else:
            # CC hierarchy — direct cctr resolution
            paths, max_depth = _resolve_hierarchy_paths(db, hierarchy_id, cctrs)

    # Pull short text (txtsh) so the leaf table can show the CC name.
    # One IN-bounded query, no row-by-row lookup.
    txtsh_map: dict[str, str] = {}
    if cctrs:
        cc_rows = db.execute(
            select(LegacyCostCenter.cctr, LegacyCostCenter.txtsh).where(
                LegacyCostCenter.cctr.in_(cctrs)
            )
        ).all()
        txtsh_map = {c: (t or "") for c, t in cc_rows}

    return {
        "hierarchy_id": hierarchy_id,
        "hierarchy_setclass": setclass,
        "fiscal_year": fiscal_year,
        "max_depth": max_depth,
        "total_items": len(rows),
        "items": [
            {
                "cctr": r.cctr,
                "ccode": r.ccode,
                "txtsh": txtsh_map.get(r.cctr, ""),
                "hierarchy_path": paths.get(r.cctr, []),
                "totals": {
                    "tc_amt": float(r.tc_amt or 0),
                    "posting_count": int(r.posting_count or 0),
                    "rows": int(r.rows or 0),
                },
            }
            for r in rows
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
                "label": h.label or "",
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
    """List the distinct setnames in a hierarchy, with depth level and parent.

    Hierarchy data is stored as parent→child edges in ``hierarchy_node``;
    the same setname can appear as a child of multiple parents in some
    SAP setups, but UI consumers want a deduplicated list of selectable
    nodes ordered by depth. This endpoint walks the edge list once,
    computes BFS depth from the root(s), and returns:

    * ``setname`` — the node identifier
    * ``parent`` — the parent setname (or empty string for roots)
    * ``level`` — 0 for root, 1 for direct children, etc.
    * ``description`` — only populated for the hierarchy's root setname
      (which lives in the ``hierarchy`` table, not ``hierarchy_node``).
      Per-node descriptions aren't stored in the schema today.

    Returned items are sorted by (level, setname) for predictable display.
    """
    hier = db.get(Hierarchy, hier_id)
    if not hier:
        raise HTTPException(status_code=404, detail="Hierarchy not found")

    edges = (
        db.execute(
            select(HierarchyNode)
            .where(HierarchyNode.hierarchy_id == hier_id)
            .order_by(HierarchyNode.seq)
        )
        .scalars()
        .all()
    )

    # Build children map + collect all setnames seen as children
    children_map: dict[str, list[str]] = {}
    all_children: set[str] = set()
    for e in edges:
        children_map.setdefault(e.parent_setname, []).append(e.child_setname)
        all_children.add(e.child_setname)

    # Roots are parents that are not themselves children of anything.
    # Fallback: the hierarchy's own setname if no edges exist.
    all_parents = set(children_map.keys())
    roots = sorted(all_parents - all_children)
    if not roots:
        roots = [hier.setname]

    # BFS to compute depth + parent for every reachable setname.
    # If a setname appears under multiple parents (rare but valid in SAP),
    # we keep the FIRST parent encountered and the SHALLOWEST depth.
    levels: dict[str, int] = {}
    parents: dict[str, str] = {}
    queue: list[tuple[str, int, str]] = [(r, 0, "") for r in roots]
    while queue:
        setname, depth, parent = queue.pop(0)
        if setname in levels:
            continue
        levels[setname] = depth
        parents[setname] = parent
        for child in children_map.get(setname, []):
            if child not in levels:
                queue.append((child, depth + 1, setname))

    items = [
        {
            "setname": s,
            "parent": parents[s],
            "level": levels[s],
            # Only the root carries a description (from the hierarchy row itself);
            # individual nodes don't have descriptions in the current schema.
            "description": (hier.description if s in roots else None),
        }
        for s in sorted(levels.keys(), key=lambda x: (levels[x], x))
    ]

    return {
        "hierarchy_id": hier_id,
        "setname": hier.setname,
        "items": items,
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
    """Build a full tree structure for a hierarchy.

    Returns both a nested ``tree`` (for tree-panel rendering) and flat
    ``nodes`` / ``leaves`` arrays (for the DOD's ``_renderHierarchical``
    which builds its own childMap). Leaf items are enriched with basic
    detail fields depending on the hierarchy's setclass:

    * CC hierarchy (0101): leaf value = cctr → look up LegacyCostCenter
    * PC hierarchy (0104): leaf value = pctr → look up LegacyProfitCenter
    * Entity hierarchy (0106): leaf value = ccode → look up Entity
    """
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

    # Build leaves map: setname → list of values
    leaves_map: dict[str, list[str]] = {}
    for lf in leaves:
        leaves_map.setdefault(lf.setname, []).append(lf.value)

    # Setclass-aware leaf enrichment
    norm_sc = normalise_setclass(hier.setclass)
    all_leaf_values = {lf.value for lf in leaves}
    leaf_detail: dict[str, dict] = {}  # value → detail dict

    if all_leaf_values:
        if norm_sc == SETCLASS_PROFIT_CENTER:
            rows = (
                db.execute(
                    select(LegacyProfitCenter).where(LegacyProfitCenter.pctr.in_(all_leaf_values))
                )
                .scalars()
                .all()
            )
            for r in rows:
                leaf_detail[r.pctr] = {
                    "id_field": r.pctr,
                    "name": r.txtsh,
                    "ccode": r.ccode,
                    "coarea": r.coarea,
                    "currency": r.currency,
                    "is_active": r.is_active,
                }
        elif norm_sc == SETCLASS_ENTITY:
            from app.models.core import Entity

            rows = (
                db.execute(select(Entity).where(Entity.ccode.in_(all_leaf_values))).scalars().all()
            )
            for r in rows:
                leaf_detail[r.ccode] = {
                    "id_field": r.ccode,
                    "name": r.name,
                    "country": r.country,
                    "region": r.region,
                    "currency": r.currency,
                    "city": r.city,
                    "is_active": r.is_active,
                }
        else:
            rows = (
                db.execute(
                    select(LegacyCostCenter).where(LegacyCostCenter.cctr.in_(all_leaf_values))
                )
                .scalars()
                .all()
            )
            for r in rows:
                leaf_detail[r.cctr] = {
                    "id_field": r.cctr,
                    "name": r.txtsh,
                    "ccode": r.ccode,
                    "pctr": r.pctr,
                    "responsible": r.responsible,
                }

    # Find root nodes
    all_parents = set(children_map.keys())
    roots = all_parents - all_children
    if not roots:
        roots = {hier.setname}

    def build_node(setname: str, depth: int = 0) -> dict:
        node_children = children_map.get(setname, [])
        node_leaves = leaves_map.get(setname, [])
        items = []
        for val in node_leaves:
            detail = leaf_detail.get(val, {})
            items.append({"value": val, **detail})
        return {
            "setname": setname,
            "type": "leaf" if (not node_children and node_leaves) else "node",
            "children": [build_node(c, depth + 1) for c in sorted(node_children)],
            "items": items,
            "leaf_count": len(node_leaves),
            "depth": depth,
        }

    tree = [build_node(r) for r in sorted(roots)]

    # Flat arrays for DOD's hierarchical renderer
    flat_nodes = [
        {"parent_setname": n.parent_setname, "child_setname": n.child_setname, "seq": n.seq}
        for n in nodes
    ]
    flat_leaves = [{"setname": lf.setname, "value": lf.value, "seq": lf.seq} for lf in leaves]

    return {
        "hierarchy_id": hier_id,
        "setclass": hier.setclass,
        "setname": hier.setname,
        "description": hier.description,
        "tree": tree,
        "nodes": flat_nodes,
        "leaves": flat_leaves,
        "total_leaves": len(leaves),
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
            | Employee.name.ilike(like)
            | Employee.vorname.ilike(like)
            | Employee.bs_name.ilike(like)
            | Employee.bs_first_name.ilike(like)
            | Employee.bs_last_name.ilike(like)
            | Employee.bs_firstname.ilike(like)
            | Employee.bs_lastname.ilike(like)
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
                "verak_display": e.verak_display,
                "is_active": e.is_active,
                "bs_name": e.bs_name,
                "bs_firstname": e.bs_firstname,
                "bs_lastname": e.bs_lastname,
                "name": e.name,
                "vorname": e.vorname,
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
    """Find near-duplicate cost center names using embeddings.

    PR #88 — async dispatch. The previous synchronous version held the
    request handler open for the full duration of the embedding model
    load (cold-start can be several seconds), the per-name embedding
    pass, and the O(n²) pairwise cosine. On a coarea with thousands of
    cost centers operators reported the cluster explorer "taking ages
    and the system becoming unresponsive" — the request was tying up
    a worker and the browser sat on a spinner.

    Now the POST creates a job in an in-memory registry, dispatches a
    daemon thread, and returns ``{job_id, status: 'queued'}`` in
    well under a second. The frontend polls
    ``GET /api/data/duplicate-check/jobs/{id}`` for progress and reads
    the final pairs out of the same endpoint when ``status == 'done'``.

    Trade-off vs a real DB-backed job table: jobs don't survive a
    backend restart. That's acceptable here — duplicate-check is a
    cheap-to-recompute analytical query, not a multi-step workflow,
    and operators can simply re-run if the backend was restarted
    mid-job. Sparing ourselves a model + migration keeps the change
    surface small.
    """
    import threading
    import uuid

    job_id = uuid.uuid4().hex
    _cluster_jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "total": 0,
        "stage": "starting",
        "result": None,
        "error": None,
        "params": {"coarea": coarea, "threshold": threshold, "limit": limit},
    }

    t = threading.Thread(
        target=_run_duplicate_check_in_thread,
        args=(job_id, coarea, threshold, limit),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "status": "queued"}


@router.get("/data/duplicate-check/jobs/{job_id}")
def get_duplicate_check_job(job_id: str) -> dict:
    """Read the status (and, when ``status == 'done'``, result) of a
    duplicate-check job.

    The frontend polls this every ~2s while ``status`` is in
    ``{'queued', 'running'}`` and renders the progress bar from the
    ``stage`` and ``progress`` fields. Once status flips to ``'done'``
    the ``result`` payload mirrors the old synchronous response shape
    (``total`` + ``pairs``), so the UI can drop in the table without
    a separate code path.
    """
    job = _cluster_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "job_id": job_id,
        "status": job["status"],
        "progress": job["progress"],
        "total": job["total"],
        "stage": job["stage"],
        "error": job["error"],
        "result": job["result"] if job["status"] == "done" else None,
    }


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
