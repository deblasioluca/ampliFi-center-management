"""Analytics configuration API (section 11.6)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_role
from app.infra.db.session import get_db
from app.models.core import AnalysisConfig, AnalysisRun, AppUser, GLAccountClassRange

router = APIRouter()


class ConfigCreate(BaseModel):
    code: str
    name: str
    description: str | None = None
    config: dict


class ConfigOut(BaseModel):
    id: int
    code: str
    version: int
    name: str
    description: str | None
    status: str
    config: dict
    # PR #90 — engine_version is derived from the routine prefixes
    # in the config blob (V2 uses ``v2.*`` codes, V1 uses ``rule.*``
    # / ``ml.*`` / ``llm.*``). Lets the dashboard's Config dropdown
    # filter by selected engine instead of showing every config
    # regardless of which engine is selected.
    engine_version: str | None = None

    model_config = {"from_attributes": True}


def _detect_engine_from_config(cfg_blob: dict | None) -> str:
    """Infer engine version from a config's routine prefixes.

    The DB schema doesn't carry an ``engine_version`` column on
    ``analysis_config`` — V1 vs V2 is a property of which routines run.
    V2 ships ``v2.retire_flag``, ``v2.balance_migrate``, ``v2.pc_approach``,
    etc. V1 uses ``rule.*`` / ``ml.*`` / ``llm.*``. Any pipeline with
    a ``v2.*`` routine is treated as V2; otherwise V1.

    Returns ``"v1"`` or ``"v2"``. Empty / unparseable configs fall back
    to ``"v1"`` (the historic default).
    """
    if not cfg_blob:
        return "v1"
    pipeline = cfg_blob.get("pipeline") or cfg_blob.get("routines") or []
    for entry in pipeline:
        code = entry.get("routine") if isinstance(entry, dict) else entry
        if isinstance(code, str) and code.startswith("v2."):
            return "v2"
    return "v1"


@router.get("")
def list_configs(
    db: Session = Depends(get_db),
    user: AppUser = Depends(get_current_user),
    engine: str | None = None,
) -> list[ConfigOut]:
    """List configs.

    PR #90 — accepts an optional ``engine=v1|v2`` filter so the analytics
    dashboard's Config dropdown can match the operator's currently
    selected engine. The filter is applied client-side equivalently
    via ``_detect_engine_from_config`` because there's no engine
    column on the row to filter at the SQL level.

    Each row also carries ``engine_version`` so the frontend can show
    a clear suffix and so other call-sites can branch on it without
    re-running the detection logic.
    """
    configs = db.execute(select(AnalysisConfig).order_by(AnalysisConfig.code)).scalars().all()
    out: list[ConfigOut] = []
    for c in configs:
        item = ConfigOut.model_validate(c)
        item.engine_version = _detect_engine_from_config(c.config)
        if engine and item.engine_version != engine:
            continue
        out.append(item)
    return out


@router.post("")
def create_config(
    body: ConfigCreate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager")),
) -> ConfigOut:
    cfg = AnalysisConfig(
        code=body.code,
        name=body.name,
        description=body.description,
        config=body.config,
        created_by=user.id,
    )
    db.add(cfg)
    db.commit()
    db.refresh(cfg)
    return ConfigOut.model_validate(cfg)


# ── Rule catalog & presets (REGISTERED BEFORE /{code} catch-all) ─────────
# FastAPI matches routes in registration order. The /{code} route below
# would otherwise treat "rule-catalog", "presets", etc. as values for the
# {code} path parameter and return 404 "Config not found".


@router.get("/rule-catalog")
def get_rule_catalog(tree: str | None = None) -> dict:
    """Return business-friendly metadata for every routine.

    The catalog is the single source of truth for the config editor UI:
    it exposes friendly labels, plain-language descriptions, parameter
    schemas with units and help text, and the verdicts each rule can
    emit. The frontend renders forms from this — no raw JSON editing.

    Optional ``tree`` filter: 'cleansing' | 'mapping' | 'v2'.
    """
    from app.domain.decision_tree.rule_catalog import list_rule_catalog

    entries = list_rule_catalog(tree=tree)
    return {
        "total": len(entries),
        "entries": entries,
    }


@router.get("/presets")
def list_config_presets(engine: str = "v1") -> dict:
    """Return preset variant templates for the given engine ('v1' | 'v2').

    Presets are starting points for new variants. A user picks one
    (e.g. 'Standard — recommended'), forks it via /presets/{name}/instantiate,
    then tweaks parameters in the editor and saves with a friendly name.
    """
    from app.domain.decision_tree.rule_catalog import list_presets

    presets = list_presets(engine)
    return {
        "engine": engine.lower(),
        "presets": [{"name": k, **v} for k, v in presets.items()],
    }


class InstantiatePresetIn(BaseModel):
    """Request body for creating a config from a preset."""

    name: str  # human-friendly name for the new config
    code: str | None = None  # optional unique code; auto-generated if omitted
    description: str | None = None


@router.post("/presets/{engine}/{preset_name}/instantiate")
def instantiate_preset(
    engine: str,
    preset_name: str,
    body: InstantiatePresetIn,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager")),
) -> dict:
    """Create a new AnalysisConfig from a preset template.

    The new config is a normal versioned config — the caller can then
    fork/amend it using the existing /{code}/fork and /{code}/amend
    endpoints, edit it via the rule-catalog driven UI, and apply it to
    analysis runs via /waves/{id}/analyse-with-engine.
    """
    from app.domain.decision_tree.rule_catalog import (
        build_v1_config_from_preset,
        build_v2_config_from_preset,
        get_preset,
    )

    if engine.lower() not in ("v1", "v2"):
        raise HTTPException(status_code=400, detail="engine must be 'v1' or 'v2'")
    if not get_preset(engine, preset_name):
        raise HTTPException(status_code=404, detail=f"Unknown {engine} preset: {preset_name}")

    if engine.lower() == "v1":
        config_data = build_v1_config_from_preset(preset_name)
        default_code = f"v1-{preset_name}"
    else:
        config_data = build_v2_config_from_preset(preset_name)
        default_code = f"v2-{preset_name}"

    code = body.code or default_code
    existing = db.execute(
        select(AnalysisConfig).where(AnalysisConfig.code == code)
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Config code '{code}' already exists (id={existing.id}). "
                "Pass a unique 'code' in the body or fork the existing one."
            ),
        )

    config = AnalysisConfig(
        code=code,
        version=1,
        name=body.name,
        description=body.description or f"Instantiated from preset '{preset_name}' ({engine})",
        status="active",
        config=config_data,
        created_by=user.id,
    )
    db.add(config)
    db.commit()
    db.refresh(config)
    return {
        "id": config.id,
        "code": config.code,
        "version": config.version,
        "name": config.name,
        "engine": engine.lower(),
        "preset": preset_name,
    }


@router.get("/{code}")
def get_config(code: str, db: Session = Depends(get_db)) -> ConfigOut:
    cfg = (
        db.execute(
            select(AnalysisConfig)
            .where(AnalysisConfig.code == code)
            .order_by(AnalysisConfig.version.desc())
        )
        .scalars()
        .first()
    )
    if not cfg:
        raise HTTPException(status_code=404, detail="Config not found")
    return ConfigOut.model_validate(cfg)


@router.get("/{code}/versions")
def list_config_versions(code: str, db: Session = Depends(get_db)) -> list[ConfigOut]:
    configs = (
        db.execute(
            select(AnalysisConfig)
            .where(AnalysisConfig.code == code)
            .order_by(AnalysisConfig.version.desc())
        )
        .scalars()
        .all()
    )
    return [ConfigOut.model_validate(c) for c in configs]


@router.post("/{code}/fork")
def fork_config(
    code: str,
    new_code: str,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager")),
) -> ConfigOut:
    parent = (
        db.execute(
            select(AnalysisConfig)
            .where(AnalysisConfig.code == code)
            .order_by(AnalysisConfig.version.desc())
        )
        .scalars()
        .first()
    )
    if not parent:
        raise HTTPException(status_code=404, detail="Parent config not found")
    child = AnalysisConfig(
        code=new_code,
        name=f"Fork of {parent.name}",
        description=parent.description,
        parent_code=code,
        config=parent.config,
        created_by=user.id,
    )
    db.add(child)
    db.commit()
    db.refresh(child)
    return ConfigOut.model_validate(child)


@router.post("/{code}/amend")
def amend_config(
    code: str,
    body: ConfigCreate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager")),
) -> ConfigOut:
    latest = (
        db.execute(
            select(AnalysisConfig)
            .where(AnalysisConfig.code == code)
            .order_by(AnalysisConfig.version.desc())
        )
        .scalars()
        .first()
    )
    new_version = (latest.version + 1) if latest else 1
    cfg = AnalysisConfig(
        code=code,
        version=new_version,
        name=body.name,
        description=body.description,
        config=body.config,
        created_by=user.id,
    )
    db.add(cfg)
    db.commit()
    db.refresh(cfg)
    return ConfigOut.model_validate(cfg)


@router.delete("/{code}")
def delete_config(
    code: str,
    force: bool = False,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    """Delete every version of a Decision Tree variant identified by ``code``.

    Default behaviour is conservative: if ANY ``analysis_run`` row
    references any version of this config, the call returns 409 with
    a count of referencing runs. This protects audit history — by
    default you cannot delete the config that produced an existing run
    because doing so would orphan the run row (the FK is RESTRICT and
    NOT NULL).

    Pass ``?force=true`` to override. With force=true the endpoint
    deletes the referencing analysis_run rows first (which cascades to
    their proposals, routine outputs, and related child rows via the
    FK cascades on those tables), then deletes the config rows. This
    is the equivalent of "I know I'm destroying audit history, do it
    anyway" — admin-only.

    Built-in code-defined configs (``cema_migration_v1``,
    ``cema_migration_v2``) can be deleted: the create-on-demand path
    in the V1 / V2 endpoints will recreate a default version next time
    one is needed. So a delete here doesn't permanently break anything.

    Returns a small summary so the UI can show what was actually
    affected: which versions were removed, how many runs got cascaded
    out (when force=true).
    """
    versions = db.execute(select(AnalysisConfig).where(AnalysisConfig.code == code)).scalars().all()
    if not versions:
        raise HTTPException(status_code=404, detail=f"No config with code '{code}'")

    version_ids = [v.id for v in versions]

    # Count referencing runs across ALL versions of this code so the
    # 409 message and the force-delete summary are accurate.
    ref_count = (
        db.execute(
            select(func.count(AnalysisRun.id)).where(AnalysisRun.config_id.in_(version_ids))
        ).scalar()
        or 0
    )

    if ref_count and not force:
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    f"Config '{code}' is referenced by {ref_count} analysis run(s). "
                    "Pass ?force=true to delete the config AND those runs (and their "
                    "child rows via cascade)."
                ),
                "code": code,
                "version_count": len(versions),
                "referencing_runs": int(ref_count),
            },
        )

    # Force path: delete the runs first so the FK constraint passes.
    # CenterProposal, RoutineOutput, etc. cascade off analysis_run.id.
    if ref_count:
        db.execute(delete(AnalysisRun).where(AnalysisRun.config_id.in_(version_ids)))

    db.execute(delete(AnalysisConfig).where(AnalysisConfig.id.in_(version_ids)))
    db.commit()

    return {
        "deleted": True,
        "code": code,
        "versions_deleted": len(versions),
        "runs_deleted": int(ref_count) if force else 0,
    }


# --- DSL Rule Engine (§04.5) ---


class DSLRuleValidation(BaseModel):
    rule: dict


@router.post("/dsl/validate")
def validate_dsl_rule(body: DSLRuleValidation) -> dict:
    """Validate a DSL rule definition without executing it."""
    from app.domain.decision_tree.dsl import validate_rule

    errors = validate_rule(body.rule)
    return {"valid": len(errors) == 0, "errors": errors}


@router.get("/dsl/operators")
def list_dsl_operators() -> dict:
    """List available DSL operators for the rule builder UI."""
    from app.domain.decision_tree.dsl import OPS

    return {
        "operators": list(OPS.keys()),
        "fields": [
            "cctr",
            "ccode",
            "coarea",
            "txtsh",
            "txtmi",
            "responsible",
            "category",
            "is_active",
            "months_since_last_posting",
            "posting_count_window",
            "bs_amt",
            "opex_amt",
            "rev_amt",
            "hierarchy_depth",
        ],
        "verdicts": ["KEEP", "RETIRE", "MERGE_MAP", "REDESIGN"],
    }


# --- GL Account Class Ranges (§03.5) ---


class GLRangeCreate(BaseModel):
    class_code: str
    class_label: str
    from_account: str
    to_account: str
    category: str | None = None


class GLRangeOut(BaseModel):
    id: int
    class_code: str
    class_label: str
    from_account: str
    to_account: str
    category: str | None

    model_config = {"from_attributes": True}


@router.get("/gl-ranges")
def list_gl_ranges(db: Session = Depends(get_db)) -> list[GLRangeOut]:
    rows = (
        db.execute(select(GLAccountClassRange).order_by(GLAccountClassRange.from_account))
        .scalars()
        .all()
    )
    return [GLRangeOut.model_validate(r) for r in rows]


@router.post("/gl-ranges")
def create_gl_range(
    body: GLRangeCreate,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> GLRangeOut:
    r = GLAccountClassRange(
        class_code=body.class_code,
        class_label=body.class_label,
        from_account=body.from_account,
        to_account=body.to_account,
        category=body.category,
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return GLRangeOut.model_validate(r)


@router.delete("/gl-ranges/{range_id}")
def delete_gl_range(
    range_id: int,
    db: Session = Depends(get_db),
    _user: AppUser = Depends(require_role("admin")),
) -> dict:
    r = db.get(GLAccountClassRange, range_id)
    if not r:
        raise HTTPException(status_code=404, detail="Range not found")
    db.delete(r)
    db.commit()
    return {"deleted": True}
