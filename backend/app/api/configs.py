"""Analytics configuration API (section 11.6)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_role
from app.infra.db.session import get_db
from app.models.core import AnalysisConfig, AppUser, GLAccountClassRange

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

    model_config = {"from_attributes": True}


@router.get("")
def list_configs(
    db: Session = Depends(get_db),
    user: AppUser = Depends(get_current_user),
) -> list[ConfigOut]:
    configs = db.execute(select(AnalysisConfig).order_by(AnalysisConfig.code)).scalars().all()
    return [ConfigOut.model_validate(c) for c in configs]


@router.post("")
def create_config(
    body: ConfigCreate,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "analyst")),
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
    user: AppUser = Depends(require_role("admin", "analyst")),
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
    user: AppUser = Depends(require_role("admin", "analyst")),
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
    user: AppUser = Depends(require_role("admin", "analyst")),
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
