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
