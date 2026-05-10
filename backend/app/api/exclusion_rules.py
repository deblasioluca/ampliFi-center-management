"""Center exclusion rules API — configure which centers to exclude from migration."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, require_role
from app.infra.db.session import get_db
from app.models.core import CenterExclusionRule

router = APIRouter()


class ExclusionRuleCreate(BaseModel):
    scope: str | None = None
    object_type: str = "both"
    name: str
    description: str | None = None
    condition: dict
    is_enabled: bool = True
    sort_order: int = 0


class ExclusionRuleUpdate(BaseModel):
    scope: str | None = None
    object_type: str | None = None
    name: str | None = None
    description: str | None = None
    condition: dict | None = None
    is_enabled: bool | None = None
    sort_order: int | None = None


class ExclusionRuleOut(BaseModel):
    id: int
    scope: str | None
    object_type: str
    name: str
    description: str | None
    condition: dict
    is_enabled: bool
    is_system: bool
    sort_order: int

    model_config = {"from_attributes": True}


@router.get("/exclusion-rules", response_model=list[ExclusionRuleOut])
def list_exclusion_rules(
    scope: str | None = None,
    db: Session = Depends(get_db),
    _user=Depends(get_current_user),
):
    """List all exclusion rules, optionally filtered by scope."""
    q = select(CenterExclusionRule).order_by(CenterExclusionRule.sort_order)
    if scope:
        # Return global rules (scope=NULL) + scope-specific
        q = q.where(
            (CenterExclusionRule.scope == None) | (CenterExclusionRule.scope == scope)  # noqa: E711
        )
    rules = db.scalars(q).all()
    return rules


@router.post("/exclusion-rules", response_model=ExclusionRuleOut, status_code=201)
def create_exclusion_rule(
    body: ExclusionRuleCreate,
    db: Session = Depends(get_db),
    _user=Depends(require_role("admin")),
):
    """Create a new exclusion rule."""
    rule = CenterExclusionRule(
        scope=body.scope,
        object_type=body.object_type,
        name=body.name,
        description=body.description,
        condition=body.condition,
        is_enabled=body.is_enabled,
        is_system=False,
        sort_order=body.sort_order,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


@router.put("/exclusion-rules/{rule_id}", response_model=ExclusionRuleOut)
def update_exclusion_rule(
    rule_id: int,
    body: ExclusionRuleUpdate,
    db: Session = Depends(get_db),
    _user=Depends(require_role("admin")),
):
    """Update an existing exclusion rule."""
    rule = db.get(CenterExclusionRule, rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found")
    updates = body.model_dump(exclude_unset=True)
    for k, v in updates.items():
        setattr(rule, k, v)
    db.commit()
    db.refresh(rule)
    return rule


@router.delete("/exclusion-rules/{rule_id}", status_code=204)
def delete_exclusion_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    _user=Depends(require_role("admin")),
):
    """Delete an exclusion rule (system rules can only be disabled, not deleted)."""
    rule = db.get(CenterExclusionRule, rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found")
    if rule.is_system:
        raise HTTPException(400, "System rules cannot be deleted — disable them instead")
    db.delete(rule)
    db.commit()


@router.post("/exclusion-rules/{rule_id}/toggle", response_model=ExclusionRuleOut)
def toggle_exclusion_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    _user=Depends(require_role("admin")),
):
    """Toggle a rule's enabled/disabled state."""
    rule = db.get(CenterExclusionRule, rule_id)
    if not rule:
        raise HTTPException(404, "Rule not found")
    rule.is_enabled = not rule.is_enabled
    db.commit()
    db.refresh(rule)
    return rule


@router.get("/exclusion-rules/evaluate")
def evaluate_exclusion(
    scope: str = "cleanup",
    object_type: str = "cost_center",
    db: Session = Depends(get_db),
    _user=Depends(get_current_user),
):
    """Return list of center IDs that would be excluded by current rules.

    This is used by the frontend to badge excluded centers in the tables.
    """
    from app.models.core import LegacyCostCenter, LegacyProfitCenter, CATEGORY_LEGACY

    # Get enabled rules for this scope and object type
    q = select(CenterExclusionRule).where(
        CenterExclusionRule.is_enabled == True,  # noqa: E712
        (CenterExclusionRule.scope == None) | (CenterExclusionRule.scope == scope),  # noqa: E711
        (CenterExclusionRule.object_type == "both") | (CenterExclusionRule.object_type == object_type),
    )
    rules = db.scalars(q).all()
    if not rules:
        return {"excluded_ids": [], "rules_applied": 0}

    # Determine model
    if object_type == "cost_center":
        Model = LegacyCostCenter
    elif object_type == "profit_center":
        Model = LegacyProfitCenter
    else:
        return {"excluded_ids": [], "rules_applied": 0}

    # Get all legacy centers for this scope
    centers = db.scalars(
        select(Model).where(
            Model.scope == scope,
            Model.data_category == CATEGORY_LEGACY,
        )
    ).all()

    # Evaluate rules against each center
    excluded_ids = set()
    for center in centers:
        for rule in rules:
            if _matches_condition(center, rule.condition):
                excluded_ids.add(center.id)
                break

    return {"excluded_ids": sorted(excluded_ids), "rules_applied": len(rules)}


def _matches_condition(obj, condition: dict) -> bool:
    """Evaluate a single condition against an object."""
    field = condition.get("field", "")
    operator = condition.get("operator", "==")
    value = condition.get("value")

    actual = getattr(obj, field, None)
    actual_str = str(actual).strip() if actual is not None else None

    if operator == "==":
        return actual_str == str(value) if actual_str is not None else False
    elif operator == "!=":
        if actual_str is None:
            return True  # NULL != anything is true
        return actual_str != str(value)
    elif operator == "in":
        if not isinstance(value, list):
            return False
        return actual_str in [str(v) for v in value] if actual_str else False
    elif operator == "not_in":
        if not isinstance(value, list):
            return True
        return actual_str not in [str(v) for v in value] if actual_str else True
    elif operator == "is_null":
        return actual is None or actual_str == ""
    elif operator == "is_not_null":
        return actual is not None and actual_str != ""
    elif operator in ("<", ">", "<=", ">="):
        if actual_str is None:
            return False
        try:
            a = float(actual_str) if "." in actual_str else int(actual_str)
            b = float(str(value)) if "." in str(value) else int(str(value))
        except (ValueError, TypeError):
            a, b = actual_str, str(value)
        if operator == "<":
            return a < b
        elif operator == ">":
            return a > b
        elif operator == "<=":
            return a <= b
        elif operator == ">=":
            return a >= b
    return False
