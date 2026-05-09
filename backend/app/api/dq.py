"""Data Quality issue API — list, resolve, and manage DQ issues."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import PaginationParams, pagination, require_role
from app.infra.db.session import get_db
from app.models.core import (
    AppUser,
    DataQualityIssue,
    Employee,
    LegacyCostCenter,
    LegacyProfitCenter,
)

router = APIRouter()


@router.get("/issues")
def list_issues(
    db: Session = Depends(get_db),
    pag: PaginationParams = Depends(pagination),
    scope: str | None = None,
    status: str | None = None,
    object_type: str | None = None,
    rule_code: str | None = None,
    batch_id: int | None = None,
    severity: str | None = None,
) -> dict:
    """List data quality issues with optional filters."""
    query = select(DataQualityIssue)
    count_q = select(func.count(DataQualityIssue.id))

    for filt_col, filt_val in (
        (DataQualityIssue.scope, scope),
        (DataQualityIssue.status, status),
        (DataQualityIssue.object_type, object_type),
        (DataQualityIssue.rule_code, rule_code),
        (DataQualityIssue.batch_id, batch_id),
        (DataQualityIssue.severity, severity),
    ):
        if filt_val is not None:
            query = query.where(filt_col == filt_val)
            count_q = count_q.where(filt_col == filt_val)

    total = db.execute(count_q).scalar() or 0
    issues = (
        db.execute(
            query.order_by(DataQualityIssue.severity.desc(), DataQualityIssue.id)
            .offset((pag.page - 1) * pag.size)
            .limit(pag.size)
        )
        .scalars()
        .all()
    )

    items = []
    for iss in issues:
        row: dict = {
            "id": iss.id,
            "scope": iss.scope,
            "object_type": iss.object_type,
            "object_id": iss.object_id,
            "field_name": iss.field_name,
            "rule_code": iss.rule_code,
            "severity": iss.severity,
            "message": iss.message,
            "current_value": iss.current_value,
            "suggested_value": iss.suggested_value,
            "suggested_employee_id": iss.suggested_employee_id,
            "status": iss.status,
            "resolved_by": iss.resolved_by,
            "resolved_at": iss.resolved_at.isoformat() if iss.resolved_at else None,
            "resolved_value": iss.resolved_value,
            "batch_id": iss.batch_id,
        }
        # Enrich with object context
        if iss.object_type == "cost_center":
            cc = db.get(LegacyCostCenter, iss.object_id)
            if cc:
                row["cctr"] = cc.cctr
                row["coarea"] = cc.coarea
                row["txtsh"] = cc.txtsh
        elif iss.object_type == "profit_center":
            pc = db.get(LegacyProfitCenter, iss.object_id)
            if pc:
                row["pctr"] = pc.pctr
                row["coarea"] = pc.coarea
                row["txtsh"] = pc.txtsh
        # Enrich suggested employee
        if iss.suggested_employee_id:
            emp = db.get(Employee, iss.suggested_employee_id)
            if emp:
                row["suggested_employee_name"] = emp.display_name
        items.append(row)

    return {
        "total": total,
        "page": pag.page,
        "size": pag.size,
        "items": items,
    }


@router.get("/issues/summary")
def issue_summary(
    db: Session = Depends(get_db),
    scope: str | None = None,
    batch_id: int | None = None,
) -> dict:
    """Aggregate counts by status and severity."""
    base = select(DataQualityIssue)
    if scope:
        base = base.where(DataQualityIssue.scope == scope)
    if batch_id:
        base = base.where(DataQualityIssue.batch_id == batch_id)

    by_status = {}
    for st in ("open", "resolved", "auto_fixed", "suppressed"):
        cnt = (
            db.execute(
                select(func.count(DataQualityIssue.id)).where(
                    DataQualityIssue.status == st,
                    *([DataQualityIssue.scope == scope] if scope else []),
                    *([DataQualityIssue.batch_id == batch_id] if batch_id else []),
                )
            ).scalar()
            or 0
        )
        by_status[st] = cnt

    by_severity = {}
    for sev in ("error", "warning", "info"):
        cnt = (
            db.execute(
                select(func.count(DataQualityIssue.id)).where(
                    DataQualityIssue.severity == sev,
                    DataQualityIssue.status == "open",
                    *([DataQualityIssue.scope == scope] if scope else []),
                    *([DataQualityIssue.batch_id == batch_id] if batch_id else []),
                )
            ).scalar()
            or 0
        )
        by_severity[sev] = cnt

    return {
        "by_status": by_status,
        "by_severity": by_severity,
        "total_open": by_status.get("open", 0),
    }


class ResolveBody(BaseModel):
    employee_id: int | None = None
    value: str | None = None
    action: str = "resolve"  # resolve, suppress


@router.post("/issues/{issue_id}/resolve")
def resolve_issue(
    issue_id: int,
    body: ResolveBody,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager", "reviewer")),
) -> dict:
    """Resolve a DQ issue — optionally provide the corrected employee/value."""
    issue = db.get(DataQualityIssue, issue_id)
    if not issue:
        raise HTTPException(status_code=404, detail="Issue not found")
    if issue.status not in ("open",):
        raise HTTPException(status_code=409, detail=f"Issue already {issue.status}")

    now = datetime.now(UTC)
    resolved_value = body.value
    resolved_emp_id = body.employee_id

    if body.action == "suppress":
        issue.status = "suppressed"
        issue.resolved_by = user.username
        issue.resolved_at = now
        db.commit()
        return {"status": "suppressed", "issue_id": issue_id}

    # Resolve: update the source object's VERAK field
    if body.employee_id:
        emp = db.get(Employee, body.employee_id)
        if not emp:
            raise HTTPException(status_code=404, detail="Employee not found")
        resolved_value = emp.verak_display
        resolved_emp_id = emp.id

        # Update the source object
        if issue.object_type == "cost_center":
            cc = db.get(LegacyCostCenter, issue.object_id)
            if cc:
                cc.responsible = resolved_value
                cc.responsible_employee_id = emp.id
        elif issue.object_type == "profit_center":
            pc = db.get(LegacyProfitCenter, issue.object_id)
            if pc:
                pc.responsible = resolved_value
                pc.responsible_employee_id = emp.id
    elif body.value:
        # Manual override without employee link
        if issue.object_type == "cost_center":
            cc = db.get(LegacyCostCenter, issue.object_id)
            if cc:
                cc.responsible = body.value
        elif issue.object_type == "profit_center":
            pc = db.get(LegacyProfitCenter, issue.object_id)
            if pc:
                pc.responsible = body.value

    issue.status = "resolved"
    issue.resolved_by = user.username
    issue.resolved_at = now
    issue.resolved_value = resolved_value
    issue.resolved_employee_id = resolved_emp_id
    db.commit()

    return {"status": "resolved", "issue_id": issue_id, "resolved_value": resolved_value}


class BulkResolveBody(BaseModel):
    issue_ids: list[int]
    action: str = "suppress"  # suppress only for bulk


@router.post("/issues/bulk-resolve")
def bulk_resolve(
    body: BulkResolveBody,
    db: Session = Depends(get_db),
    user: AppUser = Depends(require_role("admin", "data_manager")),
) -> dict:
    """Bulk suppress DQ issues."""
    now = datetime.now(UTC)
    count = 0
    for iid in body.issue_ids:
        issue = db.get(DataQualityIssue, iid)
        if issue and issue.status == "open":
            issue.status = body.action
            issue.resolved_by = user.username
            issue.resolved_at = now
            count += 1
    db.commit()
    return {"resolved": count}
