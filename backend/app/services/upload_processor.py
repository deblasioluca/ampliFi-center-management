"""Upload processing service — parses CSV/Excel files and loads into DB."""

from __future__ import annotations

import csv
import io
import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.core import (
    Balance,
    Entity,
    LegacyCostCenter,
    LegacyProfitCenter,
    UploadBatch,
    UploadError,
)

log = logging.getLogger(__name__)

# Column mappings: normalize header names to model fields
CC_COLUMNS = {
    "COAREA": "coarea",
    "CCTR": "cctr",
    "TXTSH": "txtsh",
    "TXTMI": "txtmi",
    "CCTRRESPP": "responsible",
    "RESPONSIBLE": "responsible",
    "CCTRCGY": "cctrcgy",
    "CCODECCTR": "ccode",
    "CCODE": "ccode",
    "CURRCCTR": "currency",
    "CURRENCY": "currency",
    "PCTRCCTR": "pctr",
    "PCTR": "pctr",
    "IS_ACTIVE": "is_active",
}
PC_COLUMNS = {
    "COAREA": "coarea",
    "PCTR": "pctr",
    "TXTMI": "txtmi",
    "TXTSH": "txtsh",
    "PCTRDEPT": "department",
    "DEPARTMENT": "department",
    "PCTRRESPP": "responsible",
    "RESPONSIBLE": "responsible",
    "PC_SPRAS": "language",
    "PCTRCCALL": "ccode",
    "CCODE": "ccode",
    "CURRPCTR": "currency",
    "CURRENCY": "currency",
    "IS_ACTIVE": "is_active",
}
BALANCE_COLUMNS = {
    "COAREA": "coarea",
    "COMPANY_CODE": "ccode",
    "CCODE": "ccode",
    "SAP_MANAGEMENT_CENTER": "cctr",
    "CCTR": "cctr",
    "FISCAL_YEAR": "fiscal_year",
    "PERIOD_YYYYMM": "period_raw",
    "PERIOD": "period_raw",
    "ACCOUNT": "account",
    "CURR_CODE_ISO_TC": "currency_tc",
    "CURRENCY_TC": "currency_tc",
    "CURRENCY_GC": "currency_gc",
    "CURRENCY_GC2": "currency_gc2",
    "SUM_TC": "tc_amt",
    "TC_AMT": "tc_amt",
    "GC_AMT": "gc_amt",
    "SUM_GC2": "gc2_amt",
    "GC2_AMT": "gc2_amt",
    "COUNT": "posting_count",
    "POSTING_COUNT": "posting_count",
    "ACCOUNT_CLASS": "account_class",
}
ENTITY_COLUMNS = {
    "COMPANY_CODE": "ccode",
    "CCODE": "ccode",
    "NAME": "name",
    "COUNTRY": "country",
    "REGION": "region",
    "CURRENCY": "currency",
    "IS_ACTIVE": "is_active",
}


def _read_file(path: str) -> list[dict[str, str]]:
    """Read CSV or Excel file and return list of row dicts."""
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix in (".xlsx", ".xls"):
        try:
            import openpyxl

            wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
            ws = wb.active
            rows_iter = ws.iter_rows(values_only=True)
            headers = [str(h or "").strip() for h in next(rows_iter)]
            result = []
            for row in rows_iter:
                d = {}
                for i, val in enumerate(row):
                    if i < len(headers) and headers[i]:
                        d[headers[i]] = str(val) if val is not None else ""
                result.append(d)
            wb.close()
            return result
        except ImportError as exc:
            raise ValueError("openpyxl not installed") from exc
    else:
        content = p.read_text(encoding="utf-8-sig")
        # Skip MDG header lines starting with *
        lines = content.split("\n")
        clean_lines = [ln for ln in lines if not ln.startswith("*")]
        if not clean_lines or not clean_lines[0].strip():
            return []
        reader = csv.DictReader(
            io.StringIO("\n".join(clean_lines)), delimiter="," if "," in clean_lines[0] else "\t"
        )
        return [dict(row) for row in reader]


def _normalize_headers(rows: list[dict[str, str]], mapping: dict[str, str]) -> list[dict[str, str]]:
    """Normalize column headers using mapping."""
    result = []
    for row in rows:
        normalized: dict[str, str] = {}
        extras: dict[str, str] = {}
        for key, val in row.items():
            upper_key = key.strip().upper()
            if upper_key in mapping:
                normalized[mapping[upper_key]] = val.strip() if val else ""
            else:
                extras[key.strip()] = val.strip() if val else ""
        if extras:
            normalized["_extras"] = str(extras)
        result.append(normalized)
    return result


def validate_upload(batch_id: int, db: Session) -> dict:
    """Validate an uploaded file and return summary."""
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if not batch.storage_uri:
        raise ValueError("No file associated with this batch")

    batch.status = "validating"
    db.execute(sa_delete(UploadError).where(UploadError.batch_id == batch.id))
    db.commit()

    try:
        rows = _read_file(batch.storage_uri)
    except Exception as e:
        batch.status = "failed"
        db.add(
            UploadError(
                batch_id=batch.id,
                row_number=0,
                error_code="FILE_READ",
                message=f"Cannot read file: {e}",
            )
        )
        db.commit()
        return {"status": "failed", "error": str(e)}

    mapping = {
        "cost_center": CC_COLUMNS,
        "cost_centers": CC_COLUMNS,
        "profit_center": PC_COLUMNS,
        "profit_centers": PC_COLUMNS,
        "balance": BALANCE_COLUMNS,
        "balances": BALANCE_COLUMNS,
        "entity": ENTITY_COLUMNS,
        "entities": ENTITY_COLUMNS,
    }.get(batch.kind, {})

    normalized = _normalize_headers(rows, mapping) if mapping else rows
    errors: list[dict] = []
    error_rows: set[int] = set()

    for i, row in enumerate(normalized, start=1):
        if batch.kind in ("cost_center", "cost_centers"):
            if not row.get("cctr"):
                errors.append(
                    {"row": i, "col": "CCTR", "code": "REQUIRED", "msg": "CCTR is required"},
                )
                error_rows.add(i)
            if not row.get("coarea"):
                errors.append(
                    {"row": i, "col": "COAREA", "code": "REQUIRED", "msg": "COAREA is required"},
                )
                error_rows.add(i)
        elif batch.kind in ("profit_center", "profit_centers"):
            if not row.get("pctr"):
                errors.append(
                    {"row": i, "col": "PCTR", "code": "REQUIRED", "msg": "PCTR is required"},
                )
                error_rows.add(i)
        elif batch.kind in ("balance", "balances"):
            if not row.get("cctr"):
                errors.append(
                    {
                        "row": i,
                        "col": "SAP_MANAGEMENT_CENTER",
                        "code": "REQUIRED",
                        "msg": "SAP_MANAGEMENT_CENTER is required",
                    }
                )
                error_rows.add(i)
            pr = row.get("period_raw", "")
            fy = row.get("fiscal_year", "")
            if fy and pr:
                if not fy.isdigit():
                    msg = f"FISCAL_YEAR must be numeric, got: {fy}"
                    errors.append(
                        {"row": i, "col": "FISCAL_YEAR", "code": "FORMAT", "msg": msg},
                    )
                    error_rows.add(i)
                if not pr.isdigit():
                    msg = f"PERIOD must be numeric, got: {pr}"
                    errors.append(
                        {"row": i, "col": "PERIOD", "code": "FORMAT", "msg": msg},
                    )
                    error_rows.add(i)
            elif pr and (len(pr) != 6 or not pr.isdigit()):
                errors.append(
                    {
                        "row": i,
                        "col": "PERIOD_YYYYMM",
                        "code": "FORMAT",
                        "msg": f"Period must be YYYYMM, got: {pr}",
                    }
                )
                error_rows.add(i)
        elif batch.kind in ("entity", "entities"):
            if not row.get("ccode"):
                errors.append(
                    {
                        "row": i,
                        "col": "COMPANY_CODE",
                        "code": "REQUIRED",
                        "msg": "COMPANY_CODE is required",
                    }
                )
                error_rows.add(i)

    # Store errors
    for err in errors[:5000]:
        db.add(
            UploadError(
                batch_id=batch.id,
                row_number=err["row"],
                column_name=err["col"],
                error_code=err["code"],
                message=err["msg"],
            )
        )

    batch.rows_total = len(normalized)
    batch.rows_valid = len(normalized) - len(error_rows)
    batch.rows_error = len(error_rows)
    batch.status = "validated"
    batch.validated_at = datetime.now(UTC)
    db.commit()

    return {
        "status": "validated",
        "rows_total": batch.rows_total,
        "rows_valid": batch.rows_valid,
        "rows_error": batch.rows_error,
    }


def load_upload(batch_id: int, db: Session) -> dict:
    """Load validated upload into target tables."""
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if batch.status != "validated":
        raise ValueError(f"Batch must be validated first (status: {batch.status})")

    batch.status = "loading"
    db.commit()

    try:
        rows = _read_file(batch.storage_uri)
    except Exception as e:
        batch.status = "failed"
        db.commit()
        return {"status": "failed", "error": str(e)}

    mapping = {
        "cost_center": CC_COLUMNS,
        "cost_centers": CC_COLUMNS,
        "profit_center": PC_COLUMNS,
        "profit_centers": PC_COLUMNS,
        "balance": BALANCE_COLUMNS,
        "balances": BALANCE_COLUMNS,
        "entity": ENTITY_COLUMNS,
        "entities": ENTITY_COLUMNS,
    }.get(batch.kind, {})

    normalized = _normalize_headers(rows, mapping) if mapping else rows
    loaded = 0

    if batch.kind in ("cost_center", "cost_centers"):
        for row in normalized:
            if not row.get("cctr") or not row.get("coarea"):
                continue
            existing = db.execute(
                select(LegacyCostCenter).where(
                    LegacyCostCenter.coarea == row["coarea"],
                    LegacyCostCenter.cctr == row["cctr"],
                )
            ).scalar_one_or_none()
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            if existing:
                existing.txtsh = row.get("txtsh", existing.txtsh)
                existing.txtmi = row.get("txtmi", existing.txtmi)
                existing.responsible = row.get("responsible", existing.responsible)
                if row.get("is_active"):
                    existing.is_active = is_act
            else:
                db.add(
                    LegacyCostCenter(
                        coarea=row["coarea"],
                        cctr=row["cctr"],
                        txtsh=row.get("txtsh", ""),
                        txtmi=row.get("txtmi", ""),
                        responsible=row.get("responsible", ""),
                        cctrcgy=row.get("cctrcgy", ""),
                        ccode=row.get("ccode", ""),
                        currency=row.get("currency", ""),
                        pctr=row.get("pctr", ""),
                        is_active=is_act,
                        refresh_batch=batch.id,
                    )
                )
            loaded += 1

    elif batch.kind in ("profit_center", "profit_centers"):
        for row in normalized:
            if not row.get("pctr"):
                continue
            existing = db.execute(
                select(LegacyProfitCenter).where(
                    LegacyProfitCenter.coarea == row.get("coarea", ""),
                    LegacyProfitCenter.pctr == row["pctr"],
                )
            ).scalar_one_or_none()
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            if existing:
                existing.txtsh = row.get("txtsh", existing.txtsh)
                existing.txtmi = row.get("txtmi", existing.txtmi)
                existing.responsible = row.get("responsible", existing.responsible)
                if row.get("is_active"):
                    existing.is_active = is_act
            else:
                db.add(
                    LegacyProfitCenter(
                        coarea=row.get("coarea", ""),
                        pctr=row["pctr"],
                        txtsh=row.get("txtsh", ""),
                        txtmi=row.get("txtmi", ""),
                        responsible=row.get("responsible", ""),
                        ccode=row.get("ccode", ""),
                        department=row.get("department", ""),
                        currency=row.get("currency", ""),
                        is_active=is_act,
                        refresh_batch=batch.id,
                    )
                )
            loaded += 1

    elif batch.kind in ("balance", "balances"):
        for row in normalized:
            if not row.get("cctr"):
                continue
            pr = row.get("period_raw", "")
            fy_str = row.get("fiscal_year", "")
            try:
                if fy_str:
                    fy = int(fy_str)
                    per = int(pr) if pr else 0
                elif pr and len(pr) == 6:
                    fy = int(pr[:4])
                    per = int(pr[4:])
                else:
                    fy = 0
                    per = 0
            except (ValueError, TypeError):
                fy = 0
                per = 0
            try:
                tc = Decimal(row.get("tc_amt", "0") or "0")
            except InvalidOperation:
                tc = Decimal("0")
            try:
                gc = Decimal(row.get("gc_amt", "0") or "0")
            except InvalidOperation:
                gc = Decimal("0")
            try:
                gc2 = Decimal(row.get("gc2_amt", "0") or "0")
            except InvalidOperation:
                gc2 = Decimal("0")
            try:
                pc = int(row.get("posting_count", "0") or "0")
            except ValueError:
                pc = 0
            db.add(
                Balance(
                    coarea=row.get("coarea", ""),
                    cctr=row["cctr"],
                    ccode=row.get("ccode", ""),
                    fiscal_year=fy,
                    period=per,
                    account=row.get("account", ""),
                    account_class=row.get("account_class", ""),
                    tc_amt=tc,
                    gc_amt=gc,
                    gc2_amt=gc2,
                    currency_tc=row.get("currency_tc", ""),
                    posting_count=pc,
                    refresh_batch=batch.id,
                )
            )
            loaded += 1

    elif batch.kind in ("entity", "entities"):
        for row in normalized:
            if not row.get("ccode"):
                continue
            existing = db.execute(
                select(Entity).where(Entity.ccode == row["ccode"])
            ).scalar_one_or_none()
            if existing:
                existing.name = row.get("name", existing.name)
                existing.country = row.get("country", existing.country)
                existing.region = row.get("region", existing.region)
                existing.currency = row.get("currency", existing.currency)
            else:
                db.add(
                    Entity(
                        ccode=row["ccode"],
                        name=row.get("name", row["ccode"]),
                        country=row.get("country"),
                        region=row.get("region"),
                        currency=row.get("currency"),
                    )
                )
            loaded += 1

    batch.rows_loaded = loaded
    batch.status = "loaded"
    batch.loaded_at = datetime.now(UTC)
    db.commit()

    return {"status": "loaded", "rows_loaded": loaded}


def rollback_upload(batch_id: int, db: Session) -> dict:
    """Rollback a loaded upload batch."""
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if batch.status != "loaded":
        raise ValueError(f"Only loaded batches can be rolled back (status: {batch.status})")

    deleted = 0
    if batch.kind in ("cost_center", "cost_centers"):
        r = db.execute(
            sa_delete(LegacyCostCenter).where(LegacyCostCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
    elif batch.kind in ("profit_center", "profit_centers"):
        r = db.execute(
            sa_delete(LegacyProfitCenter).where(LegacyProfitCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
    elif batch.kind in ("balance", "balances"):
        r = db.execute(sa_delete(Balance).where(Balance.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind in ("entity", "entities"):
        raise ValueError("Entity uploads cannot be rolled back (no batch tracking on entities)")

    batch.status = "rolled_back"
    db.commit()
    return {"status": "rolled_back", "rows_deleted": deleted}
