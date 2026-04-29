"""Upload processing service — parses CSV/Excel files and loads into DB."""

from __future__ import annotations

import ast
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
    Employee,
    Entity,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
    UploadBatch,
    UploadError,
)

log = logging.getLogger(__name__)

_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%Y%m%d",
)


def _parse_date(raw: str) -> datetime | None:
    """Try common date formats; return None if unparseable."""
    raw = raw.strip()
    if not raw:
        return None
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue
    return None


# Column mappings: normalize header names to model fields
CC_COLUMNS = {
    # SAP technical names (CSKS/CSKT)
    "MANDT": "mandt",
    "KOKRS": "coarea",
    "KOSTL": "cctr",
    "KTEXT": "txtsh",
    "LTEXT": "txtmi",
    "VERAK": "responsible",
    "VERAK_USER": "verak_user",
    "BUKRS": "ccode",
    "KOSAR": "cctrcgy",
    "WAERS": "currency",
    "PRCTR": "pctr",
    "GSBER": "gsber",
    "WERKS": "werks",
    "ABTEI": "abtei",
    "FUNC_AREA": "func_area",
    "LAND1": "land1",
    "NKOST": "nkost",
    "BKZKP": "bkzkp",
    "BKZKS": "bkzks",
    "PKZKP": "pkzkp",
    "PKZKS": "pkzks",
    "DATAB": "valid_from",
    "DATBI": "valid_to",
    # Legacy aliases
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
    "IS_ACTIVE": "is_active",
}
PC_COLUMNS = {
    # SAP technical names (CEPC/CEPCT)
    "MANDT": "mandt",
    "KOKRS": "coarea",
    "KTEXT": "txtsh",
    "LTEXT": "txtmi",
    "VERAK": "responsible",
    "VERAK_USER": "verak_user",
    "BUKRS": "ccode",
    "WAERS": "currency",
    "SEGMENT": "segment",
    "LAND1": "land1",
    "NAME1": "name1",
    "NAME2": "name2",
    "SPRAS": "language",
    "NPRCTR": "nprctr",
    "LOCK_IND": "lock_ind",
    "DATAB": "valid_from",
    "DATBI": "valid_to",
    # Legacy aliases
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
    # SAP technical names (T001)
    "MANDT": "mandt",
    "BUKRS": "ccode",
    "BUTXT": "name",
    "LAND1": "country",
    "WAERS": "currency",
    "ORT01": "city",
    "SPRAS": "language",
    "KTOPL": "chart_of_accounts",
    "PERIV": "fiscal_year_variant",
    "RCOMP": "company",
    "KKBER": "credit_control_area",
    "FMHRP": "fm_area",
    # Legacy aliases
    "COMPANY_CODE": "ccode",
    "CCODE": "ccode",
    "NAME": "name",
    "COUNTRY": "country",
    "REGION": "region",
    "CURRENCY": "currency",
    "IS_ACTIVE": "is_active",
    "CITY": "city",
    "LANGUAGE": "language",
}

# Primary employee columns mapped to model fields; remaining go to attrs JSON
EMPLOYEE_COLUMNS = {
    "GPN": "gpn",
    "BS_NAME": "bs_name",
    "BS_FIRSTNAME": "bs_firstname",
    "BS_LASTNAME": "bs_lastname",
    "LEGAL_FAMILY_NAM": "legal_family_name",
    "LEGAL_FIRST_NAME": "legal_first_name",
    "EMAIL_ADDRESS": "email_address",
    "EMP_STATUS": "emp_status",
    "VALID_FROM": "valid_from",
    "VALID_TO": "valid_to",
    "GENDER_CODE": "gender_code",
    "USER_ID_PID": "user_id_pid",
    "USER_ID_TNUMBER": "user_id_tnumber",
    "UUNAME": "uuname",
    "OU_PK": "ou_pk",
    "OU_CD": "ou_cd",
    "OU_DESC": "ou_desc",
    "WRK_IN_OU_PK": "wrk_in_ou_pk",
    "WRK_IN_OU_CD": "wrk_in_ou_cd",
    "WRK_IN_OU_DESC": "wrk_in_ou_desc",
    "LOCAL_CC_CD": "local_cc_cd",
    "LOCAL_CC_DESC": "local_cc_desc",
    "GCRS_COMP_CD": "gcrs_comp_cd",
    "GCRS_COMP_DESC": "gcrs_comp_desc",
    "COST_PC_CD_E_OU": "cost_pc_cd_e_ou",
    "COST_PC_CD_W_OU": "cost_pc_cd_w_ou",
    "LM_GPN": "lm_gpn",
    "LM_BS_FIRSTNAME": "lm_bs_firstname",
    "LM_BS_LASTNAME": "lm_bs_lastname",
    "SUPERVISOR_GPN": "supervisor_gpn",
    "RANK_CD": "rank_cd",
    "RANK_DESC": "rank_desc",
    "JOB_DESC": "job_desc",
    "EMPL_CLASS": "empl_class",
    "FULL_TIME_EQ": "full_time_eq",
    "HEAD_OF_OWN_OU": "head_of_own_ou",
    "REG_REGION": "reg_region",
    "LOCN_CITY_NAME_1": "locn_city_name_1",
    "LOCN_CTRY_CD_1": "locn_ctry_cd_1",
    "BUILDING_CD_1": "building_cd_1",
}
_EMPLOYEE_MODEL_FIELDS = set(EMPLOYEE_COLUMNS.values())


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

    supported = (
        "cost_center",
        "cost_centers",
        "profit_center",
        "profit_centers",
        "balance",
        "balances",
        "entity",
        "entities",
        "hierarchy",
        "hierarchies",
        "employee",
        "employees",
    )
    if batch.kind not in supported:
        raise ValueError(f"Upload kind '{batch.kind}' is not yet supported")

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
        "employee": EMPLOYEE_COLUMNS,
        "employees": EMPLOYEE_COLUMNS,
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
        elif batch.kind in ("employee", "employees"):
            if not row.get("gpn"):
                errors.append(
                    {"row": i, "col": "GPN", "code": "REQUIRED", "msg": "GPN is required"},
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
        elif batch.kind in ("hierarchy", "hierarchies"):
            row_type = (row.get("row_type") or "").upper()
            if row_type not in ("SETHEADER", "SETNODE", "SETLEAF"):
                errors.append(
                    {
                        "row": i,
                        "col": "ROW_TYPE",
                        "code": "INVALID",
                        "msg": f"ROW_TYPE must be SETHEADER/SETNODE/SETLEAF, got '{row_type}'",
                    }
                )
                error_rows.add(i)
            elif row_type == "SETHEADER" and not row.get("setname"):
                errors.append(
                    {
                        "row": i,
                        "col": "SETNAME",
                        "code": "REQUIRED",
                        "msg": "SETNAME required",
                    }
                )
                error_rows.add(i)
            elif row_type == "SETNODE" and (
                not row.get("parent_setname") or not row.get("child_setname")
            ):
                errors.append(
                    {
                        "row": i,
                        "col": "PARENT/CHILD",
                        "code": "REQUIRED",
                        "msg": "PARENT_SETNAME and CHILD_SETNAME required",
                    }
                )
                error_rows.add(i)
            elif row_type == "SETLEAF" and not row.get("value"):
                errors.append(
                    {
                        "row": i,
                        "col": "VALUE",
                        "code": "REQUIRED",
                        "msg": "VALUE required",
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
        "employee": EMPLOYEE_COLUMNS,
        "employees": EMPLOYEE_COLUMNS,
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
            cc_kwargs = {
                "mandt": row.get("mandt"),
                "coarea": row["coarea"],
                "cctr": row["cctr"],
                "txtsh": row.get("txtsh", ""),
                "txtmi": row.get("txtmi", ""),
                "responsible": row.get("responsible", ""),
                "verak_user": row.get("verak_user"),
                "cctrcgy": row.get("cctrcgy", ""),
                "ccode": row.get("ccode", ""),
                "currency": row.get("currency", ""),
                "pctr": row.get("pctr", ""),
                "gsber": row.get("gsber"),
                "werks": row.get("werks"),
                "abtei": row.get("abtei"),
                "func_area": row.get("func_area"),
                "land1": row.get("land1"),
                "nkost": row.get("nkost"),
                "bkzkp": row.get("bkzkp"),
                "bkzks": row.get("bkzks"),
                "pkzkp": row.get("pkzkp"),
                "pkzks": row.get("pkzks"),
                "is_active": is_act,
            }
            # Parse date fields
            for dt_field in ("valid_from", "valid_to"):
                raw = row.get(dt_field)
                if raw and isinstance(raw, str):
                    cc_kwargs[dt_field] = _parse_date(raw)
            if existing:
                for k, v in cc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                cc_kwargs["refresh_batch"] = batch.id
                db.add(LegacyCostCenter(**cc_kwargs))
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
            pc_kwargs = {
                "mandt": row.get("mandt"),
                "coarea": row.get("coarea", ""),
                "pctr": row["pctr"],
                "txtsh": row.get("txtsh", ""),
                "txtmi": row.get("txtmi", ""),
                "responsible": row.get("responsible", ""),
                "verak_user": row.get("verak_user"),
                "ccode": row.get("ccode", ""),
                "department": row.get("department", ""),
                "currency": row.get("currency", ""),
                "segment": row.get("segment"),
                "land1": row.get("land1"),
                "name1": row.get("name1"),
                "name2": row.get("name2"),
                "language": row.get("language"),
                "nprctr": row.get("nprctr"),
                "lock_ind": row.get("lock_ind"),
                "is_active": is_act,
            }
            for dt_field in ("valid_from", "valid_to"):
                raw = row.get(dt_field)
                if raw and isinstance(raw, str):
                    pc_kwargs[dt_field] = _parse_date(raw)
            if existing:
                for k, v in pc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                pc_kwargs["refresh_batch"] = batch.id
                db.add(LegacyProfitCenter(**pc_kwargs))
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
            ent_kwargs = {
                "mandt": row.get("mandt"),
                "ccode": row["ccode"],
                "name": row.get("name", row["ccode"]),
                "country": row.get("country"),
                "region": row.get("region"),
                "currency": row.get("currency"),
                "city": row.get("city"),
                "language": row.get("language"),
                "chart_of_accounts": row.get("chart_of_accounts"),
                "fiscal_year_variant": row.get("fiscal_year_variant"),
                "company": row.get("company"),
                "credit_control_area": row.get("credit_control_area"),
                "fm_area": row.get("fm_area"),
            }
            if existing:
                for k, v in ent_kwargs.items():
                    if k != "ccode" and v is not None:
                        setattr(existing, k, v)
            else:
                db.add(Entity(**ent_kwargs))
            loaded += 1

    elif batch.kind in ("employee", "employees"):
        for row in normalized:
            gpn = row.get("gpn", "").strip()
            if not gpn:
                continue
            existing = (
                db.execute(
                    select(Employee).where(
                        Employee.gpn == gpn,
                        Employee.refresh_batch == batch.id,
                    )
                )
                .scalars()
                .first()
            )
            # Separate model fields from extra attrs
            model_kwargs: dict = {}
            extra_attrs: dict = {}
            for k, v in row.items():
                if k in _EMPLOYEE_MODEL_FIELDS:
                    model_kwargs[k] = v if v else None
                elif k and k != "_extras" and v:
                    extra_attrs[k] = v
            # Recover unmapped CSV columns from _extras (stored as repr by _normalize_headers)
            extras_raw = row.get("_extras")
            if extras_raw and isinstance(extras_raw, str):
                try:
                    parsed = ast.literal_eval(extras_raw)
                    if isinstance(parsed, dict):
                        extra_attrs.update(parsed)
                except (ValueError, SyntaxError):
                    pass
            model_kwargs["attrs"] = extra_attrs if extra_attrs else None
            # Parse datetime fields from various CSV date formats
            for dt_field in ("valid_from", "valid_to"):
                raw = model_kwargs.get(dt_field)
                if raw and isinstance(raw, str):
                    model_kwargs[dt_field] = _parse_date(raw)
            model_kwargs["refresh_batch"] = batch.id
            if existing:
                for k, v in model_kwargs.items():
                    if k != "refresh_batch" and v is not None:
                        setattr(existing, k, v)
            else:
                db.add(Employee(**model_kwargs))
            loaded += 1

    elif batch.kind in ("hierarchy", "hierarchies"):
        # Pass 1: create Hierarchy headers
        hier_map: dict[tuple[str, str], Hierarchy] = {}
        for row in normalized:
            row_type = (row.get("row_type") or "").upper()
            if row_type != "SETHEADER":
                continue
            setclass = row.get("setclass", "0101")
            setname = row.get("setname", "")
            if not setname:
                continue
            existing = db.execute(
                select(Hierarchy).where(
                    Hierarchy.setclass == setclass,
                    Hierarchy.setname == setname,
                    Hierarchy.refresh_batch == batch.id,
                )
            ).scalar_one_or_none()
            if not existing:
                h = Hierarchy(
                    setclass=setclass,
                    setname=setname,
                    description=row.get("description", ""),
                    coarea=row.get("coarea", ""),
                    refresh_batch=batch.id,
                )
                db.add(h)
                db.flush()
                hier_map[(setclass, setname)] = h
            else:
                hier_map[(setclass, setname)] = existing
            loaded += 1

        # Pass 2: create nodes
        for row in normalized:
            row_type = (row.get("row_type") or "").upper()
            if row_type != "SETNODE":
                continue
            setclass = row.get("setclass", "0101")
            setname = row.get("setname", "")
            key = (setclass, setname)
            hier = hier_map.get(key)
            if not hier:
                hier = db.execute(
                    select(Hierarchy).where(
                        Hierarchy.setclass == setclass, Hierarchy.setname == setname
                    )
                ).scalar_one_or_none()
                if hier:
                    hier_map[key] = hier
            if not hier:
                continue
            seq = int(row.get("seq") or "0")
            db.add(
                HierarchyNode(
                    hierarchy_id=hier.id,
                    parent_setname=row.get("parent_setname", ""),
                    child_setname=row.get("child_setname", ""),
                    seq=seq,
                )
            )
            loaded += 1

        # Pass 3: create leaves
        for row in normalized:
            row_type = (row.get("row_type") or "").upper()
            if row_type != "SETLEAF":
                continue
            setclass = row.get("setclass", "0101")
            setname = row.get("setname", "")
            key = (setclass, setname)
            hier = hier_map.get(key)
            if not hier:
                hier = db.execute(
                    select(Hierarchy).where(
                        Hierarchy.setclass == setclass, Hierarchy.setname == setname
                    )
                ).scalar_one_or_none()
                if hier:
                    hier_map[key] = hier
            if not hier:
                continue
            parent_set = row.get("parent_setname") or row.get("setname", "")
            seq = int(row.get("seq") or "0")
            db.add(
                HierarchyLeaf(
                    hierarchy_id=hier.id,
                    setname=parent_set,
                    value=row.get("value", ""),
                    seq=seq,
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
    elif batch.kind in ("employee", "employees"):
        r = db.execute(sa_delete(Employee).where(Employee.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind in ("hierarchy", "hierarchies"):
        hier_ids = [
            h.id
            for h in db.execute(select(Hierarchy).where(Hierarchy.refresh_batch == batch.id))
            .scalars()
            .all()
        ]
        for hid in hier_ids:
            db.execute(sa_delete(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == hid))
            db.execute(sa_delete(HierarchyNode).where(HierarchyNode.hierarchy_id == hid))
        r = db.execute(sa_delete(Hierarchy).where(Hierarchy.refresh_batch == batch.id))
        deleted = r.rowcount

    rows_loaded = batch.rows_loaded or 0
    rows_updated = max(0, rows_loaded - deleted)
    batch.status = "rolled_back"
    db.commit()
    result: dict = {"status": "rolled_back", "rows_deleted": deleted}
    if rows_updated > 0:
        result["rows_updated_not_reverted"] = rows_updated
        result["warning"] = (
            f"{rows_updated} existing records were updated during upload "
            "and could not be reverted by rollback."
        )
    return result
