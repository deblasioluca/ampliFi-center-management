"""Upload processing service — parses CSV/Excel files and loads into DB."""

from __future__ import annotations

import ast
import contextlib
import csv
import io
import re
import time as _time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import structlog
from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.core import (
    Balance,
    CenterMapping,
    DataQualityIssue,
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
    UploadError,
)

logger = structlog.get_logger()


def _flush_progress(batch_id: int, count: int, total: int | None = None) -> None:
    """Write rows_processed to DB via an independent session.

    Uses a separate connection so the caller's main transaction is never
    committed or disturbed — only the progress counter row is updated.
    """
    from sqlalchemy import text as sa_text

    from app.infra.db.session import SessionLocal

    s = SessionLocal()
    try:
        if total is not None:
            s.execute(
                sa_text(
                    "UPDATE cleanup.upload_batch "
                    "SET rows_processed = :p, rows_total = :t WHERE id = :id"
                ),
                {"p": count, "t": total, "id": batch_id},
            )
        else:
            s.execute(
                sa_text("UPDATE cleanup.upload_batch SET rows_processed = :p WHERE id = :id"),
                {"p": count, "id": batch_id},
            )
        s.commit()
    except Exception:
        logger.warning("Failed to flush progress for batch %s", batch_id, exc_info=True)
    finally:
        s.close()


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


_truncation_warned: set[str] = set()


def _truncate_to_model(model_cls: type, kwargs: dict) -> dict:
    """Truncate string values that exceed their column's declared length."""
    from sqlalchemy import String as SAString
    from sqlalchemy import inspect as sa_inspect

    mapper = sa_inspect(model_cls)
    for key, val in list(kwargs.items()):
        if not isinstance(val, str):
            continue
        col_attr = mapper.columns.get(key)
        if col_attr is None:
            continue
        col_type = col_attr.type
        if isinstance(col_type, SAString) and col_type.length and len(val) > col_type.length:
            warn_key = f"{model_cls.__tablename__}.{key}"
            if warn_key not in _truncation_warned:
                _truncation_warned.add(warn_key)
                logger.warning(
                    "Truncating %s.%s from %d to %d chars (further warnings suppressed)",
                    model_cls.__tablename__,
                    key,
                    len(val),
                    col_type.length,
                )
            kwargs[key] = val[: col_type.length]
    return kwargs


# ---------------------------------------------------------------------------
# VERAK (responsible person) data-quality validation
# ---------------------------------------------------------------------------

# Pattern: "<GPN> <First initial>. <Lastname>"
_VERAK_PATTERN = re.compile(r"^(\S+)\s+([A-Za-z])\.\s+(.+)$")


def _build_employee_lookup(
    db: Session, scope: str
) -> tuple[dict[str, Employee], dict[str, list[Employee]]]:
    """Pre-fetch employees and build lookup dicts for VERAK matching.

    Returns:
        gpn_map: GPN → Employee (primary lookup)
        name_map: "first_lower last_lower" → [Employee, ...] (fallback)
    """
    emps = db.execute(select(Employee).where(Employee.scope == scope)).scalars().all()
    gpn_map: dict[str, Employee] = {}
    name_map: dict[str, list[Employee]] = {}
    for e in emps:
        gpn_map[e.gpn] = e
        first = (e.vorname or e.bs_first_name or e.bs_firstname or "").strip().lower()
        last = (e.name or e.bs_last_name or e.bs_lastname or "").strip().lower()
        if first and last:
            key = f"{first} {last}"
            name_map.setdefault(key, []).append(e)
            # Also index by last name only for partial matches
            name_map.setdefault(last, []).append(e)
    return gpn_map, name_map


def _match_employee_from_verak(
    verak: str,
    gpn_map: dict[str, Employee],
    name_map: dict[str, list[Employee]],
) -> tuple[Employee | None, str]:
    """Try to resolve VERAK value to an Employee.

    Returns:
        (employee_or_None, match_reason)
        match_reason: "pattern_gpn", "gpn_only", "name_match", "no_match"
    """
    verak = verak.strip()
    if not verak:
        return None, "empty"

    # 1. Standard pattern: "<GPN> <Initial>. <Lastname>"
    m = _VERAK_PATTERN.match(verak)
    if m:
        gpn = m.group(1)
        emp = gpn_map.get(gpn)
        if emp:
            return emp, "pattern_gpn"
        return None, "pattern_gpn_not_found"

    # 2. Single token — might be just a GPN
    parts = verak.split()
    if len(parts) == 1:
        emp = gpn_map.get(parts[0])
        if emp:
            return emp, "gpn_only"
        return None, "gpn_not_found"

    # 3. GPN + full name (non-standard format): first token is GPN
    if parts[0] in gpn_map:
        return gpn_map[parts[0]], "gpn_prefix"

    # 4. Name-only: "<First> <Last>" or "<Last>, <First>"
    name_lower = verak.lower().strip()
    # Try "first last"
    candidates = name_map.get(name_lower, [])
    if len(candidates) == 1:
        return candidates[0], "name_match"
    # Try "last, first" → "first last"
    if "," in verak:
        comma_parts = [p.strip() for p in verak.split(",", 1)]
        if len(comma_parts) == 2:
            rearranged = f"{comma_parts[1]} {comma_parts[0]}".lower()
            candidates = name_map.get(rearranged, [])
            if len(candidates) == 1:
                return candidates[0], "name_match"
    # Try matching just last name from the name parts
    if len(parts) >= 2:
        last_lower = parts[-1].lower()
        candidates = name_map.get(last_lower, [])
        if len(candidates) == 1:
            return candidates[0], "lastname_match"

    return None, "no_match"


_VERAK_AUTO_FIXED = "auto_fixed"  # sentinel — no DQ row persisted


def _validate_verak(
    obj_type: str,
    obj_id: int,
    verak: str,
    gpn_map: dict[str, Employee],
    name_map: dict[str, list[Employee]],
    scope: str,
    batch_id: int | None,
) -> tuple[str | None, int | None, DataQualityIssue | str | None]:
    """Validate VERAK field and return corrected value + DQ issue if needed.

    Returns:
        (corrected_verak, employee_id, dq_issue_or_sentinel_or_None)
        The third element is a ``DataQualityIssue`` for issues that need
        reviewer action, the string sentinel ``_VERAK_AUTO_FIXED`` when the
        value was silently auto-corrected (no DB row needed), or ``None``
        when the value is already correct.
    """
    verak = (verak or "").strip()
    if not verak:
        return None, None, None

    emp, reason = _match_employee_from_verak(verak, gpn_map, name_map)

    if emp:
        formatted = emp.verak_display
        emp_id = emp.id
        if not emp.is_active:
            # Employee exists but no longer active → needs reviewer
            return (
                verak,
                emp_id,
                DataQualityIssue(
                    scope=scope,
                    object_type=obj_type,
                    object_id=obj_id,
                    field_name="responsible",
                    rule_code="VERAK_EMPLOYEE_INACTIVE",
                    severity="error",
                    message=f"Employee {emp.gpn} is no longer active; new owner required",
                    current_value=verak,
                    suggested_value=formatted,
                    suggested_employee_id=emp_id,
                    status="open",
                    batch_id=batch_id,
                ),
            )
        # Check if already in standard format
        if verak == formatted:
            return formatted, emp_id, None
        # Auto-correct to standard format — no DQ row stored
        return formatted, emp_id, _VERAK_AUTO_FIXED

    # No employee found
    return (
        verak,
        None,
        DataQualityIssue(
            scope=scope,
            object_type=obj_type,
            object_id=obj_id,
            field_name="responsible",
            rule_code="VERAK_EMPLOYEE_NOT_FOUND",
            severity="error",
            message=f"Cannot resolve employee from VERAK value '{verak}' ({reason})",
            current_value=verak,
            status="open",
            batch_id=batch_id,
        ),
    )


def _check_hierarchy_orphans(
    db: Session,
    batch: UploadBatch,
    batch_scope: str,
) -> list[DataQualityIssue]:
    """Check for hierarchy leaves that reference centers not in CC/PC tables.

    Returns DQ issues for orphan leaves (center in hierarchy but not uploaded).
    """
    hierarchies = (
        db.execute(
            select(Hierarchy).where(
                Hierarchy.refresh_batch == batch.id,
                Hierarchy.scope == batch_scope,
            )
        )
        .scalars()
        .all()
    )
    if not hierarchies:
        return []

    issues: list[DataQualityIssue] = []
    for hier in hierarchies:
        leaves = (
            db.execute(
                select(HierarchyLeaf.value).where(HierarchyLeaf.hierarchy_id == hier.id).distinct()
            )
            .scalars()
            .all()
        )
        if not leaves:
            continue

        is_cc = hier.setclass in ("0101", "FLAT")
        if is_cc:
            existing = set(
                db.execute(
                    select(LegacyCostCenter.cctr).where(
                        LegacyCostCenter.scope == batch_scope,
                        LegacyCostCenter.cctr.in_(leaves),
                    )
                )
                .scalars()
                .all()
            )
            obj_type = "cost_center"
        else:
            existing = set(
                db.execute(
                    select(LegacyProfitCenter.pctr).where(
                        LegacyProfitCenter.scope == batch_scope,
                        LegacyProfitCenter.pctr.in_(leaves),
                    )
                )
                .scalars()
                .all()
            )
            obj_type = "profit_center"

        orphans = set(leaves) - existing
        label = hier.label or hier.setname
        for val in sorted(orphans):
            issues.append(
                DataQualityIssue(
                    scope=batch_scope,
                    object_type=obj_type,
                    object_id=hier.id,
                    field_name="hierarchy_leaf",
                    rule_code="HIERARCHY_ORPHAN_LEAF",
                    severity="warning",
                    message=(
                        f"Center '{val}' is referenced in hierarchy "
                        f"'{label}' but not found in uploaded "
                        f"{'cost' if is_cc else 'profit'} centers"
                    ),
                    current_value=val,
                    status="open",
                    batch_id=batch.id,
                )
            )

    if orphans_total := len(issues):
        logger.info(
            "Hierarchy orphan check: %d orphan leaves in batch %d",
            orphans_total,
            batch.id,
        )
    return issues


# Column mappings: normalize header names to model fields
CC_COLUMNS = {
    # SAP technical names (CSKS/CSKT) — 1:1 mapping
    "MANDT": "mandt",
    "KOKRS": "coarea",
    "KOSTL": "cctr",
    "KTEXT": "txtsh",
    "LTEXT": "txtmi",
    "DATBI": "datbi",
    "DATAB": "datab",
    "BKZKP": "bkzkp",
    "PKZKP": "pkzkp",
    "BUKRS": "ccode",
    "GSBER": "gsber",
    "KOSAR": "cctrcgy",
    "VERAK": "responsible",
    "VERAK_USER": "verak_user",
    "WAERS": "currency",
    "KALSM": "kalsm",
    "TXJCD": "txjcd",
    "PRCTR": "pctr",
    "WERKS": "werks",
    "LOGSYSTEM": "logsystem",
    "ERSDA": "ersda",
    "USNAM": "usnam",
    "BKZKS": "bkzks",
    "BKZER": "bkzer",
    "BKZOB": "bkzob",
    "PKZKS": "pkzks",
    "PKZER": "pkzer",
    "VMETH": "vmeth",
    "MGEFL": "mgefl",
    "ABTEI": "abtei",
    "NKOST": "nkost",
    "KVEWE": "kvewe",
    "KAPPL": "kappl",
    "KOSZSCHL": "koszschl",
    "LAND1": "land1",
    "ANRED": "anred",
    "NAME1": "name1",
    "NAME2": "name2",
    "NAME3": "name3",
    "NAME4": "name4",
    "ORT01": "ort01",
    "ORT02": "ort02",
    "STRAS": "stras",
    "PFACH": "pfach",
    "PSTLZ": "pstlz",
    "PSTL2": "pstl2",
    "REGIO": "regio",
    "SPRAS": "spras",
    "TELBX": "telbx",
    "TELF1": "telf1",
    "TELF2": "telf2",
    "TELFX": "telfx",
    "TELTX": "teltx",
    "TELX1": "telx1",
    "DATLT": "datlt",
    "DRNAM": "drnam",
    "KHINR": "khinr",
    "CCKEY": "cckey",
    "KOMPL": "kompl",
    "STAKZ": "stakz",
    "OBJNR": "objnr",
    "FUNKT": "funkt",
    "AFUNK": "afunk",
    "CPI_TEMPL": "cpi_templ",
    "CPD_TEMPL": "cpd_templ",
    "FUNC_AREA": "func_area",
    "SCI_TEMPL": "sci_templ",
    "SCD_TEMPL": "scd_templ",
    "SKI_TEMPL": "ski_templ",
    "SKD_TEMPL": "skd_templ",
    # Customer fields (CI_CSKS)
    "ZZCUEMNCFU": "zzcuemncfu",
    "ZZCUEABACC": "zzcueabacc",
    "ZZCUEGBCD": "zzcuegbcd",
    "ZZCUEUBCD": "zzcueubcd",
    "ZZCUENKOS": "zzcuenkos",
    "ZZSTRPCTYP": "zzstrpctyp",
    "ZZSTRKKLAS": "zzstrkklas",
    "ZZSTRAAGCD": "zzstraagcd",
    "ZZSTRGFD": "zzstrgfd",
    "ZZSTRFST": "zzstrfst",
    "ZZSTRMACVE": "zzstrmacve",
    "ZZSTRABUKR": "zzstrabukr",
    "ZZSTRUGCD": "zzstrugcd",
    "ZZSTRINADT": "zzstrinadt",
    "ZZSTRKSTYP": "zzstrkstyp",
    "ZZSTRVERIK": "zzstrverik",
    "ZZSTRCURR2": "zzstrcurr2",
    "ZZSTRLCCID": "zzstrlccid",
    "ZZSTRMALOC": "zzstrmaloc",
    "ZZSTRTAXCD": "zzstrtaxcd",
    "ZZSTRGRPID": "zzstrgrpid",
    "ZZSTRREGCODE": "zzstrregcode",
    "ZZSTRTAXAREA": "zzstrtaxarea",
    "ZZSTRREPSIT": "zzstrrepsit",
    "ZZSTRGSM": "zzstrgsm",
    "ZZCEMAPAR": "zzcemapar",
    "ZZLEDGER": "zzledger",
    "ZZHDSTAT": "zzhdstat",
    "ZZHDTYPE": "zzhdtype",
    "ZZFMD": "zzfmd",
    "ZZFMDCC": "zzfmdcc",
    "ZZFMDNODE": "zzfmdnode",
    "ZZSTATE": "zzstate",
    "ZZTAX": "zztax",
    "ZZSTRENTSA": "zzstrentsa",
    "ZZSTRENTZU": "zzstrentzu",
    "XBLNR": "xblnr",
    # JV fields
    "VNAME": "vname",
    "RECID": "recid",
    "ETYPE": "etype",
    "JV_OTYPE": "jv_otype",
    "JV_JIBCL": "jv_jibcl",
    "JV_JIBSA": "jv_jibsa",
    "FERC_IND": "ferc_ind",
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
    "PCTR": "pctr",
    "IS_ACTIVE": "is_active",
}
_CC_MODEL_FIELDS = set(CC_COLUMNS.values())
PC_COLUMNS = {
    # SAP technical names (CEPC/CEPCT) — 1:1 mapping
    "MANDT": "mandt",
    "PRCTR": "pctr",
    "DATBI": "datbi",
    "KOKRS": "coarea",
    "DATAB": "datab",
    "ERSDA": "ersda",
    "USNAM": "usnam",
    "MERKMAL": "merkmal",
    "ABTEI": "department",
    "VERAK": "responsible",
    "VERAK_USER": "verak_user",
    "WAERS": "currency",
    "NPRCTR": "nprctr",
    "LAND1": "land1",
    "ANRED": "anred",
    "NAME1": "name1",
    "NAME2": "name2",
    "NAME3": "name3",
    "NAME4": "name4",
    "ORT01": "ort01",
    "ORT02": "ort02",
    "STRAS": "stras",
    "PFACH": "pfach",
    "PSTLZ": "pstlz",
    "PSTL2": "pstl2",
    "SPRAS": "language",
    "TELBX": "telbx",
    "TELF1": "telf1",
    "TELF2": "telf2",
    "TELFX": "telfx",
    "TELTX": "teltx",
    "TELX1": "telx1",
    "DATLT": "datlt",
    "DRNAM": "drnam",
    "KHINR": "khinr",
    "BUKRS": "ccode",
    "VNAME": "vname",
    "RECID": "recid",
    "ETYPE": "etype",
    "TXJCD": "txjcd",
    "REGIO": "regio",
    "KVEWE": "kvewe",
    "KAPPL": "kappl",
    "KALSM": "kalsm",
    "LOGSYSTEM": "logsystem",
    "LOCK_IND": "lock_ind",
    "PCA_TEMPLATE": "pca_template",
    "SEGMENT": "segment",
    "KTEXT": "txtsh",
    "LTEXT": "txtmi",
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
_PC_MODEL_FIELDS = set(PC_COLUMNS.values())
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
    "SUM(P.GCR_POSTING_AMT_TC)": "tc_amt",
    "GC_AMT": "gc_amt",
    "SUM_GC2": "gc2_amt",
    "GC2_AMT": "gc2_amt",
    "SUM(P.GCR_POSTING_AMT_GC2)": "gc2_amt",
    "COUNT": "posting_count",
    "COUNT(*)": "posting_count",
    "POSTING_COUNT": "posting_count",
    "ACCOUNT_CLASS": "account_class",
}
ENTITY_COLUMNS = {
    # SAP technical names (T001) — 1:1 mapping
    "MANDT": "mandt",
    "BUKRS": "ccode",
    "BUTXT": "name",
    "ORT01": "city",
    "LAND1": "country",
    "WAERS": "currency",
    "SPRAS": "language",
    "KTOPL": "chart_of_accounts",
    "WAABW": "waabw",
    "PERIV": "fiscal_year_variant",
    "KOKFI": "kokfi",
    "RCOMP": "company",
    "ADRNR": "adrnr",
    "STCEG": "stceg",
    "FIKRS": "fikrs",
    "XFMCO": "xfmco",
    "XFMCB": "xfmcb",
    "XFMCA": "xfmca",
    "TXJCD": "txjcd",
    "FMHRDATE": "fmhrdate",
    "BUVAR": "buvar",
    "FDBUK": "fdbuk",
    "XFDIS": "xfdis",
    "XVALV": "xvalv",
    "XSKFN": "xskfn",
    "KKBER": "credit_control_area",
    "XMWSN": "xmwsn",
    "MREGL": "mregl",
    "XGSBE": "xgsbe",
    "XGJRV": "xgjrv",
    "XKDFT": "xkdft",
    "XPROD": "xprod",
    "XEINK": "xeink",
    "XJVAA": "xjvaa",
    "XVVWA": "xvvwa",
    "XSLTA": "xslta",
    "XFDMM": "xfdmm",
    "XFDSD": "xfdsd",
    "XEXTB": "xextb",
    "EBUKR": "ebukr",
    "KTOP2": "ktop2",
    "UMKRS": "umkrs",
    "BUKRS_GLOB": "bukrs_glob",
    "FSTVA": "fstva",
    "OPVAR": "opvar",
    "XCOVR": "xcovr",
    "TXKRS": "txkrs",
    "WFVAR": "wfvar",
    "XBBBF": "xbbbf",
    "XBBBE": "xbbbe",
    "XBBBA": "xbbba",
    "XBBKO": "xbbko",
    "XSTDT": "xstdt",
    "MWSKV": "mwskv",
    "MWSKA": "mwska",
    "IMPDA": "impda",
    "XNEGP": "xnegp",
    "XKKBI": "xkkbi",
    "WT_NEWWT": "wt_newwt",
    "PP_PDATE": "pp_pdate",
    "INFMT": "infmt",
    "FSTVARE": "fstvare",
    "KOPIM": "kopim",
    "DKWEG": "dkweg",
    "OFFSACCT": "offsacct",
    "BAPOVAR": "bapovar",
    "XCOS": "xcos",
    "XCESSION": "xcession",
    "XSPLT": "xsplt",
    "SURCCM": "surccm",
    "DTPROV": "dtprov",
    "DTAMTC": "dtamtc",
    "DTTAXC": "dttaxc",
    "DTTDSP": "dttdsp",
    "DTAXR": "dtaxr",
    "XVATDATE": "xvatdate",
    "PST_PER_VAR": "pst_per_var",
    "XBBSC": "xbbsc",
    "F_OBSOLETE": "f_obsolete",
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
    "FMHRP": "fm_area",
}

_ENTITY_MODEL_FIELDS = set(ENTITY_COLUMNS.values())
# Employee columns — SAP ZUHL_GRD_GPF 1:1 mapping + legacy aliases
EMPLOYEE_COLUMNS = {
    # SAP technical names (ZUHL_GRD_GPF)
    "MANDT": "mandt",
    "GPN": "gpn",
    "NAME": "name",
    "VORNAME": "vorname",
    "SPRACHENSCHLUESS": "sprachenschluess",
    "ANREDECODE": "anredecode",
    "USERID": "userid",
    "EINTRITTSDATUM": "eintrittsdatum",
    "OE_LEITER": "oe_leiter",
    "INT_TEL_NR_1AP": "int_tel_nr_1ap",
    "EXT_TEL_NR_1AP": "ext_tel_nr_1ap",
    "NL_CODE_GEB_1AP": "nl_code_geb_1ap",
    "STRASSE_GEB_1AP": "strasse_geb_1ap",
    "STOCKWERK_1AP": "stockwerk_1ap",
    "BUERONUMMER_1AP": "bueronummer_1ap",
    "KSTST": "kstst",
    "KSTST_TEXT": "kstst_text",
    "OE_OBJEKT_ID": "oe_objekt_id",
    "OE_CODE": "oe_code",
    "OE_TEXT": "oe_text",
    "SAP_BUKRS": "sap_bukrs",
    "SAP_BUKRS_TEXT": "sap_bukrs_text",
    "T_NUMMER": "t_nummer",
    "INSTRAD_1": "instrad_1",
    "INSTRAD_2": "instrad_2",
    "KSTST_EINSATZ_OE": "kstst_einsatz_oe",
    "PERSONALBER_TEXT": "personalber_text",
    "NL_OE_MA": "nl_oe_ma",
    "NL_TEXT": "nl_text",
    "GSFLD_OE_MA": "gsfld_oe_ma",
    "GSFLD_OE_MA_TEXT": "gsfld_oe_ma_text",
    "MA_GRUPPE": "ma_gruppe",
    "MA_GRUPPE_TEXT": "ma_gruppe_text",
    "MA_KREIS": "ma_kreis",
    "MA_KREIS_TEXT": "ma_kreis_text",
    "RANG_CODE": "rang_code",
    "RANG_TEXT": "rang_text",
    "AKADEMISCHER_TIT": "akademischer_tit",
    "UBS_FUNK": "ubs_funk",
    "UBS_FUNK_TEXT": "ubs_funk_text",
    "GPN_VG_MA": "gpn_vg_ma",
    "NAME_VG_MA": "name_vg_ma",
    "UEG_OE_OBJEKTID": "ueg_oe_objektid",
    "UEG_OE_BEZ": "ueg_oe_bez",
    "UEG_OE_KRZ": "ueg_oe_krz",
    "BSCHGRAD": "bschgrad",
    "PERSONALBEREICH": "personalbereich",
    "FAX_EXT_1AP": "fax_ext_1ap",
    "EMAIL_ADRESSE": "email_adresse",
    "MA_KZ": "ma_kz",
    "FIRMA_EXT_MA": "firma_ext_ma",
    "BEGDAT_ORGWECHS": "begdat_orgwechs",
    "AUSTRITT_DATUM": "austritt_datum",
    "NATEL_NUMMER": "natel_nummer",
    "PAGER_NUMMER": "pager_nummer",
    "PLZ_GEB_1AP": "plz_geb_1ap",
    "ORT_GEB_1AP": "ort_geb_1ap",
    "EINSATZ_OE_KRZ": "einsatz_oe_krz",
    "EINSATZ_OE_TEXT": "einsatz_oe_text",
    "DIVISION": "division",
    "GEB_COD_1AP": "geb_cod_1ap",
    "RANG_KRZ": "rang_krz",
    "SYSTEMDATUM": "systemdatum",
    "AP_NUMMER": "ap_nummer",
    "EINSATZ_OE_OBJID": "einsatz_oe_objid",
    "INT_TEL_NR_2AP": "int_tel_nr_2ap",
    "EXT_TEL_NR_2AP": "ext_tel_nr_2ap",
    "BUERONUMMER_2AP": "bueronummer_2ap",
    "GEB_COD_2AP": "geb_cod_2ap",
    "STRASSE_GEB_2AP": "strasse_geb_2ap",
    "PLZ_GEB_2AP": "plz_geb_2ap",
    "ORT_GEB_2AP": "ort_geb_2ap",
    "GEB_COD_GEB_2AP": "geb_cod_geb_2ap",
    "FAX_NR_2AP": "fax_nr_2ap",
    "STOCKWERK_2AP": "stockwerk_2ap",
    "GPIN_NUMMER": "gpin_nummer",
    "NAT": "nat",
    "LAND_GEB_1AP": "land_geb_1ap",
    "REG_NR_1AP": "reg_nr_1ap",
    "POSTF_1AP": "postf_1ap",
    "PLZ_POSTFADR_1AP": "plz_postfadr_1ap",
    "ORT_POSTFADR_1AP": "ort_postfadr_1ap",
    "LAND_GEB_2AP": "land_geb_2ap",
    "REG_NR_2AP": "reg_nr_2ap",
    "POSTF_2AP": "postf_2ap",
    "PLZ_POSTFADR_2AP": "plz_postfadr_2ap",
    "ORT_POSTFADR_2AP": "ort_postfadr_2ap",
    "LETZTER_ARB_TAG": "letzter_arb_tag",
    "ABAC_NL_AG_EINOE": "abac_nl_ag_einoe",
    "VERTR_ENDE_EXMA": "vertr_ende_exma",
    "UNTERGRP_CODE": "untergrp_code",
    "BS_FIRST_NAME": "bs_first_name",
    "BS_LAST_NAME": "bs_last_name",
    "NAME_UC": "name_uc",
    "VORNAME_UC": "vorname_uc",
    "NAME_PH": "name_ph",
    "VORNAME_PH": "vorname_ph",
    "MA_OE": "ma_oe",
    "UPDATED_ID": "updated_id",
    "MA_KSTST": "ma_kstst",
    "BUSINESS_NAME": "business_name",
    "JOB_CATEG_CODE": "job_categ_code",
    "JOB_CATEG_DESCR": "job_categ_descr",
    "COSTCENTER_CODE": "costcenter_code",
    "COSTCENTER_DESCR": "costcenter_descr",
    "MANACS_FUNC_CODE": "manacs_func_code",
    "MANACS_FUNC_DESC": "manacs_func_desc",
    "MANACS_SEGM_CODE": "manacs_segm_code",
    "MANACS_SEGM_DESC": "manacs_segm_desc",
    "MANACS_SECT_CODE": "manacs_sect_code",
    "MANACS_SECT_DESC": "manacs_sect_desc",
    "MANACS_BSAR_CODE": "manacs_bsar_code",
    "MANACS_BSAR_DESC": "manacs_bsar_desc",
    "MANACS_BSUN_CODE": "manacs_bsun_code",
    "MANACS_BSUN_DESC": "manacs_bsun_desc",
    "MANACS_BSGP_CODE": "manacs_bsgp_code",
    "MANACS_BSGP_DESC": "manacs_bsgp_desc",
    "MANACS_REG_CODE": "manacs_reg_code",
    "MANACS_REG_DESCR": "manacs_reg_descr",
    "MANACS_LOC_CODE": "manacs_loc_code",
    "MANACS_LOC_DESCR": "manacs_loc_descr",
    "REGULATORY_REG": "regulatory_reg",
    "SUPERVISORS_GPIN": "supervisors_gpin",
    "UUNAME": "uuname",
    "WEB_SSO": "web_sso",
    "SAP_USER": "sap_user",
    "HR_COMPANY": "hr_company",
    "REGULATORY_REGST": "regulatory_regst",
    "GLOBAL_CC": "global_cc",
    # Legacy aliases (backward compat)
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

HIERARCHY_FLAT_COLUMNS = {
    "MANDT": "mandt",
    "PERIOD": "period",
    "NODEID": "nodeid",
    "NODETYPE": "nodetype",
    "NODENAME": "nodename",
    "PARENTID": "parentid",
    "CHILDID": "childid",
    "NEXTID": "nextid",
    "NODETEXT": "nodetext",
}

SKA1_COLUMNS = {
    "MANDT": "mandt",
    "KTOPL": "ktopl",
    "SAKNR": "saknr",
    "XBILK": "xbilk",
    "SAKAN": "sakan",
    "BILKT": "bilkt",
    "ERDAT": "erdat",
    "ERNAM": "ernam",
    "GVTYP": "gvtyp",
    "KTOKS": "ktoks",
    "MUSTR": "mustr",
    "VBUND": "vbund",
    "XLOEV": "xloev",
    "XSPEA": "xspea",
    "XSPEB": "xspeb",
    "XSPEP": "xspep",
    "MCOD1": "mcod1",
    "FUNC_AREA": "func_area",
    "GLACCOUNT_TYPE": "glaccount_type",
    "GLACCOUNT_SUBTYPE": "glaccount_subtype",
    "MAIN_SAKNR": "main_saknr",
    "LAST_CHANGED_TS": "last_changed_ts",
    "TXT20": "txt20",
    "TXT50": "txt50",
}
_SKA1_MODEL_FIELDS = set(SKA1_COLUMNS.values())

SKB1_COLUMNS = {
    "MANDT": "mandt",
    "BUKRS": "bukrs",
    "SAKNR": "saknr",
    "BEGRU": "begru",
    "BUSAB": "busab",
    "DATLZ": "datlz",
    "ERDAT": "erdat",
    "ERNAM": "ernam",
    "FDGRV": "fdgrv",
    "FDLEV": "fdlev",
    "FIPLS": "fipls",
    "FSTAG": "fstag",
    "HBKID": "hbkid",
    "HKTID": "hktid",
    "KDFSL": "kdfsl",
    "MITKZ": "mitkz",
    "MWSKZ": "mwskz",
    "STEXT": "stext",
    "VZSKZ": "vzskz",
    "WAERS": "waers",
    "WMETH": "wmeth",
    "XGKON": "xgkon",
    "XINTB": "xintb",
    "XKRES": "xkres",
    "XLOEB": "xloeb",
    "XNKON": "xnkon",
    "XOPVW": "xopvw",
    "XSPEB": "xspeb",
    "ZINDT": "zindt",
    "ZINRT": "zinrt",
    "ZUAWA": "zuawa",
    "ALTKT": "altkt",
    "XMITK": "xmitk",
    "RECID": "recid",
    "FIPOS": "fipos",
    "XMWNO": "xmwno",
    "XSALH": "xsalh",
    "BEWGP": "bewgp",
    "INFKY": "infky",
    "TOGRU": "togru",
    "XLGCLR": "xlgclr",
    "X_UJ_CLR": "x_uj_clr",
    "MCAKEY": "mcakey",
    "COCHANGED": "cochanged",
    "LAST_CHANGED_TS": "last_changed_ts",
}
_SKB1_MODEL_FIELDS = set(SKB1_COLUMNS.values())

# Target tables now have identical SAP structure to legacy tables.
# Reuse the same column mappings plus target-specific fields.
TARGET_CC_COLUMNS = {
    **CC_COLUMNS,
    "MDG_STATUS": "mdg_status",
    "MDG_CHANGE_REQUEST_ID": "mdg_change_request_id",
}
_TARGET_CC_MODEL_FIELDS = set(TARGET_CC_COLUMNS.values())

TARGET_PC_COLUMNS = {
    **PC_COLUMNS,
}
_TARGET_PC_MODEL_FIELDS = set(TARGET_PC_COLUMNS.values())

# ── Cost Center with Hierarchy (Excel) ────────────────────────────────────
# Maps the user's Excel column headers to LegacyCostCenter model fields.
CC_HIER_EXCEL_COLUMNS: dict[str, str] = {
    "ID": "cctr",
    "EXTERNAL ID": "cctr",
    "CAREA": "coarea",
    "DESCRIPTION": "txtsh",
    "OWNER": "responsible",
    "CAT": "cctrcgy",
    "START DATE": "datab",
    "END DATE": "datbi",
    "REP CC": "nkost",
    "EXTERNAL PARENT": "khinr",
    "CEMA PARENT": "zzcemapar",
    "KOMMENTAR": "xblnr",
    "SUB CC": "zzcuenkos",
    "SUB AMBC": "zzcuemncfu",
    "RESP INT": "verak_user",
    "RESP GLOBAL": "zzstrverik",
    "CT LEAD": "zzstrmacve",
    "CLAS CTR": "zzstraagcd",
    "REVIEWER": "zzstrgfd",
    "CERTIFER": "zzstrfst",
    "OWNER11": "responsible",
    "RES": "zzstrrepsit",
    "LOP": "logsystem",
    "BUS SEG": "zzstrugcd",
    "PHDOM": "zzstrabukr",
    "BD": "zzstrentzu",
    "CC/OU": "zzstrentsa",
    "ACU/DBU": "zzstrtaxarea",
    "ACU/DBU CTR": "zzstrregcode",
    "CLASS": "zzstrkklas",
    "PCT": "pctr",
    "BU": "gsber",
    "BA": "func_area",
    "TYPE": "zzstrkstyp",
    "2ND CURR": "zzstrcurr2",
    "LCC": "zzstrlccid",
    "BMC": "zzstrmaloc",
    "TAX": "zztax",
    "REGION": "regio",
    "CTRY": "land1",
    "POSTAL CD": "pstlz",
    "CITY": "ort01",
    "DISTRICT": "ort02",
    "STATE CD": "zzstate",
    "CEMA ID": "zzcemapar",
    "PRCTR": "pctr",
    "CURRENCY": "currency",
    "AMBC": "abtei",
    "GCRS FCT": "funkt",
    "GCRS N": "afunk",
    "GCRS COMP": "zzstrgsm",
    "TAX CD": "txjcd",
    "FAREA": "func_area",
    "SPERRK IST PRIMAER": "bkzkp",
    "SPERRK IST SEKUNDAER": "bkzks",
    "PLAN PRIMARY COSTS": "pkzkp",
    "PLAN SECONDARY COSTS": "pkzks",
    "PLAN REVENUES": "pkzer",
    "ACTUAL REVENUES": "bkzer",
    "PROFIT CENTER BLOCK": "bkzob",
    "HC STAT": "zzhdstat",
    "HC TYPE": "zzhdtype",
    "FMD COMP": "zzfmd",
    "FMD CC": "zzfmdcc",
    "FMD MANAGEMENT NODE": "zzfmdnode",
    "GEAR LED ID": "zzledger",
    "IB-CODE S-CENTER": "zzstrentsa",
}

# Hierarchy level column prefixes
_EXT_HIER_LEVELS = 14  # Ext_L0 .. Ext_L13
_CEMA_HIER_LEVELS = 12  # CEMA_L0 .. CEMA_L11

CENTER_MAPPING_COLUMNS = {
    "OBJECT_TYPE": "object_type",
    "TYPE": "object_type",
    "LEGACY_COAREA": "legacy_coarea",
    "LEGACY_KOKRS": "legacy_coarea",
    "LEGACY_CENTER": "legacy_center",
    "LEGACY_KOSTL": "legacy_center",
    "LEGACY_PRCTR": "legacy_center",
    "LEGACY_NAME": "legacy_name",
    "TARGET_COAREA": "target_coarea",
    "TARGET_KOKRS": "target_coarea",
    "TARGET_CENTER": "target_center",
    "TARGET_KOSTL": "target_center",
    "TARGET_PRCTR": "target_center",
    "TARGET_NAME": "target_name",
    "MAPPING_TYPE": "mapping_type",
    "NOTES": "notes",
}
_CENTER_MAPPING_MODEL_FIELDS = {
    "object_type",
    "legacy_coarea",
    "legacy_center",
    "legacy_name",
    "target_coarea",
    "target_center",
    "target_name",
    "mapping_type",
    "notes",
}


def _read_file(path: str, batch_id: int | None = None) -> list[dict[str, str]]:
    """Read CSV or Excel file and return list of row dicts."""
    p = Path(path)
    suffix = p.suffix.lower()
    file_size_mb = round(p.stat().st_size / 1_048_576, 1)
    logger.info("_read_file start", path=str(p), format=suffix, size_mb=file_size_mb)

    if suffix in (".xlsx", ".xls"):
        import pandas as pd

        t0 = _time.monotonic()
        logger.info("_read_file reading with pandas", path=str(p), size_mb=file_size_mb)
        df = pd.read_excel(p, engine="openpyxl", dtype=str)
        logger.info(
            "_read_file pandas read complete",
            rows=len(df),
            cols=len(df.columns),
            elapsed_sec=round(_time.monotonic() - t0, 2),
        )
        df.columns = [str(c).strip() for c in df.columns]
        df = df.fillna("")
        result = df.to_dict(orient="records")
        logger.info(
            "_read_file complete",
            total_rows=len(result),
            elapsed_sec=round(_time.monotonic() - t0, 2),
        )
        return result
    else:
        t0 = _time.monotonic()
        # Try UTF-8 first, fall back to cp1252 (European Excel default)
        for enc in ("utf-8-sig", "cp1252"):
            try:
                content = p.read_text(encoding=enc)
                break
            except (UnicodeDecodeError, ValueError):
                continue
        else:
            content = p.read_bytes().decode("utf-8", errors="replace")
        # Skip MDG header lines starting with *
        lines = content.split("\n")
        clean_lines = [ln for ln in lines if not ln.startswith("*")]
        if not clean_lines or not clean_lines[0].strip():
            return []
        # Detect delimiter: comma, semicolon, or tab
        header_line = clean_lines[0]
        if "\t" in header_line:
            delim = "\t"
        elif ";" in header_line and "," not in header_line:
            delim = ";"
        else:
            delim = ","
        reader = csv.DictReader(io.StringIO("\n".join(clean_lines)), delimiter=delim)
        result = [dict(row) for row in reader]
        logger.info(
            "_read_file complete",
            total_rows=len(result),
            format="csv",
            delimiter=repr(delim),
            elapsed_sec=round(_time.monotonic() - t0, 2),
        )
        return result


def _read_excel_with_options(
    path: str,
    sheet_name: str = "Database",
    header_row: int = 2,
) -> list[dict[str, str]]:
    """Read an Excel file with configurable sheet name and header row.

    Uses pandas for significantly faster reading of large files (10-50x vs openpyxl
    row-by-row iteration).

    Args:
        path: Path to Excel file
        sheet_name: Name of the sheet to read (default "Database")
        header_row: 1-based row number where headers are (default 2)
    """
    import pandas as pd

    p = Path(path)
    file_size_mb = round(p.stat().st_size / 1_048_576, 1)
    logger.info(
        "_read_excel_with_options start",
        path=path,
        sheet=sheet_name,
        header_row=header_row,
        size_mb=file_size_mb,
    )
    t0 = _time.monotonic()

    # pandas header param is 0-based; header_row is 1-based
    pandas_header = header_row - 1

    # Try specified sheet, fall back to first sheet
    try:
        df = pd.read_excel(
            p, sheet_name=sheet_name, header=pandas_header, engine="openpyxl", dtype=str
        )
    except ValueError:
        logger.warning(
            "Sheet '%s' not found, using first sheet",
            sheet_name,
        )
        df = pd.read_excel(p, sheet_name=0, header=pandas_header, engine="openpyxl", dtype=str)

    logger.info(
        "_read_excel_with_options pandas read complete",
        rows=len(df),
        cols=len(df.columns),
        elapsed_sec=round(_time.monotonic() - t0, 2),
    )

    df.columns = [str(c).strip() for c in df.columns]
    df = df.fillna("")
    result = df.to_dict(orient="records")

    logger.info(
        "_read_excel_with_options complete",
        total_rows=len(result),
        elapsed_sec=round(_time.monotonic() - t0, 2),
    )
    return result


def _normalize_headers(
    rows: list[dict[str, str]],
    mapping: dict[str, str],
    *,
    skip_label_row: bool = False,
) -> list[dict[str, str]]:
    """Normalize column headers using mapping.

    Args:
        skip_label_row: When True, detect and skip a leading SAP description
            row (common in SAP exports that have two header rows).
    """
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
    if skip_label_row and result:
        result = _skip_label_rows(result)
    return result


# Known SAP field description fragments that indicate a label row (lowercase).
_SAP_LABEL_FRAGMENTS = frozenset(
    {
        "client",
        "cost center",
        "controlling area",
        "company code",
        "currency",
        "valid to",
        "valid from",
        "profit center",
        "created on",
        "person responsible",
        "department",
        "business area",
        "language",
        "country",
        "postal code",
        "telephone",
        "fax number",
        "logical system",
        "lock indicator",
        "overhead key",
        "functional area",
        "costing sheet",
        "indicator",
        "template",
        "hierarchy",
        "successor",
        "segment",
        "field status",
        "account group",
        "account number",
        "chart of accounts",
        "reconciliation",
        "open item",
        "line item",
    }
)


# SAP code fields that should NEVER contain spaces in real data.
# If any of these contain a space, the row is a description/label row.
_SAP_CODE_FIELDS = frozenset(
    {
        "cctr",
        "coarea",
        "pctr",
        "ccode",
        "mandt",
        "bukrs",
        "saknr",
        "ktopl",
        "gpn",
        "gsber",
        "werks",
        "land1",
        "waers",
        "currency",
        "spras",
    }
)


def _skip_label_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Skip leading rows that look like SAP field description labels, not data.

    Uses two detection methods:
    1. Primary: Check if SAP code fields (cctr, coarea, mandt, etc.) contain
       spaces — real code values never have spaces, but labels like "Valid To",
       "CO Area" do.
    2. Fallback: Check if >30% of values match known SAP field description
       fragments.
    """
    skip_count = 0
    for row in rows:
        vals = [v for k, v in row.items() if v and v != "True" and v != "False" and k != "_extras"]
        if not vals:
            break

        is_label = False

        # Primary check: code fields should never contain spaces
        for field in _SAP_CODE_FIELDS:
            val = (row.get(field) or "").strip()
            if val and " " in val:
                logger.info(
                    "Skipping SAP description row (row %d): code field '%s' contains space ('%s')",
                    skip_count + 1,
                    field,
                    val,
                )
                is_label = True
                break

        # Fallback: fragment-based detection
        if not is_label and len(vals) > 3:
            label_hits = sum(
                1 for v in vals if any(frag in v.lower().strip() for frag in _SAP_LABEL_FRAGMENTS)
            )
            if label_hits / len(vals) > 0.3:
                logger.info(
                    "Skipping SAP description row (row %d, %d/%d label-like values)",
                    skip_count + 1,
                    label_hits,
                    len(vals),
                )
                is_label = True

        if is_label:
            skip_count += 1
        else:
            break
    return rows[skip_count:]


def validate_upload(batch_id: int, db: Session) -> dict:
    """Validate an uploaded file and return summary."""
    batch = db.get(UploadBatch, batch_id)
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if not batch.storage_uri:
        raise ValueError("No file associated with this batch")

    logger.info(
        "upload.validate.start",
        batch_id=batch_id,
        kind=batch.kind,
        storage_uri=batch.storage_uri,
    )

    supported = (
        "cost_center",
        "cost_centers",
        "profit_center",
        "profit_centers",
        "balance",
        "balances",
        "balances_gcr",
        "entity",
        "entities",
        "hierarchy",
        "hierarchies",
        "hierarchies_flat",
        "entity_hierarchy",
        "employee",
        "employees",
        "gl_accounts_ska1",
        "gl_accounts_skb1",
        "target_cost_centers",
        "target_profit_centers",
        "center_mapping",
        "cc_with_hierarchy",
    )
    if batch.kind not in supported:
        raise ValueError(f"Upload kind '{batch.kind}' is not yet supported")

    batch.status = "validating"
    db.execute(sa_delete(UploadError).where(UploadError.batch_id == batch.id))
    db.commit()

    # For cc_with_hierarchy, read with custom options
    if batch.kind == "cc_with_hierarchy":
        import json as _json

        opts: dict = {}
        if batch.source_detail:
            with contextlib.suppress(ValueError, TypeError):
                opts = _json.loads(batch.source_detail)
        sheet_name = opts.get("sheet_name", "Database")
        header_row = int(opts.get("header_row", 2))

    logger.info(
        "upload.validate.reading_file",
        batch_id=batch_id,
        kind=batch.kind,
        file=batch.storage_uri,
    )
    # Signal "reading file" to frontend (rows_total=-1 triggers pulsing bar)
    _flush_progress(batch.id, 0, -1)
    _t0 = _time.monotonic()
    try:
        if batch.kind == "cc_with_hierarchy":
            rows = _read_excel_with_options(batch.storage_uri, sheet_name, header_row)
        else:
            rows = _read_file(batch.storage_uri)
    except Exception as e:
        logger.error(
            "upload.validate.file_read_error",
            batch_id=batch_id,
            storage_uri=batch.storage_uri,
            error=str(e),
        )
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

    logger.info(
        "upload.validate.file_read_complete",
        batch_id=batch_id,
        rows=len(rows),
        elapsed_sec=round(_time.monotonic() - _t0, 2),
    )

    mapping = {
        "cost_center": CC_COLUMNS,
        "cost_centers": CC_COLUMNS,
        "profit_center": PC_COLUMNS,
        "profit_centers": PC_COLUMNS,
        "balance": BALANCE_COLUMNS,
        "balances": BALANCE_COLUMNS,
        "balances_gcr": BALANCE_COLUMNS,
        "entity": ENTITY_COLUMNS,
        "entities": ENTITY_COLUMNS,
        "employee": EMPLOYEE_COLUMNS,
        "employees": EMPLOYEE_COLUMNS,
        "hierarchies_flat": HIERARCHY_FLAT_COLUMNS,
        "entity_hierarchy": HIERARCHY_FLAT_COLUMNS,
        "gl_accounts_ska1": SKA1_COLUMNS,
        "gl_accounts_skb1": SKB1_COLUMNS,
        "target_cost_centers": TARGET_CC_COLUMNS,
        "target_profit_centers": TARGET_PC_COLUMNS,
        "center_mapping": CENTER_MAPPING_COLUMNS,
        "cc_with_hierarchy": CC_HIER_EXCEL_COLUMNS,
    }.get(batch.kind, {})

    # SAP exports often have a second header row with field descriptions;
    # skip it for SAP-sourced upload kinds.
    _sap_kinds = {
        "cost_center",
        "cost_centers",
        "profit_center",
        "profit_centers",
        "entity",
        "entities",
        "employee",
        "employees",
        "gl_accounts_ska1",
        "gl_accounts_skb1",
    }
    normalized = (
        _normalize_headers(rows, mapping, skip_label_row=batch.kind in _sap_kinds)
        if mapping
        else rows
    )

    # Publish total + reset progress so frontend can show a progress bar
    _flush_progress(batch.id, 0, len(normalized))

    errors: list[dict] = []
    error_rows: set[int] = set()

    for i, row in enumerate(normalized, start=1):
        if i % 100 == 0:
            _flush_progress(batch.id, i)
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
        elif batch.kind == "target_cost_centers":
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
        elif batch.kind == "target_profit_centers":
            if not row.get("pctr"):
                errors.append(
                    {"row": i, "col": "PCTR", "code": "REQUIRED", "msg": "PCTR is required"},
                )
                error_rows.add(i)
            if not row.get("coarea"):
                errors.append(
                    {"row": i, "col": "COAREA", "code": "REQUIRED", "msg": "COAREA is required"},
                )
                error_rows.add(i)
        elif batch.kind == "center_mapping":
            if not row.get("legacy_center"):
                errors.append(
                    {
                        "row": i,
                        "col": "LEGACY_CENTER",
                        "code": "REQUIRED",
                        "msg": "LEGACY_CENTER is required",
                    },
                )
                error_rows.add(i)
            if not row.get("target_center"):
                errors.append(
                    {
                        "row": i,
                        "col": "TARGET_CENTER",
                        "code": "REQUIRED",
                        "msg": "TARGET_CENTER is required",
                    },
                )
                error_rows.add(i)
            if not row.get("object_type"):
                errors.append(
                    {
                        "row": i,
                        "col": "OBJECT_TYPE",
                        "code": "REQUIRED",
                        "msg": "OBJECT_TYPE is required",
                    },
                )
                error_rows.add(i)
            elif (row.get("object_type") or "").strip().lower() not in (
                "cost_center",
                "profit_center",
            ):
                errors.append(
                    {
                        "row": i,
                        "col": "OBJECT_TYPE",
                        "code": "INVALID",
                        "msg": "OBJECT_TYPE must be 'cost_center' or 'profit_center'",
                    },
                )
                error_rows.add(i)
        elif batch.kind in ("balance", "balances", "balances_gcr"):
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
        elif batch.kind in ("hierarchies_flat", "entity_hierarchy"):
            if not row.get("nodeid"):
                errors.append(
                    {"row": i, "col": "NODEID", "code": "REQUIRED", "msg": "NODEID is required"}
                )
                error_rows.add(i)
            if not row.get("nodename"):
                errors.append(
                    {
                        "row": i,
                        "col": "NODENAME",
                        "code": "REQUIRED",
                        "msg": "NODENAME is required",
                    }
                )
                error_rows.add(i)
        elif batch.kind == "cc_with_hierarchy":
            if not row.get("cctr"):
                errors.append(
                    {
                        "row": i,
                        "col": "External Id",
                        "code": "REQUIRED",
                        "msg": "External Id (cctr) is required",
                    }
                )
                error_rows.add(i)
        elif batch.kind == "gl_accounts_ska1":
            if not row.get("saknr"):
                errors.append(
                    {"row": i, "col": "SAKNR", "code": "REQUIRED", "msg": "SAKNR is required"}
                )
                error_rows.add(i)
            if not row.get("ktopl"):
                errors.append(
                    {"row": i, "col": "KTOPL", "code": "REQUIRED", "msg": "KTOPL is required"}
                )
                error_rows.add(i)
        elif batch.kind == "gl_accounts_skb1":
            if not row.get("saknr"):
                errors.append(
                    {"row": i, "col": "SAKNR", "code": "REQUIRED", "msg": "SAKNR is required"}
                )
                error_rows.add(i)
            if not row.get("bukrs"):
                errors.append(
                    {"row": i, "col": "BUKRS", "code": "REQUIRED", "msg": "BUKRS is required"}
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
    batch.rows_processed = len(normalized)
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
    if batch.status not in ("validated", "loading"):
        raise ValueError(f"Batch must be validated first (status: {batch.status})")

    if batch.status != "loading":
        batch.status = "loading"
        db.commit()

    logger.info(
        "upload.load.reading_file",
        batch_id=batch_id,
        kind=batch.kind,
    )
    _t0 = _time.monotonic()
    try:
        rows = [] if batch.kind == "cc_with_hierarchy" else _read_file(batch.storage_uri)
    except Exception as e:
        batch.status = "failed"
        db.add(
            UploadError(
                batch_id=batch.id,
                row_number=0,
                error_code="LOAD_FILE_READ",
                message=f"Cannot read file for loading: {e}",
            )
        )
        db.commit()
        return {"status": "failed", "error": str(e)}

    logger.info(
        "upload.load.file_read_complete",
        batch_id=batch_id,
        rows=len(rows),
        elapsed_sec=round(_time.monotonic() - _t0, 2),
    )

    mapping = {
        "cost_center": CC_COLUMNS,
        "cost_centers": CC_COLUMNS,
        "profit_center": PC_COLUMNS,
        "profit_centers": PC_COLUMNS,
        "balance": BALANCE_COLUMNS,
        "balances": BALANCE_COLUMNS,
        "balances_gcr": BALANCE_COLUMNS,
        "entity": ENTITY_COLUMNS,
        "entities": ENTITY_COLUMNS,
        "employee": EMPLOYEE_COLUMNS,
        "employees": EMPLOYEE_COLUMNS,
        "hierarchies_flat": HIERARCHY_FLAT_COLUMNS,
        "entity_hierarchy": HIERARCHY_FLAT_COLUMNS,
        "gl_accounts_ska1": SKA1_COLUMNS,
        "gl_accounts_skb1": SKB1_COLUMNS,
        "target_cost_centers": TARGET_CC_COLUMNS,
        "target_profit_centers": TARGET_PC_COLUMNS,
        "center_mapping": CENTER_MAPPING_COLUMNS,
        "cc_with_hierarchy": CC_HIER_EXCEL_COLUMNS,
    }.get(batch.kind, {})

    # SAP exports often have a second header row with field descriptions;
    # skip it for SAP-sourced upload kinds.
    _sap_kinds = {
        "cost_center",
        "cost_centers",
        "profit_center",
        "profit_centers",
        "entity",
        "entities",
        "employee",
        "employees",
        "gl_accounts_ska1",
        "gl_accounts_skb1",
    }
    normalized = (
        _normalize_headers(rows, mapping, skip_label_row=batch.kind in _sap_kinds)
        if mapping
        else rows
    )
    loaded = 0

    # Read scope + data_category from batch (defaults for backward compat)
    batch_scope = getattr(batch, "scope", None) or "cleanup"
    batch_category = getattr(batch, "data_category", None) or "legacy"

    # Extract user-defined hierarchy label from source_detail (if set during upload)
    _hier_label: str | None = None
    if batch.source_detail:
        with contextlib.suppress(ValueError, TypeError):
            import json as _json

            _sd = _json.loads(batch.source_detail)
            if isinstance(_sd, dict):
                _hier_label = _sd.get("hierarchy_label")

    # Publish total + reset progress for load phase
    # (skip for cc_with_hierarchy — _load_cc_with_hierarchy sets its own total)
    if batch.kind != "cc_with_hierarchy":
        _flush_progress(batch.id, 0, len(normalized))

    # Pre-build employee lookup for VERAK validation (CC/PC kinds)
    _verak_kinds = {
        "cost_center",
        "cost_centers",
        "profit_center",
        "profit_centers",
    }
    gpn_map: dict[str, Employee] = {}
    name_map: dict[str, list[Employee]] = {}
    dq_issues: list[DataQualityIssue] = []
    dq_auto_fixed_count = 0
    if batch.kind in _verak_kinds:
        gpn_map, name_map = _build_employee_lookup(db, batch_scope)
        # Clear previous DQ issues for this batch
        db.execute(sa_delete(DataQualityIssue).where(DataQualityIssue.batch_id == batch.id))

    if batch.kind in ("cost_center", "cost_centers"):
        # Bulk pre-fetch existing records to avoid N+1 queries
        existing_ccs = {
            (cc.coarea, cc.cctr): cc
            for cc in db.execute(
                select(LegacyCostCenter).where(LegacyCostCenter.scope == batch_scope)
            ).scalars()
        }
        batch_size = 500
        for row in normalized:
            if not row.get("cctr") or not row.get("coarea"):
                continue
            existing = existing_ccs.get((row["coarea"], row["cctr"]))
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            cc_kwargs: dict = {}
            for field_name in _CC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    cc_kwargs[field_name] = val if val else None
            cc_kwargs["coarea"] = row["coarea"]
            cc_kwargs["cctr"] = row["cctr"]
            if row.get("is_active"):
                cc_kwargs["is_active"] = is_act
            for sap_key, legacy_key in (("datab", "valid_from"), ("datbi", "valid_to")):
                raw = row.get(legacy_key) or row.get(sap_key)
                if raw and isinstance(raw, str):
                    cc_kwargs[legacy_key] = _parse_date(raw)
            _truncate_to_model(LegacyCostCenter, cc_kwargs)
            if existing:
                for k, v in cc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                _cc_defaults = (
                    "txtsh",
                    "txtmi",
                    "responsible",
                    "cctrcgy",
                    "ccode",
                    "currency",
                    "pctr",
                )
                for fld in _cc_defaults:
                    if cc_kwargs.get(fld) is None:
                        cc_kwargs[fld] = ""
                cc_kwargs.setdefault("is_active", True)
                cc_kwargs["refresh_batch"] = batch.id
                cc_kwargs["scope"] = batch_scope
                cc_kwargs["data_category"] = batch_category
                new_cc = LegacyCostCenter(**cc_kwargs)
                db.add(new_cc)
                existing_ccs[(row["coarea"], row["cctr"])] = new_cc
            loaded += 1
            if loaded % batch_size == 0:
                db.flush()
                _flush_progress(batch.id, loaded)
        if loaded % batch_size:
            db.flush()

        # VERAK validation pass (after flush so objects have IDs)
        if gpn_map:
            for cc in existing_ccs.values():
                verak = cc.responsible
                if not verak:
                    continue
                corrected, emp_id, dq = _validate_verak(
                    "cost_center",
                    cc.id,
                    verak,
                    gpn_map,
                    name_map,
                    batch_scope,
                    batch.id,
                )
                if corrected and corrected != verak:
                    cc.responsible = corrected
                if emp_id is not None:
                    cc.responsible_employee_id = emp_id
                if dq is _VERAK_AUTO_FIXED:
                    dq_auto_fixed_count += 1
                elif isinstance(dq, DataQualityIssue):
                    dq_issues.append(dq)

    elif batch.kind in ("profit_center", "profit_centers"):
        # Bulk pre-fetch existing records to avoid N+1 queries
        existing_pcs = {
            (pc.coarea, pc.pctr): pc
            for pc in db.execute(
                select(LegacyProfitCenter).where(LegacyProfitCenter.scope == batch_scope)
            ).scalars()
        }
        batch_size = 500
        for row in normalized:
            if not row.get("pctr"):
                continue
            existing = existing_pcs.get((row.get("coarea", ""), row["pctr"]))
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            pc_kwargs: dict = {}
            for field_name in _PC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    pc_kwargs[field_name] = val if val else None
            pc_kwargs["coarea"] = row.get("coarea") or ""
            pc_kwargs["pctr"] = row["pctr"]
            if row.get("is_active"):
                pc_kwargs["is_active"] = is_act
            for sap_key, legacy_key in (("datab", "valid_from"), ("datbi", "valid_to")):
                raw = row.get(legacy_key) or row.get(sap_key)
                if raw and isinstance(raw, str):
                    pc_kwargs[legacy_key] = _parse_date(raw)
            _truncate_to_model(LegacyProfitCenter, pc_kwargs)
            if existing:
                for k, v in pc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                for fld in ("txtsh", "txtmi", "responsible", "ccode", "department", "currency"):
                    if pc_kwargs.get(fld) is None:
                        pc_kwargs[fld] = ""
                pc_kwargs.setdefault("is_active", True)
                pc_kwargs["refresh_batch"] = batch.id
                pc_kwargs["scope"] = batch_scope
                pc_kwargs["data_category"] = batch_category
                new_pc = LegacyProfitCenter(**pc_kwargs)
                db.add(new_pc)
                existing_pcs[(row.get("coarea", ""), row["pctr"])] = new_pc
            loaded += 1
            if loaded % batch_size == 0:
                db.flush()
                _flush_progress(batch.id, loaded)
        if loaded % batch_size:
            db.flush()

        # VERAK validation pass (after flush so objects have IDs)
        if gpn_map:
            for pc in existing_pcs.values():
                verak = pc.responsible
                if not verak:
                    continue
                corrected, emp_id, dq = _validate_verak(
                    "profit_center",
                    pc.id,
                    verak,
                    gpn_map,
                    name_map,
                    batch_scope,
                    batch.id,
                )
                if corrected and corrected != verak:
                    pc.responsible = corrected
                if emp_id is not None:
                    pc.responsible_employee_id = emp_id
                if dq is _VERAK_AUTO_FIXED:
                    dq_auto_fixed_count += 1
                elif isinstance(dq, DataQualityIssue):
                    dq_issues.append(dq)

    elif batch.kind in ("balance", "balances", "balances_gcr"):
        batch_size = 500
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
                    scope=batch_scope,
                    data_category=batch_category,
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
            if loaded % batch_size == 0:
                db.flush()
                _flush_progress(batch.id, loaded)
        if loaded % batch_size:
            db.flush()

    elif batch.kind in ("entity", "entities"):
        # Bulk pre-fetch existing entities
        existing_ents = {
            e.ccode: e
            for e in db.execute(select(Entity).where(Entity.scope == batch_scope)).scalars()
        }
        batch_size = 500
        for row in normalized:
            if not row.get("ccode"):
                continue
            existing = existing_ents.get(row["ccode"])
            ent_kwargs: dict = {}
            for field_name in _ENTITY_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    ent_kwargs[field_name] = val if val else None
            ent_kwargs["ccode"] = row["ccode"]
            if row.get("is_active"):
                ent_kwargs["is_active"] = row["is_active"].upper() not in (
                    "FALSE",
                    "0",
                    "NO",
                    "N",
                )
            _truncate_to_model(Entity, ent_kwargs)
            if existing:
                for k, v in ent_kwargs.items():
                    if k != "ccode" and v is not None:
                        setattr(existing, k, v)
            else:
                if ent_kwargs.get("name") is None:
                    ent_kwargs["name"] = row["ccode"]
                ent_kwargs["scope"] = batch_scope
                ent_kwargs["data_category"] = batch_category
                new_ent = Entity(**ent_kwargs)
                db.add(new_ent)
                existing_ents[row["ccode"]] = new_ent
            loaded += 1
            if loaded % batch_size == 0:
                db.flush()
                _flush_progress(batch.id, loaded)
        if loaded % batch_size:
            db.flush()

    elif batch.kind in ("employee", "employees"):
        # Bulk pre-fetch ALL existing employees in scope (not just this batch)
        # so we can mark missing ones as inactive after loading.
        all_existing_emps = {
            emp.gpn: emp
            for emp in db.execute(select(Employee).where(Employee.scope == batch_scope)).scalars()
        }
        uploaded_gpns: set[str] = set()
        batch_size = 500
        for row in normalized:
            gpn = row.get("gpn", "").strip()
            if not gpn:
                continue
            uploaded_gpns.add(gpn)
            existing = all_existing_emps.get(gpn)
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
            model_kwargs["is_active"] = True  # present in upload → active
            _truncate_to_model(Employee, model_kwargs)
            if existing:
                for k, v in model_kwargs.items():
                    if k != "refresh_batch" and v is not None:
                        setattr(existing, k, v)
            else:
                model_kwargs["scope"] = batch_scope
                model_kwargs["data_category"] = batch_category
                new_emp = Employee(**model_kwargs)
                db.add(new_emp)
                all_existing_emps[gpn] = new_emp
            loaded += 1
            if loaded % batch_size == 0:
                db.flush()
                _flush_progress(batch.id, loaded)
        if loaded % batch_size:
            db.flush()

        # Mark employees NOT in this upload as inactive
        inactive_count = 0
        for gpn, emp in all_existing_emps.items():
            if gpn not in uploaded_gpns and emp.is_active:
                emp.is_active = False
                inactive_count += 1
        if inactive_count:
            db.flush()
            logger.info(
                "Marked %d employees as inactive (not in upload batch %d)",
                inactive_count,
                batch.id,
            )

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
                    Hierarchy.scope == batch_scope,
                    Hierarchy.setclass == setclass,
                    Hierarchy.setname == setname,
                    Hierarchy.refresh_batch == batch.id,
                )
            ).scalar_one_or_none()
            if not existing:
                h = Hierarchy(
                    scope=batch_scope,
                    data_category=batch_category,
                    setclass=setclass,
                    setname=setname,
                    label=_hier_label or None,
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
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

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
                        Hierarchy.scope == batch_scope,
                        Hierarchy.setclass == setclass,
                        Hierarchy.setname == setname,
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
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

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
                        Hierarchy.scope == batch_scope,
                        Hierarchy.setclass == setclass,
                        Hierarchy.setname == setname,
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
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind in ("hierarchies_flat", "entity_hierarchy"):
        # Build hierarchy from flat SAP node export (NODEID/PARENTID/CHILDID).
        # For entity_hierarchy, group by PERIOD so each period becomes a separate
        # Hierarchy record with the period shown in the label.
        is_entity_hier = batch.kind == "entity_hierarchy"

        # Group rows by period (entity_hierarchy) or treat all as one group
        period_groups: dict[str, list[dict]] = {}
        for row in normalized:
            period_key = (row.get("period", "") or "").strip() if is_entity_hier else ""
            period_groups.setdefault(period_key, []).append(row)

        for period_val, period_rows in sorted(period_groups.items()):
            node_lookup: dict[str, dict] = {}
            children_of: dict[str, list[str]] = {}
            for row in period_rows:
                nid = row.get("nodeid", "").strip()
                if not nid:
                    continue
                node_lookup[nid] = row
                pid = row.get("parentid", "").strip()
                if pid:
                    children_of.setdefault(pid, []).append(nid)

            # Find root nodes (no parent)
            roots = [
                row
                for row in period_rows
                if row.get("nodeid") and not row.get("parentid", "").strip()
            ]
            if not roots:
                all_ids = set(node_lookup.keys())
                roots = [
                    row
                    for row in period_rows
                    if row.get("nodeid") and row.get("parentid", "").strip() not in all_ids
                ]

            hier_map_flat: dict[str, Hierarchy] = {}
            for root_row in roots:
                root_id = root_row.get("nodeid", "").strip()
                raw_name = root_row.get("nodename", root_id)
                description = root_row.get("nodetext", "")
                label = _hier_label or None
                hier_attrs: dict | None = None
                setname = raw_name
                if is_entity_hier and period_val:
                    max_name = 40 - len(period_val) - 1
                    setname = f"{raw_name[:max_name]}_{period_val}"
                    if not label:
                        label = f"{raw_name} ({period_val})"
                    hier_attrs = {"period": period_val}
                elif is_entity_hier and not label:
                    label = raw_name
                h = Hierarchy(
                    scope=batch_scope,
                    data_category=batch_category,
                    setclass="GCRS" if is_entity_hier else "FLAT",
                    setname=setname,
                    label=label,
                    description=description,
                    coarea="",
                    refresh_batch=batch.id,
                    attrs=hier_attrs,
                )
                db.add(h)
                db.flush()
                hier_map_flat[root_id] = h
                loaded += 1
                if loaded % 100 == 0:
                    _flush_progress(batch.id, loaded)

            # BFS to create nodes and leaves
            from collections import deque

            queue: deque[tuple[str, Hierarchy]] = deque()
            for root_row in roots:
                rid = root_row.get("nodeid", "").strip()
                if rid in hier_map_flat:
                    queue.append((rid, hier_map_flat[rid]))

            visited: set[str] = set()
            seq_counter: dict[int, int] = {}
            while queue:
                parent_nid, hier = queue.popleft()
                if parent_nid in visited:
                    continue
                visited.add(parent_nid)
                parent_row = node_lookup.get(parent_nid, {})
                parent_name = parent_row.get("nodename", parent_nid)
                child_ids = children_of.get(parent_nid, [])
                for child_nid in child_ids:
                    child_row = node_lookup.get(child_nid, {})
                    child_name = child_row.get("nodename", child_nid)
                    hid = hier.id
                    seq_counter.setdefault(hid, 0)
                    seq_counter[hid] += 1
                    has_children = child_nid in children_of
                    if has_children:
                        db.add(
                            HierarchyNode(
                                hierarchy_id=hid,
                                parent_setname=parent_name[:40],
                                child_setname=child_name[:40],
                                seq=seq_counter[hid],
                            )
                        )
                        queue.append((child_nid, hier))
                    else:
                        db.add(
                            HierarchyLeaf(
                                hierarchy_id=hid,
                                setname=parent_name[:40],
                                value=child_name[:20],
                                seq=seq_counter[hid],
                            )
                        )
                    loaded += 1
                if loaded % 100 == 0:
                    _flush_progress(batch.id, loaded)

    elif batch.kind == "gl_accounts_ska1":
        for row in normalized:
            saknr = (row.get("saknr") or "").strip()
            ktopl = (row.get("ktopl") or "").strip()
            if not saknr or not ktopl:
                continue
            existing = db.execute(
                select(GLAccountSKA1).where(
                    GLAccountSKA1.scope == batch_scope,
                    GLAccountSKA1.ktopl == ktopl,
                    GLAccountSKA1.saknr == saknr,
                )
            ).scalar_one_or_none()
            kwargs: dict = {}
            for field_name in _SKA1_MODEL_FIELDS:
                val = row.get(field_name)
                if val is not None:
                    kwargs[field_name] = val if val else None
            kwargs["ktopl"] = ktopl
            kwargs["saknr"] = saknr
            _truncate_to_model(GLAccountSKA1, kwargs)
            if existing:
                for k, v in kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                kwargs["refresh_batch"] = batch.id
                kwargs["scope"] = batch_scope
                kwargs["data_category"] = batch_category
                db.add(GLAccountSKA1(**kwargs))
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind == "gl_accounts_skb1":
        for row in normalized:
            saknr = (row.get("saknr") or "").strip()
            bukrs = (row.get("bukrs") or "").strip()
            if not saknr or not bukrs:
                continue
            existing = db.execute(
                select(GLAccountSKB1).where(
                    GLAccountSKB1.scope == batch_scope,
                    GLAccountSKB1.bukrs == bukrs,
                    GLAccountSKB1.saknr == saknr,
                )
            ).scalar_one_or_none()
            kwargs_b: dict = {}
            for field_name in _SKB1_MODEL_FIELDS:
                val = row.get(field_name)
                if val is not None:
                    kwargs_b[field_name] = val if val else None
            kwargs_b["bukrs"] = bukrs
            kwargs_b["saknr"] = saknr
            _truncate_to_model(GLAccountSKB1, kwargs_b)
            if existing:
                for k, v in kwargs_b.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                kwargs_b["refresh_batch"] = batch.id
                kwargs_b["scope"] = batch_scope
                kwargs_b["data_category"] = batch_category
                db.add(GLAccountSKB1(**kwargs_b))
            loaded += 1
            if loaded % 100 == 0:
                _flush_progress(batch.id, loaded)

    elif batch.kind == "target_cost_centers":
        for row in normalized:
            cctr = (row.get("cctr") or "").strip()
            coarea = (row.get("coarea") or "").strip()
            if not cctr or not coarea:
                continue
            existing = db.execute(
                select(TargetCostCenter).where(
                    TargetCostCenter.scope == batch_scope,
                    TargetCostCenter.coarea == coarea,
                    TargetCostCenter.cctr == cctr,
                )
            ).scalar_one_or_none()
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            tcc_kwargs: dict = {}
            for field_name in _TARGET_CC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    tcc_kwargs[field_name] = val if val else None
            tcc_kwargs["coarea"] = coarea
            tcc_kwargs["cctr"] = cctr
            if row.get("is_active"):
                tcc_kwargs["is_active"] = is_act
            _truncate_to_model(TargetCostCenter, tcc_kwargs)
            if existing:
                for k, v in tcc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                tcc_kwargs.setdefault("is_active", True)
                tcc_kwargs["refresh_batch"] = batch.id
                tcc_kwargs["scope"] = batch_scope
                tcc_kwargs["data_category"] = batch_category
                db.add(TargetCostCenter(**tcc_kwargs))
            loaded += 1

    elif batch.kind == "target_profit_centers":
        for row in normalized:
            pctr = (row.get("pctr") or "").strip()
            coarea = (row.get("coarea") or "").strip()
            if not pctr or not coarea:
                continue
            existing = db.execute(
                select(TargetProfitCenter).where(
                    TargetProfitCenter.scope == batch_scope,
                    TargetProfitCenter.coarea == coarea,
                    TargetProfitCenter.pctr == pctr,
                )
            ).scalar_one_or_none()
            is_act = row.get("is_active", "").upper() not in ("FALSE", "0", "NO", "N")
            tpc_kwargs: dict = {}
            for field_name in _TARGET_PC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    tpc_kwargs[field_name] = val if val else None
            tpc_kwargs["coarea"] = coarea
            tpc_kwargs["pctr"] = pctr
            if row.get("is_active"):
                tpc_kwargs["is_active"] = is_act
            _truncate_to_model(TargetProfitCenter, tpc_kwargs)
            if existing:
                for k, v in tpc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                tpc_kwargs.setdefault("is_active", True)
                tpc_kwargs["refresh_batch"] = batch.id
                tpc_kwargs["scope"] = batch_scope
                tpc_kwargs["data_category"] = batch_category
                db.add(TargetProfitCenter(**tpc_kwargs))
            loaded += 1

    elif batch.kind == "center_mapping":
        for row in normalized:
            legacy_center = (row.get("legacy_center") or "").strip()
            target_center = (row.get("target_center") or "").strip()
            obj_type = (row.get("object_type") or "").strip().lower()
            if not legacy_center or not target_center or not obj_type:
                continue
            if obj_type not in ("cost_center", "profit_center"):
                continue
            legacy_co = (row.get("legacy_coarea") or "").strip() or ""
            target_co = (row.get("target_coarea") or "").strip() or ""
            existing = db.execute(
                select(CenterMapping).where(
                    CenterMapping.scope == batch_scope,
                    CenterMapping.object_type == obj_type,
                    CenterMapping.legacy_coarea == legacy_co,
                    CenterMapping.legacy_center == legacy_center,
                    CenterMapping.target_coarea == target_co,
                    CenterMapping.target_center == target_center,
                )
            ).scalar_one_or_none()
            cm_kwargs: dict = {}
            for field_name in _CENTER_MAPPING_MODEL_FIELDS:
                val = row.get(field_name)
                if val is not None:
                    cm_kwargs[field_name] = val if val else None
            cm_kwargs["object_type"] = obj_type
            cm_kwargs["legacy_center"] = legacy_center
            cm_kwargs["target_center"] = target_center
            cm_kwargs["legacy_coarea"] = legacy_co
            cm_kwargs["target_coarea"] = target_co
            _truncate_to_model(CenterMapping, cm_kwargs)
            if existing:
                for k, v in cm_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                cm_kwargs["refresh_batch"] = batch.id
                cm_kwargs["scope"] = batch_scope
                cm_kwargs["data_category"] = batch_category
                db.add(CenterMapping(**cm_kwargs))
            loaded += 1

    elif batch.kind == "cc_with_hierarchy":
        loaded = _load_cc_with_hierarchy(batch, db, batch_scope, batch_category, _hier_label)

    # Check for hierarchy orphan leaves (centers in hierarchy but not uploaded)
    _hier_kinds = {
        "hierarchy",
        "hierarchies",
        "hierarchies_flat",
        "entity_hierarchy",
        "cc_with_hierarchy",
    }
    if batch.kind in _hier_kinds:
        db.flush()  # ensure leaves are visible for the query
        dq_issues.extend(_check_hierarchy_orphans(db, batch, batch_scope))

    # Persist any data-quality issues raised during loading (in chunks)
    dq_open = len(dq_issues)  # all persisted issues are open (auto-fixed are counted separately)
    if dq_issues:
        dq_chunk = 5000
        for i in range(0, len(dq_issues), dq_chunk):
            db.add_all(dq_issues[i : i + dq_chunk])
            db.flush()
    dq_total = dq_open + dq_auto_fixed_count
    if dq_total:
        logger.info(
            "Data quality: %d issues (%d open, %d auto-fixed) for batch %d",
            dq_total,
            dq_open,
            dq_auto_fixed_count,
            batch.id,
        )

    batch.rows_loaded = loaded
    batch.rows_processed = loaded
    batch.status = "loaded"
    batch.loaded_at = datetime.now(UTC)
    db.commit()

    result: dict = {"status": "loaded", "rows_loaded": loaded}
    if dq_total:
        result["dq_issues_total"] = dq_total
        result["dq_issues_open"] = dq_open
        result["dq_issues_auto_fixed"] = dq_auto_fixed_count
    return result


def _load_cc_with_hierarchy(
    batch: UploadBatch,
    db: Session,
    batch_scope: str,
    batch_category: str,
    hierarchy_label: str | None = None,
) -> int:
    """Load cost centers + hierarchies from a combined Excel file.

    The batch's source_detail is a JSON string with options:
        sheet_name, header_row, load_cc, load_ext_hier, load_cema_hier
    """
    import json as _json

    opts: dict = {}
    if batch.source_detail:
        with contextlib.suppress(ValueError, TypeError):
            opts = _json.loads(batch.source_detail)

    sheet_name = opts.get("sheet_name", "Database")
    header_row = int(opts.get("header_row", 2))
    load_cc = opts.get("load_cc", True)
    load_ext_hier = opts.get("load_ext_hier", True)
    load_cema_hier = opts.get("load_cema_hier", True)

    rows = _read_excel_with_options(batch.storage_uri, sheet_name, header_row)
    _flush_progress(batch.id, 0, len(rows))
    loaded = 0
    cc_progress = 0  # separate counter for progress bar (CC rows only)

    # --- 1) Load cost center data ---
    if load_cc:
        cc_rows = _normalize_headers(rows, CC_HIER_EXCEL_COLUMNS)
        # Bulk pre-fetch existing CCs to avoid per-row SELECT (N+1 → 1 query)
        existing_ccs = {
            (cc.coarea, cc.cctr): cc
            for cc in db.execute(
                select(LegacyCostCenter).where(LegacyCostCenter.scope == batch_scope)
            ).scalars()
        }
        batch_size = 500
        for row in cc_rows:
            cctr = (row.get("cctr") or "").strip()
            coarea = (row.get("coarea") or "").strip()
            if not cctr:
                continue
            existing = existing_ccs.get((coarea, cctr))
            cc_kwargs: dict = {}
            for field_name in _CC_MODEL_FIELDS:
                if field_name == "is_active":
                    continue
                val = row.get(field_name)
                if val is not None:
                    cc_kwargs[field_name] = val if val else None
            cc_kwargs["coarea"] = coarea
            cc_kwargs["cctr"] = cctr
            # Populate valid_from/valid_to from date fields (same as regular CC loader)
            for sap_key, legacy_key in (("datab", "valid_from"), ("datbi", "valid_to")):
                raw = row.get(legacy_key) or row.get(sap_key)
                if raw and isinstance(raw, str):
                    cc_kwargs[legacy_key] = _parse_date(raw)
            _truncate_to_model(LegacyCostCenter, cc_kwargs)
            if existing:
                for k, v in cc_kwargs.items():
                    if v is not None:
                        setattr(existing, k, v)
            else:
                _cc_defaults = (
                    "txtsh",
                    "txtmi",
                    "responsible",
                    "cctrcgy",
                    "ccode",
                    "currency",
                    "pctr",
                )
                for fld in _cc_defaults:
                    if cc_kwargs.get(fld) is None:
                        cc_kwargs[fld] = ""
                cc_kwargs.setdefault("is_active", True)
                cc_kwargs["refresh_batch"] = batch.id
                cc_kwargs["scope"] = batch_scope
                cc_kwargs["data_category"] = batch_category
                db.add(LegacyCostCenter(**cc_kwargs))
            loaded += 1
            cc_progress += 1
            if cc_progress % batch_size == 0:
                db.flush()
                _flush_progress(batch.id, cc_progress)
        if cc_progress % batch_size:
            db.flush()

    # --- 2) Load External Hierarchy ---
    if load_ext_hier:
        loaded += _build_hierarchy_from_levels(
            rows=rows,
            db=db,
            batch=batch,
            batch_scope=batch_scope,
            batch_category=batch_category,
            hier_name_col="External_Hierarchy",
            level_prefix="Ext_L",
            desc_prefix="Ext_L",
            desc_suffix="_Desc",
            num_levels=_EXT_HIER_LEVELS,
            setclass="0101",
            loaded_so_far=loaded,
            hierarchy_label=hierarchy_label,
        )

    # --- 3) Load CEMA Hierarchy ---
    if load_cema_hier:
        loaded += _build_hierarchy_from_levels(
            rows=rows,
            db=db,
            batch=batch,
            batch_scope=batch_scope,
            batch_category=batch_category,
            hier_name_col="CEMA_Hierarchy",
            level_prefix="CEMA_L",
            desc_prefix="CEMA_L",
            desc_suffix="_Desc",
            num_levels=_CEMA_HIER_LEVELS,
            setclass="0101",
            loaded_so_far=loaded,
            hierarchy_label=hierarchy_label,
        )

    return loaded


def _build_hierarchy_from_levels(
    *,
    rows: list[dict[str, str]],
    db: Session,
    batch: UploadBatch,
    batch_scope: str,
    batch_category: str,
    hier_name_col: str,
    level_prefix: str,
    num_levels: int,
    setclass: str,
    loaded_so_far: int,
    desc_prefix: str = "",
    desc_suffix: str = "",
    hierarchy_label: str | None = None,
) -> int:
    """Build a hierarchy from flattened level columns (L0..Ln).

    Each row has level columns like Ext_L0, Ext_L1, ... Ext_L13 where
    the rightmost non-empty value is the leaf (cost center). Each level
    value is a node ID and we build parent→child relationships from the
    level structure.

    Returns the count of records created.
    """
    # Collect unique hierarchy names from the name column
    hier_names: set[str] = set()
    for row in rows:
        hname = (row.get(hier_name_col) or "").strip()
        if hname:
            hier_names.add(hname)

    if not hier_names:
        hier_names = {hier_name_col}

    loaded = 0
    for hier_setname in sorted(hier_names):
        # Filter rows belonging to this hierarchy
        hier_rows = [
            r
            for r in rows
            if (r.get(hier_name_col) or "").strip() == hier_setname
            or (not r.get(hier_name_col, "").strip() and len(hier_names) == 1)
        ]
        if not hier_rows:
            continue

        # Build a unique setname (max 40 chars).  When the raw name is
        # longer than 40 characters, append a short hash suffix so that
        # two distinct long names that share the same prefix don't collide
        # on the unique constraint.
        import hashlib as _hashlib

        if len(hier_setname) > 40:
            suffix = _hashlib.md5(hier_setname.encode(), usedforsecurity=False).hexdigest()[:6]
            sname = hier_setname[: 40 - 7] + "_" + suffix
        else:
            sname = hier_setname[:40]

        # Upsert: reuse existing hierarchy for same batch if already created
        existing = db.execute(
            select(Hierarchy).where(
                Hierarchy.scope == batch_scope,
                Hierarchy.setclass == setclass,
                Hierarchy.setname == sname,
                Hierarchy.refresh_batch == batch.id,
            )
        ).scalar_one_or_none()
        if existing:
            hier = existing
        else:
            hier = Hierarchy(
                scope=batch_scope,
                data_category=batch_category,
                setclass=setclass,
                setname=sname,
                label=hierarchy_label or hier_setname,
                description=f"Hierarchy {hier_setname} (from Excel upload)",
                coarea="",
                refresh_batch=batch.id,
            )
            db.add(hier)
            db.flush()
        loaded += 1

        # Build unique nodes and parent→child edges from level columns
        # node_key = (level_value, level_index)
        # We collect edges: parent_node → child_node
        edges: dict[str, set[str]] = {}  # parent → set of children
        leaf_parents: dict[str, set[str]] = {}  # leaf_parent → set of leaf values

        for row in hier_rows:
            levels: list[str] = []
            for i in range(num_levels):
                lval = (row.get(f"{level_prefix}{i}") or "").strip()
                levels.append(lval)

            # Find rightmost non-empty level (this is the leaf/cost center)
            last_idx = -1
            for i in range(num_levels - 1, -1, -1):
                if levels[i]:
                    last_idx = i
                    break
            if last_idx < 0:
                continue

            # Build edges from level 0 down to leaf
            for i in range(last_idx + 1):
                node_id = levels[i]
                if not node_id:
                    continue

                if i < last_idx:
                    # Find next non-empty level as child
                    for j in range(i + 1, last_idx + 1):
                        if levels[j]:
                            edges.setdefault(node_id, set()).add(levels[j])
                            break
                elif i == last_idx:
                    # This is a leaf — find its parent
                    for j in range(i - 1, -1, -1):
                        if levels[j]:
                            leaf_parents.setdefault(levels[j], set()).add(node_id)
                            break

        # Determine which nodes are internal (have children that are also nodes)
        all_children: set[str] = set()
        for children in edges.values():
            all_children.update(children)
        all_parents = set(edges.keys())
        all_leaf_values: set[str] = set()
        for lvals in leaf_parents.values():
            all_leaf_values.update(lvals)

        # Internal nodes: appear as parents OR appear as children but also have children
        internal_nodes = all_parents | (all_children - all_leaf_values)

        # Create HierarchyNode records for internal edges
        seq = 0
        created_edges: set[tuple[str, str]] = set()
        for parent_id in sorted(edges.keys()):
            for child_id in sorted(edges[parent_id]):
                edge_key = (parent_id, child_id)
                if edge_key in created_edges:
                    continue
                created_edges.add(edge_key)
                if child_id in internal_nodes:
                    seq += 1
                    db.add(
                        HierarchyNode(
                            hierarchy_id=hier.id,
                            parent_setname=parent_id[:40],
                            child_setname=child_id[:40],
                            seq=seq,
                        )
                    )
                    loaded += 1

        # Create HierarchyLeaf records for leaf values
        for parent_id in sorted(leaf_parents.keys()):
            for leaf_val in sorted(leaf_parents[parent_id]):
                if leaf_val in internal_nodes:
                    continue
                seq += 1
                db.add(
                    HierarchyLeaf(
                        hierarchy_id=hier.id,
                        setname=parent_id[:40],
                        value=leaf_val[:20],
                        seq=seq,
                    )
                )
                loaded += 1

    return loaded


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
    elif batch.kind in ("balance", "balances", "balances_gcr"):
        r = db.execute(sa_delete(Balance).where(Balance.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind in ("entity", "entities"):
        raise ValueError("Entity uploads cannot be rolled back (no batch tracking on entities)")
    elif batch.kind in ("employee", "employees"):
        r = db.execute(sa_delete(Employee).where(Employee.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind in ("hierarchy", "hierarchies", "hierarchies_flat", "entity_hierarchy"):
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
    elif batch.kind == "gl_accounts_ska1":
        r = db.execute(sa_delete(GLAccountSKA1).where(GLAccountSKA1.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind == "gl_accounts_skb1":
        r = db.execute(sa_delete(GLAccountSKB1).where(GLAccountSKB1.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind == "target_cost_centers":
        r = db.execute(
            sa_delete(TargetCostCenter).where(TargetCostCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
    elif batch.kind == "target_profit_centers":
        r = db.execute(
            sa_delete(TargetProfitCenter).where(TargetProfitCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
    elif batch.kind == "center_mapping":
        r = db.execute(sa_delete(CenterMapping).where(CenterMapping.refresh_batch == batch.id))
        deleted = r.rowcount
    elif batch.kind == "cc_with_hierarchy":
        # Delete both cost centers and hierarchies created by this batch
        r = db.execute(
            sa_delete(LegacyCostCenter).where(LegacyCostCenter.refresh_batch == batch.id)
        )
        deleted = r.rowcount
        hier_ids = [
            h.id
            for h in db.execute(select(Hierarchy).where(Hierarchy.refresh_batch == batch.id))
            .scalars()
            .all()
        ]
        for hid in hier_ids:
            r_leaf = db.execute(sa_delete(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == hid))
            deleted += r_leaf.rowcount
            r_node = db.execute(sa_delete(HierarchyNode).where(HierarchyNode.hierarchy_id == hid))
            deleted += r_node.rowcount
        r2 = db.execute(sa_delete(Hierarchy).where(Hierarchy.refresh_batch == batch.id))
        deleted += r2.rowcount

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
