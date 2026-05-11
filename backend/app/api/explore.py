"""Public explore / visualization endpoints.

Provides read access to legacy data, analysis results (ampliFi), and target
mapping views for the data visualization / browsing page (Use Case C of the
business specification: side-by-side compare of legacy vs target objects).

Auth model:
- Most endpoints are PUBLIC by default for the data-browsing UX.
- Sensitive object types (``employees``, ``balances``) are gated behind
  ``settings.explorer_require_auth``. When that flag is true (production),
  these endpoints require an authenticated analyst/admin user.

Datasphere note:
- The data domains queried here (``legacy_*``, ``target_*``, ``balance``,
  ``hierarchy``, ``entity``, ``employee``) may be routed to SAP Datasphere
  in the future via the ``infra/datasphere/storage`` layer.
- Current implementation queries SQLAlchemy directly — same pattern as the
  rest of the codebase. When the storage abstraction is integrated into
  reads, the queries here will need to dispatch through the routing layer.
- All SQL used in this module is portable (no PG-specific constructs).
"""

from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import func, inspect, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.config import settings
from app.infra.db.session import get_db
from app.models.core import (
    SCOPE_EXPLORER,
    AnalysisRun,
    AppUser,
    Balance,
    CenterMapping,
    CenterProposal,
    Employee,
    Entity,
    ExplorerDisplayConfig,
    GLAccountClassRange,
    GLAccountSKA1,
    GLAccountSKB1,
    Hierarchy,
    HierarchyLeaf,
    HierarchyNode,
    LegacyCostCenter,
    LegacyProfitCenter,
    TargetCostCenter,
    TargetProfitCenter,
)

# get_current_user already returns None when no/invalid auth — alias for clarity
get_current_user_optional = get_current_user


def _check_sensitive_access(object_type: str, user: AppUser | None) -> None:
    """Enforce auth for sensitive object types when the feature is enabled.

    Called at the top of any endpoint that exposes ``employees`` or
    ``balances``. When the ``explorer_require_auth`` setting is false
    (default), this is a no-op so the existing public-explorer UX
    continues to work in development.
    """
    if not settings.explorer_require_auth:
        return
    if object_type in _SENSITIVE_OBJECT_TYPES and user is None:
        raise HTTPException(
            status_code=401,
            detail=(
                f"Authentication required for object type '{object_type}'. "
                "Sign in as a data_manager or admin to access this data."
            ),
        )
    if object_type in _SENSITIVE_OBJECT_TYPES and user is not None:
        from app.api.deps import _user_roles

        ur = _user_roles(user)
        if not ur & {"data_manager", "admin"}:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Role '{user.role}' is not permitted to access "
                    f"'{object_type}'. Contact an admin if you need access."
                ),
            )


router = APIRouter()

# ── Object type → model mapping ──────────────────────────────────────────

_OBJECT_MODELS: dict[str, Any] = {
    "cost-centers": LegacyCostCenter,
    "profit-centers": LegacyProfitCenter,
    "target-cost-centers": TargetCostCenter,
    "target-profit-centers": TargetProfitCenter,
    "entities": Entity,
    "target-entities": Entity,
    "employees": Employee,
    "gl-accounts-ska1": GLAccountSKA1,
    "gl-accounts-skb1": GLAccountSKB1,
    "gl-accounts-group": GLAccountSKA1,
    "target-gl-accounts-ska1": GLAccountSKA1,
    "target-gl-accounts-skb1": GLAccountSKB1,
    "target-gl-accounts-group": GLAccountSKA1,
    "balances": Balance,
    "hierarchies": Hierarchy,
    "target-hierarchies": Hierarchy,
    "gl-accounts": GLAccountClassRange,
}

# Object types that contain potentially sensitive HR/financial data and
# require an authenticated user when EXPLORER_REQUIRE_AUTH is enabled.
_SENSITIVE_OBJECT_TYPES: frozenset[str] = frozenset({"employees", "balances"})

# Default columns per object type (when no ExplorerDisplayConfig exists)
_DEFAULT_TABLE_COLUMNS: dict[str, list[str]] = {
    "cost-centers": [
        "cctr",
        "txtsh",
        "ccode",
        "coarea",
        "pctr",
        "responsible",
        "cctrcgy",
        "currency",
        "is_active",
    ],
    "profit-centers": [
        "pctr",
        "txtsh",
        "ccode",
        "coarea",
        "responsible",
        "department",
        "currency",
        "is_active",
    ],
    "target-cost-centers": [
        "cctr",
        "txtsh",
        "ccode",
        "coarea",
        "pctr",
        "responsible",
        "cctrcgy",
        "currency",
        "is_active",
        "approved_in_wave",
    ],
    "target-profit-centers": [
        "pctr",
        "txtsh",
        "ccode",
        "coarea",
        "responsible",
        "currency",
        "is_active",
        "approved_in_wave",
    ],
    "entities": [
        "ccode",
        "name",
        "city",
        "country",
        "region",
        "currency",
        "language",
        "is_active",
    ],
    "employees": [
        "gpn",
        "name",
        "vorname",
        "ou_cd",
        "ou_desc",
        "local_cc_cd",
        "local_cc_desc",
        "job_desc",
        "email_adresse",
        "rang_text",
    ],
    "gl-accounts-ska1": [
        "ktopl",
        "saknr",
        "txt20",
        "txt50",
        "xbilk",
        "gvtyp",
        "ktoks",
        "bilkt",
        "func_area",
        "glaccount_type",
    ],
    "gl-accounts-group": [
        "ktopl",
        "saknr",
        "txt20",
        "txt50",
        "xbilk",
        "gvtyp",
        "ktoks",
        "bilkt",
        "func_area",
        "glaccount_type",
    ],
    "gl-accounts-skb1": [
        "bukrs",
        "saknr",
        "stext",
        "waers",
        "mitkz",
        "mwskz",
        "fstag",
        "xopvw",
        "xkres",
        "xintb",
    ],
    "balances": [
        "coarea",
        "cctr",
        "ccode",
        "fiscal_year",
        "period",
        "account",
        "tc_amt",
        "currency_tc",
    ],
    "gl-accounts": [
        "class_code",
        "class_label",
        "from_account",
        "to_account",
        "category",
    ],
}

# Target object types that share the same display config as their legacy counterparts
# (target-cost-centers and target-profit-centers have their own explicit definitions above)
for _prefix, _source in [
    ("target-entities", "entities"),
    ("target-gl-accounts-ska1", "gl-accounts-ska1"),
    ("target-gl-accounts-skb1", "gl-accounts-skb1"),
    ("target-gl-accounts-group", "gl-accounts-group"),
]:
    _DEFAULT_TABLE_COLUMNS[_prefix] = _DEFAULT_TABLE_COLUMNS[_source]

# Search fields per object
_SEARCH_FIELDS: dict[str, list[str]] = {
    "cost-centers": ["cctr", "txtsh", "ccode", "responsible"],
    "profit-centers": ["pctr", "txtsh", "ccode", "responsible"],
    "target-cost-centers": ["cctr", "txtsh", "ccode", "responsible"],
    "target-profit-centers": ["pctr", "txtsh", "ccode", "responsible"],
    "entities": ["ccode", "name", "country"],
    "target-entities": ["ccode", "name", "country"],
    "employees": ["gpn", "name", "vorname", "email_adresse", "ou_cd"],
    "gl-accounts-ska1": ["saknr", "txt20", "txt50", "ktopl", "ktoks"],
    "gl-accounts-skb1": ["saknr", "stext", "bukrs", "waers"],
    "gl-accounts-group": ["saknr", "txt20", "txt50", "ktopl", "ktoks"],
    "target-gl-accounts-ska1": ["saknr", "txt20", "txt50", "ktopl", "ktoks"],
    "target-gl-accounts-skb1": ["saknr", "stext", "bukrs", "waers"],
    "target-gl-accounts-group": ["saknr", "txt20", "txt50", "ktopl", "ktoks"],
    "balances": ["cctr", "ccode", "coarea"],
    "gl-accounts": ["class_code", "class_label"],
}

# Sort key per object
_DEFAULT_SORT: dict[str, str] = {
    "cost-centers": "cctr",
    "profit-centers": "pctr",
    "target-cost-centers": "cctr",
    "target-profit-centers": "pctr",
    "entities": "ccode",
    "target-entities": "ccode",
    "employees": "gpn",
    "gl-accounts-ska1": "saknr",
    "gl-accounts-skb1": "saknr",
    "gl-accounts-group": "saknr",
    "target-gl-accounts-ska1": "saknr",
    "target-gl-accounts-skb1": "saknr",
    "target-gl-accounts-group": "saknr",
    "balances": "cctr",
    "gl-accounts": "class_code",
}


# Human-readable default labels for technical field names
# Sourced from SAP data dictionary (CSKS, CEPC, T001, ZUHL_GRD_GPF, SKA1, SKB1)
_DEFAULT_COLUMN_LABELS: dict[str, str] = {
    # ── Cost Centers (CSKS) ──────────────────────────────────────────────
    "mandt": "Client",
    "cctr": "Cost Center",
    "coarea": "Controlling Area",
    "datbi": "Valid To Date",
    "datab": "Valid-From Date",
    "bkzkp": "Lock Ind. Actual Primary",
    "pkzkp": "Lock Ind. Plan Primary",
    "ccode": "Company Code",
    "gsber": "Business Area",
    "kosar": "CC Category",
    "responsible": "Person Responsible",
    "verak_user": "User Responsible",
    "currency": "Currency Key",
    "kalsm": "Costing Sheet",
    "txjcd": "Tax Jurisdiction",
    "pctr": "Profit Center",
    "werks": "Plant",
    "logsystem": "Logical System",
    "ersda": "Created on",
    "usnam": "Entered By",
    "bkzks": "Lock Ind. Actual Secondary",
    "bkzer": "Lock Ind. Actual Revenue",
    "bkzob": "Lock Ind. Commitment",
    "pkzks": "Lock Ind. Plan Secondary",
    "pkzer": "Lock Ind. Plan Revenue",
    "vmeth": "Allowed Allocation Methods",
    "mgefl": "Record Consumption Qty",
    "department": "Department",
    "ncctr": "Successor Cost Center",
    "kvewe": "Condition Table Usage",
    "kappl": "Application",
    "koszschl": "CO-CCA Overhead Key",
    "land1": "Country Key",
    "anred": "Title",
    "name1": "Name 1",
    "name2": "Name 2",
    "name3": "Name 3",
    "name4": "Name 4",
    "ort01": "City",
    "ort02": "District",
    "stras": "Street and House Number",
    "pfach": "PO Box",
    "pstlz": "Postal Code",
    "pstl2": "P.O. Box Postal Code",
    "regio": "Region",
    "language": "Language Key",
    "telbx": "Telebox Number",
    "telf1": "Telephone 1",
    "telf2": "Telephone 2",
    "telfx": "Fax Number",
    "teltx": "Teletex Number",
    "telx1": "Telex Number",
    "datlt": "Data Comm. Line No.",
    "drnam": "Printer Destination",
    "khinr": "Standard Hierarchy Area",
    "cckey": "Cost Collector Key",
    "kompl": "Completion Flag",
    "stakz": "Statistical Indicator",
    "objnr": "Object Number",
    "funkt": "Function",
    "afunk": "Alternative Function",
    "cpi_templ": "Template: AI Formula Planning",
    "cpd_templ": "Template: AD Formula Planning",
    "func_area": "Functional Area",
    "sci_templ": "Template: AI Allocation",
    "scd_templ": "Template: AD Allocation",
    "ski_templ": "Template: Actual SKF",
    "skd_templ": "Template: Actual SKF/Activity",
    # ── CSKS custom fields (ZZ*) ─────────────────────────────────────────
    "zzcuemncfu": "GCRS Function",
    "zzcueabacc": "ABACUS Head Company Code",
    "zzcuegbcd": "Business Area Code",
    "zzcueubcd": "Business Unit Code",
    "zzcuenkos": "Successor Cost Centre",
    "zzstrpctyp": "Profit Center Type",
    "zzstrkklas": "Cost Centre Class",
    "zzstraagcd": "ABACUS Agency",
    "zzstrgfd": "Business Field",
    "zzstrfst": "Hierarchy Level",
    "zzstrmacve": "GCRS Node",
    "zzstrabukr": "ABACUS Company Code",
    "zzstrugcd": "Business Group Code",
    "zzstrinadt": "CO Inactive Date",
    "zzstrkstyp": "Cost Center Type",
    "zzstrverik": "Responsible International",
    "zzstrcurr2": "2nd Currency",
    "zzstrlccid": "Local Cost Center",
    "zzstrmaloc": "Benchmark Group Code",
    "zzstrtaxcd": "Tax Relevance Code",
    "zzstrgrpid": "GRP-Company ID",
    "zzstrregcode": "Region Code",
    "zzstrtaxarea": "IB-code S-Center",
    "zzstrrepsit": "Reporting Site",
    "zzstrgsm": "(Old) CeMa Code",
    "zzcemapar": "(Old) CeMa Parent",
    "zzledger": "GEAR Ledger ID",
    "zzhdstat": "Headcount Status",
    "zzhdtype": "Headcount Type",
    "zzfmd": "FMD Code",
    "zzfmdcc": "FMD Cost Center",
    "zzfmdnode": "FMD Management Node",
    "zzstate": "State Code",
    "zztax": "Tax Code",
    "zzstrentsa": "Entlastungssatz",
    "zzstrentzu": "Entlastungs-Zuschlagssatz %",
    "xblnr": "Reference Document Number",
    # ── CSKS JV fields ───────────────────────────────────────────────────
    "vname": "Joint Venture",
    "recid": "Recovery Indicator",
    "etype": "Equity Type",
    "jv_otype": "JV Object Type",
    "jv_jibcl": "JIB/JIBE Class",
    "jv_jibsa": "JIB/JIBE Subclass A",
    "ferc_ind": "Regulatory Indicator",
    # ── Profit Centers (CEPC) ────────────────────────────────────────────
    "merkmal": "CO-PA Characteristic",
    "nprctr": "Successor Profit Center",
    "lock_ind": "Lock Indicator",
    "pca_template": "PCA Template",
    "segment": "Segment",
    # ── Cost Center / Profit Center display ──────────────────────────────
    "txtsh": "Short Text",
    "txtmi": "Medium Text",
    "txtmd": "Medium Text",
    "txtlg": "Long Text",
    "cctrcgy": "CC Category",
    "is_active": "Active",
    "valid_from": "Valid From",
    "valid_to": "Valid To",
    # ── Entities (T001) ──────────────────────────────────────────────────
    "name": "Company Name",
    "city": "City",
    "country": "Country Key",
    "region": "Region",
    "fiscal_year_variant": "Fiscal Year Variant",
    "chart_of_accounts": "Chart of Accounts",
    "company": "Company",
    "fm_area": "Financial Management Area",
    "waabw": "Max Exchange Rate Deviation %",
    "kokfi": "Allocation Indicator",
    "adrnr": "Address",
    "stceg": "VAT Registration Number",
    "fikrs": "Financial Management Area",
    "xfmco": "Project Cash Mgmt Active",
    "xfmcb": "Cash Budget Mgmt Active",
    "xfmca": "Funds Mgmt Update Active",
    # ── Employees (ZUHL_GRD_GPF) ─────────────────────────────────────────
    "gpn": "GPN",
    # NOTE: "name" label is "Company Name" (Entity/T001). For employees it
    # is overridden to "Last Name" via _OBJECT_LABEL_OVERRIDES below.
    "vorname": "First Name",
    "sprachenschluess": "Language Key",
    "anredecode": "Title Code",
    "userid": "User ID",
    "eintrittsdatum": "Entry Date",
    "oe_leiter": "Org Unit Head",
    "int_tel_nr_1ap": "Int. Phone 1",
    "ext_tel_nr_1ap": "Ext. Phone 1",
    "kstst": "Cost Center (SAP)",
    "kstst_text": "Cost Center Text",
    "oe_objekt_id": "Org Object ID",
    "oe_code": "Org Code",
    "oe_text": "Org Text",
    "sap_bukrs": "Company Code",
    "sap_bukrs_text": "Company Name",
    "rang_code": "Rank Code",
    "rang_text": "Rank",
    "rang_krz": "Rank Short",
    "ubs_funk": "Function Code",
    "ubs_funk_text": "Function",
    "gpn_vg_ma": "Manager GPN",
    "name_vg_ma": "Manager Name",
    "email_adresse": "Email",
    "division": "Division",
    "business_name": "Business Name",
    "ma_gruppe": "Employee Group",
    "ma_gruppe_text": "Employee Group Text",
    "ma_kreis": "Employee Subgroup",
    "ma_kreis_text": "Employee Subgroup Text",
    "personalbereich": "Personnel Area",
    "personalber_text": "Personnel Area Text",
    "job_categ_code": "Job Category Code",
    "job_categ_descr": "Job Category",
    "costcenter_code": "Cost Center Code",
    "costcenter_descr": "Cost Center Description",
    "bs_name": "Full Name (BS)",
    "bs_first_name": "First Name (BS)",
    "bs_last_name": "Last Name (BS)",
    "bs_firstname": "First Name (Legacy)",
    "bs_lastname": "Last Name (Legacy)",
    "ou_cd": "Org Unit Code",
    "ou_desc": "Org Unit",
    "local_cc_cd": "Local Cost Center",
    "local_cc_desc": "Local CC Description",
    "job_desc": "Job Title",
    "email_address": "Email (Legacy)",
    "emp_status": "Employee Status",
    "gcrs_comp_cd": "GCRS Company Code",
    "gcrs_comp_desc": "GCRS Company",
    "lm_gpn": "Line Manager GPN",
    "lm_bs_firstname": "Line Manager First Name",
    "lm_bs_lastname": "Line Manager Last Name",
    "rank_cd": "Rank Code (Legacy)",
    "rank_desc": "Rank (Legacy)",
    "reg_region": "Region",
    "locn_city_name_1": "City",
    "display_name": "Display Name",
    # ── GL Accounts (SKA1) ───────────────────────────────────────────────
    "ktopl": "Chart of Accounts",
    "saknr": "Account Number",
    "txt20": "Short Text",
    "txt50": "Long Text",
    "xbilk": "Balance Sheet Indicator",
    "sakan": "Account Number (Significant)",
    "bilkt": "Group Account Number",
    "erdat": "Record Created On",
    "ernam": "Created By",
    "gvtyp": "P&L Statement Type",
    "ktoks": "Account Group",
    "mustr": "Sample Account",
    "vbund": "Trading Partner",
    "glaccount_type": "Account Type",
    "glaccount_subtype": "Account Subtype",
    "mcod1": "Search Term",
    # ── GL Accounts (SKB1) ───────────────────────────────────────────────
    "bukrs": "Company Code",
    "stext": "Description",
    "waers": "Currency",
    "mitkz": "Reconciliation Account",
    "mwskz": "Tax Category",
    "fstag": "Field Status Group",
    "xopvw": "Open Item Management",
    "xkres": "Line Item Display",
    "xintb": "Post Automatically",
    "fdgrv": "Planning Group",
    "hbkid": "House Bank",
    "begru": "Authorization Group",
    "busab": "Accounting Clerk",
    # ── Balances ──────────────────────────────────────────────────────────
    "fiscal_year": "Fiscal Year",
    "period": "Period",
    "account": "Account",
    "account_class": "Account Class",
    "tc_amt": "Amount (TC)",
    "currency_tc": "Currency (TC)",
    "gc_amt": "Amount (GC)",
    "currency_gc": "Currency (GC)",
    "gc2_amt": "Amount (GC2)",
    "currency_gc2": "Currency (GC2)",
    "posting_count": "Posting Count",
    # ── GL Account Ranges ─────────────────────────────────────────────────
    "class_code": "Class Code",
    "class_label": "Class Label",
    "from_account": "From Account",
    "to_account": "To Account",
    "category": "Category",
    # ── Hierarchies ───────────────────────────────────────────────────────
    "setname": "Hierarchy Name",
    "setclass": "Set Class",
    "type_label": "Type",
    "label": "Label",
    "parent_set": "Parent",
    "level": "Level",
    # ── Common ────────────────────────────────────────────────────────────
    "id": "ID",
    "scope": "Scope",
    "data_category": "Data Category",
    "created_at": "Created",
    "updated_at": "Updated",
    "source_system": "Source System",
    "refresh_batch": "Refresh Batch",
}

# Per-object-type label overrides (when the same column name has different
# semantics in different tables, e.g. "name" = Company Name for entities
# but Last Name for employees).
_OBJECT_LABEL_OVERRIDES: dict[str, dict[str, str]] = {
    "employees": {"name": "Last Name"},
}


def _get_model_columns(model: Any) -> list[str]:
    """Get all column names from a SQLAlchemy model."""
    mapper = inspect(model)
    return [c.key for c in mapper.column_attrs]


def _get_display_config(db: Session, object_type: str) -> dict:
    """Get display configuration for an object type."""
    cfg = db.execute(
        select(ExplorerDisplayConfig).where(ExplorerDisplayConfig.object_type == object_type)
    ).scalar_one_or_none()
    if cfg:
        return {
            "table_columns": cfg.table_columns or [],
            "detail_columns": cfg.detail_columns or [],
            "column_labels": cfg.column_labels or {},
            "default_sort_column": cfg.default_sort_column,
            "default_sort_dir": cfg.default_sort_dir or "asc",
        }
    return {
        "table_columns": _DEFAULT_TABLE_COLUMNS.get(object_type, []),
        "detail_columns": [],
        "column_labels": {},
        "default_sort_column": _DEFAULT_SORT.get(object_type),
        "default_sort_dir": "asc",
    }


def _row_to_dict(row: Any, columns: list[str]) -> dict:
    """Convert a model instance to a dict with only specified columns."""
    result: dict = {"id": row.id}
    for col in columns:
        val = getattr(row, col, None)
        if val is not None:
            result[col] = val
        else:
            result[col] = None
    return result


def _row_to_full_dict(row: Any) -> dict:
    """Convert a model instance to a dict with all columns."""
    mapper = inspect(type(row))
    result: dict = {}
    for c in mapper.column_attrs:
        val = getattr(row, c.key, None)
        if val is not None:
            result[c.key] = val
        else:
            result[c.key] = None
    return result


# ── Counts / overview ────────────────────────────────────────────────────


@router.get("/counts")
def explore_counts(db: Session = Depends(get_db)) -> dict:
    """Object counts for the explore dashboard — only explorer-scoped data.

    Returns counts per object type, including data_category-aware splits
    for GL accounts (legacy vs target vs group reporting variants).
    """

    def _cnt(model: type, data_category: str | None = None) -> int:
        q = select(func.count(model.id)).where(model.scope == SCOPE_EXPLORER)
        if data_category and hasattr(model, "data_category"):
            q = q.where(model.data_category == data_category)
        return db.execute(q).scalar() or 0

    return {
        "entities": _cnt(Entity, "legacy"),
        "cost_centers": _cnt(LegacyCostCenter),
        "profit_centers": _cnt(LegacyProfitCenter),
        "balances": _cnt(Balance),
        "hierarchies": _cnt(Hierarchy, "legacy"),
        "employees": _cnt(Employee),
        "gl_accounts_ska1": _cnt(GLAccountSKA1, "legacy"),
        "gl_accounts_skb1": _cnt(GLAccountSKB1, "legacy"),
        "gl_accounts_group": _cnt(GLAccountSKA1, "gr_legacy"),
        "gl_ranges": db.execute(select(func.count(GLAccountClassRange.id))).scalar() or 0,
        "target_cost_centers": _cnt(TargetCostCenter),
        "target_profit_centers": _cnt(TargetProfitCenter),
        "target_entities": _cnt(Entity, "target"),
        "target_gl_accounts_ska1": _cnt(GLAccountSKA1, "target"),
        "target_gl_accounts_skb1": _cnt(GLAccountSKB1, "target"),
        "target_gl_accounts_group": _cnt(GLAccountSKA1, "gr_target"),
        "target_hierarchies": _cnt(Hierarchy, "target"),
        "center_mappings": db.execute(
            select(func.count(CenterMapping.id)).where(CenterMapping.scope == SCOPE_EXPLORER)
        ).scalar()
        or 0,
        "proposals": db.execute(select(func.count(CenterProposal.id))).scalar() or 0,
    }


# ── Display config (public) ─────────────────────────────────────────────


@router.get("/display-config/{object_type}")
def get_display_config(object_type: str, db: Session = Depends(get_db)) -> dict:
    """Public: get display configuration for an object type."""
    model = _OBJECT_MODELS.get(object_type)
    all_columns = _get_model_columns(model) if model else []
    config = _get_display_config(db, object_type)

    # Build full label mapping: defaults + per-type overrides + custom DB overrides
    all_labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in all_columns}
    all_labels.update(_OBJECT_LABEL_OVERRIDES.get(object_type, {}))
    all_labels.update(config.get("column_labels", {}))

    return {
        "object_type": object_type,
        "all_columns": all_columns,
        "default_labels": dict(_DEFAULT_COLUMN_LABELS),
        **config,
        "column_labels": all_labels,
    }


# ── Legacy: Balances (aggregated) — must be before generic {object_type} ──


@router.get("/legacy/balances-agg")
def explore_balances_agg(
    db: Session = Depends(get_db),
    coarea: str | None = None,
    cctr: str | None = None,
    max_rows: int = Query(50000, ge=1, le=200000),
    current_user: AppUser | None = Depends(get_current_user_optional),
) -> dict:
    _check_sensitive_access("balances", current_user)
    query = (
        select(
            Balance.coarea,
            Balance.cctr,
            Balance.fiscal_year,
            Balance.period,
            func.coalesce(func.sum(Balance.tc_amt), 0).label("amt"),
            func.coalesce(func.sum(Balance.posting_count), 0).label("post"),
            func.max(Balance.currency_tc).label("currency"),
        )
        .where(Balance.scope == SCOPE_EXPLORER)
        .group_by(Balance.coarea, Balance.cctr, Balance.fiscal_year, Balance.period)
        .order_by(Balance.coarea, Balance.cctr, Balance.fiscal_year, Balance.period)
    )
    if coarea:
        query = query.where(Balance.coarea == coarea)
    if cctr:
        query = query.where(Balance.cctr == cctr)
    rows = db.execute(query.limit(max_rows)).all()
    items: dict[str, list[dict]] = {}
    for ca, cc, fy, per, amt, post, curr in rows:
        key = f"{ca}:{cc}"
        items.setdefault(key, []).append(
            {
                "fiscal_year": fy,
                "period": per,
                "amount": float(amt),
                "postings": int(post),
                "currency": curr or "",
            }
        )
    return {"total_keys": len(items), "balances": items}


# ── Generic object data endpoint ─────────────────────────────────────────


@router.get("/legacy/gl-groups/{object_type}")
def explore_gl_groups(
    object_type: str,
    db: Session = Depends(get_db),
    group_type: str = Query("a", alias="type", description="a = first char, b = first 5 chars"),
    data_category: str | None = None,
    current_user: AppUser | None = Depends(get_current_user_optional),
) -> dict:
    """Return GL account prefix groups with counts for Explorer."""
    _check_sensitive_access(object_type, current_user)
    model = _OBJECT_MODELS.get(object_type)
    if not model or not hasattr(model, "saknr"):
        return {"type": group_type, "groups": []}

    prefix_len = 1 if group_type == "a" else 5
    prefix_expr = func.left(model.saknr, prefix_len)
    query = (
        select(prefix_expr.label("prefix"), func.count().label("cnt"))
        .group_by(prefix_expr)
        .order_by(prefix_expr)
    )
    if hasattr(model, "scope"):
        query = query.where(model.scope == SCOPE_EXPLORER)
    _category_overrides = {
        "gl-accounts-group": {"legacy": "legacy_gr", "target": "target_gr"},
        "target-gl-accounts-group": {"legacy": "legacy_gr", "target": "target_gr"},
    }
    effective_category = data_category
    if object_type in _category_overrides and data_category:
        effective_category = _category_overrides[object_type].get(data_category, data_category)
    if effective_category and hasattr(model, "data_category"):
        query = query.where(model.data_category == effective_category)

    rows = db.execute(query).all()
    return {"type": group_type, "groups": [{"key": r.prefix, "count": r.cnt} for r in rows]}


@router.get("/legacy/{object_type}")
def explore_object(
    object_type: str,
    db: Session = Depends(get_db),
    search: str | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=10000),
    sort: str | None = None,
    sort_dir: str | None = None,
    data_category: str | None = None,
    gl_prefix: str | None = None,
    current_user: AppUser | None = Depends(get_current_user_optional),
) -> dict:
    """Generic endpoint: fetch data for any object type with dynamic columns."""
    _check_sensitive_access(object_type, current_user)
    # Special case: hierarchies have their own endpoint
    if object_type in ("hierarchies", "target-hierarchies"):
        cat = data_category or ("target" if object_type == "target-hierarchies" else None)
        return _explore_hierarchies(db, search, data_category=cat)

    model = _OBJECT_MODELS.get(object_type)
    if not model:
        return {"error": f"Unknown object type: {object_type}", "items": [], "total": 0}

    config = _get_display_config(db, object_type)
    table_cols = config["table_columns"]

    # Build query — only explorer-scoped data
    query = select(model)
    count_q = select(func.count(model.id))
    if hasattr(model, "scope"):
        query = query.where(model.scope == SCOPE_EXPLORER)
        count_q = count_q.where(model.scope == SCOPE_EXPLORER)
    # GR accounts use a suffixed data_category to distinguish from General Ledger
    _category_overrides = {
        "gl-accounts-group": {"legacy": "legacy_gr", "target": "target_gr"},
        "target-gl-accounts-group": {"legacy": "legacy_gr", "target": "target_gr"},
    }
    effective_category = data_category
    if object_type in _category_overrides and data_category:
        effective_category = _category_overrides[object_type].get(data_category, data_category)
    if effective_category and hasattr(model, "data_category"):
        query = query.where(model.data_category == effective_category)
        count_q = count_q.where(model.data_category == effective_category)

    # GL prefix filter (for hierarchical view server-side grouping)
    if gl_prefix and hasattr(model, "saknr"):
        query = query.where(model.saknr.like(f"{gl_prefix}%"))
        count_q = count_q.where(model.saknr.like(f"{gl_prefix}%"))

    # Search
    if search:
        pat = f"%{search}%"
        search_fields = _SEARCH_FIELDS.get(object_type, [])
        if search_fields:
            conditions = []
            for field_name in search_fields:
                col = getattr(model, field_name, None)
                if col is not None:
                    conditions.append(col.ilike(pat))
            if conditions:
                from sqlalchemy import or_

                flt = or_(*conditions)
                query = query.where(flt)
                count_q = count_q.where(flt)

    # Sort (validate against actual column names to prevent 500 on non-column attrs)
    sort_col_name = sort or config.get("default_sort_column") or _DEFAULT_SORT.get(object_type)
    if sort_col_name:
        from sqlalchemy import inspect as sa_inspect

        valid_columns = {c.key for c in sa_inspect(model).column_attrs}
        if sort_col_name in valid_columns:
            sort_attr = getattr(model, sort_col_name)
            direction = sort_dir or config.get("default_sort_dir", "asc")
            if direction == "desc":
                query = query.order_by(sort_attr.desc().nulls_last())
            else:
                query = query.order_by(sort_attr.asc().nulls_last())

    total = db.execute(count_q).scalar() or 0
    rows = db.execute(query.offset((page - 1) * size).limit(size)).scalars().all()

    # Merge default labels with custom overrides
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in table_cols}
    labels.update({k: v for k, v in config.get("column_labels", {}).items() if k in table_cols})

    return {
        "total": total,
        "page": page,
        "size": size,
        "columns": table_cols,
        "column_labels": labels,
        "items": [_row_to_dict(r, table_cols) for r in rows],
    }


# ── Detail view ──────────────────────────────────────────────────────────


@router.get("/legacy/{object_type}/{item_id}")
def explore_object_detail(
    object_type: str,
    item_id: int,
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user_optional),
) -> dict:
    """Get full detail for a single item."""
    _check_sensitive_access(object_type, current_user)
    model = _OBJECT_MODELS.get(object_type)
    if not model:
        return {"error": f"Unknown object type: {object_type}"}

    row = db.get(model, item_id)
    if not row:
        return {"error": "Not found"}
    if hasattr(row, "scope") and row.scope != SCOPE_EXPLORER:
        return {"error": "Not found"}

    config = _get_display_config(db, object_type)
    detail_cols = config["detail_columns"]

    data = _row_to_dict(row, detail_cols) if detail_cols else _row_to_full_dict(row)

    # Merge default labels with custom overrides
    cols_for_labels = detail_cols if detail_cols else list(data.keys())
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in cols_for_labels}
    custom = config.get("column_labels", {})
    labels.update({k: v for k, v in custom.items() if k in cols_for_labels})

    return {"item": data, "detail_columns": detail_cols, "column_labels": labels}


# ── Hierarchies (special) ───────────────────────────────────────────────


def _explore_hierarchies(
    db: Session,
    search: str | None = None,
    data_category: str | None = None,
) -> dict:
    """Hierarchies endpoint with node/leaf data."""
    cls_labels = {"0101": "Cost Center", "0104": "Profit Center", "0106": "Entity"}
    query = select(Hierarchy).where(
        Hierarchy.scope == SCOPE_EXPLORER, Hierarchy.is_active.is_(True)
    )
    if data_category:
        query = query.where(Hierarchy.data_category == data_category)
    if search:
        pat = f"%{search}%"
        from sqlalchemy import or_

        query = query.where(
            or_(
                Hierarchy.setname.ilike(pat),
                Hierarchy.label.ilike(pat),
                Hierarchy.description.ilike(pat),
                Hierarchy.coarea.ilike(pat),
            )
        )
    hiers = db.execute(query.order_by(Hierarchy.setclass, Hierarchy.setname)).scalars().all()
    result = []
    for h in hiers:
        nodes = (
            db.execute(select(HierarchyNode).where(HierarchyNode.hierarchy_id == h.id))
            .scalars()
            .all()
        )
        leaves = (
            db.execute(select(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == h.id))
            .scalars()
            .all()
        )
        base = f"{cls_labels.get(h.setclass, h.setclass)}: {h.setname}"
        if h.description:
            base += f" — {h.description}"
        result.append(
            {
                "id": h.id,
                "setname": h.setname,
                "setclass": h.setclass,
                "type_label": cls_labels.get(h.setclass, h.setclass),
                "label": h.label or base,
                "coarea": h.coarea,
                "nodes": [
                    {"parent": n.parent_setname, "child": n.child_setname, "seq": n.seq}
                    for n in nodes
                ],
                "leaves": [
                    {"setname": lf.setname, "value": lf.value, "seq": lf.seq} for lf in leaves
                ],
            }
        )
    cols = ["setname", "setclass", "type_label", "label", "coarea"]
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in cols}
    return {
        "total": len(result),
        "items": result,
        "hierarchies": result,
        "columns": cols,
        "column_labels": labels,
    }


# ── Export (CSV/Excel) ───────────────────────────────────────────────────


@router.get("/export/{object_type}")
def export_object(
    object_type: str,
    db: Session = Depends(get_db),
    export_format: str = Query("csv", alias="format"),
    search: str | None = None,
    current_user: AppUser | None = Depends(get_current_user_optional),
) -> StreamingResponse:
    """Export object data as CSV or Excel."""
    _check_sensitive_access(object_type, current_user)
    model = _OBJECT_MODELS.get(object_type)
    if not model:
        return StreamingResponse(
            io.BytesIO(b"Unknown object type"),
            media_type="text/plain",
            status_code=400,
        )

    config = _get_display_config(db, object_type)
    table_cols = config["table_columns"]
    if not table_cols:
        table_cols = _get_model_columns(model)
        # Remove internal columns
        for skip in ("id", "created_at", "updated_at"):
            if skip in table_cols:
                table_cols.remove(skip)

    # Build query — only explorer-scoped data
    query = select(model)
    if hasattr(model, "scope"):
        query = query.where(model.scope == SCOPE_EXPLORER)
    if search:
        pat = f"%{search}%"
        search_fields = _SEARCH_FIELDS.get(object_type, [])
        if search_fields:
            conditions = []
            for field_name in search_fields:
                col = getattr(model, field_name, None)
                if col is not None:
                    conditions.append(col.ilike(pat))
            if conditions:
                from sqlalchemy import or_

                query = query.where(or_(*conditions))

    sort_col_name = _DEFAULT_SORT.get(object_type)
    if sort_col_name:
        sort_attr = getattr(model, sort_col_name, None)
        if sort_attr is not None:
            query = query.order_by(sort_attr.asc().nulls_last())

    rows = db.execute(query.limit(50000)).scalars().all()

    # Build display labels for export headers
    labels = {c: _DEFAULT_COLUMN_LABELS.get(c, c) for c in table_cols}
    labels.update({k: v for k, v in config.get("column_labels", {}).items() if k in table_cols})
    header_row = [labels.get(c, c) for c in table_cols]

    if export_format == "excel":
        try:
            import openpyxl

            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = object_type
            ws.append(header_row)
            for row in rows:
                ws.append(
                    ["" if (v := getattr(row, c, None)) is None else str(v) for c in table_cols]
                )
            buf = io.BytesIO()
            wb.save(buf)
            buf.seek(0)
            return StreamingResponse(
                buf,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={object_type}.xlsx"},
            )
        except ImportError:
            pass  # Fall back to CSV

    # CSV export
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header_row)
    for row in rows:
        writer.writerow(
            ["" if (v := getattr(row, c, None)) is None else str(v) for c in table_cols]
        )
    content = buf.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={object_type}.csv"},
    )


# ── ampliFi: Mapping (Legacy → Target) ──────────────────────────────────


@router.get("/amplifi/mapping")
def explore_mapping(
    db: Session = Depends(get_db),
    run_id: int | None = None,
    page: int = Query(1, ge=1),
    size: int = Query(500, ge=1, le=10000),
    search: str | None = None,
) -> dict:
    """Latest analysis results: legacy CC → proposed target mapping."""
    if run_id is not None:
        run = db.execute(select(AnalysisRun).where(AnalysisRun.id == run_id)).scalars().first()
    else:
        run = (
            db.execute(
                select(AnalysisRun)
                .where(AnalysisRun.status == "completed")
                .order_by(AnalysisRun.finished_at.desc())
            )
            .scalars()
            .first()
        )
    if not run:
        return {"run_id": None, "items": [], "summary": {}}

    proposals = (
        db.execute(select(CenterProposal).where(CenterProposal.run_id == run.id)).scalars().all()
    )
    cc_ids = [p.legacy_cc_id for p in proposals]
    ccs = (
        {
            c.id: c
            for c in db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids)))
            .scalars()
            .all()
        }
        if cc_ids
        else {}
    )

    items = []
    summary: dict[str, int] = {}
    for p in proposals:
        cc = ccs.get(p.legacy_cc_id)
        outcome = p.override_outcome or p.cleansing_outcome
        summary[outcome] = summary.get(outcome, 0) + 1
        items.append(
            {
                "id": p.id,
                "legacy_cctr": cc.cctr if cc else None,
                "legacy_txtsh": cc.txtsh if cc else None,
                "legacy_ccode": cc.ccode if cc else None,
                "legacy_coarea": cc.coarea if cc else None,
                "legacy_pctr": cc.pctr if cc else None,
                "outcome": outcome,
                "target_object": p.target_object,
                "merge_into_cctr": p.merge_into_cctr,
                "confidence": str(p.confidence) if p.confidence else None,
                "rule_path": p.rule_path,
            }
        )

    # Apply search filter
    if search:
        pat = search.lower()
        items = [
            i
            for i in items
            if pat in (i.get("legacy_cctr") or "").lower()
            or pat in (i.get("legacy_txtsh") or "").lower()
            or pat in (i.get("legacy_ccode") or "").lower()
            or pat in (i.get("outcome") or "").lower()
        ]

    total = len(items)
    # Paginate
    start = (page - 1) * size
    paged_items = items[start : start + size]

    return {
        "run_id": run.id,
        "run_label": f"Run #{run.id}" + (f" (Wave {run.wave_id})" if run.wave_id else " (Global)"),
        "run_status": run.status,
        "total": total,
        "page": page,
        "size": size,
        "summary": summary,
        "items": paged_items,
    }


# ── ampliFi: Available Runs ─────────────────────────────────────────────


@router.get("/amplifi/runs")
def explore_runs(db: Session = Depends(get_db)) -> dict:
    runs = (
        db.execute(
            select(AnalysisRun)
            .where(AnalysisRun.status == "completed")
            .order_by(AnalysisRun.finished_at.desc())
        )
        .scalars()
        .all()
    )
    return {
        "total": len(runs),
        "runs": [
            {
                "id": r.id,
                "label": f"Run #{r.id}" + (f" (Wave {r.wave_id})" if r.wave_id else " (Global)"),
                "status": r.status,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            }
            for r in runs
        ],
    }


# ── Data Source Configuration ────────────────────────────────────────────


@router.get("/data-sources")
def list_data_sources(db: Session = Depends(get_db)) -> dict:
    return {
        "sources": [
            {
                "object": "cost_centers",
                "label": "Cost Centers",
                "source": "Local DB",
                "count": db.execute(select(func.count(LegacyCostCenter.id))).scalar() or 0,
            },
            {
                "object": "profit_centers",
                "label": "Profit Centers",
                "source": "Local DB",
                "count": db.execute(select(func.count(LegacyProfitCenter.id))).scalar() or 0,
            },
            {
                "object": "entities",
                "label": "Entities",
                "source": "Local DB",
                "count": db.execute(select(func.count(Entity.id))).scalar() or 0,
            },
            {
                "object": "balances",
                "label": "Balances",
                "source": "Local DB",
                "count": db.execute(select(func.count(Balance.id))).scalar() or 0,
            },
            {
                "object": "hierarchies",
                "label": "Hierarchies",
                "source": "Local DB",
                "count": db.execute(select(func.count(Hierarchy.id))).scalar() or 0,
            },
            {
                "object": "employees",
                "label": "Employees",
                "source": "Local DB",
                "count": db.execute(select(func.count(Employee.id))).scalar() or 0,
            },
            {
                "object": "gl_accounts_ska1",
                "label": "GL Accounts (Chart of Accounts)",
                "source": "Local DB",
                "count": db.execute(select(func.count(GLAccountSKA1.id))).scalar() or 0,
            },
            {
                "object": "gl_accounts_skb1",
                "label": "GL Accounts (Company Code)",
                "source": "Local DB",
                "count": db.execute(select(func.count(GLAccountSKB1.id))).scalar() or 0,
            },
            {
                "object": "gl_ranges",
                "label": "GL Account Ranges",
                "source": "Local DB",
                "count": db.execute(select(func.count(GLAccountClassRange.id))).scalar() or 0,
            },
        ]
    }


@router.get("/source-config")
def explore_source_config(db: Session = Depends(get_db)) -> dict:
    """Public endpoint: returns enabled explorer source configs per area."""
    from app.models.core import ExplorerSourceConfig

    stmt = (
        select(ExplorerSourceConfig)
        .where(ExplorerSourceConfig.enabled.is_(True))
        .order_by(ExplorerSourceConfig.area, ExplorerSourceConfig.display_order)
    )
    rows = db.execute(stmt).scalars().all()
    legacy = []
    amplifi = []
    for r in rows:
        item = {
            "object_type": r.object_type,
            "label": r.label,
            "source_system": r.source_system,
            "protocol": r.protocol,
            "mode": r.mode,
        }
        if r.area == "amplifi":
            amplifi.append(item)
        else:
            legacy.append(item)
    return {"legacy": legacy, "amplifi": amplifi}


# ── Compare view (Use Case C: side-by-side legacy vs target) ─────────────


@router.get("/compare")
def compare_objects(
    db: Session = Depends(get_db),
    legacy_cctr: str | None = None,
    target_cctr: str | None = None,
    coarea: str | None = None,
    current_user: AppUser | None = Depends(get_current_user_optional),
) -> dict:
    """Side-by-side compare of a legacy CC and the target object(s) it mapped to.

    One of ``legacy_cctr`` or ``target_cctr`` must be provided. The endpoint
    returns the matching legacy row, all proposals across waves that
    referenced it, and the target CC + target PC that the proposals
    eventually produced.

    Datasphere note: queries the legacy_*, center_proposal, and target_*
    domains directly via SQLAlchemy — same pattern as the rest of the
    explorer. When these domains are routed to Datasphere via the
    ``infra/datasphere/storage`` layer, this endpoint will need to dispatch
    through the routing layer.
    """
    if not legacy_cctr and not target_cctr:
        raise HTTPException(
            status_code=400,
            detail="Provide either legacy_cctr or target_cctr",
        )

    # Resolve legacy CC. Either: looked up directly, OR derived from target
    # via the source_proposal_id chain.
    legacy: LegacyCostCenter | None = None
    target_cc: TargetCostCenter | None = None
    target_pc: TargetProfitCenter | None = None

    if legacy_cctr:
        q = select(LegacyCostCenter).where(LegacyCostCenter.cctr == legacy_cctr)
        if coarea:
            q = q.where(LegacyCostCenter.coarea == coarea)
        legacy = db.execute(q.limit(1)).scalar_one_or_none()
    elif target_cctr:
        q = select(TargetCostCenter).where(TargetCostCenter.cctr == target_cctr)
        if coarea:
            q = q.where(TargetCostCenter.coarea == coarea)
        target_cc = db.execute(q.limit(1)).scalar_one_or_none()
        if target_cc and target_cc.source_proposal_id:
            prop = db.get(CenterProposal, target_cc.source_proposal_id)
            if prop:
                legacy = db.get(LegacyCostCenter, prop.legacy_cc_id)

    if not legacy and not target_cc:
        raise HTTPException(status_code=404, detail="No matching legacy or target object")

    # Fetch all proposals for this legacy CC across runs (V1 + V2)
    proposals: list[dict] = []
    if legacy:
        prop_rows = db.execute(
            select(CenterProposal, AnalysisRun)
            .join(AnalysisRun, CenterProposal.run_id == AnalysisRun.id)
            .where(CenterProposal.legacy_cc_id == legacy.id)
            .order_by(AnalysisRun.created_at.desc())
        ).all()
        for prop, run in prop_rows:
            attrs = prop.attrs or {}
            proposals.append(
                {
                    "proposal_id": prop.id,
                    "run_id": run.id,
                    "wave_id": run.wave_id,
                    "engine_version": run.engine_version,
                    "engine_label": (
                        "V2" if str(run.engine_version or "").startswith("v2") else "V1"
                    ),
                    "mode": run.mode,
                    "outcome": prop.cleansing_outcome,
                    "target_object": prop.target_object,
                    "override_outcome": prop.override_outcome,
                    "override_target": prop.override_target,
                    "v2": {
                        "cc_id": attrs.get("cc_id"),
                        "pc_id": attrs.get("pc_id"),
                        "group_key": attrs.get("group_key"),
                        "approach": attrs.get("approach"),
                    }
                    if str(run.engine_version or "").startswith("v2")
                    else None,
                }
            )

        # Resolve target objects from proposals (any approved/locked)
        if not target_cc:
            target_cc = db.execute(
                select(TargetCostCenter).where(
                    TargetCostCenter.coarea == legacy.coarea,
                    TargetCostCenter.cctr == legacy.cctr,
                )
            ).scalar_one_or_none()
        # If V2 produced a different cc_id, look that up too
        if not target_cc:
            for p in proposals:
                cc_from_attrs = (p.get("v2") or {}).get("cc_id")
                if cc_from_attrs:
                    target_cc = db.execute(
                        select(TargetCostCenter).where(
                            TargetCostCenter.cctr == cc_from_attrs,
                            TargetCostCenter.coarea == legacy.coarea,
                        )
                    ).scalar_one_or_none()
                    if target_cc:
                        break

    if target_cc:
        # Resolve the PC the target CC points to
        target_pc = db.execute(
            select(TargetProfitCenter).where(
                TargetProfitCenter.coarea == target_cc.coarea,
                TargetProfitCenter.pctr == target_cc.pctr,
            )
        ).scalar_one_or_none()

    def _serialize(obj: Any | None) -> dict | None:
        if obj is None:
            return None
        return {c.key: getattr(obj, c.key) for c in inspect(obj.__class__).column_attrs}

    return {
        "legacy_cc": _serialize(legacy),
        "target_cc": _serialize(target_cc),
        "target_pc": _serialize(target_pc),
        "proposals": proposals,
        "summary": {
            "has_legacy": legacy is not None,
            "has_target_cc": target_cc is not None,
            "has_target_pc": target_pc is not None,
            "proposal_count": len(proposals),
            "engines_used": sorted({p["engine_label"] for p in proposals}),
        },
    }


# ── Transfer: Cleanup → Explorer ─────────────────────────────────────────


@router.post("/transfer-to-explorer")
def transfer_to_explorer(
    db: Session = Depends(get_db),
    object_types: list[str] | None = Query(None),
    current_user: AppUser | None = Depends(get_current_user_optional),
) -> dict:
    """Copy data from cleanup scope into explorer scope.

    If ``object_types`` is provided, only those types are transferred.
    Otherwise all supported types are transferred.

    Existing explorer-scope data for each type is deleted first (full replace).
    """
    if current_user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    from app.api.deps import _user_roles

    user_roles = _user_roles(current_user)
    if not user_roles & {"admin", "data_manager"}:
        raise HTTPException(status_code=403, detail="Insufficient permissions")

    supported = [
        "cost-centers",
        "profit-centers",
        "entities",
        "employees",
        "balances",
        "gl-accounts",
        "gl-accounts-skb1",
        "hierarchies",
        "target-cost-centers",
        "target-profit-centers",
        "center-mappings",
    ]
    types_to_transfer = object_types or supported
    types_to_transfer = [t for t in types_to_transfer if t in supported]

    from app.models.core import SCOPE_CLEANUP, SCOPE_EXPLORER  # noqa: F811

    model_map: dict[str, Any] = {
        "cost-centers": LegacyCostCenter,
        "profit-centers": LegacyProfitCenter,
        "entities": Entity,
        "employees": Employee,
        "balances": Balance,
        "gl-accounts": GLAccountSKA1,
        "gl-accounts-skb1": GLAccountSKB1,
        "target-cost-centers": TargetCostCenter,
        "target-profit-centers": TargetProfitCenter,
        "center-mappings": CenterMapping,
    }

    results: dict[str, dict] = {}
    for obj_type in types_to_transfer:
        if obj_type == "hierarchies":
            results[obj_type] = _transfer_hierarchies(db, SCOPE_CLEANUP, SCOPE_EXPLORER)
            continue

        model = model_map.get(obj_type)
        if not model or not hasattr(model, "scope"):
            results[obj_type] = {"status": "skipped", "reason": "no scope field"}
            continue

        # Delete existing explorer data
        deleted = db.execute(model.__table__.delete().where(model.scope == SCOPE_EXPLORER)).rowcount

        # Copy cleanup data with explorer scope
        cleanup_rows = db.execute(select(model).where(model.scope == SCOPE_CLEANUP)).scalars().all()

        inserted = 0
        for row in cleanup_rows:
            row_data = {
                c.key: getattr(row, c.key) for c in inspect(model).column_attrs if c.key != "id"
            }
            row_data["scope"] = SCOPE_EXPLORER
            new_obj = model(**row_data)
            db.add(new_obj)
            inserted += 1
            if inserted % 1000 == 0:
                db.flush()

        results[obj_type] = {"deleted": deleted, "inserted": inserted}

    db.commit()
    return {"status": "ok", "results": results}


def _transfer_hierarchies(db: Session, src_scope: str, dst_scope: str) -> dict:
    """Transfer hierarchies including nodes and leaves."""
    # Delete existing explorer hierarchies (leaves, nodes, then hierarchy records)
    explorer_hiers = (
        db.execute(select(Hierarchy).where(Hierarchy.scope == dst_scope)).scalars().all()
    )
    for h in explorer_hiers:
        db.execute(HierarchyLeaf.__table__.delete().where(HierarchyLeaf.hierarchy_id == h.id))
        db.execute(HierarchyNode.__table__.delete().where(HierarchyNode.hierarchy_id == h.id))
    deleted = db.execute(Hierarchy.__table__.delete().where(Hierarchy.scope == dst_scope)).rowcount

    # Copy cleanup hierarchies
    src_hiers = db.execute(select(Hierarchy).where(Hierarchy.scope == src_scope)).scalars().all()
    inserted = 0
    for h in src_hiers:
        old_id = h.id
        h_data = {
            c.key: getattr(h, c.key) for c in inspect(Hierarchy).column_attrs if c.key != "id"
        }
        h_data["scope"] = dst_scope
        new_h = Hierarchy(**h_data)
        db.add(new_h)
        db.flush()

        # Copy nodes
        nodes = (
            db.execute(select(HierarchyNode).where(HierarchyNode.hierarchy_id == old_id))
            .scalars()
            .all()
        )
        for n in nodes:
            n_data = {
                c.key: getattr(n, c.key)
                for c in inspect(HierarchyNode).column_attrs
                if c.key != "id"
            }
            n_data["hierarchy_id"] = new_h.id
            db.add(HierarchyNode(**n_data))

        # Copy leaves
        leaves = (
            db.execute(select(HierarchyLeaf).where(HierarchyLeaf.hierarchy_id == old_id))
            .scalars()
            .all()
        )
        for lf in leaves:
            lf_data = {
                c.key: getattr(lf, c.key)
                for c in inspect(HierarchyLeaf).column_attrs
                if c.key != "id"
            }
            lf_data["hierarchy_id"] = new_h.id
            db.add(HierarchyLeaf(**lf_data))

        inserted += 1
        if inserted % 10 == 0:
            db.flush()

    return {"deleted": deleted, "inserted": inserted}
