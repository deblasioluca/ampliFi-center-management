"""SAP data extraction service — OData, ADT, RFC/SOAP.

Pulls cost centers, profit centers, hierarchies, balances, GL accounts,
employees, and entities from SAP via multiple protocols and creates
upload batches for the standard upload processing pipeline.
"""

from __future__ import annotations

import csv
import io
import re
import tempfile
from pathlib import Path

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.infra.sap.client import (
    call_bapi,
    call_rfc_read_table,
    fetch_adt_table,
    fetch_odata,
)
from app.models.core import SAPConnection, UploadBatch

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# OData entity set mappings
# ---------------------------------------------------------------------------

ENTITY_SETS = {
    "cost_center": "API_COSTCENTER_SRV/A_CostCenter",
    "cost_centers": "API_COSTCENTER_SRV/A_CostCenter",
    "profit_center": "API_PROFITCENTER_SRV/A_ProfitCenter",
    "profit_centers": "API_PROFITCENTER_SRV/A_ProfitCenter",
    "entity": "API_COMPANYCODE_SRV/A_CompanyCode",
    "entities": "API_COMPANYCODE_SRV/A_CompanyCode",
    "hierarchy": "API_COSTCENTER_SRV/A_CostCenterHierarchy",
    "hierarchies": "API_COSTCENTER_SRV/A_CostCenterHierarchy",
    "balance": "YY1_GLACCOUNTBALANCE/A_GLAccountBalance",
    "balances": "YY1_GLACCOUNTBALANCE/A_GLAccountBalance",
    "gl_account": "API_GLACCOUNTINCHARTOFACCOUNTS_SRV/A_GLAccountInChartOfAccounts",
    "gl_accounts": "API_GLACCOUNTINCHARTOFACCOUNTS_SRV/A_GLAccountInChartOfAccounts",
    "employee": "API_BUSINESS_PARTNER/A_BusinessPartner",
    "employees": "API_BUSINESS_PARTNER/A_BusinessPartner",
}

CANONICAL_KINDS = [
    "cost_center",
    "profit_center",
    "hierarchy",
    "balance",
    "gl_account",
    "employee",
    "entity",
]

# ---------------------------------------------------------------------------
# SAP table names per object type (for ADT / RFC_READ_TABLE)
# ---------------------------------------------------------------------------

SAP_TABLES = {
    "cost_center": ["CSKS", "CSKT"],
    "profit_center": ["CEPC", "CEPCT"],
    "hierarchy": ["SETNODE", "SETHEADER"],
    "balance": ["ACDOCA", "FAGLFLEXT"],
    "gl_account": ["SKA1"],
    "employee": ["ZUHL_GRD_GPF"],
    "entity": ["T001"],
}

# ---------------------------------------------------------------------------
# Field mappings — OData response fields to our CSV format
# ---------------------------------------------------------------------------

FIELD_MAP_ODATA = {
    "cost_center": {
        "ControllingArea": "COAREA",
        "CostCenter": "CCTR",
        "CompanyCode": "CCODE",
        "CostCenterShortName": "TXTSH",
        "CostCenterLongName": "TXTMI",
        "PersonResponsible": "RESPONSIBLE",
        "ValidityStartDate": "START_DATE",
        "ValidityEndDate": "END_DATE",
    },
    "profit_center": {
        "ControllingArea": "COAREA",
        "ProfitCenter": "PCTR",
        "CompanyCode": "CCODE",
        "ProfitCenterShortName": "TXTSH",
        "ProfitCenterLongName": "TXTMI",
    },
    "hierarchy": {
        "SetClass": "SET_CLASS",
        "SetType": "SET_TYPE",
        "SetName": "SET_NAME",
        "Subset": "SUBSET",
        "SubsetDescription": "SUBSET_DESC",
        "HierarchyNode": "NODE_ID",
        "ParentNode": "PARENT_NODE",
        "NodeType": "NODE_TYPE",
        "ValidityStartDate": "START_DATE",
        "ValidityEndDate": "END_DATE",
    },
    "balance": {
        "CompanyCode": "CCODE",
        "GLAccount": "GL_ACCOUNT",
        "FiscalYear": "FISCAL_YEAR",
        "FiscalPeriod": "PERIOD",
        "ControllingArea": "COAREA",
        "CostCenter": "CCTR",
        "ProfitCenter": "PCTR",
        "Ledger": "LEDGER",
        "AmountInCompanyCodeCurrency": "AMOUNT_CC",
        "CompanyCodeCurrency": "CURRENCY_CC",
        "AmountInGlobalCurrency": "AMOUNT_GC",
        "GlobalCurrency": "CURRENCY_GC",
        "DebitAmountInCompanyCodeCurrency": "DEBIT_CC",
        "CreditAmountInCompanyCodeCurrency": "CREDIT_CC",
    },
    "gl_account": {
        "ChartOfAccounts": "CHART_OF_ACCOUNTS",
        "GLAccount": "GL_ACCOUNT",
        "GLAccountName": "GL_NAME",
        "GLAccountLongName": "GL_LONG_NAME",
        "GLAccountGroup": "GL_GROUP",
        "CompanyCode": "CCODE",
        "AccountType": "ACCOUNT_TYPE",
        "IsBalanceSheetAccount": "IS_BS_ACCOUNT",
        "IsProfitAndLossAccount": "IS_PL_ACCOUNT",
    },
    "employee": {
        "BusinessPartner": "GPN",
        "FirstName": "FIRST_NAME",
        "LastName": "LAST_NAME",
        "BusinessPartnerFullName": "FULL_NAME",
        "OrganizationBPName1": "ORG_NAME",
        "BusinessPartnerCategory": "CATEGORY",
        "BusinessPartnerGrouping": "GROUPING",
        "Language": "LANGUAGE",
        "CreationDate": "CREATED_DATE",
    },
    "entity": {
        "CompanyCode": "CCODE",
        "CompanyCodeName": "NAME",
        "Country": "COUNTRY",
        "Currency": "CURRENCY",
        "Language": "LANGUAGE",
        "ChartOfAccounts": "CHART_OF_ACCOUNTS",
        "FiscalYearVariant": "FISCAL_YEAR_VARIANT",
        "CityName": "CITY",
    },
}

# ---------------------------------------------------------------------------
# Field mappings — SAP table technical names (ADT/RFC) to our CSV format
# When using ADT or RFC_READ_TABLE, column names are SAP table field names.
# We pass through all columns; this map renames known ones.
# ---------------------------------------------------------------------------

FIELD_MAP_TABLE: dict[str, dict[str, str]] = {
    "cost_center": {
        "KOKRS": "COAREA",
        "KOSTL": "CCTR",
        "BUKRS": "CCODE",
        "KTEXT": "TXTSH",
        "LTEXT": "TXTMI",
        "VERAK": "RESPONSIBLE",
        "DATAB": "START_DATE",
        "DATBI": "END_DATE",
        # CSKT text table
        "KLTXT": "TXTSH",
    },
    "profit_center": {
        "KOKRS": "COAREA",
        "PRCTR": "PCTR",
        "BUKRS": "CCODE",
        "KTEXT": "TXTSH",
        "LTEXT": "TXTMI",
        "DATAB": "START_DATE",
        "DATBI": "END_DATE",
    },
    "hierarchy": {
        "SETCLASS": "SET_CLASS",
        "SUBCLASS": "SET_TYPE",
        "SETNAME": "SET_NAME",
        "SUBSETNAME": "SUBSET",
        "LINEID": "NODE_ID",
        "SETTYPE": "NODE_TYPE",
    },
    "balance": {
        "RBUKRS": "CCODE",
        "RACCT": "GL_ACCOUNT",
        "GJAHR": "FISCAL_YEAR",
        "POPER": "PERIOD",
        "KOKRS": "COAREA",
        "RCNTR": "CCTR",
        "PRCTR": "PCTR",
        "RLDNR": "LEDGER",
        "HSL": "AMOUNT_CC",
        "RKCUR": "CURRENCY_CC",
        "KSL": "AMOUNT_GC",
        # FAGLFLEXT fields
        "BUKRS": "CCODE",
        "HSLVT": "AMOUNT_CC",
    },
    "gl_account": {
        "KTOPL": "CHART_OF_ACCOUNTS",
        "SAKNR": "GL_ACCOUNT",
        "TXT20": "GL_NAME",
        "TXT50": "GL_LONG_NAME",
        "KTOKS": "GL_GROUP",
        "XBILK": "IS_BS_ACCOUNT",
        "GVTYP": "ACCOUNT_TYPE",
    },
    "employee": {
        # ZUHL_GRD_GPF custom table
        "GPN": "GPN",
        "BS_FIRSTNAME": "FIRST_NAME",
        "BS_LASTNAME": "LAST_NAME",
        "BS_NAME": "FULL_NAME",
        "EMAIL_ADDRESS": "EMAIL",
        "OU_CD": "ORG_UNIT",
        "OU_DESC": "ORG_UNIT_DESC",
        "COST_PC_CD_E_OU": "COST_CENTER",
        "LOCAL_CC_CD": "LOCAL_CC",
        "LOCAL_CC_DESC": "LOCAL_CC_DESC",
        "LM_GPN": "MANAGER_GPN",
        "LM_BS_FIRSTNAME": "MANAGER_FIRST_NAME",
        "LM_BS_LASTNAME": "MANAGER_LAST_NAME",
        "RANK_CD": "RANK_CODE",
        "RANK_DESC": "RANK_DESC",
        "JOB_DESC": "JOB_TITLE",
        "PERS_AREA_CODE": "PERSONNEL_AREA",
        "PERS_AREA_DESC": "PERSONNEL_AREA_DESC",
        "EMPL_CLASS": "EMPLOYEE_CLASS",
        "EMP_STATUS": "STATUS",
        "VALID_FROM": "VALID_FROM",
        "VALID_TO": "VALID_TO",
        "SERVICE_DATE": "SERVICE_DATE",
        "USER_ID_PID": "USER_ID",
        "FULL_TIME_EQ": "FTE",
        "BUILDING_CD_1": "BUILDING",
        "LOCN_CITY_NAME_1": "CITY",
        "LOCN_CTRY_CD_1": "COUNTRY",
        "GCRS_COMP_CD": "COMPANY_CODE",
        "GCRS_COMP_DESC": "COMPANY_DESC",
        "FUNC_E_BUS_AREA": "BUSINESS_AREA",
        "FUNC_E_BUS_GROUP": "BUSINESS_GROUP",
        "SUPERVISOR_GPN": "SUPERVISOR_GPN",
        "SUPPLIER_PK": "SUPPLIER_PK",
        "SUPPLIER_DESC": "SUPPLIER_DESC",
        "WRK_IN_OU_CD": "WORK_IN_OU",
        "WRK_IN_OU_DESC": "WORK_IN_OU_DESC",
        "HEAD_OF_OWN_OU": "IS_HEAD_OF_OU",
    },
    "entity": {
        "BUKRS": "CCODE",
        "BUTXT": "NAME",
        "LAND1": "COUNTRY",
        "WAERS": "CURRENCY",
        "SPRAS": "LANGUAGE",
        "KTOPL": "CHART_OF_ACCOUNTS",
        "PERIV": "FISCAL_YEAR_VARIANT",
        "ORT01": "CITY",
    },
}

# BAPI result table names and their field mappings
BAPI_CONFIGS: dict[str, dict] = {
    "cost_center": {
        "bapi": "BAPI_COSTCENTER_GETLIST",
        "imports": {},
        "result_table": "COSTCENTER_LIST",
    },
    "profit_center": {
        "bapi": "BAPI_PROFITCENTER_GETLIST",
        "imports": {},
        "result_table": "PROFITCENTER_LIST",
    },
}

# Normalize plural kind names to singular
_PLURAL_MAP = {
    "cost_centers": "cost_center",
    "profit_centers": "profit_center",
    "entities": "entity",
    "hierarchies": "hierarchy",
    "balances": "balance",
    "gl_accounts": "gl_account",
    "employees": "employee",
}


_SAFE_VALUE_RE = re.compile(r"^[A-Za-z0-9_\-./]+$")


def _sanitize(value: str) -> str:
    """Reject values containing SQL metacharacters."""
    if not _SAFE_VALUE_RE.match(value):
        raise ValueError(f"Invalid filter value: {value!r}")
    return value


def _build_where_clause(kind: str, params: dict | None) -> str:
    """Build a WHERE clause from extraction parameters."""
    if not params:
        return ""
    clauses: list[str] = []
    co_area = params.get("co_area") or params.get("controlling_area")
    if co_area and kind in ("cost_center", "profit_center", "balance"):
        clauses.append(f"KOKRS = '{_sanitize(co_area)}'")

    hierarchy_name = params.get("hierarchy_name") or params.get("set_name")
    if hierarchy_name and kind == "hierarchy":
        clauses.append(f"SETNAME = '{_sanitize(hierarchy_name)}'")

    company_code = params.get("company_code")
    if company_code:
        field = "BUKRS" if kind != "balance" else "RBUKRS"
        clauses.append(f"{field} = '{_sanitize(company_code)}'")

    return " AND ".join(clauses)


def _build_odata_filter(kind: str, params: dict | None) -> dict | None:
    """Build OData $filter params from extraction parameters."""
    if not params:
        return None
    filters: list[str] = []
    co_area = params.get("co_area") or params.get("controlling_area")
    if co_area and kind in ("cost_center", "profit_center", "balance"):
        filters.append(f"ControllingArea eq '{_sanitize(co_area)}'")

    hierarchy_name = params.get("hierarchy_name") or params.get("set_name")
    if hierarchy_name and kind == "hierarchy":
        filters.append(f"SetName eq '{_sanitize(hierarchy_name)}'")

    company_code = params.get("company_code")
    if company_code:
        filters.append(f"CompanyCode eq '{_sanitize(company_code)}'")

    # Balance-specific filters
    period_from = params.get("period_from")
    period_to = params.get("period_to")
    if period_from and kind == "balance":
        filters.append(f"FiscalPeriod ge '{_sanitize(period_from)}'")
    if period_to and kind == "balance":
        filters.append(f"FiscalPeriod le '{_sanitize(period_to)}'")

    ledger = params.get("ledger") or params.get("gaap")
    if ledger and kind == "balance":
        filters.append(f"Ledger eq '{_sanitize(ledger)}'")

    if not filters:
        return None
    return {"$filter": " and ".join(filters)}


def _map_fields(raw_data: list[dict], field_map: dict[str, str]) -> list[dict]:
    """Map SAP field names to our CSV column names.

    If a field map is provided, rename known fields. Unknown fields
    from the raw data are passed through as-is.
    """
    rows: list[dict] = []
    for item in raw_data:
        row: dict = {}
        mapped_keys: set[str] = set()
        for sap_field, our_field in field_map.items():
            if sap_field in item:
                row[our_field] = item[sap_field]
                mapped_keys.add(sap_field)
        # Pass through unmapped fields
        for k, v in item.items():
            if k not in mapped_keys and k not in row:
                row[k] = v
        rows.append(row)
    return rows


def extract_from_sap(
    db: Session,
    connection_id: int,
    kind: str,
    odata_params: dict | None = None,
    *,
    retrieval_method: str | None = None,
) -> dict:
    """Extract data from SAP and create an upload batch.

    Supports OData, ADT table reads, RFC_READ_TABLE, and BAPIs depending
    on the retrieval_method. If retrieval_method is None, defaults to OData.
    """
    conn = db.get(SAPConnection, connection_id)
    if not conn:
        raise ValueError(f"SAP connection {connection_id} not found")

    normalized = _PLURAL_MAP.get(kind, kind)

    logger.info(
        "sap.extract.start",
        connection=conn.name,
        kind=normalized,
        method=retrieval_method or "odata",
    )

    raw_data: list[dict] = []
    is_table_source = False  # True when data comes from ADT/RFC (table field names)

    if retrieval_method and ":" in retrieval_method:
        proto, rest = retrieval_method.split(":", 1)
        proto = proto.lower()

        if proto == "adt":
            # ADT table read: "adt:CSKS" or "adt:/sap/bc/adt/datapreview/ddic?table=CSKS"
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(rest)
            table_name = parse_qs(parsed.query).get("table", [rest.split(":")[-1]])[0]
            where = _build_where_clause(normalized, odata_params)
            raw_data = fetch_adt_table(conn, table_name, where=where)
            is_table_source = True

        elif proto == "rfc":
            parts = rest.split(":")
            fm_name = parts[0] if parts else "RFC_READ_TABLE"

            if fm_name == "RFC_READ_TABLE" and len(parts) > 1:
                # "rfc:RFC_READ_TABLE:CSKS" — direct table read
                table_name = parts[-1]
                where = _build_where_clause(normalized, odata_params)
                raw_data = call_rfc_read_table(conn, table_name, where=where)
                is_table_source = True
            elif fm_name.startswith("BAPI_"):
                # "rfc:BAPI_COSTCENTER_GETLIST" — BAPI call
                bapi_imports = {}
                co_area = (odata_params or {}).get("co_area")
                if co_area:
                    bapi_imports["CONTROLLINGAREA"] = co_area
                result = call_bapi(conn, fm_name, imports=bapi_imports)
                if not result.get("success"):
                    raise RuntimeError(
                        f"BAPI {fm_name} failed: {result.get('error_message', 'Unknown error')}"
                    )
                # Extract rows from the first table in the result
                for _tname, trows in result.get("tables", {}).items():
                    raw_data = trows
                    break
                is_table_source = True
            else:
                # Generic RFC call — try as table read
                where = _build_where_clause(normalized, odata_params)
                raw_data = call_rfc_read_table(
                    conn, parts[-1] if len(parts) > 1 else fm_name, where=where
                )
                is_table_source = True

        elif proto == "odata":
            # Explicit OData: "odata:API_COSTCENTER_SRV/A_CostCenter"
            odata_filter = _build_odata_filter(normalized, odata_params)
            raw_data = fetch_odata(conn, rest, params=odata_filter)

        else:
            raise ValueError(f"Unsupported extraction protocol: {proto}")
    else:
        # Default: OData via entity set lookup
        entity_set = ENTITY_SETS.get(normalized) or ENTITY_SETS.get(kind)
        if not entity_set:
            raise ValueError(f"Unknown extraction kind: {kind}")
        odata_filter = _build_odata_filter(normalized, odata_params)
        raw_data = fetch_odata(conn, entity_set, params=odata_filter)

    if not raw_data:
        return {"rows_extracted": 0, "batch_id": None}

    # Map fields based on source type
    if is_table_source:
        field_map = FIELD_MAP_TABLE.get(normalized, {})
    else:
        field_map = FIELD_MAP_ODATA.get(normalized, {})

    rows = _map_fields(raw_data, field_map) if field_map else raw_data

    # Determine CSV columns — use field_map values + any extra columns
    csv_columns = list(rows[0].keys()) if rows else (list(field_map.values()) if field_map else [])

    # Write CSV to temp file
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=csv_columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    tmp_dir = Path(tempfile.gettempdir()) / "amplifi_sap_extract"
    tmp_dir.mkdir(exist_ok=True)
    filename = f"sap_{normalized}_{conn.name}.csv"
    file_path = tmp_dir / filename
    file_path.write_text(csv_buffer.getvalue())

    batch = UploadBatch(
        filename=filename,
        kind=normalized,
        storage_uri=str(file_path),
        status="uploaded",
        rows_total=len(rows),
    )
    db.add(batch)
    db.commit()
    db.refresh(batch)

    logger.info(
        "sap.extract.done",
        connection=conn.name,
        kind=normalized,
        rows=len(rows),
        batch_id=batch.id,
    )

    # Auto-validate and auto-load into the target tables
    from app.services.upload_processor import load_upload, validate_upload

    try:
        validate_upload(batch.id, db)
        load_result = load_upload(batch.id, db)
        logger.info(
            "sap.extract.auto_loaded",
            batch_id=batch.id,
            rows_loaded=load_result.get("rows_loaded", 0),
        )
    except Exception as exc:
        logger.warning(
            "sap.extract.auto_load_failed",
            batch_id=batch.id,
            error=str(exc),
        )

    return {"rows_extracted": len(rows), "batch_id": batch.id}


def list_available_extractions(db: Session) -> list[dict]:
    """List SAP connections available for extraction."""
    connections = (
        db.execute(select(SAPConnection).where(SAPConnection.is_active.is_(True))).scalars().all()
    )
    return [
        {
            "connection_id": c.id,
            "name": c.name,
            "protocol": c.protocol,
            "available_kinds": CANONICAL_KINDS,
        }
        for c in connections
    ]
