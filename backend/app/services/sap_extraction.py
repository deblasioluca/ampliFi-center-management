"""SAP OData live extraction service (§03.7).

Pulls cost centers, profit centers, and balances from SAP via OData
and creates upload batches for the standard upload processing pipeline.
"""

from __future__ import annotations

import csv
import io
import tempfile
from pathlib import Path

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.infra.sap.client import fetch_odata
from app.models.core import SAPConnection, UploadBatch

logger = structlog.get_logger()

# SAP OData entity set mappings (accept both singular and plural forms)
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

# Canonical kind names (singular) for available_kinds listing
CANONICAL_KINDS = [
    "cost_center",
    "profit_center",
    "hierarchy",
    "balance",
    "gl_account",
    "employee",
    "entity",
]

# Field mappings from SAP OData response to our CSV format
FIELD_MAP_CC = {
    "ControllingArea": "COAREA",
    "CostCenter": "CCTR",
    "CompanyCode": "CCODE",
    "CostCenterShortName": "TXTSH",
    "CostCenterLongName": "TXTMI",
    "PersonResponsible": "RESPONSIBLE",
    "ValidityStartDate": "START_DATE",
    "ValidityEndDate": "END_DATE",
}

FIELD_MAP_PC = {
    "ControllingArea": "COAREA",
    "ProfitCenter": "PCTR",
    "CompanyCode": "CCODE",
    "ProfitCenterShortName": "TXTSH",
    "ProfitCenterLongName": "TXTMI",
}

FIELD_MAP_HIERARCHY = {
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
}

FIELD_MAP_BALANCE = {
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
}

FIELD_MAP_GL = {
    "ChartOfAccounts": "CHART_OF_ACCOUNTS",
    "GLAccount": "GL_ACCOUNT",
    "GLAccountName": "GL_NAME",
    "GLAccountLongName": "GL_LONG_NAME",
    "GLAccountGroup": "GL_GROUP",
    "CompanyCode": "CCODE",
    "AccountType": "ACCOUNT_TYPE",
    "IsBalanceSheetAccount": "IS_BS_ACCOUNT",
    "IsProfitAndLossAccount": "IS_PL_ACCOUNT",
}

FIELD_MAP_EMPLOYEE = {
    "BusinessPartner": "GPN",
    "FirstName": "FIRST_NAME",
    "LastName": "LAST_NAME",
    "BusinessPartnerFullName": "FULL_NAME",
    "OrganizationBPName1": "ORG_NAME",
    "BusinessPartnerCategory": "CATEGORY",
    "BusinessPartnerGrouping": "GROUPING",
    "Language": "LANGUAGE",
    "CreationDate": "CREATED_DATE",
}

FIELD_MAP_ENTITY = {
    "CompanyCode": "CCODE",
    "CompanyCodeName": "NAME",
    "Country": "COUNTRY",
    "Currency": "CURRENCY",
    "Language": "LANGUAGE",
    "ChartOfAccounts": "CHART_OF_ACCOUNTS",
    "FiscalYearVariant": "FISCAL_YEAR_VARIANT",
    "CityName": "CITY",
}

# Lookup dict for all field maps
FIELD_MAPS = {
    "cost_center": FIELD_MAP_CC,
    "profit_center": FIELD_MAP_PC,
    "hierarchy": FIELD_MAP_HIERARCHY,
    "balance": FIELD_MAP_BALANCE,
    "gl_account": FIELD_MAP_GL,
    "employee": FIELD_MAP_EMPLOYEE,
    "entity": FIELD_MAP_ENTITY,
}


def extract_from_sap(
    db: Session,
    connection_id: int,
    kind: str,
    odata_params: dict | None = None,
) -> dict:
    """Extract data from SAP via OData and create an upload batch."""
    conn = db.get(SAPConnection, connection_id)
    if not conn:
        raise ValueError(f"SAP connection {connection_id} not found")

    entity_set = ENTITY_SETS.get(kind)
    if not entity_set:
        raise ValueError(f"Unknown extraction kind: {kind}")

    logger.info("sap.extract.start", connection=conn.name, kind=kind)
    raw_data = fetch_odata(conn, entity_set, params=odata_params)

    if not raw_data:
        return {"rows_extracted": 0, "batch_id": None}

    # Normalize kind to singular canonical form
    plural_map = {
        "cost_centers": "cost_center",
        "profit_centers": "profit_center",
        "entities": "entity",
        "hierarchies": "hierarchy",
        "balances": "balance",
        "gl_accounts": "gl_account",
        "employees": "employee",
    }
    normalized = plural_map.get(kind, kind)

    # Map SAP fields to our CSV format
    field_map = FIELD_MAPS.get(normalized)
    if not field_map:
        raise ValueError(f"No field mapping for extraction kind: {normalized}")
    rows: list[dict] = []
    for item in raw_data:
        row = {}
        for sap_field, our_field in field_map.items():
            row[our_field] = item.get(sap_field, "")
        rows.append(row)

    # Write CSV to temp file
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=list(field_map.values()))
    writer.writeheader()
    writer.writerows(rows)

    tmp_dir = Path(tempfile.gettempdir()) / "amplifi_sap_extract"
    tmp_dir.mkdir(exist_ok=True)
    filename = f"sap_{kind}_{conn.name}.csv"
    file_path = tmp_dir / filename
    file_path.write_text(csv_buffer.getvalue())

    # Create upload batch
    batch = UploadBatch(
        filename=filename,
        kind=normalized,
        file_path=str(file_path),
        status="uploaded",
        rows_total=len(rows),
        source=f"sap_odata:{conn.name}",
    )
    db.add(batch)
    db.commit()
    db.refresh(batch)

    logger.info(
        "sap.extract.done",
        connection=conn.name,
        kind=kind,
        rows=len(rows),
        batch_id=batch.id,
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
