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
    "balance": "YY1_GLACCOUNTBALANCE/A_GLAccountBalance",
    "gl_account": "API_GLACCOUNTINCHARTOFACCOUNTS_SRV/A_GLAccountInChartOfAccounts",
    "employee": "API_BUSINESS_PARTNER/A_BusinessPartner",
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

    # Normalize kind to singular
    normalized = kind.rstrip("s") if kind.endswith("ies") is False and kind.endswith("s") else kind
    if kind in ("cost_centers", "cost_center"):
        normalized = "cost_center"
    elif kind in ("profit_centers", "profit_center"):
        normalized = "profit_center"
    elif kind in ("entities", "entity"):
        normalized = "entity"

    # Map SAP fields to our CSV format
    field_map = FIELD_MAP_CC if normalized == "cost_center" else FIELD_MAP_PC
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
        kind=kind.rstrip("s"),
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
