"""MDG export service (§09.4, §12).

File-based export in SAP MDG 0G template format for cost centers
and profit centers. Generates CSV files matching the MDG import schema.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime

import structlog

logger = structlog.get_logger()


@dataclass
class MDGExportResult:
    """Result of an MDG export operation."""

    filename: str
    content: str  # CSV content as string
    record_count: int
    export_type: str  # cost_center | profit_center
    wave_id: int
    exported_at: datetime


# SAP MDG 0G template column definitions
CC_MDG_COLUMNS = [
    "OBJECT_TYPE",  # CCTR
    "CO_AREA",  # Controlling area
    "COSTCENTER",  # Cost center ID
    "VALID_FROM",  # Valid from date (YYYYMMDD)
    "VALID_TO",  # Valid to date (YYYYMMDD)
    "NAME",  # Short text
    "DESCRIPT",  # Long text
    "PERSON_IN_CH",  # Responsible person
    "COSTCENTER_T",  # CC category
    "COMP_CODE",  # Company code
    "CURRENCY",  # Currency
    "PROFIT_CTR",  # Profit center
    "FUNC_AREA",  # Functional area
    "ACTION",  # CREATE | CHANGE | DEACTIVATE
    "STATUS",  # New status
]

PC_MDG_COLUMNS = [
    "OBJECT_TYPE",  # PCTR
    "CO_AREA",  # Controlling area
    "PROFIT_CTR",  # Profit center ID
    "VALID_FROM",  # Valid from date
    "VALID_TO",  # Valid to date
    "NAME",  # Short text
    "DESCRIPT",  # Long text
    "PERSON_IN_CH",  # Responsible person
    "COMP_CODE",  # Company code
    "DEPARTMENT",  # Department
    "CURRENCY",  # Currency
    "ACTION",  # CREATE | CHANGE | DEACTIVATE
    "STATUS",  # New status
]


def _format_date(dt: datetime | None, default: str = "99991231") -> str:
    """Format a date as YYYYMMDD for MDG."""
    if dt is None:
        return default
    return dt.strftime("%Y%m%d")


def export_cost_centers(
    centers: list[dict],
    wave_id: int,
    action_map: dict[str, str] | None = None,
) -> MDGExportResult:
    """Export cost centers in MDG 0G template format.

    Args:
        centers: List of target cost center dicts
        wave_id: Wave ID for tracking
        action_map: Optional override of default actions per center ID
    """
    actions = action_map or {}
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CC_MDG_COLUMNS, delimiter=";")
    writer.writeheader()

    for center in centers:
        cctr = center.get("cctr", "")
        action = actions.get(cctr, "CREATE")

        row = {
            "OBJECT_TYPE": "CCTR",
            "CO_AREA": center.get("coarea", ""),
            "COSTCENTER": cctr,
            "VALID_FROM": _format_date(center.get("valid_from"), "20260101"),
            "VALID_TO": _format_date(center.get("valid_to")),
            "NAME": center.get("txtsh", ""),
            "DESCRIPT": center.get("txtmi", ""),
            "PERSON_IN_CH": center.get("responsible", ""),
            "COSTCENTER_T": center.get("cctrcgy", ""),
            "COMP_CODE": center.get("ccode", ""),
            "CURRENCY": center.get("currency", ""),
            "PROFIT_CTR": center.get("pctr", ""),
            "FUNC_AREA": center.get("func_area", ""),
            "ACTION": action,
            "STATUS": "NEW" if action == "CREATE" else "CHANGED",
        }
        writer.writerow(row)

    content = output.getvalue()
    now = datetime.utcnow()
    filename = f"MDG_CC_WAVE{wave_id}_{now.strftime('%Y%m%d_%H%M%S')}.csv"

    return MDGExportResult(
        filename=filename,
        content=content,
        record_count=len(centers),
        export_type="cost_center",
        wave_id=wave_id,
        exported_at=now,
    )


def export_profit_centers(
    centers: list[dict],
    wave_id: int,
    action_map: dict[str, str] | None = None,
) -> MDGExportResult:
    """Export profit centers in MDG 0G template format."""
    actions = action_map or {}
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=PC_MDG_COLUMNS, delimiter=";")
    writer.writeheader()

    for center in centers:
        pctr = center.get("pctr", "")
        action = actions.get(pctr, "CREATE")

        row = {
            "OBJECT_TYPE": "PCTR",
            "CO_AREA": center.get("coarea", ""),
            "PROFIT_CTR": pctr,
            "VALID_FROM": _format_date(center.get("valid_from"), "20260101"),
            "VALID_TO": _format_date(center.get("valid_to")),
            "NAME": center.get("txtsh", ""),
            "DESCRIPT": center.get("txtmi", ""),
            "PERSON_IN_CH": center.get("responsible", ""),
            "COMP_CODE": center.get("ccode", ""),
            "DEPARTMENT": center.get("department", ""),
            "CURRENCY": center.get("currency", ""),
            "ACTION": action,
            "STATUS": "NEW" if action == "CREATE" else "CHANGED",
        }
        writer.writerow(row)

    content = output.getvalue()
    now = datetime.utcnow()
    filename = f"MDG_PC_WAVE{wave_id}_{now.strftime('%Y%m%d_%H%M%S')}.csv"

    return MDGExportResult(
        filename=filename,
        content=content,
        record_count=len(centers),
        export_type="profit_center",
        wave_id=wave_id,
        exported_at=now,
    )


MAPPING_COLUMNS = [
    "OBJECT_TYPE",  # CCTR or PCTR
    "CO_AREA",  # Controlling area
    "LEGACY_CENTER",  # Old center ID
    "LEGACY_NAME",  # Old center name
    "TARGET_CENTER",  # New center ID
    "TARGET_NAME",  # New center name
    "MAPPING_TYPE",  # 1:1 | merge | redesign | retire
]


def export_mapping_table(
    mappings: list,
    wave_id: int,
) -> MDGExportResult:
    """Export old→new center mapping table."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=MAPPING_COLUMNS, delimiter=";")
    writer.writeheader()

    for m in mappings:
        row = {
            "OBJECT_TYPE": "CCTR" if m.object_type == "cost_center" else "PCTR",
            "CO_AREA": m.legacy_coarea,
            "LEGACY_CENTER": m.legacy_center,
            "LEGACY_NAME": m.legacy_name or "",
            "TARGET_CENTER": m.target_center,
            "TARGET_NAME": m.target_name or "",
            "MAPPING_TYPE": m.mapping_type or "",
        }
        writer.writerow(row)

    content = output.getvalue()
    now = datetime.utcnow()
    filename = f"MDG_MAPPING_WAVE{wave_id}_{now.strftime('%Y%m%d_%H%M%S')}.csv"

    return MDGExportResult(
        filename=filename,
        content=content,
        record_count=len(mappings),
        export_type="mapping",
        wave_id=wave_id,
        exported_at=now,
    )


def export_retire_list(
    centers: list[dict],
    wave_id: int,
) -> MDGExportResult:
    """Export a list of cost centers to be deactivated."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CC_MDG_COLUMNS, delimiter=";")
    writer.writeheader()

    for center in centers:
        row = {
            "OBJECT_TYPE": "CCTR",
            "CO_AREA": center.get("coarea", ""),
            "COSTCENTER": center.get("cctr", ""),
            "VALID_FROM": center.get("valid_from", ""),
            "VALID_TO": _format_date(datetime.utcnow()),
            "NAME": center.get("txtsh", ""),
            "DESCRIPT": center.get("txtmi", ""),
            "PERSON_IN_CH": center.get("responsible", ""),
            "COSTCENTER_T": center.get("cctrcgy", ""),
            "COMP_CODE": center.get("ccode", ""),
            "CURRENCY": center.get("currency", ""),
            "PROFIT_CTR": center.get("pctr", ""),
            "FUNC_AREA": "",
            "ACTION": "DEACTIVATE",
            "STATUS": "DEACTIVATED",
        }
        writer.writerow(row)

    content = output.getvalue()
    now = datetime.utcnow()
    filename = f"MDG_RETIRE_WAVE{wave_id}_{now.strftime('%Y%m%d_%H%M%S')}.csv"

    return MDGExportResult(
        filename=filename,
        content=content,
        record_count=len(centers),
        export_type="retire",
        wave_id=wave_id,
        exported_at=now,
    )
