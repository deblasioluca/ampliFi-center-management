"""SAP XML response parsers — ADT datapreview and SOAP RFC.

Adopted from sap-ai-consultant patterns. Handles multiple SAP namespace
variants and response formats across different SAP releases.
"""

from __future__ import annotations

import logging
from typing import Any
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

# SAP ADT datapreview namespace variants
_DP_NAMESPACES = (
    "http://www.sap.com/adt/datapreview",
    "http://www.sap.com/adt/dataPreview",
)


def _local_name(tag: str) -> str:
    """Strip namespace from an XML tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _text(el: ET.Element | None) -> str:
    """Get text content of an element, or empty string."""
    if el is None:
        return ""
    return (el.text or "").strip()


# ---------------------------------------------------------------------------
# ADT Datapreview parser
# ---------------------------------------------------------------------------


def parse_datapreview(xml_bytes: bytes | str) -> dict[str, Any]:
    """Parse ADT datapreview XML response into columns + rows.

    Handles both namespace variants SAP uses (camelCase and lowercase),
    plus row-oriented and column-oriented response formats.

    Returns: dict with 'columns' (list[str]), 'rows' (list[dict]),
             'total_rows' (int).
    """
    if isinstance(xml_bytes, str):
        xml_bytes = xml_bytes.encode("utf-8")

    try:
        root = ET.fromstring(xml_bytes)  # noqa: S314
    except ET.ParseError:
        logger.warning("Failed to parse datapreview XML")
        return {"columns": [], "rows": [], "total_rows": 0}

    def _iter_dp(local_name: str):
        """Iterate elements matching local_name under either namespace."""
        found = False
        for ns in _DP_NAMESPACES:
            for el in root.iter(f"{{{ns}}}{local_name}"):
                found = True
                yield el
        if not found:
            for el in root.iter():
                if _local_name(el.tag) == local_name:
                    yield el

    def _find_dp(parent: ET.Element, local_name: str):
        for ns in _DP_NAMESPACES:
            el = parent.find(f"{{{ns}}}{local_name}")
            if el is not None:
                return el
        for child in parent:
            if _local_name(child.tag) == local_name:
                return child
        return None

    def _get_attr_name(el: ET.Element) -> str:
        for ns in _DP_NAMESPACES:
            val = el.get(f"{{{ns}}}name", "")
            if val:
                return val
        return el.get("name", "")

    columns: list[str] = []
    rows: list[dict[str, str]] = []

    # --- Column-oriented format (S/4HANA 2025+) ---
    col_blocks: list[tuple[str, list[str]]] = []
    for col_el in _iter_dp("columns"):
        meta = _find_dp(col_el, "metadata")
        if meta is None:
            continue
        col_name = _get_attr_name(meta)
        if not col_name:
            continue
        values: list[str] = []
        dataset = _find_dp(col_el, "dataSet")
        if dataset is not None:
            for data_el in dataset:
                if _local_name(data_el.tag) == "data":
                    values.append(_text(data_el))
        col_blocks.append((col_name, values))

    if col_blocks:
        columns = [name for name, _ in col_blocks]
        num_rows = max((len(vals) for _, vals in col_blocks), default=0)
        for i in range(num_rows):
            row = {}
            for col_name, vals in col_blocks:
                row[col_name] = vals[i] if i < len(vals) else ""
            rows.append(row)
        return {"columns": columns, "rows": rows, "total_rows": len(rows)}

    # --- Row-oriented format (older SAP) ---
    for row_el in _iter_dp("dataRow"):
        row = {}
        for cell_el in row_el:
            tag = _local_name(cell_el.tag)
            col_name = _get_attr_name(cell_el) or tag
            if col_name == "dataColumn":
                col_name = _get_attr_name(cell_el)
            row[col_name] = _text(cell_el)
        if row:
            rows.append(row)
            for k in row:
                if k not in columns:
                    columns.append(k)

    return {"columns": columns, "rows": rows, "total_rows": len(rows)}


# ---------------------------------------------------------------------------
# SOAP RFC response parser
# ---------------------------------------------------------------------------


def parse_soap_rfc_response(xml_bytes: bytes | str) -> dict[str, Any]:
    """Parse a SOAP RFC response from SAP.

    Returns: dict with 'exports' (list of dicts with name/value),
             'tables' (dict of table_name -> list of row dicts),
             'error_message' (str).
    """
    if isinstance(xml_bytes, str):
        xml_bytes = xml_bytes.encode("utf-8")

    try:
        root = ET.fromstring(xml_bytes)  # noqa: S314
    except ET.ParseError:
        return {"exports": [], "tables": {}, "error_message": "Failed to parse SOAP XML"}

    # Check for SOAP fault
    fault = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Fault")
    if fault is not None:
        fault_string = _text(fault.find("faultstring"))
        detail = _text(fault.find("detail"))
        return {
            "exports": [],
            "tables": {},
            "error_message": fault_string or detail or "SOAP fault",
        }

    # Find the response body
    body = root.find("{http://schemas.xmlsoap.org/soap/envelope/}Body")
    if body is None:
        body = root

    # First child of Body is the FM response element
    response_el = None
    for child in body:
        tag = _local_name(child.tag)
        if tag.endswith(("Response", ".Response")):
            response_el = child
            break
    if response_el is None:
        children = list(body)
        response_el = children[0] if children else body

    exports: list[dict[str, str]] = []
    tables: dict[str, list[dict[str, str]]] = {}

    for child in response_el:
        tag = _local_name(child.tag)
        if not tag:
            continue

        sub_children = list(child)
        if not sub_children:
            exports.append({"name": tag, "value": _text(child)})
        else:
            items = child.findall("item")
            if not items:
                items = [c for c in sub_children if _local_name(c.tag) == "item"]

            if items:
                table_rows: list[dict[str, str]] = []
                for item in items:
                    row: dict[str, str] = {}
                    for field_el in item:
                        field_tag = _local_name(field_el.tag)
                        row[field_tag] = _text(field_el)
                    if row:
                        table_rows.append(row)
                tables[tag] = table_rows
            else:
                struct_parts: list[str] = []
                for sub in sub_children:
                    sub_tag = _local_name(sub.tag)
                    sub_val = _text(sub)
                    if sub_val:
                        struct_parts.append(f"{sub_tag}={sub_val}")
                exports.append({"name": tag, "value": "; ".join(struct_parts)})

    return {"exports": exports, "tables": tables, "error_message": ""}
