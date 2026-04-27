"""HANA column-store DDL generator for SAP Datasphere tables.

Generates CREATE TABLE statements for all data domains that can be
moved to Datasphere, using HANA-optimized column store types.
"""

from __future__ import annotations

# PostgreSQL → HANA type mapping
PG_TO_HANA = {
    "VARCHAR": "NVARCHAR",
    "TEXT": "NCLOB",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "BOOLEAN": "BOOLEAN",
    "NUMERIC": "DECIMAL",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    "JSONB": "NCLOB",  # HANA stores JSON as NCLOB
    "JSON": "NCLOB",
}


def _col(name: str, hana_type: str, nullable: bool = True, pk: bool = False) -> str:
    """Format a single column definition."""
    parts = [f'    "{name}" {hana_type}']
    if pk:
        parts.append("PRIMARY KEY")
    if not nullable and not pk:
        parts.append("NOT NULL")
    return " ".join(parts)


def _table(schema: str, name: str, columns: list[str], comment: str = "") -> str:
    """Generate a CREATE TABLE statement with COLUMN STORE."""
    cols = ",\n".join(columns)
    ddl = f'CREATE COLUMN TABLE "{schema}"."{name}" (\n{cols}\n)'
    if comment:
        ddl += f" COMMENT '{comment}'"
    return ddl + ";\n"


def generate_all_ddl(schema: str = "ACM") -> dict[str, str]:
    """Generate HANA DDL for all Datasphere-eligible tables.

    Returns a dict mapping domain name → DDL string.
    """
    tables: dict[str, str] = {}

    # ── Entity (Company Code) ──
    tables["entity"] = _table(
        schema,
        "ENTITY",
        [
            _col("ID", "INTEGER", pk=True),
            _col("CCODE", "NVARCHAR(10)", nullable=False),
            _col("NAME", "NVARCHAR(200)", nullable=False),
            _col("COUNTRY", "NVARCHAR(3)"),
            _col("REGION", "NVARCHAR(50)"),
            _col("CURRENCY", "NVARCHAR(3)"),
            _col("IS_ACTIVE", "BOOLEAN"),
            _col("ATTRS", "NCLOB"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Company codes / entities",
    )

    # ── Legacy Cost Center ──
    tables["cost_center"] = _table(
        schema,
        "LEGACY_COST_CENTER",
        [
            _col("ID", "INTEGER", pk=True),
            _col("COAREA", "NVARCHAR(10)", nullable=False),
            _col("CCTR", "NVARCHAR(20)", nullable=False),
            _col("TXTSH", "NVARCHAR(40)"),
            _col("TXTMI", "NVARCHAR(200)"),
            _col("RESPONSIBLE", "NVARCHAR(100)"),
            _col("CCODE", "NVARCHAR(10)"),
            _col("CCTRCGY", "NVARCHAR(4)"),
            _col("CURRENCY", "NVARCHAR(3)"),
            _col("PCTR", "NVARCHAR(20)"),
            _col("IS_ACTIVE", "BOOLEAN"),
            _col("VALID_FROM", "TIMESTAMP"),
            _col("VALID_TO", "TIMESTAMP"),
            _col("ATTRS", "NCLOB"),
            _col("REFRESH_BATCH", "INTEGER"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Legacy cost centers (source data from SAP)",
    )

    # ── Legacy Profit Center ──
    tables["profit_center"] = _table(
        schema,
        "LEGACY_PROFIT_CENTER",
        [
            _col("ID", "INTEGER", pk=True),
            _col("COAREA", "NVARCHAR(10)", nullable=False),
            _col("PCTR", "NVARCHAR(20)", nullable=False),
            _col("TXTSH", "NVARCHAR(40)"),
            _col("TXTMI", "NVARCHAR(200)"),
            _col("RESPONSIBLE", "NVARCHAR(100)"),
            _col("CCODE", "NVARCHAR(10)"),
            _col("DEPARTMENT", "NVARCHAR(20)"),
            _col("CURRENCY", "NVARCHAR(3)"),
            _col("IS_ACTIVE", "BOOLEAN"),
            _col("VALID_FROM", "TIMESTAMP"),
            _col("VALID_TO", "TIMESTAMP"),
            _col("ATTRS", "NCLOB"),
            _col("REFRESH_BATCH", "INTEGER"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Legacy profit centers (source data from SAP)",
    )

    # ── Balance ──
    tables["balance"] = _table(
        schema,
        "BALANCE",
        [
            _col("ID", "INTEGER", pk=True),
            _col("COAREA", "NVARCHAR(10)", nullable=False),
            _col("CCTR", "NVARCHAR(20)", nullable=False),
            _col("CCODE", "NVARCHAR(10)"),
            _col("FISCAL_YEAR", "INTEGER", nullable=False),
            _col("PERIOD", "INTEGER", nullable=False),
            _col("ACCOUNT", "NVARCHAR(20)"),
            _col("ACCOUNT_CLASS", "NVARCHAR(20)"),
            _col("TC_AMT", "DECIMAL(23,2)"),
            _col("GC_AMT", "DECIMAL(23,2)"),
            _col("GC2_AMT", "DECIMAL(23,2)"),
            _col("CURRENCY_TC", "NVARCHAR(3)"),
            _col("CURRENCY_GC", "NVARCHAR(3)"),
            _col("CURRENCY_GC2", "NVARCHAR(3)"),
            _col("POSTING_COUNT", "INTEGER"),
            _col("ATTRS", "NCLOB"),
            _col("REFRESH_BATCH", "INTEGER"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "GL balance data (S/4 ACDOCA or ECC FAGLFLEXT/GLT0)",
    )

    # ── GL Account ──
    tables["gl_account"] = _table(
        schema,
        "GL_ACCOUNT",
        [
            _col("ID", "INTEGER", pk=True),
            _col("ACCOUNT", "NVARCHAR(20)", nullable=False),
            _col("ACCOUNT_NAME", "NVARCHAR(200)"),
            _col("ACCOUNT_CLASS", "NVARCHAR(20)"),
            _col("CHART_OF_ACCOUNTS", "NVARCHAR(10)"),
            _col("CCODE", "NVARCHAR(10)"),
            _col("IS_ACTIVE", "BOOLEAN"),
            _col("ATTRS", "NCLOB"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "GL account master data",
    )

    # ── Hierarchy ──
    tables["hierarchy"] = _table(
        schema,
        "HIERARCHY",
        [
            _col("ID", "INTEGER", pk=True),
            _col("SETCLASS", "NVARCHAR(10)", nullable=False),
            _col("SETNAME", "NVARCHAR(40)", nullable=False),
            _col("DESCRIPTION", "NVARCHAR(200)"),
            _col("COAREA", "NVARCHAR(10)"),
            _col("IS_ACTIVE", "BOOLEAN"),
            _col("ATTRS", "NCLOB"),
            _col("REFRESH_BATCH", "INTEGER"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Cost center hierarchy headers",
    )

    tables["hierarchy_node"] = _table(
        schema,
        "HIERARCHY_NODE",
        [
            _col("ID", "INTEGER", pk=True),
            _col("HIERARCHY_ID", "INTEGER", nullable=False),
            _col("PARENT_SETNAME", "NVARCHAR(40)", nullable=False),
            _col("CHILD_SETNAME", "NVARCHAR(40)", nullable=False),
            _col("SEQ", "INTEGER"),
        ],
        "Hierarchy node-to-node relationships",
    )

    tables["hierarchy_leaf"] = _table(
        schema,
        "HIERARCHY_LEAF",
        [
            _col("ID", "INTEGER", pk=True),
            _col("HIERARCHY_ID", "INTEGER", nullable=False),
            _col("SETNAME", "NVARCHAR(40)", nullable=False),
            _col("VALUE", "NVARCHAR(20)", nullable=False),
            _col("SEQ", "INTEGER"),
        ],
        "Hierarchy leaf values (cost centers under nodes)",
    )

    # ── Employee ──
    tables["employee"] = _table(
        schema,
        "EMPLOYEE",
        [
            _col("ID", "INTEGER", pk=True),
            _col("GPN", "NVARCHAR(20)", nullable=False),
            _col("BS_NAME", "NVARCHAR(200)"),
            _col("BS_FIRSTNAME", "NVARCHAR(100)"),
            _col("BS_LASTNAME", "NVARCHAR(100)"),
            _col("LEGAL_FAMILY_NAME", "NVARCHAR(100)"),
            _col("LEGAL_FIRST_NAME", "NVARCHAR(100)"),
            _col("EMAIL_ADDRESS", "NVARCHAR(200)"),
            _col("EMP_STATUS", "NVARCHAR(20)"),
            _col("VALID_FROM", "TIMESTAMP"),
            _col("VALID_TO", "TIMESTAMP"),
            _col("GENDER_CODE", "NVARCHAR(5)"),
            _col("USER_ID_PID", "NVARCHAR(30)"),
            _col("USER_ID_TNUMBER", "NVARCHAR(30)"),
            _col("UUNAME", "NVARCHAR(30)"),
            _col("OU_PK", "NVARCHAR(20)"),
            _col("OU_CD", "NVARCHAR(20)"),
            _col("OU_DESC", "NVARCHAR(200)"),
            _col("WRK_IN_OU_PK", "NVARCHAR(20)"),
            _col("WRK_IN_OU_CD", "NVARCHAR(20)"),
            _col("WRK_IN_OU_DESC", "NVARCHAR(200)"),
            _col("LOCAL_CC_CD", "NVARCHAR(20)"),
            _col("LOCAL_CC_DESC", "NVARCHAR(200)"),
            _col("GCRS_COMP_CD", "NVARCHAR(20)"),
            _col("GCRS_COMP_DESC", "NVARCHAR(200)"),
            _col("COST_PC_CD_E_OU", "NVARCHAR(20)"),
            _col("COST_PC_CD_W_OU", "NVARCHAR(20)"),
            _col("LM_GPN", "NVARCHAR(20)"),
            _col("LM_BS_FIRSTNAME", "NVARCHAR(100)"),
            _col("LM_BS_LASTNAME", "NVARCHAR(100)"),
            _col("SUPERVISOR_GPN", "NVARCHAR(20)"),
            _col("RANK_CD", "NVARCHAR(20)"),
            _col("RANK_DESC", "NVARCHAR(200)"),
            _col("JOB_DESC", "NVARCHAR(200)"),
            _col("EMPL_CLASS", "NVARCHAR(20)"),
            _col("FULL_TIME_EQ", "NVARCHAR(10)"),
            _col("HEAD_OF_OWN_OU", "NVARCHAR(5)"),
            _col("REG_REGION", "NVARCHAR(50)"),
            _col("LOCN_CITY_NAME_1", "NVARCHAR(100)"),
            _col("LOCN_CTRY_CD_1", "NVARCHAR(5)"),
            _col("BUILDING_CD_1", "NVARCHAR(20)"),
            _col("ATTRS", "NCLOB"),
            _col("REFRESH_BATCH", "INTEGER"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Employee master data (SAP HR)",
    )

    # ── Analysis Run ──
    tables["analysis_run"] = _table(
        schema,
        "ANALYSIS_RUN",
        [
            _col("ID", "INTEGER", pk=True),
            _col("WAVE_ID", "INTEGER"),
            _col("CONFIG_ID", "INTEGER", nullable=False),
            _col("STATUS", "NVARCHAR(20)", nullable=False),
            _col("STARTED_AT", "TIMESTAMP"),
            _col("FINISHED_AT", "TIMESTAMP"),
            _col("KPIS", "NCLOB"),
            _col("ERROR", "NCLOB"),
            _col("DATA_SNAPSHOT", "NVARCHAR(64)"),
            _col("TRIGGERED_BY", "INTEGER"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Analysis execution runs",
    )

    # ── Routine Output ──
    tables["routine_output"] = _table(
        schema,
        "ROUTINE_OUTPUT",
        [
            _col("ID", "INTEGER", pk=True),
            _col("RUN_ID", "INTEGER", nullable=False),
            _col("ROUTINE_CODE", "NVARCHAR(60)", nullable=False),
            _col("LEGACY_CC_ID", "INTEGER", nullable=False),
            _col("VERDICT", "NVARCHAR(30)"),
            _col("CONFIDENCE", "DECIMAL(5,4)"),
            _col("PAYLOAD", "NCLOB"),
            _col("CREATED_AT", "TIMESTAMP"),
        ],
        "Per-routine analysis results",
    )

    # ── Center Proposal ──
    tables["center_proposal"] = _table(
        schema,
        "CENTER_PROPOSAL",
        [
            _col("ID", "INTEGER", pk=True),
            _col("RUN_ID", "INTEGER", nullable=False),
            _col("LEGACY_CC_ID", "INTEGER", nullable=False),
            _col("ENTITY_CODE", "NVARCHAR(10)"),
            _col("CLEANSING_OUTCOME", "NVARCHAR(20)", nullable=False),
            _col("TARGET_OBJECT", "NVARCHAR(20)"),
            _col("MERGE_INTO_CCTR", "NVARCHAR(20)"),
            _col("RULE_PATH", "NCLOB"),
            _col("ML_SCORES", "NCLOB"),
            _col("LLM_COMMENTARY", "NCLOB"),
            _col("CONFIDENCE", "DECIMAL(5,4)"),
            _col("OVERRIDE_OUTCOME", "NVARCHAR(20)"),
            _col("OVERRIDE_TARGET", "NVARCHAR(20)"),
            _col("OVERRIDE_REASON", "NCLOB"),
            _col("OVERRIDE_BY", "INTEGER"),
            _col("OVERRIDE_AT", "TIMESTAMP"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Proposed new/merged/retired cost center decisions",
    )

    # ── Target Cost Center ──
    tables["target_cost_center"] = _table(
        schema,
        "TARGET_COST_CENTER",
        [
            _col("ID", "INTEGER", pk=True),
            _col("COAREA", "NVARCHAR(10)", nullable=False),
            _col("CCTR", "NVARCHAR(20)", nullable=False),
            _col("TXTSH", "NVARCHAR(40)"),
            _col("TXTMI", "NVARCHAR(200)"),
            _col("RESPONSIBLE", "NVARCHAR(100)"),
            _col("CCODE", "NVARCHAR(10)"),
            _col("CCTRCGY", "NVARCHAR(4)"),
            _col("CURRENCY", "NVARCHAR(3)"),
            _col("PCTR", "NVARCHAR(20)"),
            _col("IS_ACTIVE", "BOOLEAN"),
            _col("SOURCE_PROPOSAL_ID", "INTEGER"),
            _col("APPROVED_IN_WAVE", "INTEGER"),
            _col("MDG_STATUS", "NVARCHAR(30)"),
            _col("MDG_CHANGE_REQUEST_ID", "NVARCHAR(40)"),
            _col("CLOSED_AT", "TIMESTAMP"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Target (new/proposed) cost centers after review approval",
    )

    # ── Target Profit Center ──
    tables["target_profit_center"] = _table(
        schema,
        "TARGET_PROFIT_CENTER",
        [
            _col("ID", "INTEGER", pk=True),
            _col("COAREA", "NVARCHAR(10)", nullable=False),
            _col("PCTR", "NVARCHAR(20)", nullable=False),
            _col("TXTSH", "NVARCHAR(40)"),
            _col("TXTMI", "NVARCHAR(200)"),
            _col("RESPONSIBLE", "NVARCHAR(100)"),
            _col("CCODE", "NVARCHAR(10)"),
            _col("DEPARTMENT", "NVARCHAR(20)"),
            _col("CURRENCY", "NVARCHAR(3)"),
            _col("IS_ACTIVE", "BOOLEAN"),
            _col("SOURCE_PROPOSAL_ID", "INTEGER"),
            _col("APPROVED_IN_WAVE", "INTEGER"),
            _col("CLOSED_AT", "TIMESTAMP"),
            _col("CREATED_AT", "TIMESTAMP"),
            _col("UPDATED_AT", "TIMESTAMP"),
        ],
        "Target (new/proposed) profit centers after review approval",
    )

    # ── Sign-off ──
    tables["signoff"] = _table(
        schema,
        "SIGNOFF_RECORD",
        [
            _col("ID", "INTEGER", pk=True),
            _col("WAVE_ID", "INTEGER", nullable=False),
            _col("SCOPE_ID", "INTEGER"),
            _col("PROPOSAL_ID", "INTEGER"),
            _col("DECISION", "NVARCHAR(20)", nullable=False),
            _col("COMMENT", "NCLOB"),
            _col("DECIDED_BY", "NVARCHAR(100)"),
            _col("DECIDED_AT", "TIMESTAMP"),
            _col("CREATED_AT", "TIMESTAMP"),
        ],
        "Final sign-off decisions for reviewed proposals",
    )

    return tables


def generate_full_ddl(schema: str = "ACM") -> str:
    """Return complete DDL script for all Datasphere tables."""
    tables = generate_all_ddl(schema)
    header = f"""-- ============================================================
-- ACM (ampliFi Center Management) — SAP Datasphere DDL
-- Schema: {schema}
-- Generated for HANA Cloud Column Store
-- ============================================================

CREATE SCHEMA "{schema}";

"""
    body = "\n\n".join(tables.values())
    return header + body


# Default table name mapping (domain → HANA table name)
DEFAULT_TABLE_NAMES: dict[str, str] = {
    "entity": "ENTITY",
    "cost_center": "LEGACY_COST_CENTER",
    "profit_center": "LEGACY_PROFIT_CENTER",
    "balance": "BALANCE",
    "gl_account": "GL_ACCOUNT",
    "hierarchy": "HIERARCHY",
    "hierarchy_node": "HIERARCHY_NODE",
    "hierarchy_leaf": "HIERARCHY_LEAF",
    "employee": "EMPLOYEE",
    "analysis_run": "ANALYSIS_RUN",
    "routine_output": "ROUTINE_OUTPUT",
    "center_proposal": "CENTER_PROPOSAL",
    "target_cost_center": "TARGET_COST_CENTER",
    "target_profit_center": "TARGET_PROFIT_CENTER",
    "signoff": "SIGNOFF_RECORD",
}
