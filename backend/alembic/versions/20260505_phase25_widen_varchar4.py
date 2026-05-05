"""Widen VARCHAR(4) columns and expand target tables to full SAP structure.

1. Widen all VARCHAR(4) columns to VARCHAR(20) in SAP-aligned tables to
   prevent StringDataRightTruncation errors during upload.
2. Add all missing SAP CSKS columns to target_cost_center (108 columns).
3. Add all missing SAP CEPC columns to target_profit_center (45 columns).
This ensures target tables have identical structure to legacy tables.

Revision ID: phase25_widen_varchar4
Revises: phase24_scope_segregation
Create Date: 2026-05-05
"""

import sqlalchemy as sa

from alembic import op

revision = "phase25_widen_varchar4"
down_revision = "phase24_scope_segregation"
branch_labels = None
depends_on = None

SCHEMA = "cleanup"


def _column_exists(table: str, column: str) -> bool:
    conn = op.get_bind()
    result = conn.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table "
            "AND column_name = :col"
        ),
        {"schema": SCHEMA, "table": table, "col": column},
    )
    return result.fetchone() is not None


# --- Part 1: Widen VARCHAR(4) → VARCHAR(20) ---

COLUMNS_TO_WIDEN: dict[str, list[str]] = {
    "entity": [
        "chart_of_accounts", "fikrs", "fm_area", "fdbuk",
        "credit_control_area", "mregl", "ebukr", "ktop2", "umkrs",
        "fstva", "opvar", "wfvar", "infmt", "fstvare", "dtaxr",
    ],
    "employee": [
        "sap_bukrs", "rang_code", "personalbereich", "rang_krz",
        "reg_nr_1ap", "reg_nr_2ap", "untergrp_code", "ma_oe", "ma_kstst",
    ],
    "legacy_cost_center": [
        "gsber", "cctrcgy", "werks", "drnam",
        "zzcueabacc", "zzcuegbcd", "zzcueubcd", "zzstrkklas",
        "zzstrabukr", "zzstrugcd", "zzstrtaxcd", "zzstrgrpid",
        "jv_otype", "ferc_ind",
    ],
    "legacy_profit_center": [
        "drnam",
    ],
    "gl_account_ska1": [
        "ktopl", "ktoks",
    ],
    "gl_account_skb1": [
        "bukrs", "begru", "fstag", "kdfsl", "togru",
    ],
}

# --- Part 2: New columns for target_cost_center (full CSKS alignment) ---

TCC_NEW_COLUMNS: list[tuple[str, sa.types.TypeEngine]] = [
    ("mandt", sa.String(3)),
    ("datbi", sa.String(8)),
    ("datab", sa.String(8)),
    ("bkzkp", sa.String(1)),
    ("pkzkp", sa.String(1)),
    ("gsber", sa.String(20)),
    ("verak_user", sa.String(12)),
    ("kalsm", sa.String(6)),
    ("txjcd", sa.String(15)),
    ("werks", sa.String(20)),
    ("logsystem", sa.String(10)),
    ("ersda", sa.String(8)),
    ("usnam", sa.String(12)),
    ("bkzks", sa.String(1)),
    ("bkzer", sa.String(1)),
    ("bkzob", sa.String(1)),
    ("pkzks", sa.String(1)),
    ("pkzer", sa.String(1)),
    ("vmeth", sa.String(2)),
    ("mgefl", sa.String(1)),
    ("abtei", sa.String(12)),
    ("nkost", sa.String(10)),
    ("kvewe", sa.String(1)),
    ("kappl", sa.String(2)),
    ("koszschl", sa.String(6)),
    ("land1", sa.String(3)),
    ("anred", sa.String(15)),
    ("name1", sa.String(35)),
    ("name2", sa.String(35)),
    ("name3", sa.String(35)),
    ("name4", sa.String(35)),
    ("ort01", sa.String(35)),
    ("ort02", sa.String(35)),
    ("stras", sa.String(35)),
    ("pfach", sa.String(10)),
    ("pstlz", sa.String(10)),
    ("pstl2", sa.String(10)),
    ("regio", sa.String(3)),
    ("spras", sa.String(1)),
    ("telbx", sa.String(15)),
    ("telf1", sa.String(16)),
    ("telf2", sa.String(16)),
    ("telfx", sa.String(31)),
    ("teltx", sa.String(30)),
    ("telx1", sa.String(30)),
    ("datlt", sa.String(14)),
    ("drnam", sa.String(20)),
    ("khinr", sa.String(12)),
    ("cckey", sa.String(23)),
    ("kompl", sa.String(1)),
    ("stakz", sa.String(1)),
    ("objnr", sa.String(22)),
    ("funkt", sa.String(3)),
    ("afunk", sa.String(3)),
    ("cpi_templ", sa.String(10)),
    ("cpd_templ", sa.String(10)),
    ("func_area", sa.String(16)),
    ("sci_templ", sa.String(10)),
    ("scd_templ", sa.String(10)),
    ("ski_templ", sa.String(10)),
    ("skd_templ", sa.String(10)),
    ("zzcuemncfu", sa.String(5)),
    ("zzcueabacc", sa.String(20)),
    ("zzcuegbcd", sa.String(20)),
    ("zzcueubcd", sa.String(20)),
    ("zzcuenkos", sa.String(10)),
    ("zzstrpctyp", sa.String(3)),
    ("zzstrkklas", sa.String(20)),
    ("zzstraagcd", sa.String(2)),
    ("zzstrgfd", sa.String(3)),
    ("zzstrfst", sa.String(2)),
    ("zzstrmacve", sa.String(6)),
    ("zzstrabukr", sa.String(20)),
    ("zzstrugcd", sa.String(20)),
    ("zzstrinadt", sa.String(8)),
    ("zzstrkstyp", sa.String(1)),
    ("zzstrverik", sa.String(20)),
    ("zzstrcurr2", sa.String(3)),
    ("zzstrlccid", sa.String(10)),
    ("zzstrmaloc", sa.String(10)),
    ("zzstrtaxcd", sa.String(20)),
    ("zzstrgrpid", sa.String(20)),
    ("zzstrregcode", sa.String(6)),
    ("zzstrtaxarea", sa.String(10)),
    ("zzstrrepsit", sa.String(10)),
    ("zzstrgsm", sa.String(10)),
    ("zzcemapar", sa.String(10)),
    ("zzledger", sa.String(5)),
    ("zzhdstat", sa.String(1)),
    ("zzhdtype", sa.String(1)),
    ("zzfmd", sa.String(5)),
    ("zzfmdcc", sa.String(3)),
    ("zzfmdnode", sa.String(5)),
    ("zzstate", sa.String(2)),
    ("zztax", sa.String(2)),
    ("zzstrentsa", sa.String(11)),
    ("zzstrentzu", sa.String(11)),
    ("xblnr", sa.String(16)),
    ("vname", sa.String(6)),
    ("recid", sa.String(2)),
    ("etype", sa.String(3)),
    ("jv_otype", sa.String(20)),
    ("jv_jibcl", sa.String(3)),
    ("jv_jibsa", sa.String(5)),
    ("ferc_ind", sa.String(20)),
    ("valid_from", sa.DateTime(timezone=True)),
    ("valid_to", sa.DateTime(timezone=True)),
    ("attrs", sa.dialects.postgresql.JSONB()),
]

# --- Part 3: New columns for target_profit_center (full CEPC alignment) ---

TPC_NEW_COLUMNS: list[tuple[str, sa.types.TypeEngine]] = [
    ("mandt", sa.String(3)),
    ("datbi", sa.String(8)),
    ("datab", sa.String(8)),
    ("ersda", sa.String(8)),
    ("usnam", sa.String(12)),
    ("merkmal", sa.String(30)),
    ("verak_user", sa.String(12)),
    ("nprctr", sa.String(10)),
    ("land1", sa.String(3)),
    ("anred", sa.String(15)),
    ("name1", sa.String(35)),
    ("name2", sa.String(35)),
    ("name3", sa.String(35)),
    ("name4", sa.String(35)),
    ("ort01", sa.String(35)),
    ("ort02", sa.String(35)),
    ("stras", sa.String(35)),
    ("pfach", sa.String(10)),
    ("pstlz", sa.String(10)),
    ("pstl2", sa.String(10)),
    ("language", sa.String(2)),
    ("telbx", sa.String(15)),
    ("telf1", sa.String(16)),
    ("telf2", sa.String(16)),
    ("telfx", sa.String(31)),
    ("teltx", sa.String(30)),
    ("telx1", sa.String(30)),
    ("datlt", sa.String(14)),
    ("drnam", sa.String(20)),
    ("khinr", sa.String(12)),
    ("vname", sa.String(6)),
    ("recid", sa.String(2)),
    ("etype", sa.String(3)),
    ("txjcd", sa.String(15)),
    ("regio", sa.String(3)),
    ("kvewe", sa.String(1)),
    ("kappl", sa.String(2)),
    ("kalsm", sa.String(6)),
    ("logsystem", sa.String(10)),
    ("lock_ind", sa.String(1)),
    ("pca_template", sa.String(10)),
    ("segment", sa.String(10)),
    ("valid_from", sa.DateTime(timezone=True)),
    ("valid_to", sa.DateTime(timezone=True)),
    ("attrs", sa.dialects.postgresql.JSONB()),
]


def upgrade() -> None:
    # Part 1: Widen existing VARCHAR(4) columns to VARCHAR(20)
    for table, columns in COLUMNS_TO_WIDEN.items():
        for col in columns:
            if _column_exists(table, col):
                op.alter_column(
                    table,
                    col,
                    type_=sa.String(20),
                    schema=SCHEMA,
                    existing_nullable=True,
                )

    # Part 2: Add new columns to target_cost_center
    for col_name, col_type in TCC_NEW_COLUMNS:
        if not _column_exists("target_cost_center", col_name):
            op.add_column(
                "target_cost_center",
                sa.Column(col_name, col_type, nullable=True),
                schema=SCHEMA,
            )

    # Part 3: Add new columns to target_profit_center
    for col_name, col_type in TPC_NEW_COLUMNS:
        if not _column_exists("target_profit_center", col_name):
            op.add_column(
                "target_profit_center",
                sa.Column(col_name, col_type, nullable=True),
                schema=SCHEMA,
            )


def downgrade() -> None:
    # Remove added target_profit_center columns
    for col_name, _ in reversed(TPC_NEW_COLUMNS):
        if _column_exists("target_profit_center", col_name):
            op.drop_column("target_profit_center", col_name, schema=SCHEMA)

    # Remove added target_cost_center columns
    for col_name, _ in reversed(TCC_NEW_COLUMNS):
        if _column_exists("target_cost_center", col_name):
            op.drop_column("target_cost_center", col_name, schema=SCHEMA)

    # Revert VARCHAR(20) back to VARCHAR(4)
    for table, columns in COLUMNS_TO_WIDEN.items():
        for col in columns:
            if _column_exists(table, col):
                op.alter_column(
                    table,
                    col,
                    type_=sa.String(4),
                    schema=SCHEMA,
                    existing_nullable=True,
                )
