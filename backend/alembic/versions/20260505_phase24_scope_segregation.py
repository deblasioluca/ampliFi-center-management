"""Add scope + data_category columns to all data tables for 3-scope segregation.

Revision ID: phase24_scope_segregation
Revises: phase23_column_labels
Create Date: 2026-05-05
"""

import sqlalchemy as sa

from alembic import op

revision = "phase24_scope_segregation"
down_revision = "phase23_column_labels"
branch_labels = None
depends_on = None

SCHEMA = "cleanup"

# Tables that get scope + data_category columns
DATA_TABLES = [
    "entity",
    "employee",
    "legacy_cost_center",
    "legacy_profit_center",
    "balance",
    "hierarchy",
    "target_cost_center",
    "target_profit_center",
    "center_mapping",
    "gl_account_ska1",
    "gl_account_skb1",
    "upload_batch",
    "sap_object_binding",
]

# Default scope/category per table
DEFAULTS = {
    "entity": ("cleanup", "legacy"),
    "employee": ("cleanup", "legacy"),
    "legacy_cost_center": ("cleanup", "legacy"),
    "legacy_profit_center": ("cleanup", "legacy"),
    "balance": ("cleanup", "legacy"),
    "hierarchy": ("cleanup", "legacy"),
    "target_cost_center": ("cleanup", "target"),
    "target_profit_center": ("cleanup", "target"),
    "center_mapping": ("cleanup", "legacy"),
    "gl_account_ska1": ("cleanup", "legacy"),
    "gl_account_skb1": ("cleanup", "legacy"),
    "upload_batch": ("cleanup", "legacy"),
    "sap_object_binding": ("cleanup", "legacy"),
}

# Old unique constraints to drop and new ones to create
# Format: (table, old_constraint_name, old_cols, new_cols)
CONSTRAINT_CHANGES = [
    # entity: had unique=True on ccode column directly
    ("entity", "entity_ccode_key", ["ccode"], ["scope", "ccode"]),
    # employee
    (
        "employee",
        "employee_gpn_refresh_batch_key",
        ["gpn", "refresh_batch"],
        ["scope", "gpn", "refresh_batch"],
    ),
    # legacy_cost_center
    (
        "legacy_cost_center",
        "legacy_cost_center_coarea_cctr_refresh_batch_key",
        ["coarea", "cctr", "refresh_batch"],
        ["scope", "coarea", "cctr", "refresh_batch"],
    ),
    # legacy_profit_center
    (
        "legacy_profit_center",
        "legacy_profit_center_coarea_pctr_refresh_batch_key",
        ["coarea", "pctr", "refresh_batch"],
        ["scope", "coarea", "pctr", "refresh_batch"],
    ),
    # hierarchy
    (
        "hierarchy",
        "hierarchy_setclass_setname_refresh_batch_key",
        ["setclass", "setname", "refresh_batch"],
        ["scope", "setclass", "setname", "refresh_batch"],
    ),
    # target_cost_center
    (
        "target_cost_center",
        "target_cost_center_coarea_cctr_key",
        ["coarea", "cctr"],
        ["scope", "coarea", "cctr"],
    ),
    # target_profit_center
    (
        "target_profit_center",
        "target_profit_center_coarea_pctr_key",
        ["coarea", "pctr"],
        ["scope", "coarea", "pctr"],
    ),
    # center_mapping
    (
        "center_mapping",
        "center_mapping_object_type_legacy_coarea_legacy_center_targe_key",
        ["object_type", "legacy_coarea", "legacy_center", "target_coarea", "target_center"],
        [
            "scope",
            "object_type",
            "legacy_coarea",
            "legacy_center",
            "target_coarea",
            "target_center",
        ],
    ),
    # gl_account_ska1
    (
        "gl_account_ska1",
        "gl_account_ska1_ktopl_saknr_key",
        ["ktopl", "saknr"],
        ["scope", "ktopl", "saknr"],
    ),
    # gl_account_skb1
    (
        "gl_account_skb1",
        "gl_account_skb1_bukrs_saknr_key",
        ["bukrs", "saknr"],
        ["scope", "bukrs", "saknr"],
    ),
    # sap_object_binding
    (
        "sap_object_binding",
        "sap_object_binding_connection_id_object_type_key",
        ["connection_id", "object_type"],
        ["connection_id", "scope", "data_category", "object_type"],
    ),
]

# New indexes to create
NEW_INDEXES = [
    ("ix_entity_scope", "entity", ["scope"]),
    ("ix_emp_scope", "employee", ["scope"]),
    ("ix_lcc_scope", "legacy_cost_center", ["scope"]),
    ("ix_lpc_scope", "legacy_profit_center", ["scope"]),
    ("ix_bal_scope", "balance", ["scope"]),
    ("ix_hier_scope", "hierarchy", ["scope"]),
    ("ix_tcc_scope", "target_cost_center", ["scope"]),
    ("ix_tpc_scope", "target_profit_center", ["scope"]),
    ("ix_cm_scope", "center_mapping", ["scope"]),
    ("ix_ska1_scope", "gl_account_ska1", ["scope"]),
    ("ix_skb1_scope", "gl_account_skb1", ["scope"]),
    ("ix_ub_scope", "upload_batch", ["scope"]),
    ("ix_sob_scope", "sap_object_binding", ["scope"]),
]


def _column_exists(table: str, column: str) -> bool:
    """Check if a column already exists in the cleanup schema."""
    conn = op.get_bind()
    result = conn.execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table AND column_name = :col"
        ),
        {"schema": "cleanup", "table": table, "col": column},
    )
    return result.fetchone() is not None


def _constraint_exists(name: str) -> bool:
    """Check if a constraint exists in the cleanup schema."""
    conn = op.get_bind()
    result = conn.execute(
        sa.text(
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE constraint_schema = :schema AND constraint_name = :name"
        ),
        {"schema": "cleanup", "name": name},
    )
    return result.fetchone() is not None


def _index_exists(name: str) -> bool:
    """Check if an index exists in the cleanup schema."""
    conn = op.get_bind()
    result = conn.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :name"),
        {"schema": "cleanup", "name": name},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    # 1. Add scope + data_category columns to all tables (with server defaults)
    for table in DATA_TABLES:
        default_scope, default_cat = DEFAULTS[table]
        if not _column_exists(table, "scope"):
            op.add_column(
                table,
                sa.Column("scope", sa.String(20), nullable=False, server_default=default_scope),
                schema=SCHEMA,
            )
        if not _column_exists(table, "data_category"):
            op.add_column(
                table,
                sa.Column(
                    "data_category", sa.String(10), nullable=False, server_default=default_cat
                ),
                schema=SCHEMA,
            )

    # 2. Add source_method and source_detail to upload_batch
    if not _column_exists("upload_batch", "source_method"):
        op.add_column(
            "upload_batch",
            sa.Column("source_method", sa.String(20), nullable=False, server_default="file"),
            schema=SCHEMA,
        )
    if not _column_exists("upload_batch", "source_detail"):
        op.add_column(
            "upload_batch",
            sa.Column("source_detail", sa.String(200), nullable=True),
            schema=SCHEMA,
        )

    # 3. Drop old unique constraints and create new scope-aware ones
    for table, old_name, _old_cols, new_cols in CONSTRAINT_CHANGES:
        new_name = f"uq_{table}_scope_{'_'.join(c for c in new_cols if c != 'scope')}"
        if len(new_name) > 63:
            new_name = new_name[:63]
        if _constraint_exists(old_name):
            op.drop_constraint(old_name, table, schema=SCHEMA, type_="unique")
        if not _constraint_exists(new_name):
            op.create_unique_constraint(new_name, table, new_cols, schema=SCHEMA)

    # 4. Create scope indexes
    for idx_name, table, cols in NEW_INDEXES:
        if not _index_exists(idx_name):
            op.create_index(idx_name, table, cols, schema=SCHEMA)

    # 5. Remove server defaults (keep column defaults in ORM only)
    for table in DATA_TABLES:
        op.alter_column(table, "scope", server_default=None, schema=SCHEMA)
        op.alter_column(table, "data_category", server_default=None, schema=SCHEMA)
    op.alter_column("upload_batch", "source_method", server_default=None, schema=SCHEMA)


def downgrade() -> None:
    # Drop indexes
    for idx_name, table, _cols in NEW_INDEXES:
        if _index_exists(idx_name):
            op.drop_index(idx_name, table_name=table, schema=SCHEMA)

    # Restore old constraints
    for table, old_name, old_cols, new_cols in CONSTRAINT_CHANGES:
        new_name = f"uq_{table}_scope_{'_'.join(c for c in new_cols if c != 'scope')}"
        if len(new_name) > 63:
            new_name = new_name[:63]
        if _constraint_exists(new_name):
            op.drop_constraint(new_name, table, schema=SCHEMA, type_="unique")
        if not _constraint_exists(old_name):
            op.create_unique_constraint(old_name, table, old_cols, schema=SCHEMA)

    # Drop source columns from upload_batch
    if _column_exists("upload_batch", "source_detail"):
        op.drop_column("upload_batch", "source_detail", schema=SCHEMA)
    if _column_exists("upload_batch", "source_method"):
        op.drop_column("upload_batch", "source_method", schema=SCHEMA)

    # Drop scope + data_category from all tables
    for table in DATA_TABLES:
        if _column_exists(table, "data_category"):
            op.drop_column(table, "data_category", schema=SCHEMA)
        if _column_exists(table, "scope"):
            op.drop_column(table, "scope", schema=SCHEMA)
