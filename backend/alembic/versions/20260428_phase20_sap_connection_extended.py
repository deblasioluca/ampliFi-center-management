"""Extended SAP connection config — host/port split, web dispatcher, per-endpoint
overrides, ICF aliases, principal propagation.

Mirrors the sap-ai-consultant SAPSystemRequest fields for feature parity.

Revision ID: phase20_sap_conn_ext
Revises: phase19_gl_display
Create Date: 2026-04-28
"""

import sqlalchemy as sa

from alembic import op

revision = "phase20_sap_conn_ext"
down_revision = "phase19_gl_display"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Host / Port / Protocol split
    op.add_column(
        "sap_connection",
        sa.Column("host", sa.String(200), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("port", sa.String(10), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("conn_protocol", sa.String(10), server_default="https", nullable=True),
        schema="cleanup",
    )

    # Fiori / WebGUI URLs
    op.add_column(
        "sap_connection",
        sa.Column("fiori_launchpad_url", sa.String(500), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("webgui_url", sa.String(500), nullable=True),
        schema="cleanup",
    )

    # Web Dispatcher
    op.add_column(
        "sap_connection",
        sa.Column("webdisp_host", sa.String(200), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("webdisp_port", sa.String(10), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("webdisp_protocol", sa.String(10), server_default="https", nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("use_webdisp", sa.Boolean(), server_default="false", nullable=False),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("adt_use_webdisp", sa.Boolean(), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("soap_use_webdisp", sa.Boolean(), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("odata_use_webdisp", sa.Boolean(), nullable=True),
        schema="cleanup",
    )

    # Per-endpoint overrides: verify_ssl, use_proxy, saml2_disabled
    for ep in ("adt", "soap", "odata"):
        op.add_column(
            "sap_connection",
            sa.Column(f"{ep}_verify_ssl", sa.Boolean(), nullable=True),
            schema="cleanup",
        )
        op.add_column(
            "sap_connection",
            sa.Column(f"{ep}_use_proxy", sa.Boolean(), nullable=True),
            schema="cleanup",
        )
        op.add_column(
            "sap_connection",
            sa.Column(f"{ep}_saml2_disabled", sa.Boolean(), nullable=True),
            schema="cleanup",
        )

    # ICF Aliases
    op.add_column(
        "sap_connection",
        sa.Column("use_icf_aliases", sa.Boolean(), server_default="false", nullable=False),
        schema="cleanup",
    )
    for ep in ("adt", "soap", "odata"):
        op.add_column(
            "sap_connection",
            sa.Column(f"{ep}_icf_source", sa.String(20), nullable=True),
            schema="cleanup",
        )
        op.add_column(
            "sap_connection",
            sa.Column(f"{ep}_icf_cert", sa.String(200), nullable=True),
            schema="cleanup",
        )
        op.add_column(
            "sap_connection",
            sa.Column(f"{ep}_icf_basic", sa.String(200), nullable=True),
            schema="cleanup",
        )

    # Certificate sources
    for ep in ("adt", "soap", "odata"):
        op.add_column(
            "sap_connection",
            sa.Column(f"{ep}_cert_source", sa.String(20), nullable=True),
            schema="cleanup",
        )

    # Principal Propagation
    op.add_column(
        "sap_connection",
        sa.Column("pp_enabled", sa.Boolean(), server_default="false", nullable=False),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column(
            "pp_sap_oauth_token_url",
            sa.String(200),
            server_default="/sap/bc/sec/oauth2/token",
            nullable=True,
        ),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("pp_sap_oauth_client_id", sa.String(200), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("pp_sap_oauth_client_secret_enc", sa.Text(), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("pp_saml_issuer", sa.String(200), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("pp_saml_audience", sa.String(200), nullable=True),
        schema="cleanup",
    )
    op.add_column(
        "sap_connection",
        sa.Column("pp_user_mapping", sa.String(20), server_default="email", nullable=True),
        schema="cleanup",
    )


def downgrade() -> None:
    cols = [
        "host", "port", "conn_protocol",
        "fiori_launchpad_url", "webgui_url",
        "webdisp_host", "webdisp_port", "webdisp_protocol", "use_webdisp",
        "adt_use_webdisp", "soap_use_webdisp", "odata_use_webdisp",
        "adt_verify_ssl", "adt_use_proxy", "adt_saml2_disabled",
        "soap_verify_ssl", "soap_use_proxy", "soap_saml2_disabled",
        "odata_verify_ssl", "odata_use_proxy", "odata_saml2_disabled",
        "use_icf_aliases",
        "adt_icf_source", "soap_icf_source", "odata_icf_source",
        "adt_icf_cert", "soap_icf_cert", "odata_icf_cert",
        "adt_icf_basic", "soap_icf_basic", "odata_icf_basic",
        "adt_cert_source", "soap_cert_source", "odata_cert_source",
        "pp_enabled", "pp_sap_oauth_token_url", "pp_sap_oauth_client_id",
        "pp_sap_oauth_client_secret_enc", "pp_saml_issuer", "pp_saml_audience",
        "pp_user_mapping",
    ]
    for col in cols:
        op.drop_column("sap_connection", col, schema="cleanup")
