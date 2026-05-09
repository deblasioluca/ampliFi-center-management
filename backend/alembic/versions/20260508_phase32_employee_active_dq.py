"""Add Employee.is_active + DataQualityIssue table."""

import sqlalchemy as sa

from alembic import op

revision = "phase32_employee_active_dq"
down_revision = "phase31_widen_zzstrinadt"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Employee.is_active column
    op.add_column(
        "employee",
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        schema="cleanup",
    )

    # 2. DataQualityIssue table
    op.create_table(
        "data_quality_issue",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scope", sa.String(20), nullable=False, server_default="cleanup"),
        sa.Column("object_type", sa.String(30), nullable=False),
        sa.Column("object_id", sa.Integer(), nullable=False),
        sa.Column("field_name", sa.String(50), nullable=False),
        sa.Column("rule_code", sa.String(50), nullable=False),
        sa.Column("severity", sa.String(20), nullable=False, server_default="warning"),
        sa.Column("message", sa.Text()),
        sa.Column("current_value", sa.Text()),
        sa.Column("suggested_value", sa.Text()),
        sa.Column(
            "suggested_employee_id",
            sa.Integer(),
            sa.ForeignKey("cleanup.employee.id", ondelete="SET NULL"),
        ),
        sa.Column("status", sa.String(20), nullable=False, server_default="open"),
        sa.Column("resolved_by", sa.String(100)),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column("resolved_value", sa.Text()),
        sa.Column(
            "resolved_employee_id",
            sa.Integer(),
            sa.ForeignKey("cleanup.employee.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "batch_id",
            sa.Integer(),
            sa.ForeignKey("cleanup.upload_batch.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
        schema="cleanup",
    )
    op.create_index("ix_dqi_scope", "data_quality_issue", ["scope"], schema="cleanup")
    op.create_index("ix_dqi_status", "data_quality_issue", ["status"], schema="cleanup")
    op.create_index(
        "ix_dqi_object",
        "data_quality_issue",
        ["object_type", "object_id"],
        schema="cleanup",
    )


def downgrade() -> None:
    op.drop_index("ix_dqi_object", table_name="data_quality_issue", schema="cleanup")
    op.drop_index("ix_dqi_status", table_name="data_quality_issue", schema="cleanup")
    op.drop_index("ix_dqi_scope", table_name="data_quality_issue", schema="cleanup")
    op.drop_table("data_quality_issue", schema="cleanup")
    op.drop_column("employee", "is_active", schema="cleanup")
