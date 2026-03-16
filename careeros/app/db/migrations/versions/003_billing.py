"""subscriptions, usage_records

Revision ID: 003
Revises: 002
Create Date: 2025-01-03 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── subscriptions ──────────────────────────────────────────────────────
    op.create_table(
        "subscriptions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("plan_tier", sa.String(20), nullable=False, server_default="free"),
        sa.Column("status", sa.String(30), nullable=False, server_default="free"),
        sa.Column("stripe_customer_id", sa.String(255), nullable=True, unique=True),
        sa.Column("stripe_subscription_id", sa.String(255), nullable=True, unique=True),
        sa.Column("stripe_price_id", sa.String(255), nullable=True),
        sa.Column("current_period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_at_period_end", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stripe_metadata", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_subscriptions_id", "subscriptions", ["id"])
    op.create_index("ix_subscriptions_user_id", "subscriptions", ["user_id"])
    op.create_index("ix_subscriptions_plan_tier", "subscriptions", ["plan_tier"])
    op.create_index("ix_subscriptions_status", "subscriptions", ["status"])
    op.create_index("ix_subscriptions_stripe_customer_id", "subscriptions", ["stripe_customer_id"])
    op.create_index("ix_subscriptions_stripe_subscription_id", "subscriptions", ["stripe_subscription_id"])

    # ── usage_records ──────────────────────────────────────────────────────
    op.create_table(
        "usage_records",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("subscription_id", UUID(as_uuid=True), sa.ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("job_run_id", UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("plan_tier_at_time", sa.String(20), nullable=False),
        sa.Column("billing_period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("billing_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_usage_records_id", "usage_records", ["id"])
    op.create_index("ix_usage_records_subscription_id", "usage_records", ["subscription_id"])
    op.create_index("ix_usage_records_user_id", "usage_records", ["user_id"])
    op.create_index("ix_usage_records_job_run_id", "usage_records", ["job_run_id"])
    # Composite index for fast quota queries: user + created_at
    op.create_index(
        "ix_usage_records_subscription_created",
        "usage_records",
        ["subscription_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_table("usage_records")
    op.drop_table("subscriptions")
