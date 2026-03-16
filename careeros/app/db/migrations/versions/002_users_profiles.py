"""users, candidate_profiles, experience_entries

Revision ID: 002
Revises: 001
Create Date: 2025-01-02 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── users ──────────────────────────────────────────────────────────────
    op.create_table(
        "users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("email", sa.String(255), nullable=False, unique=True),
        sa.Column("password_hash", sa.String(512), nullable=False),
        sa.Column("full_name", sa.String(255), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("is_verified", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("refresh_token_hash", sa.String(512), nullable=True),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_users_id", "users", ["id"])
    op.create_index("ix_users_email", "users", ["email"])

    # ── candidate_profiles ─────────────────────────────────────────────────
    op.create_table(
        "candidate_profiles",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", sa.String(255), nullable=False),
        sa.Column("version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("status", sa.String(20), nullable=False, server_default="draft"),
        sa.Column("headline", sa.Text, nullable=True),
        sa.Column("global_context", sa.Text, nullable=True),
        sa.Column("global_notes", sa.Text, nullable=True),
        sa.Column("cv_id", UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("user_id", "version", name="uq_profile_user_version"),
    )
    op.create_index("ix_candidate_profiles_id", "candidate_profiles", ["id"])
    op.create_index("ix_candidate_profiles_user_id", "candidate_profiles", ["user_id"])
    op.create_index("ix_candidate_profiles_status", "candidate_profiles", ["status"])

    # ── experience_entries ─────────────────────────────────────────────────
    op.create_table(
        "experience_entries",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("profile_id", UUID(as_uuid=True), sa.ForeignKey("candidate_profiles.id", ondelete="CASCADE"), nullable=False),
        sa.Column("company_name", sa.String(255), nullable=False),
        sa.Column("role_title", sa.String(255), nullable=False),
        sa.Column("start_date", sa.String(20), nullable=True),
        sa.Column("end_date", sa.String(20), nullable=True),
        sa.Column("location", sa.String(100), nullable=True),
        sa.Column("context", sa.Text, nullable=True),
        sa.Column("contribution", sa.Text, nullable=True),
        sa.Column("outcomes", sa.Text, nullable=True),
        sa.Column("methods", sa.Text, nullable=True),
        sa.Column("hidden", sa.Text, nullable=True),
        sa.Column("freeform", sa.Text, nullable=True),
        sa.Column("display_order", sa.Integer, nullable=False, server_default="0"),
        sa.Column("is_complete", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("extracted_signals", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_experience_entries_id", "experience_entries", ["id"])
    op.create_index("ix_experience_entries_profile_id", "experience_entries", ["profile_id"])

    # ── Add profile_id FK to job_runs (now that candidate_profiles exists) ─
    op.create_index("ix_job_runs_profile_id", "job_runs", ["profile_id"])


def downgrade() -> None:
    op.drop_index("ix_job_runs_profile_id", "job_runs")
    op.drop_table("experience_entries")
    op.drop_table("candidate_profiles")
    op.drop_table("users")
