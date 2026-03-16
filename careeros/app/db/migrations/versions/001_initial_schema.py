"""initial schema — cv, job_run, job_step

Revision ID: 001
Revises: 
Create Date: 2025-01-01 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── cvs ────────────────────────────────────────────────────────────────
    op.create_table(
        "cvs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", sa.String(255), nullable=False),
        sa.Column("original_filename", sa.String(500), nullable=False),
        sa.Column("s3_key", sa.String(1000), nullable=True),
        sa.Column("s3_bucket", sa.String(255), nullable=True),
        sa.Column("file_size_bytes", sa.Integer, nullable=True),
        sa.Column("mime_type", sa.String(100), nullable=True),
        sa.Column("status", sa.String(50), nullable=False, server_default="uploaded"),
        sa.Column("parsed_content", JSONB, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_cvs_id", "cvs", ["id"])
    op.create_index("ix_cvs_user_id", "cvs", ["user_id"])
    op.create_index("ix_cvs_status", "cvs", ["status"])

    # ── job_runs ───────────────────────────────────────────────────────────
    op.create_table(
        "job_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", sa.String(255), nullable=False),
        sa.Column("cv_id", UUID(as_uuid=True), sa.ForeignKey("cvs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("profile_id", UUID(as_uuid=True), nullable=True),
        sa.Column("profile_overrides", JSONB, nullable=True),
        sa.Column("jd_text", sa.Text, nullable=True),
        sa.Column("jd_url", sa.String(2000), nullable=True),
        sa.Column("preferences", JSONB, nullable=False, server_default="{}"),
        sa.Column("retrieval_data", JSONB, nullable=True),
        sa.Column("edit_plan", JSONB, nullable=True),
        sa.Column("positioning", JSONB, nullable=True),
        sa.Column("reports", JSONB, nullable=True),
        sa.Column("output_s3_key", sa.String(1000), nullable=True),
        sa.Column("output_s3_bucket", sa.String(255), nullable=True),
        sa.Column("status", sa.String(50), nullable=False, server_default="created"),
        sa.Column("failed_step", sa.String(100), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_job_runs_id", "job_runs", ["id"])
    op.create_index("ix_job_runs_user_id", "job_runs", ["user_id"])
    op.create_index("ix_job_runs_cv_id", "job_runs", ["cv_id"])
    op.create_index("ix_job_runs_status", "job_runs", ["status"])
    op.create_index("ix_job_runs_celery_task_id", "job_runs", ["celery_task_id"])

    # ── job_steps ──────────────────────────────────────────────────────────
    op.create_table(
        "job_steps",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("job_run_id", UUID(as_uuid=True), sa.ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("step_name", sa.String(100), nullable=False),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float, nullable=True),
        sa.Column("error_type", sa.String(255), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("metadata_json", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_job_steps_id", "job_steps", ["id"])
    op.create_index("ix_job_steps_job_run_id", "job_steps", ["job_run_id"])


def downgrade() -> None:
    op.drop_table("job_steps")
    op.drop_table("job_runs")
    op.drop_table("cvs")
