"""
Sprint H+I: template renderer support, best practices feedback fields

No new tables — all changes are additive columns and index additions.

Changes:
  - cv.quality_feedback already stores best practices (merged in parse_cv task)
  - No schema changes needed for JD extraction (stored in job_run.jd_text as before)
  - Add index on job_runs.jd_url for future deduplication queries

Revision ID: 005
Revises: 004
Create Date: 2025-01-05 00:00:00
"""

from alembic import op
import sqlalchemy as sa

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Index on jd_url — enables deduplication and "already applied here" checks
    op.create_index(
        "ix_job_runs_jd_url",
        "job_runs",
        ["jd_url"],
        postgresql_ops={"jd_url": "text_pattern_ops"},
    )


def downgrade() -> None:
    op.drop_index("ix_job_runs_jd_url", "job_runs")
