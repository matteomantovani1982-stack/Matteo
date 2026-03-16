"""
app/models/job_run.py

Represents one complete "Application Intelligence Pack" generation run.

A JobRun ties together:
  - A CV (source document)
  - A job description (text or link)
  - The LLM outputs (edit_plan, reports)
  - The final rendered DOCX

State machine:
  CREATED → PARSING → RETRIEVING → LLM_PROCESSING → RENDERING → COMPLETED
                                                                      ↓
                                                                   FAILED (at any step)

One CV can generate many JobRuns (apply to multiple roles).
"""

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDMixin


class JobRunStatus(StrEnum):
    """
    Full lifecycle of a JobRun.
    Each status maps to a Celery task or an API action.
    """
    CREATED         = "created"          # JobRun record inserted, no work started
    PARSING         = "parsing"          # parse_cv task running (if CV not yet parsed)
    RETRIEVING      = "retrieving"       # web_search task fetching company/salary data
    LLM_PROCESSING  = "llm_processing"  # run_llm task calling Claude API
    RENDERING       = "rendering"        # render_docx task applying edits to DOCX
    COMPLETED       = "completed"        # Output DOCX ready for download
    FAILED          = "failed"           # Terminal failure — see error_message + failed_step


class JobRun(Base, UUIDMixin, TimestampMixin):
    """
    One end-to-end generation of an Application Intelligence Pack.

    Stores all inputs, all LLM outputs, and the final output S3 location.
    Everything needed to replay or audit the run lives on this record.
    """
    __tablename__ = "job_runs"

    # ── Owner ──────────────────────────────────────────────────────────────
    user_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="User who created this run — matches CV.user_id",
    )

    # ── CV link ────────────────────────────────────────────────────────────
    cv_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("cvs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    cv: Mapped["CV"] = relationship(  # type: ignore[name-defined]
        "CV",
        back_populates="job_runs",
        lazy="selectin",
    )

    # ── Job description inputs ─────────────────────────────────────────────
    jd_text: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Raw job description text pasted by the user",
    )
    jd_url: Mapped[str | None] = mapped_column(
        String(2000),
        nullable=True,
        comment="URL to the job posting — used by retrieval service if no jd_text",
    )

    # ── User preferences ───────────────────────────────────────────────────
    # Stored as JSONB so we can add fields without schema migrations
    preferences: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default="{}",
        comment=(
            "User preferences for this run. "
            "Schema: {tone, region, page_limit, positioning, include_unverified}"
        ),
    )


    # ── Candidate profile link ─────────────────────────────────────────────
    profile_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        index=True,
        comment="CandidateProfile used for this run.",
    )
    profile_overrides: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="Per-application overrides: highlight/suppress/extra context per experience.",
    )
    positioning: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="PositioningStrategy: strongest angles, gaps, narrative, red flags.",
    )
    # ── Retrieval results ──────────────────────────────────────────────────
    retrieval_data: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Raw data fetched by the retrieval service. "
            "Schema: {company_overview, salary_data, news, competitors}"
        ),
    )

    # ── LLM outputs ────────────────────────────────────────────────────────
    edit_plan: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Structured edit instructions from Claude. "
            "Applied by the rendering service to produce the output DOCX."
        ),
    )
    reports: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Company intelligence report, salary analysis, "
            "networking plan, and application strategy from Claude."
        ),
    )

    # ── Output ─────────────────────────────────────────────────────────────
    pipeline_stage: Mapped[str | None] = mapped_column(
        nullable=True,
        default="watching",
        comment="Kanban stage: watching|applied|interviewing|offer|rejected|withdrawn",
    )
    pipeline_notes: Mapped[str | None] = mapped_column(
        nullable=True,
        comment="User notes on this application",
    )
    applied_at: Mapped[datetime | None] = mapped_column(
        nullable=True,
        comment="When user marked as applied",
    )

    keyword_match_score: Mapped[int | None] = mapped_column(
        nullable=True,
        comment="0-100 keyword match score from edit plan",
    )

    role_title: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Role title extracted from reports"
    )
    company_name: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Company name extracted from reports"
    )
    apply_url: Mapped[str | None] = mapped_column(
        String(2000), nullable=True, comment="Direct application URL"
    )

    output_s3_key: Mapped[str | None] = mapped_column(
        String(1000),
        nullable=True,
        comment="S3 key for the final rendered DOCX output",
    )
    output_s3_bucket: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    # ── Lifecycle ──────────────────────────────────────────────────────────
    status: Mapped[JobRunStatus] = mapped_column(
        String(50),
        nullable=False,
        default=JobRunStatus.CREATED,
        server_default=JobRunStatus.CREATED,
        index=True,
    )
    failed_step: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment="Which step failed — matches JobRunStatus values",
    )
    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Exception message from the failed step",
    )

    # ── Celery task tracking ───────────────────────────────────────────────
    celery_task_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Celery task ID of the currently running task — for status polling",
    )

    # ── Relationships ──────────────────────────────────────────────────────
    steps: Mapped[list["JobStep"]] = relationship(  # type: ignore[name-defined]
        "JobStep",
        back_populates="job_run",
        cascade="all, delete-orphan",
        order_by="JobStep.created_at",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<JobRun id={self.id} "
            f"status={self.status} "
            f"cv_id={self.cv_id}>"
        )

    @property
    def output_s3_uri(self) -> str | None:
        """Full S3 URI for the output DOCX — None if not yet rendered."""
        if self.output_s3_key and self.output_s3_bucket:
            return f"s3://{self.output_s3_bucket}/{self.output_s3_key}"
        return None

    @property
    def is_terminal(self) -> bool:
        """True if the run has reached a final state (completed or failed)."""
        return self.status in (JobRunStatus.COMPLETED, JobRunStatus.FAILED)

    @property
    def has_jd(self) -> bool:
        """True if the run has at least one job description source."""
        return bool(self.jd_text or self.jd_url)
