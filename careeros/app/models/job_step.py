"""
app/models/job_step.py

Granular audit trail for each processing step within a JobRun.

Every time a Celery task starts, completes, or fails, it writes a JobStep.
This gives us:
  - Full traceability for debugging
  - Duration tracking per step
  - Structured error capture with context
  - A log the frontend can poll for real-time progress updates

One JobRun → many JobSteps (one per pipeline stage attempt).
"""

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import DateTime, Float, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDMixin


class StepName(StrEnum):
    """
    Named pipeline steps — must stay in sync with Celery task names.
    Each step has exactly one JobStep record per attempt.
    """
    PARSE_CV    = "parse_cv"       # DOCX/PDF → node map
    RETRIEVE    = "retrieve"       # Web search → company/salary data
    LLM_CALL    = "llm_call"       # Claude API → edit_plan + reports JSON
    RENDER_DOCX = "render_docx"    # edit_plan → output DOCX


class StepStatus(StrEnum):
    """Lifecycle of a single step."""
    PENDING   = "pending"    # Step created but not yet started
    RUNNING   = "running"    # Celery task is actively executing
    COMPLETED = "completed"  # Step finished successfully
    FAILED    = "failed"     # Step raised an exception


class JobStep(Base, UUIDMixin, TimestampMixin):
    """
    Audit record for one processing step within a JobRun.

    Created at PENDING when the task is dispatched.
    Updated to RUNNING when the worker picks it up.
    Updated to COMPLETED or FAILED when the task finishes.

    The metadata_json field stores step-specific context:
    - parse_cv:    {node_count, section_count, word_count}
    - retrieve:    {sources_used, queries_executed}
    - llm_call:    {model, input_tokens, output_tokens, cost_usd}
    - render_docx: {edits_applied, output_s3_key}
    """
    __tablename__ = "job_steps"

    # ── Parent run ─────────────────────────────────────────────────────────
    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    job_run: Mapped["JobRun"] = relationship(  # type: ignore[name-defined]
        "JobRun",
        back_populates="steps",
        lazy="selectin",
    )

    # ── Step identity ──────────────────────────────────────────────────────
    step_name: Mapped[StepName] = mapped_column(
        String(100),
        nullable=False,
        index=True,
        comment="Which pipeline step this record tracks",
    )
    status: Mapped[StepStatus] = mapped_column(
        String(50),
        nullable=False,
        default=StepStatus.PENDING,
        server_default=StepStatus.PENDING,
        index=True,
    )

    # ── Timing ─────────────────────────────────────────────────────────────
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the Celery worker actually started processing",
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the step finished (success or failure)",
    )
    duration_seconds: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Wall-clock duration in seconds — computed on completion",
    )

    # ── Celery ─────────────────────────────────────────────────────────────
    celery_task_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Celery task ID — use to inspect task state via flower or CLI",
    )

    # ── Error capture ──────────────────────────────────────────────────────
    error_type: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Exception class name, e.g. 'anthropic.APIError'",
    )
    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Full exception message",
    )

    # ── Step-specific context ──────────────────────────────────────────────
    metadata_json: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default="{}",
        comment="Step-specific structured context — schema varies by step_name",
    )

    def __repr__(self) -> str:
        return (
            f"<JobStep id={self.id} "
            f"step={self.step_name} "
            f"status={self.status} "
            f"run_id={self.job_run_id}>"
        )

    def mark_running(self, celery_task_id: str) -> None:
        """Call when the Celery worker picks up this step."""
        self.status = StepStatus.RUNNING
        self.celery_task_id = celery_task_id
        self.started_at = datetime.now(UTC)

    def mark_completed(self, metadata: dict | None = None) -> None:
        """Call when the step finishes successfully."""
        self.status = StepStatus.COMPLETED
        self.completed_at = datetime.now(UTC)
        if self.started_at:
            self.duration_seconds = (
                self.completed_at - self.started_at
            ).total_seconds()
        if metadata:
            self.metadata_json = metadata

    def mark_failed(
        self,
        error: Exception,
        metadata: dict | None = None,
    ) -> None:
        """Call when the step raises an exception."""
        self.status = StepStatus.FAILED
        self.completed_at = datetime.now(UTC)
        if self.started_at:
            self.duration_seconds = (
                self.completed_at - self.started_at
            ).total_seconds()
        self.error_type = type(error).__name__
        self.error_message = str(error)
        if metadata:
            self.metadata_json = metadata
