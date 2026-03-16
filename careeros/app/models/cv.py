"""
app/models/cv.py

Represents a CV file uploaded by a user.

Lifecycle:
  UPLOADED → PARSING → PARSED → FAILED

One CV can be used across multiple JobRuns (e.g. apply to 10 different roles
with the same base CV). The parsed_content stores the extracted node map
produced by the ingest service — this is what the LLM reads.
"""

import uuid
from enum import StrEnum

from sqlalchemy import BigInteger, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDMixin


class CVStatus(StrEnum):
    """Lifecycle states for a CV upload."""
    UPLOADED = "uploaded"    # File received and stored in S3
    PARSING  = "parsing"     # Celery parse_cv task is running
    PARSED   = "parsed"      # Node map extracted and stored
    FAILED   = "failed"      # Parsing failed — see error_message


class CVBuildMode(StrEnum):
    """
    How the output DOCX is produced for this CV.

    EDIT       — LLM edits the uploaded DOCX in place (original flow)
    REBUILD    — LLM rewrites content from scratch, user's DOCX used as layout template
    FROM_SCRATCH — No uploaded CV; LLM writes full CV using a system template
    """
    EDIT         = "edit"
    REBUILD      = "rebuild"
    FROM_SCRATCH = "from_scratch"


class CV(Base, UUIDMixin, TimestampMixin):
    """
    Stores metadata for an uploaded CV file.

    The raw file lives in S3 at s3_key.
    The parsed content (paragraph nodes, runs, styles) lives in parsed_content.
    """
    __tablename__ = "cvs"

    # ── Owner (future: link to users table) ───────────────────────────────
    # Using a plain string for now — swap to ForeignKey("users.id") later
    user_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
        comment="Identifier of the user who uploaded this CV",
    )

    # ── File metadata ──────────────────────────────────────────────────────
    original_filename: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
        comment="Original filename as uploaded by the user",
    )
    file_size_bytes: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="File size in bytes — used for storage quota enforcement",
    )
    mime_type: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="MIME type: application/vnd.openxmlformats-officedocument.wordprocessingml.document or application/pdf",
    )

    # ── S3 storage ─────────────────────────────────────────────────────────
    s3_key: Mapped[str] = mapped_column(
        String(1000),
        nullable=False,
        unique=True,
        comment="Full S3 object key, e.g. cvs/{user_id}/{uuid}/original.docx",
    )
    s3_bucket: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="S3 bucket name — stored so we can handle multi-bucket configs later",
    )

    # ── Lifecycle ──────────────────────────────────────────────────────────
    status: Mapped[CVStatus] = mapped_column(
        String(50),
        nullable=False,
        default=CVStatus.UPLOADED,
        server_default=CVStatus.UPLOADED,
        index=True,
    )
    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Set when status=FAILED — stores the exception message",
    )

    # ── Parsed content ─────────────────────────────────────────────────────
    parsed_content: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Structured node map extracted from the DOCX. "
            "Schema: {sections: [{heading, paragraphs: [{index, text, runs, style}]}]}"
        ),
    )

    # ── Quality scoring ────────────────────────────────────────────────────
    quality_score: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment=(
            "LLM quality score 0-100. <40=poor, 40-70=acceptable, >70=good. "
            "Set after parsing. Used to recommend rebuild vs edit."
        ),
    )
    quality_feedback: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="Structured quality feedback: {score, issues, recommendation, verdict}",
    )

    # ── Build mode ──────────────────────────────────────────────────────────
    build_mode: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="edit",
        server_default="edit",
        comment="CVBuildMode: edit | rebuild | from_scratch",
    )

    # ── Template (for rebuild / from_scratch modes) ─────────────────────────
    template_slug: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
        comment="Template slug selected by user for rebuild/from_scratch. Null = use uploaded layout.",
    )

    # ── Relationships ──────────────────────────────────────────────────────
    job_runs: Mapped[list["JobRun"]] = relationship(  # type: ignore[name-defined]
        "JobRun",
        back_populates="cv",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<CV id={self.id} "
            f"filename='{self.original_filename}' "
            f"status={self.status}>"
        )

    @property
    def s3_uri(self) -> str:
        """Full S3 URI for logging and debugging."""
        return f"s3://{self.s3_bucket}/{self.s3_key}"

    @property
    def is_parsed(self) -> bool:
        return self.status == CVStatus.PARSED

    @property
    def is_failed(self) -> bool:
        return self.status == CVStatus.FAILED
