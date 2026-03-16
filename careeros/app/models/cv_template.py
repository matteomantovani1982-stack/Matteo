"""
app/models/cv_template.py

CV templates — pre-designed DOCX layouts the CV builder fills with content
when generating a CV from scratch (no uploaded document to use as template).

Templates are stored as DOCX files in S3 under templates/{slug}.docx.
They are seeded once at deployment time and rarely change.

Five templates:
  classic    — Traditional chronological, serif headings, conservative
  modern     — Clean sans-serif, subtle colour accents, ATS-friendly
  executive  — Wide margins, strong hierarchy, suited to C-suite
  compact    — One-page optimised, tight spacing, junior/mid roles
  minimal    — Absolute minimal — text only, maximum ATS compatibility
"""

import uuid
from enum import StrEnum

from sqlalchemy import Boolean, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDMixin


class TemplateStyle(StrEnum):
    CLASSIC   = "classic"
    MODERN    = "modern"
    EXECUTIVE = "executive"
    COMPACT   = "compact"
    MINIMAL   = "minimal"


class CVTemplate(Base, UUIDMixin, TimestampMixin):
    """
    A named DOCX template stored in S3.
    The CV builder uses these when generating a CV from scratch.
    """
    __tablename__ = "cv_templates"

    slug: Mapped[TemplateStyle] = mapped_column(
        String(50),
        nullable=False,
        unique=True,
        index=True,
        comment="Machine-readable identifier — used in API requests",
    )

    display_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Human-readable name shown in the UI",
    )

    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Short description shown in template picker",
    )

    s3_key: Mapped[str] = mapped_column(
        String(1000),
        nullable=False,
        comment="S3 key of the template DOCX — e.g. templates/classic.docx",
    )

    s3_bucket: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        comment="If False, template is hidden from the picker (deprecated/broken)",
    )

    sort_order: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
        comment="Display order in the template picker",
    )

    # Visual preview metadata (for UI picker)
    preview_metadata: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment=(
            "Preview info for the UI template picker. "
            "Schema: {thumbnail_url, accent_color, font_name, page_count}"
        ),
    )

    def __repr__(self) -> str:
        return f"<CVTemplate slug={self.slug} name={self.display_name}>"
