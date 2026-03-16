"""
app/schemas/cv.py

Pydantic schemas for CV-related API request/response.

Rules:
- Schemas are NEVER imported into models (one-way dependency)
- Response schemas expose only what the client needs
- Never expose internal S3 keys or raw DB fields directly
"""

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models.cv import CVStatus


# ── Response schemas ────────────────────────────────────────────────────────

class CVUploadResponse(BaseModel):
    """
    Returned immediately after a CV file is accepted.
    The client uses cv_id to create JobRuns and poll for parse status.
    """
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: CVStatus
    original_filename: str
    file_size_bytes: int
    mime_type: str
    created_at: datetime

    # Intentionally excluded: s3_key, s3_bucket, user_id (internal)


class CVStatusResponse(BaseModel):
    """
    Returned when polling GET /api/v1/cvs/{cv_id}/status
    """
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: CVStatus
    error_message: str | None = None

    # Included only when status=PARSED — lets the client verify parse quality
    parsed_section_count: int | None = None
    parsed_word_count: int | None = None

    # CV quality assessment — populated after parse completes
    quality_score: int | None = None
    quality_verdict: str | None = None           # poor | weak | good | strong
    rebuild_recommended: bool | None = None
    build_mode: str | None = None                # edit | rebuild | from_scratch
    template_slug: str | None = None


class CVListItem(BaseModel):
    """
    One item in GET /api/v1/cvs (list endpoint).
    """
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    original_filename: str
    status: CVStatus
    file_size_bytes: int
    created_at: datetime
    quality_score: int | None = None
    build_mode: str | None = None


# ── Internal schemas (not exposed via API) ──────────────────────────────────

class CVCreate(BaseModel):
    """
    Internal schema used by the ingest service to create a CV record.
    Not exposed as an API input — file upload uses multipart form data.
    """
    user_id: str
    original_filename: str
    file_size_bytes: int
    mime_type: str
    s3_key: str
    s3_bucket: str


class ParsedNode(BaseModel):
    """
    One paragraph node extracted from the DOCX.
    Stored in CV.parsed_content.sections[].paragraphs[].
    """
    index: int = Field(..., description="Zero-based paragraph index in the DOCX")
    text: str = Field(..., description="Full plain text of the paragraph")
    style: str = Field(..., description="Paragraph style name, e.g. 'Normal', 'Heading 1'")
    runs: list[dict] = Field(
        default_factory=list,
        description="Individual runs with their formatting: [{text, bold, italic, font_size}]",
    )
    is_empty: bool = Field(default=False)


class ParsedSection(BaseModel):
    """One logical section of the CV (e.g. Experience, Education)."""
    heading: str = Field(..., description="Section heading text")
    heading_index: int = Field(..., description="Paragraph index of the heading")
    paragraphs: list[ParsedNode] = Field(default_factory=list)


class ParsedCV(BaseModel):
    """
    Full structured representation of a parsed CV.
    Stored in CV.parsed_content.
    This is what the LLM receives as input.
    """
    total_paragraphs: int
    total_words: int
    sections: list[ParsedSection] = Field(default_factory=list)
    # Raw flat list for cases where section detection fails
    raw_paragraphs: list[ParsedNode] = Field(default_factory=list)
