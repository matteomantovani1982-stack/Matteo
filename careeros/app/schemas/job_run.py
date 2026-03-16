"""
app/schemas/job_run.py

Pydantic schemas for JobRun API request/response.
"""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.models.job_run import JobRunStatus
from app.models.job_step import StepName, StepStatus


class JobRunPreferences(BaseModel):
    tone: Literal["ats", "executive", "human"] = "executive"
    region: Literal["UAE", "KSA", "EU", "US", "APAC", "OTHER"] = "UAE"
    page_limit: Literal[1, 2, 3, 0] = Field(default=2)
    positioning: Literal["auto", "specialist", "strategic_leader", "generalist", "career_pivot"] = "auto"
    include_unverified: bool = False
    career_notes: str | None = Field(default=None, max_length=5000)


class JobRunCreate(BaseModel):
    """POST /api/v1/jobs — kick off a new Application Intelligence Pack run."""
    cv_id: uuid.UUID = Field(..., description="ID of a previously uploaded and parsed CV")

    jd_text: str | None = Field(default=None, max_length=50_000)
    jd_url: str | None = Field(default=None, max_length=2000)

    # Profile fields — new in Sprint B
    profile_id: uuid.UUID | None = Field(
        default=None,
        description=(
            "Candidate profile to use. If omitted, uses the user's active profile. "
            "If no active profile exists, falls back to CV-only mode."
        ),
    )
    profile_overrides: dict | None = Field(
        default=None,
        description=(
            "Per-application overrides: highlight/suppress/add context "
            "to specific experiences for this role."
        ),
    )

    preferences: JobRunPreferences = Field(default_factory=JobRunPreferences)

    known_contacts: list[str] | None = Field(
        default=None,
        max_length=20,
        description=(
            "People the candidate knows at this company. "
            "e.g. ['Ahmed Al-Rashidi (former colleague at McKinsey)', 'Sarah J. (MBA classmate)']. "
            "CareerOS will write a specific warm-intro ask for each person."
        ),
    )

    @model_validator(mode="after")
    def require_jd_source(self) -> "JobRunCreate":
        if not self.jd_text and not self.jd_url:
            raise ValueError(
                "Provide at least one job description source: "
                "jd_text (pasted text) or jd_url (link to posting)."
            )
        return self


# ── Response schemas ─────────────────────────────────────────────────────────

class JobStepResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    step_name: StepName
    status: StepStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    error_type: str | None = None
    error_message: str | None = None


class JobRunResponse(BaseModel):
    """Returned immediately after POST /api/v1/jobs."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: JobRunStatus
    cv_id: uuid.UUID
    profile_id: uuid.UUID | None = None
    created_at: datetime


class JobRunStatusResponse(BaseModel):
    """Full status — returned by GET /api/v1/jobs/{id}."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: JobRunStatus
    cv_id: uuid.UUID
    profile_id: uuid.UUID | None = None
    created_at: datetime
    updated_at: datetime

    steps: list[JobStepResponse] = Field(default_factory=list)

    # When completed
    download_url: str | None = None
    positioning: dict | None = Field(default=None)
    reports: dict | None = Field(default=None)
    completed_at: datetime | None = None

    # When failed
    failed_step: str | None = None
    error_message: str | None = None


class JobRunListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: JobRunStatus
    cv_id: uuid.UUID
    profile_id: uuid.UUID | None = None
    created_at: datetime
    updated_at: datetime
    failed_step: str | None = None
    jd_text: str | None = None
    jd_url: str | None = None
    keyword_match_score: int | None = None
    pipeline_stage: str | None = 'watching'
    pipeline_notes: str | None = None
    applied_at: datetime | None = None
    output_s3_key: str | None = None
    role_title: str | None = None
    company_name: str | None = None
    apply_url: str | None = None
