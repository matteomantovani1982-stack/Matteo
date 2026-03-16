"""
app/schemas/candidate_profile.py

Pydantic schemas for CandidateProfile and ExperienceEntry.

Three layers:
  - Create schemas  (what the API accepts)
  - Update schemas  (partial updates, all fields optional)
  - Response schemas (what the API returns)

The intake questions are surfaced here with their full prompts
so the frontend can render them dynamically from the schema.
"""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


# ── Intake question definitions ───────────────────────────────────────────────
# These are the five structured questions asked per experience.
# The frontend renders these as labelled text areas.
# Stored here (not hardcoded in the frontend) so they can evolve centrally.

INTAKE_QUESTIONS = {
    "context": {
        "label": "Context & Situation",
        "prompt": (
            "What was the situation when you joined or took this role? "
            "Think: company stage, team size, what was missing or broken, "
            "what you inherited."
        ),
        "placeholder": "e.g. The company had just raised Series A but had no operational structure. "
                       "I was the first non-technical hire.",
        "required": False,
    },
    "contribution": {
        "label": "Your Specific Contribution",
        "prompt": (
            "What did YOU specifically own and drive — not the team, not the company. "
            "What decisions, initiatives, or workstreams were yours?"
        ),
        "placeholder": "e.g. I personally built the go-to-market playbook, "
                       "made the call to launch in Baghdad before Basra, "
                       "and negotiated our anchor partnership with the municipality.",
        "required": False,
    },
    "outcomes": {
        "label": "Outcomes & Impact",
        "prompt": (
            "What changed because of you? Include numbers wherever possible — "
            "revenue, headcount, valuation, time saved, cost reduced, users acquired."
        ),
        "placeholder": "e.g. Grew from 3 to 500+ employees in 24 months. "
                       "Achieved 8-15X valuation growth. Launched 4 verticals.",
        "required": False,
    },
    "methods": {
        "label": "How You Did It",
        "prompt": (
            "What skills, frameworks, tools, or approaches did you use? "
            "This is where methodology lives — the HOW behind the outcomes."
        ),
        "placeholder": "e.g. OKR-based planning, weekly P&L reviews with BU heads, "
                       "direct founder selling for first 50 enterprise clients, "
                       "McKinsey issue tree for market sizing.",
        "required": False,
    },
    "hidden": {
        "label": "What the CV Doesn't Show",
        "prompt": (
            "What's important about this role that didn't make it onto your CV? "
            "The hardest challenge, the near-failure, the thing you're most proud of "
            "that sounds too specific to include in a bullet point."
        ),
        "placeholder": "e.g. We nearly ran out of cash in month 8. "
                       "I renegotiated our parent company term sheet while simultaneously "
                       "closing a local investor. That saved the business.",
        "required": False,
    },
}


# ── ExperienceEntry schemas ───────────────────────────────────────────────────

class ExperienceEntryCreate(BaseModel):
    """Create a new experience entry."""
    company_name: str = Field(..., min_length=1, max_length=255)
    role_title: str = Field(..., min_length=1, max_length=255)
    start_date: str | None = Field(None, description="e.g. '2021-01' or '2021'")
    end_date: str | None = Field(None, description="e.g. '2023-06' or 'Present'")
    location: str | None = Field(None, max_length=100)

    # Five structured intake fields
    context: str | None = Field(None, description=INTAKE_QUESTIONS["context"]["prompt"])
    contribution: str | None = Field(None, description=INTAKE_QUESTIONS["contribution"]["prompt"])
    outcomes: str | None = Field(None, description=INTAKE_QUESTIONS["outcomes"]["prompt"])
    methods: str | None = Field(None, description=INTAKE_QUESTIONS["methods"]["prompt"])
    hidden: str | None = Field(None, description=INTAKE_QUESTIONS["hidden"]["prompt"])

    # Freeform overflow
    freeform: str | None = Field(None, description="Anything else not captured above")

    display_order: int = Field(0, ge=0)
    is_complete: bool = Field(False)

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def strip_date(cls, v: str | None) -> str | None:
        return v.strip() if v else None


class ExperienceEntryUpdate(BaseModel):
    """Partial update — all fields optional."""
    company_name: str | None = Field(None, min_length=1, max_length=255)
    role_title: str | None = Field(None, min_length=1, max_length=255)
    start_date: str | None = None
    end_date: str | None = None
    location: str | None = None
    context: str | None = None
    contribution: str | None = None
    outcomes: str | None = None
    methods: str | None = None
    hidden: str | None = None
    freeform: str | None = None
    display_order: int | None = None
    is_complete: bool | None = None


class ExperienceEntryResponse(BaseModel):
    """Full experience entry response."""
    id: uuid.UUID
    profile_id: uuid.UUID
    company_name: str
    role_title: str
    start_date: str | None
    end_date: str | None
    location: str | None
    context: str | None
    contribution: str | None
    outcomes: str | None
    methods: str | None
    hidden: str | None
    freeform: str | None
    display_order: int
    is_complete: bool
    fields_completed: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_with_computed(cls, entry) -> "ExperienceEntryResponse":
        return cls(
            id=entry.id,
            profile_id=entry.profile_id,
            company_name=entry.company_name,
            role_title=entry.role_title,
            start_date=entry.start_date,
            end_date=entry.end_date,
            location=entry.location,
            context=entry.context,
            contribution=entry.contribution,
            outcomes=entry.outcomes,
            methods=entry.methods,
            hidden=entry.hidden,
            freeform=entry.freeform,
            display_order=entry.display_order,
            is_complete=entry.is_complete,
            fields_completed=entry.fields_completed,
            created_at=entry.created_at,
            updated_at=entry.updated_at,
        )


# ── CandidateProfile schemas ──────────────────────────────────────────────────

class CandidateProfileCreate(BaseModel):
    """
    Create a new candidate profile.
    Experiences can be added later via the experience endpoints.
    """
    headline: str | None = Field(
        None,
        description="How would you describe yourself in one sentence? "
                    "This is YOUR words, not a CV headline.",
        max_length=500,
    )
    global_context: str | None = Field(
        None,
        description=(
            "What's your career context right now? "
            "Are you pivoting? Returning to a certain type of role? "
            "Any constraints (location, sector, seniority)? "
            "The engine uses this to understand your intent."
        ),
    )
    global_notes: str | None = Field(
        None,
        description=(
            "Anything that doesn't fit into a single experience: "
            "unpublished achievements, side projects, soft skills, "
            "things you're proud of that your CV doesn't capture."
        ),
    )
    cv_id: uuid.UUID | None = Field(
        None,
        description="The CV file to use as formatting template for this profile.",
    )


class JobPreferences(BaseModel):
    regions: list[str] = []
    roles: list[str] = []
    seniority: list[str] = []
    companyType: list[str] = []
    stage: list[str] = []
    sectors: list[str] = []
    salaryMin: str | None = None
    openToRelo: str = "yes"


class CandidateProfileUpdate(BaseModel):
    """Partial update to profile-level fields."""
    headline: str | None = None
    location: str | None = None
    global_context: str | None = None
    global_notes: str | None = None
    cv_id: uuid.UUID | None = None
    preferences: JobPreferences | None = None


class CandidateProfileResponse(BaseModel):
    """Full profile response including all experiences."""
    id: uuid.UUID
    user_id: str
    version: int
    status: str
    headline: str | None
    location: str | None = None
    global_context: str | None
    global_notes: str | None
    cv_id: uuid.UUID | None
    is_ready: bool
    preferences: dict | None = None
    experiences: list[ExperienceEntryResponse]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_with_computed(cls, profile) -> "CandidateProfileResponse":
        return cls(
            id=profile.id,
            user_id=profile.user_id,
            version=profile.version,
            status=profile.status,
            headline=profile.headline,
            location=getattr(profile, 'location', None),
            global_context=profile.global_context,
            global_notes=profile.global_notes,
            cv_id=profile.cv_id,
            is_ready=profile.is_ready,
            preferences=profile.preferences,
            experiences=[
                ExperienceEntryResponse.from_orm_with_computed(e)
                for e in sorted(profile.experiences or [], key=lambda x: x.display_order)
            ],
            created_at=profile.created_at,
            updated_at=profile.updated_at,
        )


class CandidateProfileListItem(BaseModel):
    """Lightweight profile item for list responses."""
    id: uuid.UUID
    version: int
    status: str
    headline: str | None
    is_ready: bool
    experience_count: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ── Intake questions schema (for frontend rendering) ─────────────────────────

class IntakeQuestion(BaseModel):
    """One intake question definition — returned by the API for frontend rendering."""
    field: str
    label: str
    prompt: str
    placeholder: str
    required: bool


class IntakeQuestionsResponse(BaseModel):
    """All intake questions — frontend fetches this to render the intake form."""
    questions: list[IntakeQuestion]

    @classmethod
    def build(cls) -> "IntakeQuestionsResponse":
        return cls(
            questions=[
                IntakeQuestion(field=k, **v)
                for k, v in INTAKE_QUESTIONS.items()
            ]
        )


# ── Per-application profile override ─────────────────────────────────────────

class ExperienceOverride(BaseModel):
    """
    Per-application override for a specific experience.
    User can add role-specific context on top of their base profile
    when creating a job_run — without modifying the global profile.
    """
    experience_id: uuid.UUID
    additional_context: str | None = Field(
        None,
        description="Extra context for this specific application. "
                    "e.g. 'For this role, emphasise the fintech angle of Baly'",
    )
    highlight: bool = Field(
        False,
        description="If True, this experience should be foregrounded in the tailored CV.",
    )
    suppress: bool = Field(
        False,
        description="If True, de-emphasise or omit this experience for this application.",
    )


class ApplicationProfileOverrides(BaseModel):
    """
    Stored in job_run.profile_overrides (JSONB).
    Allows per-application customisation without mutating the base profile.
    """
    experience_overrides: list[ExperienceOverride] = Field(default_factory=list)
    additional_global_context: str | None = Field(
        None,
        description="Application-specific global context, e.g. positioning preference "
                    "for this role that differs from the default.",
    )

    def get_override(self, experience_id: uuid.UUID) -> ExperienceOverride | None:
        for o in self.experience_overrides:
            if o.experience_id == experience_id:
                return o
        return None
