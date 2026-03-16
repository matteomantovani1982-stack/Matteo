"""
app/models/candidate_profile.py

CandidateProfile and ExperienceEntry — the candidate knowledge layer.

Design philosophy:
  A CV is a marketing document. It hides context, compresses outcomes,
  and omits anything the candidate didn't think to include.

  The CandidateProfile is the engine's source of truth — the full,
  unfiltered story of what the person has actually done. It is built
  once at onboarding and reused across every job application.

  ExperienceEntry maps 1:1 to a role on the candidate's CV, but stores
  the rich structured context that makes Claude's output genuinely
  intelligent rather than just keyword-matched.

Schema:
  users
    └── candidate_profiles  (one active per user)
          └── experience_entries  (one per role, ordered)

  job_runs reference candidate_profile_id so the LLM prompt always
  has access to the full profile, with per-application overrides stored
  in job_run.profile_overrides (JSONB).

Versioning:
  Profiles are versioned (version integer). When a user makes a
  significant update, a new version is created and the old one archived.
  This means past job_runs always reference the profile state that
  was active when they were created — no retroactive changes.
"""

import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDMixin


class ProfileStatus(str, Enum):
    """
    Lifecycle of a candidate profile.

    DRAFT     — being filled in, not yet used for applications
    ACTIVE    — current profile, used for new job_runs
    ARCHIVED  — superseded by a newer version, kept for historical job_runs
    """
    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"


class CandidateProfile(Base, UUIDMixin, TimestampMixin):
    """
    The candidate's full professional story.

    One user can have multiple profiles (versioned), but only one ACTIVE
    at a time. The profile is the primary input to the LLM prompt —
    it provides far richer context than a CV alone.

    Fields:
      user_id         — owner (no FK until auth Sprint; stored as string)
      version         — increments on each significant update
      status          — draft / active / archived
      headline        — candidate's own description of themselves (freeform)
      global_context  — anything that applies across all experiences
                        (e.g. "I'm pivoting from founder back to corporate",
                         "I want to stay in UAE", "I'm targeting strategy roles")
      global_notes    — unpublished achievements, side projects, soft skills,
                        anything that doesn't fit neatly into a single experience
      cv_id           — the CV file this profile is paired with for formatting
    """
    __tablename__ = "candidate_profiles"
    __table_args__ = (
        UniqueConstraint("user_id", "version", name="uq_profile_user_version"),
    )

    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    status: Mapped[ProfileStatus] = mapped_column(
        String(20),
        nullable=False,
        default=ProfileStatus.DRAFT,
        index=True,
    )

    # The candidate's own self-description — used in prompt as context
    headline: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Cross-cutting context: career goals, constraints, pivot intent
    global_context: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Unpublished achievements, skills, side projects not on CV
    global_notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Reference CV for formatting template
    preferences: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True,
        comment="Job search preferences: regions, roles, seniority, company type, stage, sectors",
    )

    cv_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        index=True,
    )

    # ── Relationships ──────────────────────────────────────────────────────
    experiences: Mapped[list["ExperienceEntry"]] = relationship(
        "ExperienceEntry",
        back_populates="profile",
        order_by="ExperienceEntry.display_order",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return (
            f"<CandidateProfile id={self.id} user={self.user_id} "
            f"v{self.version} status={self.status}>"
        )

    @property
    def is_ready(self) -> bool:
        """True if the profile has at least one completed experience."""
        return any(e.is_complete for e in (self.experiences or []))

    def to_prompt_dict(self) -> dict:
        """
        Serialise to a dict suitable for injection into LLM prompts.
        Returns the full candidate knowledge layer.
        """
        return {
            "headline": self.headline or "",
            "global_context": self.global_context or "",
            "global_notes": self.global_notes or "",
            "experiences": [
                e.to_prompt_dict()
                for e in sorted(self.experiences or [], key=lambda x: x.display_order)
            ],
        }


class ExperienceEntry(Base, UUIDMixin, TimestampMixin):
    """
    One role / experience in the candidate's history.

    Maps 1:1 to a position on the CV, but captures the rich context
    that doesn't fit in bullet points.

    The five structured fields correspond to the five intake questions:
      1. context        — situation when they joined / took this role
      2. contribution   — what they specifically owned and drove
      3. outcomes       — what changed because of them (with numbers)
      4. methods        — skills, tools, approaches they used
      5. hidden         — anything important the CV doesn't show

    Plus a freeform field for anything else.

    The engine uses ALL of these when building the LLM prompt —
    structured fields give Claude the signal, freeform gives it nuance.
    """
    __tablename__ = "experience_entries"

    profile_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("candidate_profiles.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # ── Role identification ────────────────────────────────────────────────
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    role_title: Mapped[str] = mapped_column(String(255), nullable=False)
    start_date: Mapped[str | None] = mapped_column(String(20), nullable=True)   # "2021-01" or "2021"
    end_date: Mapped[str | None] = mapped_column(String(20), nullable=True)     # "2023-06" or "Present"
    location: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # ── The five structured intake questions ───────────────────────────────

    # Q1: Context — what was the situation?
    # "Iraq's ride-hailing market was entirely cash-based, no digital infrastructure,
    #  and we were building from scratch with a team of 3."
    context: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Q2: Contribution — what did YOU specifically drive?
    # "I personally built the ops playbook, hired the first 50 people,
    #  and made the call to launch ride-hailing before food delivery."
    contribution: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Q3: Outcomes — what changed because of you?
    # "500+ employees in 2 years. 8-15X valuation. 4 verticals live."
    outcomes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Q4: Methods — how did you do it?
    # "OKR framework, weekly P&L reviews, external tech partners for infra,
    #  direct founder selling for first 100 enterprise clients."
    methods: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Q5: Hidden — what doesn't the CV show?
    # "We nearly ran out of cash in month 8. I renegotiated the Rocket Internet
    #  term sheet while simultaneously closing a local investor. That saved the company."
    hidden: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Freeform overflow ──────────────────────────────────────────────────
    # Anything that doesn't fit the five structured fields
    freeform: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Display ordering ───────────────────────────────────────────────────
    # Matches the order on the CV (0 = most recent, ascending = older)
    display_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # ── Completion flag ────────────────────────────────────────────────────
    # Set to True when the user marks this experience as "done"
    # The engine will only use completed experiences in the prompt
    is_complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # ── Structured metadata (optional, AI-extracted) ───────────────────────
    # After intake, Claude can optionally extract structured signals
    # (key skills, industries, seniority indicators) for future retrieval
    extracted_signals: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # ── Relationship ───────────────────────────────────────────────────────
    profile: Mapped["CandidateProfile"] = relationship(
        "CandidateProfile",
        back_populates="experiences",
    )

    def __repr__(self) -> str:
        return (
            f"<ExperienceEntry id={self.id} "
            f"{self.role_title} @ {self.company_name} "
            f"({self.start_date}–{self.end_date})>"
        )

    @property
    def date_range(self) -> str:
        start = self.start_date or ""
        end = self.end_date or "Present"
        return f"{start}–{end}" if start else end

    @property
    def fields_completed(self) -> int:
        """How many of the 5 structured fields have been filled in."""
        fields = [self.context, self.contribution, self.outcomes, self.methods, self.hidden]
        return sum(1 for f in fields if f and f.strip())

    def to_prompt_dict(self) -> dict:
        """
        Serialise to a dict for LLM prompt injection.
        Only includes non-empty fields to keep prompts concise.
        """
        d: dict = {
            "company": self.company_name,
            "role": self.role_title,
            "dates": self.date_range,
        }
        if self.location:
            d["location"] = self.location
        if self.context:
            d["context"] = self.context.strip()
        if self.contribution:
            d["contribution"] = self.contribution.strip()
        if self.outcomes:
            d["outcomes"] = self.outcomes.strip()
        if self.methods:
            d["methods"] = self.methods.strip()
        if self.hidden:
            d["hidden"] = self.hidden.strip()
        if self.freeform:
            d["additional"] = self.freeform.strip()
        return d
