"""
app/services/profile/profile_service.py

Business logic for CandidateProfile and ExperienceEntry.

Responsibilities:
  - Create and activate candidate profiles
  - CRUD for experience entries
  - Profile versioning (create new version on significant update)
  - Profile → LLM prompt rendering (the core output of this service)
  - Validate profile readiness before job run creation

Profile versioning rules:
  - Minor updates (fixing typos, adding detail) → update in place
  - Major updates (new job, significant rewrite) → user can request
    a new version, which archives the old one
  - job_runs always reference the profile version active at creation time
"""

import uuid

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.middleware.error_handler import NotFoundError, ValidationError
from app.models.candidate_profile import (
    CandidateProfile,
    ExperienceEntry,
    ProfileStatus,
)
from app.schemas.candidate_profile import (
    ApplicationProfileOverrides,
    CandidateProfileCreate,
    CandidateProfileListItem,
    CandidateProfileResponse,
    CandidateProfileUpdate,
    ExperienceEntryCreate,
    ExperienceEntryResponse,
    ExperienceEntryUpdate,
)

import structlog

logger = structlog.get_logger(__name__)


class ProfileService:
    """CRUD and business logic for candidate profiles."""

    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    # ── Profile CRUD ──────────────────────────────────────────────────────

    async def create_profile(
        self,
        user_id: str,
        payload: CandidateProfileCreate,
    ) -> CandidateProfileResponse:
        """
        Create a new DRAFT candidate profile.

        If the user already has an ACTIVE profile, this creates a new DRAFT
        alongside it. The user must explicitly activate the new profile when ready,
        which archives the old one.
        """
        profile = CandidateProfile(
            user_id=user_id,
            version=await self._next_version(user_id),
            status=ProfileStatus.DRAFT,
            headline=payload.headline,
            global_context=payload.global_context,
            global_notes=payload.global_notes,
            cv_id=payload.cv_id,
        )
        self._db.add(profile)
        await self._db.flush()

        logger.info(
            "profile_created",
            extra={"profile_id": str(profile.id), "user_id": user_id},
        )

        # Build response directly — do NOT call from_orm_with_computed here
        # because experiences relationship is not loaded and async lazy-load crashes
        return CandidateProfileResponse(
            id=profile.id,
            user_id=profile.user_id,
            version=profile.version,
            status=profile.status,
            headline=profile.headline,
            global_context=profile.global_context,
            global_notes=profile.global_notes,
            cv_id=profile.cv_id,
            is_ready=False,
            experiences=[],
            created_at=profile.created_at,
            updated_at=profile.updated_at,
        )

    async def get_profile(
        self,
        profile_id: uuid.UUID,
        user_id: str,
    ) -> CandidateProfileResponse:
        """Fetch a profile by ID, ensuring ownership."""
        profile = await self._get_or_404(profile_id, user_id)
        return CandidateProfileResponse.from_orm_with_computed(profile)

    async def get_active_profile(self, user_id: str) -> CandidateProfileResponse | None:
        """Fetch the user's currently ACTIVE profile, or None."""
        result = await self._db.execute(
            select(CandidateProfile)
            .options(selectinload(CandidateProfile.experiences))
            .where(
                CandidateProfile.user_id == user_id,
                CandidateProfile.status == ProfileStatus.ACTIVE,
            )
        )
        profile = result.scalar_one_or_none()
        if profile is None:
            return None
        return CandidateProfileResponse.from_orm_with_computed(profile)

    async def list_profiles(self, user_id: str) -> list[CandidateProfileListItem]:
        """List all profiles for a user, newest first."""
        result = await self._db.execute(
            select(CandidateProfile)
            .options(selectinload(CandidateProfile.experiences))
            .where(CandidateProfile.user_id == user_id)
            .order_by(CandidateProfile.created_at.desc())
        )
        profiles = result.scalars().all()
        return [
            CandidateProfileListItem(
                id=p.id,
                version=p.version,
                status=p.status,
                headline=p.headline,
                is_ready=p.is_ready,
                experience_count=len(p.experiences or []),
                created_at=p.created_at,
                updated_at=p.updated_at,
            )
            for p in profiles
        ]

    async def update_profile(
        self,
        profile_id: uuid.UUID,
        user_id: str,
        payload: CandidateProfileUpdate,
    ) -> CandidateProfileResponse:
        """Update profile-level fields in place (minor update)."""
        profile = await self._get_or_404(profile_id, user_id)

        if payload.headline is not None:
            profile.headline = payload.headline
        if payload.location is not None:
            profile.location = payload.location
        if payload.global_context is not None:
            profile.global_context = payload.global_context
        if payload.global_notes is not None:
            profile.global_notes = payload.global_notes
        if payload.preferences is not None:
            profile.preferences = payload.preferences.model_dump()
        if payload.cv_id is not None:
            profile.cv_id = payload.cv_id

        await self._db.flush()
        # Re-fetch with experiences loaded to avoid async lazy-load crash
        from sqlalchemy import select
        result = await self._db.execute(
            select(CandidateProfile)
            .options(selectinload(CandidateProfile.experiences))
            .where(CandidateProfile.id == profile.id)
        )
        profile = result.scalar_one()
        return CandidateProfileResponse.from_orm_with_computed(profile)

    async def activate_profile(
        self,
        profile_id: uuid.UUID,
        user_id: str,
    ) -> CandidateProfileResponse:
        """
        Activate a DRAFT profile:
          1. Validate it has at least one complete experience
          2. Archive any existing ACTIVE profile
          3. Set this profile to ACTIVE
        """
        profile = await self._get_or_404(profile_id, user_id)

        if profile.status == ProfileStatus.ACTIVE:
            return CandidateProfileResponse.from_orm_with_computed(profile)

        if profile.status == ProfileStatus.ARCHIVED:
            raise ValidationError("Cannot activate an archived profile.")

        if not profile.is_ready:
            raise ValidationError(
                "Profile must have at least one completed experience before activation. "
                "Mark at least one experience as complete to proceed."
            )

        # Archive the current active profile if one exists
        result = await self._db.execute(
            select(CandidateProfile)
            .options(selectinload(CandidateProfile.experiences))
            .where(
                CandidateProfile.user_id == user_id,
                CandidateProfile.status == ProfileStatus.ACTIVE,
            )
        )
        current_active = result.scalar_one_or_none()
        if current_active:
            current_active.status = ProfileStatus.ARCHIVED
            logger.info(
                "profile_archived",
                extra={
                    "profile_id": str(current_active.id),
                    "superseded_by": str(profile_id),
                },
            )

        profile.status = ProfileStatus.ACTIVE
        await self._db.flush()

        logger.info(
            "profile_activated",
            extra={"profile_id": str(profile_id), "user_id": user_id},
        )

        return CandidateProfileResponse.from_orm_with_computed(profile)

    # ── Experience CRUD ────────────────────────────────────────────────────

    async def add_experience(
        self,
        profile_id: uuid.UUID,
        user_id: str,
        payload: ExperienceEntryCreate,
    ) -> ExperienceEntryResponse:
        """Add a new experience entry to a profile."""
        profile = await self._get_or_404(profile_id, user_id)

        # Auto-assign display_order if not provided
        existing_count = len(profile.experiences or [])
        display_order = payload.display_order if payload.display_order else existing_count

        entry = ExperienceEntry(
            profile_id=profile.id,
            company_name=payload.company_name,
            role_title=payload.role_title,
            start_date=payload.start_date,
            end_date=payload.end_date,
            location=payload.location,
            context=payload.context,
            contribution=payload.contribution,
            outcomes=payload.outcomes,
            methods=payload.methods,
            hidden=payload.hidden,
            freeform=payload.freeform,
            display_order=display_order,
            is_complete=payload.is_complete,
        )
        self._db.add(entry)
        await self._db.flush()

        logger.info(
            "experience_added",
            extra={
                "entry_id": str(entry.id),
                "profile_id": str(profile_id),
                "role": f"{payload.role_title} @ {payload.company_name}",
            },
        )

        return ExperienceEntryResponse.from_orm_with_computed(entry)

    async def update_experience(
        self,
        profile_id: uuid.UUID,
        entry_id: uuid.UUID,
        user_id: str,
        payload: ExperienceEntryUpdate,
    ) -> ExperienceEntryResponse:
        """Update an experience entry."""
        entry = await self._get_entry_or_404(
            profile_id=profile_id,
            entry_id=entry_id,
            user_id=user_id,
        )

        update_fields = payload.model_dump(exclude_none=True)
        for field, value in update_fields.items():
            setattr(entry, field, value)

        await self._db.flush()
        return ExperienceEntryResponse.from_orm_with_computed(entry)

    async def delete_experience(
        self,
        profile_id: uuid.UUID,
        entry_id: uuid.UUID,
        user_id: str,
    ) -> None:
        """Delete an experience entry."""
        entry = await self._get_entry_or_404(
            profile_id=profile_id,
            entry_id=entry_id,
            user_id=user_id,
        )
        await self._db.delete(entry)
        await self._db.flush()

    async def reorder_experiences(
        self,
        profile_id: uuid.UUID,
        user_id: str,
        ordered_ids: list[uuid.UUID],
    ) -> CandidateProfileResponse:
        """
        Reorder experiences by providing the full ordered list of IDs.
        Assigns display_order 0, 1, 2... to match the provided order.
        """
        profile = await self._get_or_404(profile_id, user_id)
        entry_map = {e.id: e for e in (profile.experiences or [])}

        for idx, entry_id in enumerate(ordered_ids):
            if entry_id in entry_map:
                entry_map[entry_id].display_order = idx

        await self._db.flush()
        return CandidateProfileResponse.from_orm_with_computed(profile)

    # ── Prompt building ────────────────────────────────────────────────────

    async def build_profile_for_prompt(
        self,
        profile_id: uuid.UUID,
        user_id: str,
        overrides: ApplicationProfileOverrides | None = None,
    ) -> dict:
        """
        Build the full candidate knowledge dict for LLM prompt injection.

        Merges:
          - Base profile (global context, notes)
          - All complete experience entries
          - Per-application overrides (highlight/suppress/additional context)

        Returns a dict with structure:
          {
            "headline": str,
            "global_context": str,
            "global_notes": str,
            "experiences": [
              {
                "company": str,
                "role": str,
                "dates": str,
                "context": str,
                ...
                "override_context": str | None,  # if override exists
                "suppressed": bool,
              }
            ]
          }
        """
        profile = await self._get_or_404(profile_id, user_id)
        base = profile.to_prompt_dict()

        if not overrides:
            return base

        # Apply per-experience overrides
        enriched_experiences = []
        for exp_dict in base["experiences"]:
            # Find the matching entry to get its ID
            exp_id = None
            for entry in (profile.experiences or []):
                if (
                    entry.company_name == exp_dict.get("company")
                    and entry.role_title == exp_dict.get("role")
                ):
                    exp_id = entry.id
                    break

            override = overrides.get_override(exp_id) if exp_id else None

            if override and override.suppress:
                exp_dict["suppressed"] = True
            if override and override.highlight:
                exp_dict["highlight"] = True
            if override and override.additional_context:
                exp_dict["override_context"] = override.additional_context

            enriched_experiences.append(exp_dict)

        base["experiences"] = enriched_experiences

        if overrides.additional_global_context:
            base["application_context"] = overrides.additional_global_context

        return base

    # ── Private helpers ────────────────────────────────────────────────────

    async def _get_or_404(
        self,
        profile_id: uuid.UUID,
        user_id: str,
    ) -> CandidateProfile:
        result = await self._db.execute(
            select(CandidateProfile)
            .options(selectinload(CandidateProfile.experiences))
            .where(
                CandidateProfile.id == profile_id,
                CandidateProfile.user_id == user_id,
            )
        )
        profile = result.scalar_one_or_none()
        if profile is None:
            raise NotFoundError(resource="CandidateProfile", resource_id=str(profile_id))
        return profile

    async def _get_entry_or_404(
        self,
        profile_id: uuid.UUID,
        entry_id: uuid.UUID,
        user_id: str,
    ) -> ExperienceEntry:
        # First verify profile ownership
        await self._get_or_404(profile_id, user_id)

        result = await self._db.execute(
            select(ExperienceEntry).where(
                ExperienceEntry.id == entry_id,
                ExperienceEntry.profile_id == profile_id,
            )
        )
        entry = result.scalar_one_or_none()
        if entry is None:
            raise NotFoundError(resource="ExperienceEntry", resource_id=str(entry_id))
        return entry

    async def _next_version(self, user_id: str) -> int:
        """Return the next version number for a user's profile."""
        result = await self._db.execute(
            select(CandidateProfile.version)
            .where(CandidateProfile.user_id == user_id)
            .order_by(CandidateProfile.version.desc())
            .limit(1)
        )
        latest = result.scalar_one_or_none()
        return (latest or 0) + 1
