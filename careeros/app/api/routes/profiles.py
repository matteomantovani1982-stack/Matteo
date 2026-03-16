"""
app/api/routes/profiles.py

Candidate Profile endpoints.

Routes:
  GET    /api/v1/profile/questions          — intake question definitions (for frontend)
  POST   /api/v1/profiles                   — create new profile
  GET    /api/v1/profiles                   — list user's profiles
  GET    /api/v1/profiles/active            — get active profile
  GET    /api/v1/profiles/{id}              — get specific profile
  PATCH  /api/v1/profiles/{id}              — update profile-level fields
  POST   /api/v1/profiles/{id}/activate     — activate a draft profile

  POST   /api/v1/profiles/{id}/experiences          — add experience entry
  PATCH  /api/v1/profiles/{id}/experiences/{eid}    — update experience
  DELETE /api/v1/profiles/{id}/experiences/{eid}    — delete experience
  POST   /api/v1/profiles/{id}/experiences/reorder  — reorder experiences
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Header, status

from app.dependencies import DB, CurrentUserId
from app.schemas.candidate_profile import (
    ApplicationProfileOverrides,
    CandidateProfileCreate,
    CandidateProfileListItem,
    CandidateProfileResponse,
    CandidateProfileUpdate,
    ExperienceEntryCreate,
    ExperienceEntryResponse,
    ExperienceEntryUpdate,
    IntakeQuestionsResponse,
)
from app.services.profile.profile_service import ProfileService

router = APIRouter(tags=["Candidate Profile"])




def _service(db: DB) -> ProfileService:
    return ProfileService(db=db)


# ── Intake questions ──────────────────────────────────────────────────────────

@router.get(
    "/api/v1/profile/questions",
    response_model=IntakeQuestionsResponse,
    summary="Get intake question definitions",
    description=(
        "Returns the five structured intake questions with labels, prompts, "
        "and placeholders. Frontend renders these dynamically."
    ),
)
async def get_intake_questions() -> IntakeQuestionsResponse:
    return IntakeQuestionsResponse.build()


# ── Profile CRUD ──────────────────────────────────────────────────────────────

@router.post(
    "/api/v1/profiles",
    status_code=status.HTTP_201_CREATED,
    response_model=CandidateProfileResponse,
    summary="Create candidate profile",
)
async def create_profile(
    payload: CandidateProfileCreate,
    db: DB,
    x_user_id: CurrentUserId,
) -> CandidateProfileResponse:
    return await _service(db).create_profile(user_id=x_user_id, payload=payload)


@router.get(
    "/api/v1/profiles",
    response_model=list[CandidateProfileListItem],
    summary="List candidate profiles",
)
async def list_profiles(
    db: DB,
    x_user_id: CurrentUserId,
) -> list[CandidateProfileListItem]:
    return await _service(db).list_profiles(user_id=x_user_id)


@router.get(
    "/api/v1/profiles/active",
    response_model=CandidateProfileResponse | None,
    summary="Get active candidate profile",
)
async def get_active_profile(
    db: DB,
    x_user_id: CurrentUserId,
) -> CandidateProfileResponse | None:
    return await _service(db).get_active_profile(user_id=x_user_id)


@router.get(
    "/api/v1/profiles/{profile_id}",
    response_model=CandidateProfileResponse,
    summary="Get candidate profile by ID",
)
async def get_profile(
    profile_id: uuid.UUID,
    db: DB,
    x_user_id: CurrentUserId,
) -> CandidateProfileResponse:
    return await _service(db).get_profile(
        profile_id=profile_id,
        user_id=x_user_id,
    )


@router.patch(
    "/api/v1/profiles/{profile_id}",
    response_model=CandidateProfileResponse,
    summary="Update profile-level fields",
)
async def update_profile(
    profile_id: uuid.UUID,
    payload: CandidateProfileUpdate,
    db: DB,
    x_user_id: CurrentUserId,
) -> CandidateProfileResponse:
    return await _service(db).update_profile(
        profile_id=profile_id,
        user_id=x_user_id,
        payload=payload,
    )


@router.post(
    "/api/v1/profiles/{profile_id}/activate",
    response_model=CandidateProfileResponse,
    summary="Activate a draft profile",
    description=(
        "Validates the profile has at least one complete experience, "
        "archives any existing active profile, and activates this one."
    ),
)
async def activate_profile(
    profile_id: uuid.UUID,
    db: DB,
    x_user_id: CurrentUserId,
) -> CandidateProfileResponse:
    return await _service(db).activate_profile(
        profile_id=profile_id,
        user_id=x_user_id,
    )


# ── Experience CRUD ───────────────────────────────────────────────────────────

@router.post(
    "/api/v1/profiles/{profile_id}/experiences",
    status_code=status.HTTP_201_CREATED,
    response_model=ExperienceEntryResponse,
    summary="Add experience entry",
)
async def add_experience(
    profile_id: uuid.UUID,
    payload: ExperienceEntryCreate,
    db: DB,
    x_user_id: CurrentUserId,
) -> ExperienceEntryResponse:
    return await _service(db).add_experience(
        profile_id=profile_id,
        user_id=x_user_id,
        payload=payload,
    )


@router.patch(
    "/api/v1/profiles/{profile_id}/experiences/{entry_id}",
    response_model=ExperienceEntryResponse,
    summary="Update experience entry",
)
async def update_experience(
    profile_id: uuid.UUID,
    entry_id: uuid.UUID,
    payload: ExperienceEntryUpdate,
    db: DB,
    x_user_id: CurrentUserId,
) -> ExperienceEntryResponse:
    return await _service(db).update_experience(
        profile_id=profile_id,
        entry_id=entry_id,
        user_id=x_user_id,
        payload=payload,
    )


@router.delete(
    "/api/v1/profiles/{profile_id}/experiences/{entry_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete experience entry",
)
async def delete_experience(
    profile_id: uuid.UUID,
    entry_id: uuid.UUID,
    db: DB,
    x_user_id: CurrentUserId,
) -> None:
    await _service(db).delete_experience(
        profile_id=profile_id,
        entry_id=entry_id,
        user_id=x_user_id,
    )


@router.post(
    "/api/v1/profiles/{profile_id}/experiences/reorder",
    response_model=CandidateProfileResponse,
    summary="Reorder experience entries",
    description="Pass the full list of experience IDs in desired order (index 0 = most recent).",
)
async def reorder_experiences(
    profile_id: uuid.UUID,
    ordered_ids: list[uuid.UUID],
    db: DB,
    x_user_id: CurrentUserId,
) -> CandidateProfileResponse:
    return await _service(db).reorder_experiences(
        profile_id=profile_id,
        user_id=x_user_id,
        ordered_ids=ordered_ids,
    )
