"""
app/api/routes/cv_builder.py

CV Builder endpoints — handles the three user flows:

  1. User has a CV:
     GET /api/v1/cv-builder/quality/{cv_id}
     Returns the quality score + recommendation.

  2. User chooses build mode:
     PATCH /api/v1/cv-builder/mode/{cv_id}
     Sets build_mode (edit | rebuild | from_scratch) and optional template.

  3. User has no CV — start from scratch:
     POST /api/v1/cv-builder/scratch
     Creates a placeholder CV record (no file) with build_mode=from_scratch.

  4. Template picker:
     GET /api/v1/cv-builder/templates
     Returns available DOCX templates.
"""

import uuid

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.dependencies import DB, CurrentUserId
from app.models.cv import CVBuildMode

router = APIRouter(prefix="/api/v1/cv-builder", tags=["CV Builder"])


# ── Request / response models ─────────────────────────────────────────────────

class QualityResponse(BaseModel):
    cv_id: uuid.UUID
    score: int
    verdict: str                     # poor | weak | good | strong
    top_issues: list[str]
    recommendation: str
    rebuild_recommended: bool


class SetBuildModeRequest(BaseModel):
    build_mode: str                  # edit | rebuild | from_scratch
    template_slug: str | None = None # Required for rebuild / from_scratch


class SetBuildModeResponse(BaseModel):
    cv_id: uuid.UUID
    build_mode: str
    template_slug: str | None
    message: str


class CreateScratchCVRequest(BaseModel):
    template_slug: str               # Must pick a template when starting from scratch


class CreateScratchCVResponse(BaseModel):
    cv_id: uuid.UUID
    build_mode: str
    template_slug: str
    message: str


class TemplateResponse(BaseModel):
    slug: str
    display_name: str
    description: str | None
    sort_order: int
    preview_metadata: dict | None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get(
    "/templates",
    response_model=list[TemplateResponse],
    summary="List available CV templates",
    description=(
        "Returns templates available for rebuild and from-scratch CV generation. "
        "Used in the template picker UI."
    ),
)
async def list_templates(db: DB) -> list[TemplateResponse]:
    from sqlalchemy import select
    from app.models.cv_template import CVTemplate

    result = await db.execute(
        select(CVTemplate)
        .where(CVTemplate.is_active == True)
        .order_by(CVTemplate.sort_order)
    )
    templates = result.scalars().all()

    # If no templates in DB yet, return hardcoded defaults
    if not templates:
        return _default_templates()

    return [
        TemplateResponse(
            slug=t.slug,
            display_name=t.display_name,
            description=t.description,
            sort_order=t.sort_order,
            preview_metadata=t.preview_metadata,
        )
        for t in templates
    ]


@router.get(
    "/quality/{cv_id}",
    response_model=QualityResponse,
    summary="Get CV quality score and recommendation",
    description=(
        "Returns the automated quality assessment for an uploaded CV. "
        "If quality_score is null (parse not yet complete), poll again. "
        "rebuild_recommended=true means the user should consider rebuilding."
    ),
)
async def get_quality(
    cv_id: uuid.UUID,
    db: DB,
    x_user_id: CurrentUserId,
) -> QualityResponse:
    from app.models.cv import CV

    cv = await db.get(CV, cv_id)
    if cv is None or str(cv.user_id) != x_user_id:
        raise HTTPException(status_code=404, detail="CV not found")

    if cv.quality_score is None:
        raise HTTPException(
            status_code=202,
            detail="Quality scoring not yet complete. The CV is still being parsed.",
        )

    feedback = cv.quality_feedback or {}
    return QualityResponse(
        cv_id=cv.id,
        score=cv.quality_score,
        verdict=feedback.get("verdict", "good"),
        top_issues=feedback.get("top_issues", []),
        recommendation=feedback.get("recommendation", ""),
        rebuild_recommended=feedback.get("rebuild_recommended", False),
    )


class BestPracticesResponse(BaseModel):
    cv_id: uuid.UUID
    suggestions: list[dict]
    top_strength: str
    summary: str


@router.get(
    "/feedback/{cv_id}",
    response_model=BestPracticesResponse,
    summary="Get best-practices feedback for a CV",
    description=(
        "Returns practical, actionable suggestions for improving the CV. "
        "Generated automatically after upload — this endpoint retrieves stored results. "
        "Returns 202 if analysis is still running."
    ),
)
async def get_feedback(
    cv_id: uuid.UUID,
    db: DB,
    x_user_id: CurrentUserId,
) -> BestPracticesResponse:
    from app.models.cv import CV

    cv = await db.get(CV, cv_id)
    if cv is None or str(cv.user_id) != x_user_id:
        raise HTTPException(status_code=404, detail="CV not found")

    feedback = cv.quality_feedback or {}
    suggestions = feedback.get("suggestions", [])

    if not suggestions and cv.quality_score is None:
        raise HTTPException(
            status_code=202,
            detail="Analysis not yet complete. The CV is still being parsed.",
        )

    return BestPracticesResponse(
        cv_id=cv.id,
        suggestions=suggestions,
        top_strength=feedback.get("top_strength", ""),
        summary=feedback.get("bp_summary", ""),
    )


@router.patch(
    "/mode/{cv_id}",
    response_model=SetBuildModeResponse,
    summary="Set build mode for a CV",
    description=(
        "Sets whether the output CV will be generated by editing the uploaded document (edit), "
        "rebuilding content from scratch using a template (rebuild), "
        "or building from scratch with no uploaded document (from_scratch). "
        "Must be called before creating a job run."
    ),
)
async def set_build_mode(
    cv_id: uuid.UUID,
    payload: SetBuildModeRequest,
    db: DB,
    x_user_id: CurrentUserId,
) -> SetBuildModeResponse:
    from app.models.cv import CV

    # Validate build_mode value
    try:
        mode = CVBuildMode(payload.build_mode)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid build_mode. Must be one of: {[m.value for m in CVBuildMode]}",
        )

    # Require template for rebuild / from_scratch
    if mode in (CVBuildMode.REBUILD, CVBuildMode.FROM_SCRATCH) and not payload.template_slug:
        raise HTTPException(
            status_code=400,
            detail="template_slug is required when build_mode is 'rebuild' or 'from_scratch'.",
        )

    cv = await db.get(CV, cv_id)
    if cv is None or str(cv.user_id) != x_user_id:
        raise HTTPException(status_code=404, detail="CV not found")

    cv.build_mode = mode.value
    cv.template_slug = payload.template_slug
    await db.commit()

    mode_messages = {
        CVBuildMode.EDIT: "CV will be edited using the uploaded document as the base.",
        CVBuildMode.REBUILD: "CV content will be rebuilt from your profile. Your document's layout will be preserved.",
        CVBuildMode.FROM_SCRATCH: "A new CV will be generated from scratch using the selected template.",
    }

    return SetBuildModeResponse(
        cv_id=cv.id,
        build_mode=cv.build_mode,
        template_slug=cv.template_slug,
        message=mode_messages[mode],
    )


@router.post(
    "/scratch",
    status_code=status.HTTP_201_CREATED,
    response_model=CreateScratchCVResponse,
    summary="Start a CV from scratch (no upload needed)",
    description=(
        "Creates a placeholder CV record with build_mode=from_scratch. "
        "Use this when the user has no existing CV to upload. "
        "A template must be selected. The CV content will be generated entirely "
        "from the candidate profile when the first job run is created."
    ),
)
async def create_scratch_cv(
    payload: CreateScratchCVRequest,
    db: DB,
    x_user_id: CurrentUserId,
) -> CreateScratchCVResponse:
    from app.models.cv import CV, CVStatus

    cv = CV(
        user_id=x_user_id,
        original_filename="[Generated from profile]",
        file_size_bytes=0,
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        s3_key=f"scratch/{x_user_id}/{uuid.uuid4()}/placeholder",
        s3_bucket="",     # No real file — renderer will use template
        status=CVStatus.PARSED,     # Skip parse step — nothing to parse
        build_mode=CVBuildMode.FROM_SCRATCH.value,
        template_slug=payload.template_slug,
        quality_score=None,
        quality_feedback=None,
        parsed_content={"sections": [], "total_words": 0, "total_paragraphs": 0},
    )
    db.add(cv)
    await db.commit()
    await db.refresh(cv)

    return CreateScratchCVResponse(
        cv_id=cv.id,
        build_mode=cv.build_mode,
        template_slug=cv.template_slug,
        message=(
            "Placeholder CV created. Your CV will be generated from your profile "
            "when you create your first job run."
        ),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _default_templates() -> list[TemplateResponse]:
    """Fallback if cv_templates table is empty."""
    return [
        TemplateResponse(
            slug="classic",
            display_name="Classic",
            description="Traditional chronological format. Conservative, widely accepted.",
            sort_order=0,
            preview_metadata={"accent_color": "#1a1a2e", "font_name": "Times New Roman"},
        ),
        TemplateResponse(
            slug="modern",
            display_name="Modern",
            description="Clean sans-serif, subtle accents. ATS-friendly, contemporary.",
            sort_order=1,
            preview_metadata={"accent_color": "#2d6cdf", "font_name": "Calibri"},
        ),
        TemplateResponse(
            slug="executive",
            display_name="Executive",
            description="Wide margins, strong hierarchy. Suited to director and C-suite roles.",
            sort_order=2,
            preview_metadata={"accent_color": "#1c1c1c", "font_name": "Garamond"},
        ),
        TemplateResponse(
            slug="compact",
            display_name="Compact",
            description="One-page optimised. Tight spacing for early-career professionals.",
            sort_order=3,
            preview_metadata={"accent_color": "#2c6e49", "font_name": "Arial"},
        ),
        TemplateResponse(
            slug="minimal",
            display_name="Minimal",
            description="Text only, maximum ATS compatibility. No formatting distractions.",
            sort_order=4,
            preview_metadata={"accent_color": "#000000", "font_name": "Arial"},
        ),
    ]
