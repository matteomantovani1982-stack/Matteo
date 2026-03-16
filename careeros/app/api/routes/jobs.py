"""
app/api/routes/jobs.py

Job run endpoints — create and poll Application Intelligence Pack runs.

Routes:
  POST /api/v1/jobs             Create a new run (CV + JD → Intelligence Pack)
  GET  /api/v1/jobs             List user's job runs
  GET  /api/v1/jobs/{id}        Get full status + step detail
  GET  /api/v1/jobs/{id}/download  Generate pre-signed download URL for output DOCX

Design:
  - No business logic here — delegate to JobRunService
  - POST returns 202 immediately; processing is fully async
  - GET /status includes per-step progress for frontend polling
  - Download URL is generated on demand (1hr TTL pre-signed S3 URL)
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel

from app.dependencies import DB, CurrentUserId, CurrentUser, S3Client
from app.services.billing.quota_guard import QuotaCheck, PlanFeatures
from app.schemas.job_run import (
    JobRunCreate,
    JobRunListItem,
    JobRunResponse,
    JobRunStatusResponse,
)
from app.services.ingest.storage import S3StorageService
from app.services.jobs.job_run_service import JobRunService

router = APIRouter(
    prefix="/api/v1/jobs",
    tags=["Job Runs"],
)


# ── JD extraction ──────────────────────────────────────────────────────────────

class JDExtractRequest(BaseModel):
    url: str


class JDExtractResponse(BaseModel):
    url: str
    jd_text: str
    char_count: int


@router.post(
    "/extract-jd",
    response_model=JDExtractResponse,
    summary="Extract job description from a URL",
    description=(
        "Fetches the given URL and extracts clean job description text. "
        "Works with LinkedIn, company careers pages, Bayt, GulfTalent, and most job boards. "
        "LinkedIn may block automated access — if it fails, paste the text manually."
    ),
)
async def extract_jd(
    payload: JDExtractRequest,
    current_user: CurrentUser,
) -> JDExtractResponse:
    from app.services.jd.extractor import JDExtractor, JDExtractionError
    extractor = JDExtractor()
    try:
        jd_text = await extractor.extract(payload.url)
    except JDExtractionError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return JDExtractResponse(
        url=payload.url,
        jd_text=jd_text,
        char_count=len(jd_text),
    )




def _make_service(db: DB, s3_client: S3Client) -> JobRunService:
    storage = S3StorageService(client=s3_client)
    return JobRunService(db=db, storage=storage)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobRunResponse,
    summary="Create a new Application Intelligence Pack run",
    description=(
        "Starts an async pipeline: CV parse check → retrieval → LLM → DOCX render. "
        "Returns immediately with a job_run_id. Poll GET /api/v1/jobs/{id} for progress."
    ),
)
async def create_job_run(
    payload: JobRunCreate,
    db: DB,
    s3_client: S3Client,
    x_user_id: CurrentUserId,
    _quota: QuotaCheck,
    plan_features: PlanFeatures,
) -> JobRunResponse:
    service = _make_service(db=db, s3_client=s3_client)
    return await service.create_run(
        user_id=x_user_id,
        payload=payload,
        plan_features=plan_features,
    )


@router.get(
    "",
    response_model=list[JobRunListItem],
    summary="List job runs",
)
async def list_job_runs(
    db: DB,
    s3_client: S3Client,
    x_user_id: CurrentUserId,
) -> list[JobRunListItem]:
    service = _make_service(db=db, s3_client=s3_client)
    return await service.list_runs(user_id=x_user_id)


@router.get(
    "/{job_run_id}",
    response_model=JobRunStatusResponse,
    summary="Get job run status and step detail",
    description=(
        "Poll this endpoint after creating a run. "
        "When status=completed, a download_url is included."
    ),
)
async def get_job_run_status(
    job_run_id: uuid.UUID,
    db: DB,
    s3_client: S3Client,
    x_user_id: CurrentUserId,
) -> JobRunStatusResponse:
    service = _make_service(db=db, s3_client=s3_client)
    return await service.get_status(
        job_run_id=job_run_id,
        user_id=x_user_id,
    )


@router.get(
    "/{job_run_id}/download",
    summary="Get pre-signed download URL for output DOCX",
    description="Returns a time-limited URL (1 hour) to download the tailored CV.",
)
async def get_download_url(
    job_run_id: uuid.UUID,
    db: DB,
    s3_client: S3Client,
    x_user_id: CurrentUserId,
) -> dict:
    service = _make_service(db=db, s3_client=s3_client)
    url = await service.get_download_url(
        job_run_id=job_run_id,
        user_id=x_user_id,
    )
    return {"download_url": url, "expires_in_seconds": 3600}


# ── Pipeline stage update ─────────────────────────────────────────────────────

class PipelineUpdateRequest(BaseModel):
    stage: str
    notes: str | None = None


@router.patch(
    "/{run_id}/stage",
    summary="Update pipeline stage for a job run",
)
async def update_pipeline_stage(
    run_id: uuid.UUID,
    payload: PipelineUpdateRequest,
    db: DB,
    x_user_id: CurrentUserId,
) -> dict:
    from app.models.job_run import JobRun
    from sqlalchemy import select
    from datetime import datetime, timezone

    VALID_STAGES = {"watching", "applied", "interviewing", "offer", "rejected", "withdrawn"}
    if payload.stage not in VALID_STAGES:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {VALID_STAGES}")

    result = await db.execute(
        select(JobRun).where(JobRun.id == run_id, JobRun.user_id == uuid.UUID(x_user_id))
    )
    job_run = result.scalar_one_or_none()
    if not job_run:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Job run not found")

    job_run.pipeline_stage = payload.stage
    if payload.notes is not None:
        job_run.pipeline_notes = payload.notes
    if payload.stage == "applied" and not job_run.applied_at:
        job_run.applied_at = datetime.now(timezone.utc)

    await db.flush()
    await db.commit()
    return {"id": str(job_run.id), "pipeline_stage": job_run.pipeline_stage, "pipeline_notes": job_run.pipeline_notes}
