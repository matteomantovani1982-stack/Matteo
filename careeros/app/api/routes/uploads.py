"""
app/api/routes/uploads.py

CV upload and status endpoints.

Routes:
  POST   /api/v1/cvs          Upload a CV file (DOCX or PDF)
  GET    /api/v1/cvs          List user's uploaded CVs
  GET    /api/v1/cvs/{cv_id}  Get parse status of a specific CV

Design rules enforced here:
  - No business logic — delegate everything to CVService
  - No direct DB or S3 access — injected via dependencies
  - File data read into memory here (max 10MB enforced by service)
  - user_id sourced from API key header for now (replace with JWT later)
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, File, Form, Header, UploadFile, status

from app.dependencies import DB, CurrentUserId, S3Client
from app.schemas.cv import CVListItem, CVStatusResponse, CVUploadResponse
from app.services.ingest.cv_service import CVService
from app.services.ingest.storage import S3StorageService

router = APIRouter(
    prefix="/api/v1/cvs",
    tags=["CV Upload"],
)

# ── Temporary user identity ─────────────────────────────────────────────────
# In production this will come from a JWT / auth middleware.
# For now, clients pass X-User-Id header.
# This is intentionally simple — auth is not in Sprint 1 scope.



# ── Helper: build service from injected deps ────────────────────────────────

def _make_cv_service(db: DB, s3_client: S3Client) -> CVService:
    """
    Constructs a CVService from injected dependencies.
    Wraps the raw boto3 client in our S3StorageService abstraction.
    """
    storage = S3StorageService(client=s3_client)
    return CVService(db=db, storage=storage)


# ── Routes ──────────────────────────────────────────────────────────────────

@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=CVUploadResponse,
    summary="Upload a CV file",
    description=(
        "Upload a DOCX or PDF CV file. "
        "Returns immediately with status=uploaded. "
        "Parsing runs asynchronously — poll GET /api/v1/cvs/{cv_id} for status."
    ),
)
async def upload_cv(
    db: DB,
    s3_client: S3Client,
    x_user_id: CurrentUserId,
    file: UploadFile = File(
        ...,
        description="CV file to upload. Must be .docx or .pdf, max 10MB.",
    ),
) -> CVUploadResponse:
    """
    Accepts a multipart file upload.

    Flow:
    1. Read file bytes into memory
    2. Delegate to CVService.upload_cv (validate → S3 → DB → dispatch task)
    3. Return 202 Accepted with CV id and initial status

    Why 202 and not 201?
    The resource (parsed CV) isn't ready yet — parsing is async.
    202 correctly signals "accepted for processing".
    """
    # Read file bytes — FastAPI's UploadFile is a SpooledTemporaryFile
    file_data = await file.read()

    # Normalise content_type — browsers often send wrong MIME for .docx
    # Always derive from filename extension to be safe
    filename = file.filename or "untitled.docx"
    _ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    _mime_map = {
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pdf": "application/pdf",
    }
    content_type = _mime_map.get(_ext) or file.content_type or "application/octet-stream"

    service = _make_cv_service(db=db, s3_client=s3_client)

    return await service.upload_cv(
        user_id=x_user_id,
        filename=filename,
        content_type=content_type,
        file_data=file_data,
    )


@router.get(
    "",
    response_model=list[CVListItem],
    summary="List uploaded CVs",
    description="Returns all CVs uploaded by the authenticated user, newest first.",
)
async def list_cvs(
    db: DB,
    s3_client: S3Client,
    x_user_id: CurrentUserId,
) -> list[CVListItem]:
    service = _make_cv_service(db=db, s3_client=s3_client)
    return await service.list_cvs(user_id=x_user_id)


@router.get(
    "/{cv_id}",
    response_model=CVStatusResponse,
    summary="Get CV parse status",
    description=(
        "Poll this endpoint after uploading a CV to check parse progress. "
        "When status=parsed, the CV is ready to use in a job run."
    ),
)
async def get_cv_status(
    cv_id: uuid.UUID,
    db: DB,
    s3_client: S3Client,
    x_user_id: CurrentUserId,
) -> CVStatusResponse:
    service = _make_cv_service(db=db, s3_client=s3_client)
    return await service.get_cv_status(cv_id=cv_id, user_id=x_user_id)
