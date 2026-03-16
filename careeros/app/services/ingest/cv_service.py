import asyncio
"""
app/services/ingest/cv_service.py

Business logic for CV ingestion.

Responsibilities:
- Coordinate file validation, S3 upload, and DB record creation
- Generate S3 keys
- Dispatch the parse_cv Celery task after upload
- Fetch CV records for status polling

This is the ONLY place that knows about both the DB and S3.
Routes call this service — they never touch S3 or DB directly.
"""

import uuid
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.middleware.error_handler import NotFoundError
from app.config import settings
from app.models.cv import CV, CVStatus
from app.schemas.cv import CVCreate, CVListItem, CVStatusResponse, CVUploadResponse
from app.services.ingest.storage import (
    S3StorageService,
    build_cv_s3_key,
    compute_md5,
    validate_cv_file,
)

import structlog

logger = structlog.get_logger(__name__)

# Thread pool for running sync S3 operations without blocking the event loop
_executor = ThreadPoolExecutor(max_workers=4)


class CVService:
    """
    Orchestrates the CV upload lifecycle.

    Injected with a DB session and S3 service — fully testable.
    """

    def __init__(self, db: AsyncSession, storage: S3StorageService) -> None:
        self._db = db
        self._storage = storage

    async def upload_cv(
        self,
        user_id: str,
        filename: str,
        content_type: str,
        file_data: bytes,
    ) -> CVUploadResponse:
        """
        Full upload flow:
        1. Validate file type, size
        2. Generate a new CV UUID and S3 key
        3. Upload to S3 (in thread pool — boto3 is sync)
        4. Create CV record in DB with status=UPLOADED
        5. Dispatch parse_cv Celery task
        6. Return CVUploadResponse

        Args:
            user_id:      Caller's user identifier
            filename:     Original filename from the upload
            content_type: MIME type from the multipart upload
            file_data:    Raw file bytes (already read into memory)

        Returns:
            CVUploadResponse with the new CV id and status

        Raises:
            ValidationError: if file type/size is invalid
            StorageError:    if S3 upload fails
        """
        # Step 1 — validate before touching S3
        validate_cv_file(
            filename=filename,
            content_type=content_type,
            file_size=len(file_data),
        )

        # Step 2 — generate identifiers
        cv_id = uuid.uuid4()
        s3_key = build_cv_s3_key(
            user_id=user_id,
            cv_id=cv_id,
            filename=filename,
        )
        md5_hash = compute_md5(file_data)

        # Step 3 — upload to S3 in thread pool (boto3 is blocking)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            _executor,
            lambda: self._storage.upload_bytes(
                data=file_data,
                s3_key=s3_key,
                content_type=content_type,
                metadata={
                    "user_id": user_id,
                    "cv_id": str(cv_id),
                    "original_filename": filename,
                    "md5": md5_hash,
                },
            ),
        )

        logger.info(
            "cv_uploaded_to_s3",
            extra={
                "cv_id": str(cv_id),
                "user_id": user_id,
                "s3_key": s3_key,
                "size_bytes": len(file_data),
            },
        )

        # Step 4 — create DB record
        cv = CV(
            id=cv_id,
            user_id=user_id,
            original_filename=filename,
            file_size_bytes=len(file_data),
            mime_type=content_type,
            s3_key=s3_key,
            s3_bucket=settings.s3_bucket_name,
            status=CVStatus.UPLOADED,
        )
        self._db.add(cv)

        logger.info(
            "cv_db_record_created",
            extra={"cv_id": str(cv_id), "user_id": user_id},
        )

        # Step 5 — flush so the CV record is persisted before task dispatch
        await self._db.flush()

        # Step 6 — dispatch Celery task (non-fatal — workers may be starting up)
        # Import here to avoid circular imports at module load time
        try:
            from app.workers.tasks.parse_cv import parse_cv_task
            task = parse_cv_task.delay(str(cv_id))
            logger.info(
                "parse_cv_task_dispatched",
                extra={"cv_id": str(cv_id), "task_id": task.id},
            )
        except Exception as e:
            # Task dispatch failed (e.g. Redis not ready) — CV record still saved
            # Status stays UPLOADED and can be retried later
            logger.warning(
                "parse_cv_task_dispatch_failed",
                extra={"cv_id": str(cv_id), "error": str(e)},
            )

        # Step 7 — return response (session commits in dependency)
        return CVUploadResponse(
            id=cv.id,
            status=cv.status,
            original_filename=cv.original_filename,
            file_size_bytes=cv.file_size_bytes,
            mime_type=cv.mime_type,
            created_at=cv.created_at,
        )

    async def get_cv_status(
        self,
        cv_id: uuid.UUID,
        user_id: str,
    ) -> CVStatusResponse:
        """
        Fetch current status of a CV record.
        Raises NotFoundError if not found or belongs to different user.
        """
        cv = await self._get_cv_or_404(cv_id=cv_id, user_id=user_id)

        # Extract summary stats from parsed_content if available
        parsed_section_count: int | None = None
        parsed_word_count: int | None = None

        if cv.parsed_content:
            sections = cv.parsed_content.get("sections", [])
            parsed_section_count = len(sections)
            parsed_word_count = cv.parsed_content.get("total_words", 0)

        return CVStatusResponse(
            id=cv.id,
            status=cv.status,
            error_message=cv.error_message,
            parsed_section_count=parsed_section_count,
            parsed_word_count=parsed_word_count,
        )

    async def list_cvs(self, user_id: str) -> list[CVListItem]:
        """
        List all CVs belonging to a user, newest first.
        """
        result = await self._db.execute(
            select(CV)
            .where(CV.user_id == user_id)
            .order_by(CV.created_at.desc())
            .limit(50)
        )
        cvs = result.scalars().all()
        return [
            CVListItem(
                id=cv.id,
                original_filename=cv.original_filename,
                status=cv.status,
                file_size_bytes=cv.file_size_bytes,
                created_at=cv.created_at,
            )
            for cv in cvs
        ]

    async def _get_cv_or_404(
        self,
        cv_id: uuid.UUID,
        user_id: str,
    ) -> CV:
        """
        Fetch a CV record by ID, scoped to the requesting user.
        Raises NotFoundError if missing or owned by another user.
        """
        result = await self._db.execute(
            select(CV).where(CV.id == cv_id, CV.user_id == user_id)
        )
        cv = result.scalar_one_or_none()
        if cv is None:
            raise NotFoundError(resource="CV", resource_id=str(cv_id))
        return cv
