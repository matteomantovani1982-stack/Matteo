"""
app/workers/tasks/parse_cv.py

Celery task: parse_cv_task

Triggered immediately after a CV is uploaded.
Reads the DOCX from S3, parses it into a structured node map,
and stores the result back in the DB.

On success:  CV.status → PARSED,  CV.parsed_content set
On failure:  CV.status → FAILED,  CV.error_message set

State machine transitions managed here:
  UPLOADED → PARSING → PARSED
  UPLOADED → PARSING → FAILED

After PARSED, the orchestrator task (Sprint 5+) will chain to:
  PARSING → RETRIEVING → LLM_PROCESSING → RENDERING → COMPLETED
"""

import uuid
from datetime import UTC, datetime

import structlog
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded

from app.workers.celery_app import celery
from app.workers.db_utils import get_sync_db

logger = structlog.get_logger(__name__)


class ParseCVTask(Task):
    """
    Custom Celery Task base class for parse_cv.

    Provides:
    - on_failure hook to update DB status on unexpected errors
    - Structured logging throughout
    """

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """
        Called by Celery when the task raises an unhandled exception.
        Ensures the CV record is marked FAILED even if our try/except missed it.
        """
        cv_id_str = args[0] if args else kwargs.get("cv_id")
        if not cv_id_str:
            return

        logger.error(
            "parse_cv_task_failed",
            cv_id=cv_id_str,
            task_id=task_id,
            error=str(exc),
        )

        try:
            cv_id = uuid.UUID(cv_id_str)
            _mark_cv_failed(cv_id, error_message=str(exc))
        except Exception as e:
            logger.error(
                "parse_cv_failed_to_update_db",
                cv_id=cv_id_str,
                error=str(e),
            )


@celery.task(
    bind=True,
    base=ParseCVTask,
    name="app.workers.tasks.parse_cv.parse_cv_task",
    max_retries=3,
    default_retry_delay=30,  # seconds between retries
    # Serializer already set globally in celery_app.py
)
def parse_cv_task(self: Task, cv_id: str) -> dict:
    """
    Main parse_cv Celery task.

    Args:
        cv_id: String UUID of the CV record to parse

    Returns:
        dict with parse results summary (stored as Celery result)

    Raises:
        Retries on transient S3/DB errors (up to max_retries).
        Does NOT retry on ValueError (invalid DOCX — permanent failure).
    """
    log = logger.bind(cv_id=cv_id, task_id=self.request.id)
    log.info("parse_cv_task_started")

    # ── Resolve CV record ──────────────────────────────────────────────────
    try:
        cv_uuid = uuid.UUID(cv_id)
    except ValueError as e:
        log.error("parse_cv_invalid_uuid", error=str(e))
        raise  # Don't retry — invalid input

    # ── Step 1: Mark CV as PARSING and create JobStep record ──────────────
    from app.models.cv import CVStatus

    with get_sync_db() as db:
        from app.models.cv import CV
        cv = db.get(CV, cv_uuid)
        if cv is None:
            log.error("parse_cv_cv_not_found")
            raise ValueError(f"CV {cv_id} not found in database")

        if cv.status != CVStatus.UPLOADED:
            log.warning(
                "parse_cv_unexpected_status",
                current_status=cv.status,
            )
            # Idempotent — if already parsed, return success
            if cv.status == CVStatus.PARSED:
                return {
                    "cv_id": cv_id,
                    "status": "already_parsed",
                    "total_words": cv.parsed_content.get("total_words", 0),
                }

        cv.status = CVStatus.PARSING
        db.commit()
        log.info("cv_status_set_to_parsing")

        # Capture S3 info before closing DB session
        s3_key = cv.s3_key
        s3_bucket = cv.s3_bucket

    # ── Step 2: Download DOCX from S3 ─────────────────────────────────────
    try:
        from app.services.ingest.storage import S3StorageService
        storage = S3StorageService()

        log.info("downloading_cv_from_s3", s3_key=s3_key)
        docx_bytes = storage.download_bytes(s3_key)
        log.info("cv_downloaded", size_bytes=len(docx_bytes))

    except Exception as exc:
        log.error("parse_cv_s3_download_failed", error=str(exc))
        # S3 failures are transient — retry
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            _mark_cv_failed(cv_uuid, str(exc))
            raise

    # ── Step 3: Parse DOCX into node map ──────────────────────────────────
    try:
        from app.services.ingest.parser import DOCXParser
        parser = DOCXParser()

        log.info("parsing_docx")
        parsed_cv = parser.parse(docx_bytes)
        log.info(
            "docx_parsed",
            sections=len(parsed_cv.sections),
            words=parsed_cv.total_words,
            paragraphs=parsed_cv.total_paragraphs,
        )

    except ValueError as exc:
        # Invalid DOCX — permanent failure, no retry
        log.error("parse_cv_invalid_docx", error=str(exc))
        _mark_cv_failed(cv_uuid, str(exc))
        raise

    except SoftTimeLimitExceeded:
        log.warning("parse_cv_soft_time_limit_exceeded")
        _mark_cv_failed(cv_uuid, "Task exceeded time limit during parsing")
        raise

    except Exception as exc:
        log.error("parse_cv_unexpected_error", error=str(exc))
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            _mark_cv_failed(cv_uuid, str(exc))
            raise

    # ── Step 4: Persist parsed content and update status ──────────────────
    with get_sync_db() as db:
        from app.models.cv import CV
        cv = db.get(CV, cv_uuid)
        if cv is None:
            raise ValueError(f"CV {cv_id} disappeared from database during parsing")

        cv.status = CVStatus.PARSED
        cv.parsed_content = parsed_cv.model_dump()
        db.commit()

        log.info("cv_status_set_to_parsed")

    # ── Quality scoring (non-fatal) ───────────────────────────────────────
    try:
        from app.services.cv.quality_service import CVQualityService
        quality_svc = CVQualityService()
        feedback = quality_svc.score(parsed_cv)

        with get_sync_db() as db:
            from app.models.cv import CV
            cv = db.get(CV, cv_uuid)
            if cv is not None:
                cv.quality_score = feedback["score"]
                cv.quality_feedback = feedback
                db.commit()

        log.info(
            "cv_quality_scored",
            score=feedback["score"],
            verdict=feedback["verdict"],
            rebuild_recommended=feedback["rebuild_recommended"],
        )
    except Exception as e:
        log.warning("cv_quality_scoring_failed_non_fatal", error=str(e))

    # ── Best practices feedback (non-fatal) ───────────────────────────────
    try:
        from app.services.cv.best_practices_service import BestPracticesService
        bp_svc = BestPracticesService()
        bp_feedback = bp_svc.analyse(parsed_cv)

        with get_sync_db() as db:
            from app.models.cv import CV
            cv = db.get(CV, cv_uuid)
            if cv is not None:
                # Merge into quality_feedback alongside score
                existing = dict(cv.quality_feedback or {})
                existing["suggestions"] = bp_feedback.get("suggestions", [])
                existing["top_strength"] = bp_feedback.get("top_strength", "")
                existing["bp_summary"] = bp_feedback.get("summary", "")
                cv.quality_feedback = existing
                db.commit()

        log.info(
            "best_practices_stored",
            suggestions=len(bp_feedback.get("suggestions", [])),
        )
    except Exception as e:
        log.warning("best_practices_failed_non_fatal", error=str(e))

    return {
        "cv_id": cv_id,
        "status": "parsed",
        "total_sections": len(parsed_cv.sections),
        "total_words": parsed_cv.total_words,
        "total_paragraphs": parsed_cv.total_paragraphs,
        "quality_score": getattr(parsed_cv, "quality_score", None),
    }


# ── Helpers ─────────────────────────────────────────────────────────────────

def _mark_cv_failed(cv_id: uuid.UUID, error_message: str) -> None:
    """
    Mark a CV record as FAILED in the DB.
    Called from both the task body and the on_failure hook.
    Safe to call multiple times (idempotent).
    """
    try:
        from app.models.cv import CV, CVStatus
        with get_sync_db() as db:
            cv = db.get(CV, cv_id)
            if cv is not None:
                cv.status = CVStatus.FAILED
                cv.error_message = error_message[:2000]  # Truncate very long traces
                db.commit()
    except Exception as e:
        logger.error(
            "failed_to_mark_cv_failed",
            cv_id=str(cv_id),
            error=str(e),
        )
