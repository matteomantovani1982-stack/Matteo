"""
app/workers/tasks/render_docx.py

Celery task: render_docx_task

Final step in the pipeline. Takes a JobRun that has:
  - edit_plan (from run_llm)
  - original CV accessible in S3 (from upload)

And produces:
  - A tailored DOCX with edits applied
  - Uploaded to S3 at outputs/{user_id}/{job_run_id}/tailored_cv.docx
  - JobRun.output_s3_key set
  - JobRun.status → COMPLETED

State machine:
  RENDERING → COMPLETED  (success)
  RENDERING → FAILED     (terminal failure after retries)

Queue: rendering (CPU-bound, higher concurrency OK)
"""

import uuid
from datetime import UTC, datetime

import structlog
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded

from app.workers.celery_app import celery
from app.workers.db_utils import get_sync_db

logger = structlog.get_logger(__name__)


class RenderDocxTask(Task):
    """Custom base for render_docx_task."""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        job_run_id_str = args[0] if args else kwargs.get("job_run_id")
        if not job_run_id_str:
            return

        logger.error(
            "render_docx_task_failed",
            job_run_id=job_run_id_str,
            task_id=task_id,
            error=str(exc),
        )

        try:
            job_run_id = uuid.UUID(job_run_id_str)
            _mark_run_failed(
                job_run_id=job_run_id,
                failed_step="render_docx",
                error_message=str(exc),
            )
        except Exception as e:
            logger.error(
                "render_docx_failed_to_update_db",
                job_run_id=job_run_id_str,
                error=str(e),
            )


@celery.task(
    bind=True,
    base=RenderDocxTask,
    name="app.workers.tasks.render_docx.render_docx_task",
    max_retries=2,
    default_retry_delay=15,
)
def render_docx_task(self: Task, job_run_id: str) -> dict:
    """
    Apply EditPlan to original DOCX, upload output to S3, mark run COMPLETED.

    Args:
        job_run_id: String UUID of the JobRun to render

    Returns:
        dict with output s3_key and render stats
    """
    log = logger.bind(job_run_id=job_run_id, task_id=self.request.id)
    log.info("render_docx_task_started")

    try:
        run_uuid = uuid.UUID(job_run_id)
    except ValueError as e:
        log.error("render_docx_invalid_uuid", error=str(e))
        raise

    # ── Step 1: Load JobRun from DB ────────────────────────────────────────
    with get_sync_db() as db:
        from app.models.job_run import JobRun, JobRunStatus

        job_run = db.get(JobRun, run_uuid)
        if job_run is None:
            raise ValueError(f"JobRun {job_run_id} not found")

        if job_run.status == JobRunStatus.COMPLETED:
            log.info("render_docx_already_completed")
            return {"status": "already_completed", "output_s3_key": job_run.output_s3_key}

        if job_run.edit_plan is None:
            raise ValueError(f"JobRun {job_run_id} has no edit_plan — run_llm must complete first")

        # Snapshot all we need
        cv_id = job_run.cv_id
        user_id = job_run.user_id
        edit_plan = dict(job_run.edit_plan)

    # ── Step 2: Fetch CV metadata ──────────────────────────────────────────
    with get_sync_db() as db:
        from app.models.cv import CV, CVBuildMode

        cv = db.get(CV, cv_id)
        if cv is None:
            raise ValueError(f"CV {cv_id} not found")

        original_s3_key = cv.s3_key
        original_filename = cv.original_filename
        cv_build_mode = cv.build_mode or CVBuildMode.EDIT.value
        template_slug = cv.template_slug

    # Detect build mode from edit_plan wrapper key set by run_llm
    is_built_cv = "built_cv" in edit_plan

    # ── Step 3: Create JobStep for this render operation ───────────────────
    render_step_id = _create_render_step(
        job_run_id=run_uuid,
        celery_task_id=self.request.id,
    )

    # ── Step 4 + 5: Download source + render ─────────────────────────────
    try:
        from app.services.ingest.storage import S3StorageService
        storage = S3StorageService()

        if is_built_cv:
            # ── FROM_SCRATCH / REBUILD: load template, fill with BuiltCV ──
            log.info("render_mode_built_cv", build_mode=cv_build_mode, template_slug=template_slug)
            template_bytes = _load_template(storage, template_slug, log)
            render_result = _render_built_cv(
                built_cv=edit_plan["built_cv"],
                template_bytes=template_bytes,
                template_slug=template_slug,
                log=log,
            )
            log.info(
                "built_cv_rendered",
                sections=render_result.sections_written,
                bullets=render_result.bullets_written,
                mode=render_result.mode,
                warnings=len(render_result.warnings),
            )
        else:
            # ── EDIT: apply EditPlan diff to uploaded DOCX ─────────────────
            log.info("render_mode_edit_plan", s3_key=original_s3_key)
            docx_bytes = storage.download_bytes(original_s3_key)
            log.info("original_cv_downloaded", size_bytes=len(docx_bytes))

            from app.services.rendering.docx_renderer import DOCXRenderer
            renderer = DOCXRenderer()
            render_result = renderer.render(docx_bytes=docx_bytes, edit_plan=edit_plan)
            log.info(
                "edit_plan_applied",
                edits_applied=render_result.edits_applied,
                edits_skipped=render_result.edits_skipped,
                warnings=len(render_result.warnings),
            )

        if render_result.warnings:
            for w in render_result.warnings:
                log.warning("render_warning", warning=w)

    except SoftTimeLimitExceeded:
        _fail_render_step(render_step_id, SoftTimeLimitExceeded("Time limit"))
        _mark_run_failed(run_uuid, "render_docx", "Task exceeded time limit during rendering")
        raise

    except Exception as exc:
        _fail_render_step(render_step_id, exc)
        log.error("render_docx_apply_failed", error=str(exc))
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            _mark_run_failed(run_uuid, "render_docx", str(exc))
            raise

    # ── Step 6: Upload rendered DOCX to S3 ────────────────────────────────
    try:
        from app.services.ingest.storage import build_output_s3_key
        from app.config import settings

        # Output filename: tailored_ for edits, generated_cv for built CVs
        if is_built_cv:
            output_filename = "generated_cv.docx"
        else:
            base_name = original_filename.rsplit(".", 1)[0] if "." in original_filename else original_filename
            output_filename = f"tailored_{base_name}.docx"
        output_s3_key = build_output_s3_key(
            user_id=user_id,
            job_run_id=run_uuid,
            filename=output_filename,
        )

        log.info("uploading_tailored_cv", s3_key=output_s3_key)
        storage.upload_bytes(
            data=render_result.docx_bytes,
            s3_key=output_s3_key,
            content_type=(
                "application/vnd.openxmlformats-officedocument"
                ".wordprocessingml.document"
            ),
            metadata={
                "job_run_id": job_run_id,
                "user_id": user_id,
                "edits_applied": str(render_result.edits_applied),
            },
        )

        log.info(
            "tailored_cv_uploaded",
            s3_key=output_s3_key,
            size_bytes=len(render_result.docx_bytes),
        )

    except Exception as exc:
        _fail_render_step(render_step_id, exc)
        log.error("render_docx_upload_failed", error=str(exc))
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            _mark_run_failed(run_uuid, "render_docx", str(exc))
            raise

    # ── Step 7: Mark JobRun COMPLETED ─────────────────────────────────────
    with get_sync_db() as db:
        from app.models.job_run import JobRun, JobRunStatus

        job_run = db.get(JobRun, run_uuid)
        if job_run is None:
            raise ValueError(f"JobRun {job_run_id} disappeared during render")

        # Save keyword match score from edit plan if available
        try:
            edit_plan = job_run.edit_plan or {}
            score = edit_plan.get("keyword_match_score")
            if score is not None:
                job_run.keyword_match_score = int(score)
        except Exception:
            pass
        job_run.status = JobRunStatus.COMPLETED
        job_run.output_s3_key = output_s3_key
        job_run.output_s3_bucket = settings.s3_bucket_name
        user_id_str = job_run.user_id
        job_run_uuid = job_run.id
        db.commit()

        log.info(
            "job_run_completed",
            output_s3_key=output_s3_key,
        )

    # ── Record usage (billing) ─────────────────────────────────────────
    try:
        import asyncio
        from app.db.session import get_async_db_for_sync
        from app.services.billing.billing_service import BillingService

        async def _record():
            async with get_async_db_for_sync() as async_db:
                svc = BillingService(db=async_db)
                await svc.record_usage(
                    user_id=uuid.UUID(user_id_str),
                    job_run_id=job_run_uuid,
                )
                await async_db.commit()

        asyncio.run(_record())
        log.info("usage_recorded", job_run_id=job_run_id)
    except Exception as e:
        # Usage recording failure must never fail the job run
        log.warning("usage_recording_failed_non_fatal", error=str(e))

    # ── Step 8: Mark render step COMPLETED ────────────────────────────────
    _complete_render_step(
        step_id=render_step_id,
        metadata={
            **render_result.to_metadata(),
            "output_s3_key": output_s3_key,
        },
    )

    return {
        "job_run_id": job_run_id,
        "status": "completed",
        "output_s3_key": output_s3_key,
        "edits_applied": render_result.edits_applied,
        "edits_skipped": render_result.edits_skipped,
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

def _create_render_step(
    job_run_id: uuid.UUID,
    celery_task_id: str,
) -> uuid.UUID:
    """Create a JobStep record in RUNNING state for this render operation."""
    from app.models.job_step import JobStep, StepName, StepStatus

    step_id = uuid.uuid4()
    with get_sync_db() as db:
        step = JobStep(
            id=step_id,
            job_run_id=job_run_id,
            step_name=StepName.RENDER_DOCX,
            status=StepStatus.RUNNING,
            celery_task_id=celery_task_id,
            started_at=datetime.now(UTC),
        )
        db.add(step)
        db.commit()
    return step_id


def _complete_render_step(step_id: uuid.UUID, metadata: dict) -> None:
    """Mark render JobStep as COMPLETED."""
    from app.models.job_step import JobStep, StepStatus

    with get_sync_db() as db:
        step = db.get(JobStep, step_id)
        if step:
            completed_at = datetime.now(UTC)
            step.status = StepStatus.COMPLETED
            step.completed_at = completed_at
            if step.started_at:
                step.duration_seconds = (
                    completed_at - step.started_at
                ).total_seconds()
            step.metadata_json = metadata
            db.commit()


def _fail_render_step(step_id: uuid.UUID, error: Exception) -> None:
    """Mark render JobStep as FAILED."""
    from app.models.job_step import JobStep, StepStatus

    with get_sync_db() as db:
        step = db.get(JobStep, step_id)
        if step:
            completed_at = datetime.now(UTC)
            step.status = StepStatus.FAILED
            step.completed_at = completed_at
            if step.started_at:
                step.duration_seconds = (
                    completed_at - step.started_at
                ).total_seconds()
            step.error_type = type(error).__name__
            step.error_message = str(error)[:2000]
            db.commit()


def _load_template(storage, template_slug: str | None, log) -> bytes | None:
    """
    Load a DOCX template from S3 by slug.
    Returns bytes if found, None if not found (caller uses generated fallback).
    """
    if not template_slug:
        log.info("no_template_slug_using_generated_fallback")
        return None

    from app.config import settings
    s3_key = f"templates/{template_slug}.docx"
    try:
        template_bytes = storage.download_bytes(s3_key)
        log.info("template_loaded", slug=template_slug, size=len(template_bytes))
        return template_bytes
    except Exception as e:
        log.warning("template_load_failed_using_generated_fallback", slug=template_slug, error=str(e))
        return None


def _render_built_cv(built_cv: dict, template_bytes: bytes | None, template_slug: str | None, log) -> object:
    """
    Render a BuiltCV dict using the TemplateRenderer.
    Returns a TemplateRenderResult.
    """
    from app.services.rendering.template_renderer import TemplateRenderer
    renderer = TemplateRenderer()
    result = renderer.render(
        built_cv=built_cv,
        template_bytes=template_bytes,
        template_slug=template_slug,
    )
    return result


def _mark_run_failed(
    job_run_id: uuid.UUID,
    failed_step: str,
    error_message: str,
) -> None:
    """Mark a JobRun as FAILED. Idempotent."""
    from app.models.job_run import JobRun, JobRunStatus

    try:
        with get_sync_db() as db:
            job_run = db.get(JobRun, job_run_id)
            if job_run and not job_run.is_terminal:
                job_run.status = JobRunStatus.FAILED
                job_run.failed_step = failed_step
                job_run.error_message = error_message[:2000]
                db.commit()
    except Exception as e:
        logger.error(
            "failed_to_mark_run_failed",
            job_run_id=str(job_run_id),
            error=str(e),
        )
