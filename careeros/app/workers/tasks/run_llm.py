"""
app/workers/tasks/run_llm.py

Celery task: run_llm_task — profile-aware, 3-output pipeline.

Pipeline:
  1. Load JobRun + CV + CandidateProfile
  2. Apply per-application profile overrides
  3. Web retrieval (Serper)
  4. LLM A — EditPlan
  5. LLM B — PositioningStrategy (profile mode only)
  6. LLM C — ReportPack
  7. Persist all outputs
  8. Chain → render_docx_task

Fallback: if no profile exists, gracefully degrades to CV-only prompts.
Positioning failure is non-fatal — CV + reports still produced.
"""

import uuid
from datetime import UTC, datetime

import structlog
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded

from app.workers.celery_app import celery
from app.workers.db_utils import get_sync_db

logger = structlog.get_logger(__name__)


class RunLLMTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        job_run_id_str = args[0] if args else kwargs.get("job_run_id")
        if not job_run_id_str:
            return
        logger.error("run_llm_unexpected_failure", job_run_id=job_run_id_str, error=str(exc))
        try:
            _mark_run_failed(uuid.UUID(job_run_id_str), "llm_processing", str(exc))
        except Exception as e:
            logger.error("failed_to_mark_run_failed", error=str(e))


@celery.task(
    bind=True,
    base=RunLLMTask,
    name="app.workers.tasks.run_llm.run_llm_task",
    max_retries=2,
    default_retry_delay=5,
)
def run_llm_task(self: Task, job_run_id: str) -> dict:
    log = logger.bind(job_run_id=job_run_id, task_id=self.request.id)
    log.info("run_llm_task_started")

    # Set Sentry context for this job run (populated after DB fetch below)
    from app.monitoring.sentry import set_job_run_context, capture_retrieval_breadcrumb

    try:
        run_uuid = uuid.UUID(job_run_id)
    except ValueError as e:
        log.error("invalid_uuid", error=str(e))
        raise

    # ── 1. Load inputs ────────────────────────────────────────────────────
    with get_sync_db() as db:
        from app.models.job_run import JobRun, JobRunStatus
        from app.models.cv import CV
        from app.models.candidate_profile import CandidateProfile, ProfileStatus
        from sqlalchemy import select

        job_run = db.get(JobRun, run_uuid)
        if job_run is None:
            raise ValueError(f"JobRun {job_run_id} not found")
        if job_run.is_terminal:
            return {"status": "already_terminal"}

        cv = db.get(CV, job_run.cv_id)
        if cv is None or cv.parsed_content is None:
            raise ValueError(f"CV {job_run.cv_id} not parsed")

        jd_text = job_run.jd_text or ""
        preferences = dict(job_run.preferences or {})

        # If no jd_text but jd_url provided, fetch it now in the worker
        if not jd_text.strip() and job_run.jd_url:
            try:
                import httpx as _httpx
                _headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                }
                _r = _httpx.get(job_run.jd_url, headers=_headers, timeout=20.0, follow_redirects=True)
                _r.raise_for_status()
                # Strip HTML quickly
                import re as _re
                _html = _r.text
                _html = _re.sub(r"<(script|style|nav|footer)[^>]*>.*?</\1>", "", _html, flags=_re.DOTALL|_re.IGNORECASE)
                _html = _re.sub(r"<[^>]+>", " ", _html)
                _html = _re.sub(r"[ \t]+", " ", _html)
                _html = _re.sub(r"\n{3,}", "\n\n", _html)
                jd_text = _html.strip()[:6000]
                job_run.jd_text = jd_text
                db.commit()
                log.info("jd_fetched_from_url", chars=len(jd_text))
            except Exception as _e:
                log.warning("jd_url_fetch_failed", error=str(_e))
        parsed_content = dict(cv.parsed_content)
        profile_id = job_run.profile_id
        profile_overrides_raw = dict(job_run.profile_overrides or {})
        cv_build_mode = cv.build_mode or "edit"
        cv_quality_feedback = dict(cv.quality_feedback or {})
        known_contacts = list(job_run.preferences.get("known_contacts", []) or [])

        # Resolve profile — explicit > active > any > None
        profile_dict = None
        if profile_id:
            p = db.get(CandidateProfile, profile_id)
            if p:
                profile_dict = p.to_prompt_dict()
        else:
            # Try ACTIVE first
            result = db.execute(
                select(CandidateProfile).where(
                    CandidateProfile.user_id == job_run.user_id,
                    CandidateProfile.status == ProfileStatus.ACTIVE,
                )
            )
            p = result.scalar_one_or_none()
            # Fall back to any profile for this user (DRAFT etc.)
            if not p:
                result = db.execute(
                    select(CandidateProfile).where(
                        CandidateProfile.user_id == job_run.user_id,
                    ).order_by(CandidateProfile.updated_at.desc())
                )
                p = result.scalars().first()
            if p:
                profile_dict = p.to_prompt_dict()
                profile_id = p.id

        job_run.status = JobRunStatus.RETRIEVING
        job_run.celery_task_id = self.request.id
        if profile_id:
            job_run.profile_id = profile_id
        db.commit()

    log.info("inputs_loaded", has_profile=profile_dict is not None)

    # ── 2. Deserialise ParsedCV ───────────────────────────────────────────
    from app.schemas.cv import ParsedCV
    try:
        parsed_cv = ParsedCV.model_validate(parsed_content)
    except Exception as e:
        _mark_run_failed(run_uuid, "llm_processing", str(e))
        raise ValueError(f"Invalid ParsedCV: {e}") from e

    # ── 3. Apply profile overrides ────────────────────────────────────────
    if profile_dict and profile_overrides_raw:
        try:
            from app.schemas.candidate_profile import ApplicationProfileOverrides
            overrides = ApplicationProfileOverrides.model_validate(profile_overrides_raw)
            if overrides.additional_global_context:
                profile_dict["application_context"] = overrides.additional_global_context
        except Exception as e:
            log.warning("profile_overrides_invalid_ignoring", error=str(e))

    # ── 4. Retrieval ──────────────────────────────────────────────────────
    retrieve_step_id = _create_job_step(run_uuid, "retrieve", self.request.id)
    try:
        # Tag Sentry scope with run + user IDs for all events in this task
        set_job_run_context(str(job_run.id), str(job_run.user_id))

        retrieval_data = _run_retrieval(jd_text, preferences, log)
        _complete_job_step(retrieve_step_id, {
            "sources": retrieval_data.get("sources", []),
            "partial_failure": retrieval_data.get("partial_failure", False),
        })
    except SoftTimeLimitExceeded:
        _mark_run_failed(run_uuid, "retrieve", "Time limit exceeded")
        capture_retrieval_breadcrumb(
            sources=len(retrieval_data.get("sources", [])),
            contacts_found=len(retrieval_data.get("contacts", [])),
            partial_failure=retrieval_data.get("partial_failure", False),
        )
        raise
    except Exception as e:
        log.warning("retrieval_failed_continuing", error=str(e))
        _fail_job_step(retrieve_step_id, e)
        retrieval_data = {}

    # ── 5. Transition → LLM_PROCESSING ───────────────────────────────────
    _update_run_status(run_uuid, "llm_processing")

    # ── 6 + 7. LLM calls — fork on build mode ────────────────────────────
    # edit        → normal EditPlan + Positioning + ReportPack pipeline
    # rebuild     → CVBuildService generates full CV, then Positioning + ReportPack
    # from_scratch → CVBuildService generates full CV, then Positioning + ReportPack
    from app.models.cv import CVBuildMode

    llm_step_id = _create_job_step(run_uuid, "llm_call", self.request.id)
    try:
        if cv_build_mode in (CVBuildMode.FROM_SCRATCH, CVBuildMode.REBUILD):
            log.info("cv_build_mode_active", mode=cv_build_mode)
            edit_plan, positioning, report_pack, llm_meta = _run_cv_build_pipeline(
                profile_dict=profile_dict,
                build_mode=cv_build_mode,
                quality_feedback=cv_quality_feedback,
                jd_text=jd_text,
                preferences=preferences,
                retrieval_data=retrieval_data,
                known_contacts=known_contacts,
                log=log,
            )
        else:
            # Standard edit pipeline
            prompts = _build_prompts(
                profile_dict, parsed_cv, jd_text, preferences, retrieval_data, log,
                known_contacts=known_contacts,
            )
            edit_plan, positioning, report_pack, llm_meta = _run_llm_calls(
                prompts=prompts,
                has_profile=profile_dict is not None,
                log=log,
            )
        _complete_job_step(llm_step_id, llm_meta)
    except SoftTimeLimitExceeded:
        _fail_job_step(llm_step_id, SoftTimeLimitExceeded())
        _mark_run_failed(run_uuid, "llm_processing", "Time limit exceeded")
        raise
    except (ValueError, RuntimeError) as exc:
        _fail_job_step(llm_step_id, exc)
        log.error("llm_calls_failed", error=str(exc))
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            _mark_run_failed(run_uuid, "llm_processing", str(exc))
            raise

    # ── 8. Persist ────────────────────────────────────────────────────────
    with get_sync_db() as db:
        from app.models.job_run import JobRun, JobRunStatus
        job_run = db.get(JobRun, run_uuid)
        if job_run is None:
            raise ValueError(f"JobRun {job_run_id} disappeared")
        job_run.retrieval_data = retrieval_data
        job_run.edit_plan = edit_plan
        job_run.positioning = positioning
        job_run.reports = report_pack
        job_run.status = JobRunStatus.RENDERING
        # Save role/company for kanban display
        if report_pack:
            job_run.role_title = (report_pack.get("role") or {}).get("role_title") or None
            job_run.company_name = (report_pack.get("company") or {}).get("company_name") or None
        # Save match score immediately
        score = edit_plan.get("keyword_match_score")
        if score is not None:
            job_run.keyword_match_score = int(score)
        db.commit()

    headline = (positioning or {}).get("positioning_headline", "")
    log.info("run_llm_complete", score=edit_plan.get("keyword_match_score", 0), headline=headline)

    # ── 9. Chain → render ─────────────────────────────────────────────────
    from app.workers.tasks.render_docx import render_docx_task
    render_docx_task.delay(job_run_id)

    return {
        "job_run_id": job_run_id,
        "status": "llm_complete_rendering_dispatched",
        "keyword_match_score": edit_plan.get("keyword_match_score", 0),
        "positioning_headline": headline,
        "has_positioning": positioning is not None,
        "report_sections": list(report_pack.keys()) if report_pack else [],
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_cv_build_pipeline(
    profile_dict: dict | None,
    build_mode: str,
    quality_feedback: dict,
    jd_text: str,
    preferences: dict,
    retrieval_data: dict,
    log,
    known_contacts: list | None = None,
) -> tuple[dict, dict | None, dict, dict]:
    """
    CV build pipeline — used when build_mode is FROM_SCRATCH or REBUILD.

    Instead of an EditPlan diff, generates a full BuiltCV spec.
    Still runs Positioning (profile only) and ReportPack.

    Returns the same tuple shape as _run_llm_calls so the persist step
    is unchanged: (edit_plan_or_built_cv, positioning, report_pack, meta)
    """
    from app.services.cv.cv_build_service import CVBuildService
    from app.services.llm.client import ClaudeClient
    from app.services.llm.schemas import ReportPack
    from app.services.llm.profile_prompt import (
        build_positioning_strategy_prompt,
        _render_candidate_profile,
    )
    from app.services.llm.prompts import build_report_pack_user_prompt, REPORT_PACK_SYSTEM
    import json, re

    total_tokens = 0
    total_cost = 0.0
    meta = {"build_mode": build_mode}

    # Call A: CV Build (replaces EditPlan)
    build_svc = CVBuildService()
    built_cv = build_svc.build_from_profile(
        profile_dict=profile_dict or {},
        build_mode=build_mode,
        quality_feedback=quality_feedback,
        jd_text=jd_text,
        preferences=preferences,
    )
    # Wrap in a marker so renderer knows this is a built CV, not an EditPlan
    edit_plan_slot = {"built_cv": built_cv, "build_mode": build_mode}
    meta["cv_build"] = {"sections": len(built_cv.get("sections", []))}
    log.info("cv_build_complete", mode=build_mode, sections=len(built_cv.get("sections", [])))

    client = ClaudeClient()

    # Call B: PositioningStrategy (profile only, non-fatal)
    positioning_dict = None
    if profile_dict:
        pos_system, pos_user = build_positioning_strategy_prompt(
            profile_dict=profile_dict,
            jd_text=jd_text,
            preferences=preferences,
            retrieval_data=retrieval_data,
        )
        try:
            raw, pos_result = client.call_raw(
                system_prompt=pos_system, user_prompt=pos_user, temperature=0.3,
            )
            text = re.sub(r"^```(?:json)?\s*", "", raw.strip())
            text = re.sub(r"\s*```$", "", text).strip()
            positioning_dict = json.loads(text)
            total_tokens += pos_result.input_tokens + pos_result.output_tokens
            total_cost += pos_result.cost_usd
            meta["positioning"] = pos_result.to_metadata()
            log.info("positioning_done", headline=positioning_dict.get("positioning_headline", ""))
        except Exception as e:
            log.warning("positioning_failed_continuing", error=str(e))

    # Call C: ReportPack
    profile_summary = _render_candidate_profile(profile_dict) if profile_dict else None
    rp_user = build_report_pack_user_prompt(
        parsed_cv=None,
        jd_text=jd_text,
        retrieval_data=retrieval_data or {},
        preferences=preferences,
        profile_summary=profile_summary,
        known_contacts=known_contacts,
    )
    rp_obj, rp_result = client.call_structured(
        system_prompt=REPORT_PACK_SYSTEM,
        user_prompt=rp_user,
        schema=ReportPack,
        temperature=0.3,
    )
    total_tokens += rp_result.input_tokens + rp_result.output_tokens
    total_cost += rp_result.cost_usd
    meta["report_pack"] = rp_result.to_metadata()
    meta["total_tokens"] = total_tokens
    meta["total_cost_usd"] = round(total_cost, 6)
    meta["has_positioning"] = positioning_dict is not None
    log.info("report_pack_done")

    return edit_plan_slot, positioning_dict, rp_obj.model_dump(), meta


def _build_prompts(profile_dict, parsed_cv, jd_text, preferences, retrieval_data, log, known_contacts=None) -> dict:
    if profile_dict:
        log.info("using_profile_prompts")
        from app.services.llm.profile_prompt import build_all_profile_prompts
        return build_all_profile_prompts(
            profile_dict=profile_dict,
            parsed_cv=parsed_cv,
            jd_text=jd_text,
            preferences=preferences,
            retrieval_data=retrieval_data,
            known_contacts=known_contacts,
        )
    else:
        log.info("using_cv_only_prompts")
        from app.services.llm.prompts import build_all_prompts
        prompts = build_all_prompts(
            parsed_cv=parsed_cv,
            jd_text=jd_text,
            retrieval_data=retrieval_data,
            preferences=preferences,
        )
        prompts["positioning"] = None
        return prompts


def _run_retrieval(jd_text: str, preferences: dict, log) -> dict:
    from app.services.retrieval.web_search import RetrievalService
    service = RetrievalService()
    try:
        result = service.retrieve(
            jd_text=jd_text,
            role_title=preferences.get("role_title", ""),
            region=preferences.get("region", "UAE"),
        )
        log.info("retrieval_complete", sources=len(result.sources))
        return result.to_dict()
    finally:
        service.close()


def _run_llm_calls(prompts: dict, has_profile: bool, log) -> tuple[dict, dict | None, dict, dict]:
    from app.services.llm.client import ClaudeClient
    from app.services.llm.schemas import EditPlan, ReportPack
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import json, re

    client = ClaudeClient()
    total_tokens = 0
    total_cost = 0.0
    meta = {}

    # Run EditPlan and ReportPack in PARALLEL — they are independent
    edit_system, edit_user = prompts["edit_plan"]
    rp_system, rp_user = prompts["report_pack"]

    ep_obj = ep_result = rp_obj = rp_result = None

    def call_edit_plan():
        from app.services.llm.client import ClaudeClient as _C
        return _C().call_structured(
            system_prompt=edit_system, user_prompt=edit_user, schema=EditPlan, temperature=0.2,
        )

    def call_report_pack():
        from app.services.llm.client import ClaudeClient as _C
        return _C().call_structured(
            system_prompt=rp_system, user_prompt=rp_user, schema=ReportPack, temperature=0.3,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_ep = executor.submit(call_edit_plan)
        future_rp = executor.submit(call_report_pack)
        ep_obj, ep_result = future_ep.result()
        rp_obj, rp_result = future_rp.result()

    total_tokens += ep_result.input_tokens + ep_result.output_tokens
    total_cost += ep_result.cost_usd
    meta["edit_plan"] = ep_result.to_metadata()
    log.info("edit_plan_done", score=ep_obj.keyword_match_score)

    total_tokens += rp_result.input_tokens + rp_result.output_tokens
    total_cost += rp_result.cost_usd
    meta["report_pack"] = rp_result.to_metadata()
    log.info("report_pack_done")

    # Call B: PositioningStrategy (depends on edit_plan score — run after)
    positioning_dict = None
    if has_profile and prompts.get("positioning"):
        pos_system, pos_user = prompts["positioning"]
        try:
            raw, pos_result = client.call_raw(
                system_prompt=pos_system, user_prompt=pos_user, temperature=0.3,
            )
            text = re.sub(r"^```(?:json)?\s*", "", raw.strip())
            text = re.sub(r"\s*```$", "", text).strip()
            positioning_dict = json.loads(text)
            total_tokens += pos_result.input_tokens + pos_result.output_tokens
            total_cost += pos_result.cost_usd
            meta["positioning"] = pos_result.to_metadata()
            log.info("positioning_done", headline=positioning_dict.get("positioning_headline", ""))
        except Exception as e:
            log.warning("positioning_failed_continuing", error=str(e))

    meta["total_tokens"] = total_tokens
    meta["total_cost_usd"] = round(total_cost, 6)
    meta["has_positioning"] = positioning_dict is not None

    return ep_obj.model_dump(), positioning_dict, rp_obj.model_dump(), meta


def _create_job_step(job_run_id: uuid.UUID, step_name: str, celery_task_id: str) -> uuid.UUID:
    from app.models.job_step import JobStep, StepStatus
    step_id = uuid.uuid4()
    with get_sync_db() as db:
        db.add(JobStep(
            id=step_id, job_run_id=job_run_id, step_name=step_name,
            status=StepStatus.RUNNING, celery_task_id=celery_task_id,
            started_at=datetime.now(UTC),
        ))
        db.commit()
    return step_id


def _complete_job_step(step_id: uuid.UUID, metadata: dict) -> None:
    from app.models.job_step import JobStep, StepStatus
    with get_sync_db() as db:
        step = db.get(JobStep, step_id)
        if step:
            now = datetime.now(UTC)
            step.status = StepStatus.COMPLETED
            step.completed_at = now
            if step.started_at:
                step.duration_seconds = (now - step.started_at).total_seconds()
            step.metadata_json = metadata
            db.commit()


def _fail_job_step(step_id: uuid.UUID, error: Exception) -> None:
    from app.models.job_step import JobStep, StepStatus
    with get_sync_db() as db:
        step = db.get(JobStep, step_id)
        if step:
            now = datetime.now(UTC)
            step.status = StepStatus.FAILED
            step.completed_at = now
            if step.started_at:
                step.duration_seconds = (now - step.started_at).total_seconds()
            step.error_type = type(error).__name__
            step.error_message = str(error)[:2000]
            db.commit()


def _update_run_status(job_run_id: uuid.UUID, status_name: str) -> None:
    from app.models.job_run import JobRun, JobRunStatus
    with get_sync_db() as db:
        jr = db.get(JobRun, job_run_id)
        if jr:
            jr.status = JobRunStatus(status_name)
            db.commit()


def _mark_run_failed(job_run_id: uuid.UUID, failed_step: str, error_message: str) -> None:
    from app.models.job_run import JobRun, JobRunStatus
    try:
        with get_sync_db() as db:
            jr = db.get(JobRun, job_run_id)
            if jr and not jr.is_terminal:
                jr.status = JobRunStatus.FAILED
                jr.failed_step = failed_step
                jr.error_message = error_message[:2000]
                db.commit()
    except Exception as e:
        logger.error("failed_to_mark_run_failed", job_run_id=str(job_run_id), error=str(e))
