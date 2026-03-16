"""
app/services/cv/cv_build_service.py

CV Build Service — generates a complete CV from profile data.

Handles two build modes:
  FROM_SCRATCH — no uploaded CV; full generation
  REBUILD      — poor CV uploaded; regenerate content, keep template layout

Output: a BuiltCV dict that the renderer consumes to fill a DOCX template.

Flow:
  1. Load profile + template
  2. Call LLM with appropriate prompt (build or rebuild)
  3. Parse and validate BuiltCV JSON response
  4. Store BuiltCV on the JobRun (in edit_plan field, build_mode flag)
  5. Renderer reads build_mode and uses BuiltCV instead of EditPlan

Integration point:
  Called from run_llm_task when job_run has a CV with build_mode != "edit".
"""

import json
import re
import uuid

import structlog

logger = structlog.get_logger(__name__)


class CVBuildService:
    """Generates a complete CV from a candidate profile."""

    def build_from_profile(
        self,
        profile_dict: dict,
        build_mode: str,
        quality_feedback: dict | None = None,
        jd_text: str | None = None,
        preferences: dict | None = None,
    ) -> dict:
        """
        Generate a full CV from scratch or rebuild a poor one.

        Args:
            profile_dict:     Candidate profile from CandidateProfile.to_prompt_dict()
            build_mode:       "from_scratch" or "rebuild"
            quality_feedback: Quality assessment of the original CV (rebuild mode only)
            jd_text:          Optional job description for keyword tailoring
            preferences:      JobRun preferences dict

        Returns:
            BuiltCV dict matching the CV_BUILD_SCHEMA structure.
            Stored in JobRun.edit_plan with a "built_cv" wrapper key.

        Raises:
            ValueError if LLM response cannot be parsed.
        """
        from app.services.llm.client import ClaudeClient
        from app.services.llm.cv_build_prompt import (
            build_cv_from_scratch_prompt,
            build_cv_rebuild_prompt,
        )
        from app.models.cv import CVBuildMode

        client = ClaudeClient(max_tokens=4096)

        if build_mode == CVBuildMode.REBUILD:
            issues = (quality_feedback or {}).get("top_issues", [])
            system, user = build_cv_rebuild_prompt(
                profile_dict=profile_dict,
                original_cv_issues=issues,
                jd_text=jd_text,
                preferences=preferences,
            )
            log_mode = "rebuild"
        else:
            system, user = build_cv_from_scratch_prompt(
                profile_dict=profile_dict,
                jd_text=jd_text,
                preferences=preferences,
            )
            log_mode = "from_scratch"

        logger.info("cv_build_start", mode=log_mode)

        raw, result = client.call_text(
            system_prompt=system,
            user_prompt=user,
            temperature=0.3,
        )

        built_cv = self._parse_built_cv(raw, log_mode)

        logger.info(
            "cv_build_complete",
            mode=log_mode,
            sections=len(built_cv.get("sections", [])),
            tokens=result.input_tokens + result.output_tokens,
            cost_usd=result.cost_usd,
        )

        return built_cv

    def _parse_built_cv(self, raw: str, mode: str) -> dict:
        """Parse and minimally validate the LLM JSON response."""
        text = re.sub(r"^```(?:json)?\s*", "", raw.strip())
        text = re.sub(r"\s*```$", "", text).strip()

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"CV build ({mode}): could not parse JSON: {e}") from e

        # Minimal validation
        required = ["name", "sections"]
        missing = [f for f in required if f not in data]
        if missing:
            raise ValueError(f"CV build ({mode}): missing required fields: {missing}")

        if not data.get("sections"):
            raise ValueError(f"CV build ({mode}): sections array is empty")

        return data
