"""
tests/test_cv_builder.py

Tests for Sprint G: CV quality scoring, build mode selection, prompt builders.
"""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.cv import CVBuildMode
from app.services.cv.quality_service import (
    CVQualityService,
    get_verdict,
    should_recommend_rebuild,
    QUALITY_POOR,
    QUALITY_WEAK,
    QUALITY_GOOD,
)
from app.services.llm.cv_build_prompt import (
    build_cv_from_scratch_prompt,
    build_cv_rebuild_prompt,
    _render_profile_for_cv_build,
)


# ── Quality verdict logic ─────────────────────────────────────────────────────

class TestQualityVerdicts:

    def test_score_below_poor_is_poor(self):
        assert get_verdict(0) == "poor"
        assert get_verdict(QUALITY_POOR - 1) == "poor"

    def test_score_at_poor_threshold_is_weak(self):
        assert get_verdict(QUALITY_POOR) == "weak"
        assert get_verdict(QUALITY_WEAK - 1) == "weak"

    def test_score_at_weak_threshold_is_good(self):
        assert get_verdict(QUALITY_WEAK) == "good"
        assert get_verdict(QUALITY_GOOD - 1) == "good"

    def test_score_at_good_threshold_is_strong(self):
        assert get_verdict(QUALITY_GOOD) == "strong"
        assert get_verdict(100) == "strong"

    def test_rebuild_recommended_below_weak(self):
        assert should_recommend_rebuild(0) is True
        assert should_recommend_rebuild(39) is True
        assert should_recommend_rebuild(QUALITY_WEAK - 1) is True

    def test_rebuild_not_recommended_at_weak_and_above(self):
        assert should_recommend_rebuild(QUALITY_WEAK) is False
        assert should_recommend_rebuild(90) is False


# ── Quality service ───────────────────────────────────────────────────────────

class TestCVQualityService:

    def _make_parsed_cv(self, sections=None):
        from app.schemas.cv import ParsedCV
        mock = MagicMock(spec=ParsedCV)
        mock.sections = sections or [
            {
                "heading": "Experience",
                "paragraphs": [
                    {"text": "Led product team of 8 engineers.", "runs": [], "style": ""},
                    {"text": "Grew revenue 3x in 18 months.", "runs": [], "style": ""},
                ],
            }
        ]
        return mock

    def test_score_parses_valid_json(self):
        svc = CVQualityService()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.input_tokens = 100
        mock_result.output_tokens = 50

        feedback_json = json.dumps({
            "score": 45,
            "verdict": "weak",
            "top_issues": ["No quantified achievements", "Generic bullet points"],
            "recommendation": "Rebuild recommended — replace duties with outcomes.",
            "rebuild_recommended": True,
        })
        mock_client.call_text = MagicMock(return_value=(feedback_json, mock_result))

        with patch("app.services.cv.quality_service.ClaudeClient", return_value=mock_client):
            result = svc.score(self._make_parsed_cv())

        assert result["score"] == 45
        assert result["verdict"] == "weak"
        assert result["rebuild_recommended"] is True
        assert len(result["top_issues"]) == 2

    def test_score_clamps_to_0_100(self):
        svc = CVQualityService()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.input_tokens = 50
        mock_result.output_tokens = 30
        feedback_json = json.dumps({
            "score": 150,  # Out of range
            "verdict": "strong",
            "top_issues": [],
            "recommendation": "Good.",
            "rebuild_recommended": False,
        })
        mock_client.call_text = MagicMock(return_value=(feedback_json, mock_result))

        with patch("app.services.cv.quality_service.ClaudeClient", return_value=mock_client):
            result = svc.score(self._make_parsed_cv())

        assert result["score"] == 100  # Clamped

    def test_score_returns_safe_default_on_llm_failure(self):
        svc = CVQualityService()
        mock_client = MagicMock()
        mock_client.call_text = MagicMock(side_effect=Exception("API timeout"))

        with patch("app.services.cv.quality_service.ClaudeClient", return_value=mock_client):
            result = svc.score(self._make_parsed_cv())

        # Should return a safe default, not raise
        assert "score" in result
        assert result["score"] == 60
        assert result["rebuild_recommended"] is False
        assert "error" in result

    def test_score_strips_markdown_fences(self):
        svc = CVQualityService()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.input_tokens = 50
        mock_result.output_tokens = 30
        # LLM wraps in markdown code fence
        feedback_json = "```json\n" + json.dumps({
            "score": 72,
            "verdict": "good",
            "top_issues": [],
            "recommendation": "Minor improvements needed.",
            "rebuild_recommended": False,
        }) + "\n```"
        mock_client.call_text = MagicMock(return_value=(feedback_json, mock_result))

        with patch("app.services.cv.quality_service.ClaudeClient", return_value=mock_client):
            result = svc.score(self._make_parsed_cv())

        assert result["score"] == 72


# ── CV build prompt builders ──────────────────────────────────────────────────

class TestCVBuildPrompts:

    def _make_profile(self):
        return {
            "headline": "Operator & Strategist | MENA",
            "global_context": "Former McKinsey consultant turned founder.",
            "global_notes": "",
            "experiences": [
                {
                    "company_name": "Baly",
                    "role_title": "Co-Founder & CEO",
                    "start_date": "2021",
                    "end_date": "2023",
                    "location": "Baghdad, Iraq",
                    "context": "Built Iraq's first on-demand delivery platform from scratch.",
                    "contribution": "Led all functions: product, ops, growth, fundraising.",
                    "outcomes": "Reached 500k orders/month, raised $10M Series A.",
                    "methods": "OKR planning, direct founder selling, external tech partners.",
                    "hidden": "Personally negotiated all 50 first restaurant partnerships.",
                    "freeform": "",
                }
            ],
        }

    def test_from_scratch_prompt_returns_tuple(self):
        system, user = build_cv_from_scratch_prompt(self._make_profile())
        assert isinstance(system, str)
        assert isinstance(user, str)
        assert len(system) > 100
        assert len(user) > 100

    def test_from_scratch_prompt_includes_profile_content(self):
        profile = self._make_profile()
        _, user = build_cv_from_scratch_prompt(profile)
        assert "Baly" in user
        assert "Co-Founder" in user
        assert "500k orders" in user

    def test_from_scratch_prompt_includes_jd_when_provided(self):
        _, user = build_cv_from_scratch_prompt(
            self._make_profile(),
            jd_text="VP Strategy role at e& Enterprise requiring MENA operator experience",
        )
        assert "e&" in user
        assert "VP Strategy" in user

    def test_from_scratch_prompt_includes_schema(self):
        _, user = build_cv_from_scratch_prompt(self._make_profile())
        assert "OUTPUT SCHEMA" in user
        assert "sections" in user
        assert "bullets" in user

    def test_rebuild_prompt_includes_issues(self):
        issues = ["No quantified achievements", "Uses passive voice throughout"]
        _, user = build_cv_rebuild_prompt(
            self._make_profile(),
            original_cv_issues=issues,
        )
        assert "No quantified achievements" in user
        assert "passive voice" in user

    def test_rebuild_prompt_differs_from_scratch(self):
        system_scratch, _ = build_cv_from_scratch_prompt(self._make_profile())
        system_rebuild, _ = build_cv_rebuild_prompt(self._make_profile())
        # Different system prompts for different modes
        assert system_scratch != system_rebuild

    def test_profile_renderer_includes_all_fields(self):
        profile = self._make_profile()
        rendered = _render_profile_for_cv_build(profile)
        assert "Baly" in rendered
        assert "Co-Founder & CEO" in rendered
        assert "500k orders" in rendered
        assert "personally negotiated" in rendered.lower()

    def test_profile_renderer_handles_empty_fields(self):
        profile = {
            "headline": "Test",
            "global_context": "",
            "global_notes": "",
            "experiences": [
                {
                    "company_name": "ACME",
                    "role_title": "Manager",
                    "start_date": "2020",
                    "end_date": "2022",
                    "context": "",
                    "contribution": "",
                    "outcomes": "",
                    "methods": "",
                    "hidden": "",
                    "freeform": "",
                }
            ],
        }
        # Should not raise
        rendered = _render_profile_for_cv_build(profile)
        assert "ACME" in rendered
        assert "Manager" in rendered


# ── CVBuildMode enum ──────────────────────────────────────────────────────────

class TestCVBuildMode:

    def test_all_modes_defined(self):
        assert CVBuildMode.EDIT.value == "edit"
        assert CVBuildMode.REBUILD.value == "rebuild"
        assert CVBuildMode.FROM_SCRATCH.value == "from_scratch"

    def test_build_modes_are_strings(self):
        for mode in CVBuildMode:
            assert isinstance(mode.value, str)


# ── CVBuildService ────────────────────────────────────────────────────────────

class TestCVBuildService:

    def _make_profile(self):
        return {
            "headline": "Test Candidate",
            "global_context": "Background context.",
            "experiences": [
                {
                    "company_name": "Acme Corp",
                    "role_title": "Director",
                    "start_date": "2020",
                    "end_date": "2023",
                    "context": "Led digital transformation.",
                    "contribution": "Built new operating model.",
                    "outcomes": "Revenue up 40%.",
                    "methods": "Agile, OKRs",
                    "hidden": "",
                    "freeform": "",
                }
            ],
        }

    def _make_built_cv_json(self):
        return json.dumps({
            "name": "Test Candidate",
            "headline": "Digital Transformation Leader",
            "contact": {"email": "test@example.com", "location": "Dubai, UAE"},
            "summary": "Experienced leader with track record of results.",
            "sections": [
                {
                    "section_type": "experience",
                    "title": "Professional Experience",
                    "entries": [
                        {
                            "role": "Director",
                            "company": "Acme Corp",
                            "start_date": "Jan 2020",
                            "end_date": "Dec 2023",
                            "bullets": ["Led digital transformation, revenue +40%."],
                        }
                    ],
                }
            ],
            "page_recommendation": "1",
        })

    @pytest.mark.asyncio
    async def test_build_from_profile_from_scratch(self):
        from app.services.cv.cv_build_service import CVBuildService

        svc = CVBuildService()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.input_tokens = 500
        mock_result.output_tokens = 800
        mock_result.cost_usd = 0.02
        mock_client.call_text = MagicMock(
            return_value=(self._make_built_cv_json(), mock_result)
        )

        with patch("app.services.cv.cv_build_service.ClaudeClient", return_value=mock_client):
            result = svc.build_from_profile(
                profile_dict=self._make_profile(),
                build_mode=CVBuildMode.FROM_SCRATCH.value,
            )

        assert result["name"] == "Test Candidate"
        assert len(result["sections"]) > 0

    def test_parse_raises_on_empty_sections(self):
        from app.services.cv.cv_build_service import CVBuildService
        svc = CVBuildService()
        bad_json = json.dumps({"name": "Test", "sections": []})
        with pytest.raises(ValueError, match="sections array is empty"):
            svc._parse_built_cv(bad_json, "from_scratch")

    def test_parse_raises_on_missing_required_fields(self):
        from app.services.cv.cv_build_service import CVBuildService
        svc = CVBuildService()
        bad_json = json.dumps({"headline": "Missing name and sections"})
        with pytest.raises(ValueError, match="missing required fields"):
            svc._parse_built_cv(bad_json, "from_scratch")

    def test_parse_strips_markdown_fences(self):
        from app.services.cv.cv_build_service import CVBuildService
        svc = CVBuildService()
        wrapped = "```json\n" + self._make_built_cv_json() + "\n```"
        result = svc._parse_built_cv(wrapped, "from_scratch")
        assert result["name"] == "Test Candidate"
