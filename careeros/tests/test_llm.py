"""
tests/test_llm.py

Tests for the Claude API client and prompt builders.

All Anthropic API calls are mocked — no real API calls made.
Tests verify:
  - Retry logic on rate limits and transient errors
  - JSON parsing and schema validation
  - Cost estimation
  - Prompt building (structure and required keywords)
  - JSON fence stripping

Run with: pytest tests/test_llm.py -v
"""

import json
import time
from unittest.mock import MagicMock, patch, call

import pytest
from pydantic import BaseModel

from app.services.llm.client import (
    ClaudeClient,
    LLMCallResult,
    _strip_json_fences,
    _estimate_cost_usd,
)
from app.services.llm.schemas import (
    EditPlan,
    ReportPack,
    ParagraphEdit,
    EditOperation,
)
from app.services.llm.prompts import (
    build_edit_plan_user_prompt,
    build_report_pack_user_prompt,
    _render_cv_for_prompt,
    _render_cv_summary_for_prompt,
    build_all_prompts,
)


# ── Fixtures ────────────────────────────────────────────────────────────────

def make_parsed_cv():
    """Build a minimal ParsedCV for prompt tests."""
    from app.schemas.cv import ParsedCV, ParsedSection, ParsedNode

    nodes = [
        ParsedNode(index=0, text="Matteo Mantovani", style="Normal", runs=[]),
        ParsedNode(index=1, text="CEO | Founder | INSEAD MBA", style="Normal", runs=[]),
        ParsedNode(index=2, text="", style="Normal", runs=[], is_empty=True),
        ParsedNode(index=3, text="EXPERIENCE", style="Normal", runs=[]),
        ParsedNode(index=4, text="CEO, Liively — Dubai (2024–present)", style="Normal", runs=[]),
        ParsedNode(index=5, text="Built product 0→$1M ARR in 12 months.", style="Normal", runs=[]),
        ParsedNode(index=6, text="", style="Normal", runs=[], is_empty=True),
        ParsedNode(index=7, text="EDUCATION", style="Normal", runs=[]),
        ParsedNode(index=8, text="INSEAD — MBA (2018–2019)", style="Normal", runs=[]),
    ]

    sections = [
        ParsedSection(heading="Preamble", heading_index=0, paragraphs=nodes[:3]),
        ParsedSection(heading="EXPERIENCE", heading_index=3, paragraphs=nodes[3:7]),
        ParsedSection(heading="EDUCATION", heading_index=7, paragraphs=nodes[7:]),
    ]

    return ParsedCV(
        total_paragraphs=9,
        total_words=42,
        sections=sections,
        raw_paragraphs=nodes,
    )


def make_mock_anthropic_response(content: str) -> MagicMock:
    """Build a mock anthropic.messages.create() response."""
    response = MagicMock()
    response.content = [MagicMock(text=content)]
    response.usage.input_tokens = 1000
    response.usage.output_tokens = 500
    response.model = "claude-opus-4-6"
    return response


VALID_EDIT_PLAN_JSON = json.dumps({
    "headline_summary": {
        "new_headline": "Senior Strategy Executive",
        "new_summary": "Value creation specialist with 10 years...",
        "rationale": "Aligns with JD requirement"
    },
    "paragraph_edits": [
        {
            "paragraph_index": 5,
            "operation": "replace_text",
            "new_text": "Delivered 8-15X value creation across 4 verticals.",
            "run_index": None,
            "style": None,
            "rationale": "Incorporates 'value creation' keyword from JD"
        }
    ],
    "keyword_additions": ["value creation", "operating model"],
    "sections_to_add": [],
    "positioning_note": "Position as strategy practitioner returning to corp role.",
    "keyword_match_score": 75
})

VALID_REPORT_PACK_JSON = json.dumps({
    "company": {
        "company_name": "e& UAE",
        "hq_location": "Abu Dhabi, UAE",
        "business_description": "UAE-headquartered global tech group.",
        "revenue_and_scale": "AED 34.9B H1 2025",
        "recent_news": ["Vodafone stake acquired"],
        "strategic_priorities": ["e& 2030 transformation"],
        "culture_signals": ["Execution-focused"],
        "competitor_landscape": "du, Zain, STC",
        "hiring_signals": ["VP understaffed"],
        "red_flags": []
    },
    "role": {
        "role_title": "Senior Director Growth Strategy",
        "seniority_level": "Senior Director",
        "reporting_line": "VP Value Creation",
        "what_they_really_want": "Independent project runner.",
        "hidden_requirements": ["Semi-gov navigation"],
        "hiring_manager_worries": ["Will candidate take direction?"],
        "keyword_match_gaps": ["synergies", "playbooks"],
        "positioning_recommendation": "Lead with McKinsey identity."
    },
    "salary": [
        {
            "title": "Senior Director",
            "base_monthly_aed_low": 35000,
            "base_monthly_aed_high": 55000,
            "base_annual_aed_low": 420000,
            "base_annual_aed_high": 660000,
            "bonus_pct_low": 15,
            "bonus_pct_high": 25,
            "total_comp_note": "Plus housing allowance",
            "source": "Market estimate",
            "confidence": "medium"
        }
    ],
    "networking": {
        "target_contacts": ["VP Strategy", "CPO"],
        "warm_path_hypotheses": ["INSEAD alumni at e&"],
        "linkedin_search_strings": ["\"e&\" AND \"INSEAD\""],
        "outreach_template_hiring_manager": "Hi, I saw the role...",
        "outreach_template_alumni": "Hi, we share INSEAD...",
        "outreach_template_recruiter": "Hi, I applied for...",
        "seven_day_action_plan": ["Day 1: INSEAD alumni search"]
    },
    "application": {
        "positioning_headline": "McKinsey EM + Founder",
        "cover_letter_angle": "Strategy practitioner who executes.",
        "interview_prep_themes": ["Value creation frameworks"],
        "thirty_sixty_ninety": {
            "30": "Understand VP priorities",
            "60": "Own first workstream",
            "90": "Lead full initiative"
        },
        "risks_to_address": ["Pivot from operator to strategist"],
        "differentiators": ["Baly: 0→500 in 18 months"]
    },
    "exec_summary": [
        "Strong McKinsey + founder combo",
        "MENA market expertise is rare",
        "Salary range: AED 420K-660K base"
    ]
})


# ── Unit tests: utility functions ───────────────────────────────────────────

class TestStripJsonFences:
    """Tests for the JSON fence stripping helper."""

    def test_clean_json_unchanged(self):
        data = '{"key": "value"}'
        assert _strip_json_fences(data) == data

    def test_json_fence_stripped(self):
        data = '```json\n{"key": "value"}\n```'
        result = _strip_json_fences(data)
        assert result == '{"key": "value"}'

    def test_bare_fence_stripped(self):
        data = '```\n{"key": "value"}\n```'
        result = _strip_json_fences(data)
        assert result == '{"key": "value"}'

    def test_whitespace_trimmed(self):
        data = '  \n  {"key": "value"}  \n  '
        result = _strip_json_fences(data)
        assert result == '{"key": "value"}'


class TestCostEstimation:
    """Tests for token cost estimation."""

    def test_opus_cost_calculated(self):
        cost = _estimate_cost_usd("claude-opus-4-6", 1_000_000, 1_000_000)
        # Input: $15 + Output: $75 = $90
        assert cost == 90.0

    def test_unknown_model_returns_zero(self):
        cost = _estimate_cost_usd("unknown-model", 1000, 500)
        assert cost == 0.0

    def test_small_call_cost_is_fractional(self):
        # 1K input + 500 output tokens on opus
        cost = _estimate_cost_usd("claude-opus-4-6", 1000, 500)
        assert cost > 0
        assert cost < 0.10  # Should be well under 10 cents


class TestLLMCallResult:
    """Tests for LLMCallResult metadata serialisation."""

    def test_to_metadata_contains_required_fields(self):
        result = LLMCallResult(
            content="{}",
            input_tokens=1000,
            output_tokens=500,
            model="claude-opus-4-6",
            duration_seconds=2.5,
        )
        meta = result.to_metadata()
        assert meta["input_tokens"] == 1000
        assert meta["output_tokens"] == 500
        assert meta["total_tokens"] == 1500
        assert meta["duration_seconds"] == 2.5
        assert "cost_usd" in meta
        assert "model" in meta


# ── Unit tests: ClaudeClient ────────────────────────────────────────────────

class TestClaudeClientStructured:
    """Tests for call_structured with mocked Anthropic SDK."""

    def _make_client(self):
        with patch("app.services.llm.client.anthropic.Anthropic"):
            client = ClaudeClient()
        return client

    @patch("anthropic.Anthropic")
    def test_happy_path_returns_parsed_schema(self, mock_anthropic_class):
        """Valid JSON response is parsed into EditPlan."""
        mock_instance = mock_anthropic_class.return_value
        mock_instance.messages.create.return_value = make_mock_anthropic_response(
            VALID_EDIT_PLAN_JSON
        )

        client = ClaudeClient()
        result, metadata = client.call_structured(
            system_prompt="system",
            user_prompt="user",
            schema=EditPlan,
        )

        assert isinstance(result, EditPlan)
        assert result.keyword_match_score == 75
        assert result.headline_summary.new_headline == "Senior Strategy Executive"

    @patch("anthropic.Anthropic")
    def test_json_fence_in_response_is_handled(self, mock_anthropic_class):
        """Response wrapped in ```json fences should still parse."""
        mock_instance = mock_anthropic_class.return_value
        mock_instance.messages.create.return_value = make_mock_anthropic_response(
            f"```json\n{VALID_EDIT_PLAN_JSON}\n```"
        )

        client = ClaudeClient()
        result, _ = client.call_structured(
            system_prompt="s",
            user_prompt="u",
            schema=EditPlan,
        )
        assert isinstance(result, EditPlan)

    @patch("anthropic.Anthropic")
    def test_invalid_json_raises_value_error(self, mock_anthropic_class):
        """Non-JSON response raises ValueError."""
        mock_instance = mock_anthropic_class.return_value
        mock_instance.messages.create.return_value = make_mock_anthropic_response(
            "I cannot provide a JSON response for this request."
        )

        client = ClaudeClient()
        with pytest.raises(ValueError, match="invalid JSON"):
            client.call_structured(
                system_prompt="s",
                user_prompt="u",
                schema=EditPlan,
            )

    @patch("time.sleep")
    @patch("anthropic.Anthropic")
    def test_rate_limit_retries_then_succeeds(
        self, mock_anthropic_class, mock_sleep
    ):
        """Rate limit on first call should trigger retry and eventually succeed."""
        from anthropic import RateLimitError

        mock_instance = mock_anthropic_class.return_value

        # First call: rate limit. Second call: success.
        mock_instance.messages.create.side_effect = [
            RateLimitError(
                message="Rate limit exceeded",
                response=MagicMock(status_code=429, headers={}),
                body={},
            ),
            make_mock_anthropic_response(VALID_EDIT_PLAN_JSON),
        ]

        client = ClaudeClient()
        result, _ = client.call_structured(
            system_prompt="s",
            user_prompt="u",
            schema=EditPlan,
        )

        assert isinstance(result, EditPlan)
        assert mock_instance.messages.create.call_count == 2
        mock_sleep.assert_called_once()  # Should have slept once between retries

    @patch("time.sleep")
    @patch("anthropic.Anthropic")
    def test_max_retries_exceeded_raises_runtime_error(
        self, mock_anthropic_class, mock_sleep
    ):
        """Consistent rate limits should exhaust retries and raise RuntimeError."""
        from anthropic import RateLimitError

        mock_instance = mock_anthropic_class.return_value
        mock_instance.messages.create.side_effect = RateLimitError(
            message="Rate limit",
            response=MagicMock(status_code=429, headers={}),
            body={},
        )

        client = ClaudeClient()
        with pytest.raises(RuntimeError, match="failed after"):
            client.call_structured(
                system_prompt="s",
                user_prompt="u",
                schema=EditPlan,
            )

    @patch("anthropic.Anthropic")
    def test_report_pack_schema_parsed(self, mock_anthropic_class):
        """Valid ReportPack JSON is parsed correctly."""
        mock_instance = mock_anthropic_class.return_value
        mock_instance.messages.create.return_value = make_mock_anthropic_response(
            VALID_REPORT_PACK_JSON
        )

        client = ClaudeClient()
        result, _ = client.call_structured(
            system_prompt="s",
            user_prompt="u",
            schema=ReportPack,
        )

        assert isinstance(result, ReportPack)
        assert result.company.company_name == "e& UAE"
        assert result.role.role_title == "Senior Director Growth Strategy"
        assert len(result.salary) == 1
        assert result.salary[0].confidence == "medium"


# ── Unit tests: prompt builders ─────────────────────────────────────────────

class TestPromptBuilders:
    """Tests that prompts are built correctly and contain required elements."""

    def test_edit_plan_prompt_contains_jd_text(self):
        parsed_cv = make_parsed_cv()
        prompt = build_edit_plan_user_prompt(
            parsed_cv=parsed_cv,
            jd_text="We need someone with value creation experience.",
            preferences={"tone": "executive", "region": "UAE", "page_limit": 2},
        )
        assert "value creation" in prompt

    def test_edit_plan_prompt_contains_paragraph_indices(self):
        """Prompt must include [P:N] notation so Claude can reference indices."""
        parsed_cv = make_parsed_cv()
        prompt = build_edit_plan_user_prompt(
            parsed_cv=parsed_cv,
            jd_text="test jd",
            preferences={},
        )
        assert "[P:0]" in prompt
        assert "[P:4]" in prompt

    def test_edit_plan_prompt_includes_career_notes(self):
        parsed_cv = make_parsed_cv()
        prompt = build_edit_plan_user_prompt(
            parsed_cv=parsed_cv,
            jd_text="test jd",
            preferences={"career_notes": "Raised $5M seed round in 2022."},
        )
        assert "Raised $5M" in prompt

    def test_report_pack_prompt_contains_jd(self):
        parsed_cv = make_parsed_cv()
        prompt = build_report_pack_user_prompt(
            parsed_cv=parsed_cv,
            jd_text="EiR role at Revolut UAE",
            retrieval_data={},
            preferences={"region": "UAE"},
        )
        assert "Revolut" in prompt

    def test_report_pack_prompt_includes_retrieval_data(self):
        parsed_cv = make_parsed_cv()
        retrieval = {
            "company_overview": "Revolut is a global fintech with 60M users.",
            "salary_data": "EiR roles range AED 45K-65K/month.",
        }
        prompt = build_report_pack_user_prompt(
            parsed_cv=parsed_cv,
            jd_text="test",
            retrieval_data=retrieval,
            preferences={},
        )
        assert "60M users" in prompt
        assert "45K-65K" in prompt

    def test_report_pack_empty_retrieval_handled(self):
        """Empty retrieval data should not cause errors."""
        parsed_cv = make_parsed_cv()
        prompt = build_report_pack_user_prompt(
            parsed_cv=parsed_cv,
            jd_text="test",
            retrieval_data={},
            preferences={},
        )
        assert "No retrieval data available" in prompt

    def test_cv_render_includes_section_names(self):
        parsed_cv = make_parsed_cv()
        rendered = _render_cv_for_prompt(parsed_cv)
        assert "[SECTION: EXPERIENCE]" in rendered
        assert "[SECTION: EDUCATION]" in rendered

    def test_cv_render_marks_empty_paragraphs(self):
        parsed_cv = make_parsed_cv()
        rendered = _render_cv_for_prompt(parsed_cv)
        assert "<empty>" in rendered

    def test_build_all_prompts_returns_both_pairs(self):
        parsed_cv = make_parsed_cv()
        prompts = build_all_prompts(
            parsed_cv=parsed_cv,
            jd_text="Test JD",
            retrieval_data={},
            preferences={},
        )
        assert "edit_plan" in prompts
        assert "report_pack" in prompts
        assert len(prompts["edit_plan"]) == 2   # (system, user)
        assert len(prompts["report_pack"]) == 2


# ── Schema validation tests ─────────────────────────────────────────────────

class TestLLMOutputSchemas:
    """Tests for Pydantic schema validation of LLM outputs."""

    def test_edit_plan_minimal_is_valid(self):
        """EditPlan with only required fields should validate."""
        plan = EditPlan()
        assert plan.paragraph_edits == []
        assert plan.keyword_match_score == 0

    def test_paragraph_edit_replace_text_valid(self):
        edit = ParagraphEdit(
            paragraph_index=3,
            operation=EditOperation.REPLACE_TEXT,
            new_text="New bullet text.",
            rationale="Adds keyword",
        )
        edit.validate_for_operation()  # Should not raise

    def test_paragraph_edit_replace_text_missing_text_raises(self):
        edit = ParagraphEdit(
            paragraph_index=3,
            operation=EditOperation.REPLACE_TEXT,
            # new_text deliberately omitted
        )
        with pytest.raises(ValueError, match="requires new_text"):
            edit.validate_for_operation()

    def test_paragraph_edit_replace_run_missing_run_index_raises(self):
        edit = ParagraphEdit(
            paragraph_index=3,
            operation=EditOperation.REPLACE_RUN,
            new_text="New text",
            # run_index deliberately omitted
        )
        with pytest.raises(ValueError, match="replace_run requires"):
            edit.validate_for_operation()

    def test_keyword_match_score_bounds(self):
        with pytest.raises(Exception):
            EditPlan(keyword_match_score=101)
        with pytest.raises(Exception):
            EditPlan(keyword_match_score=-1)

    def test_full_edit_plan_from_json(self):
        data = json.loads(VALID_EDIT_PLAN_JSON)
        plan = EditPlan.model_validate(data)
        assert len(plan.paragraph_edits) == 1
        assert plan.paragraph_edits[0].operation == EditOperation.REPLACE_TEXT

    def test_full_report_pack_from_json(self):
        data = json.loads(VALID_REPORT_PACK_JSON)
        pack = ReportPack.model_validate(data)
        assert pack.company.company_name == "e& UAE"
        assert pack.networking.linkedin_search_strings[0] == '"e&" AND "INSEAD"'
        assert pack.application.thirty_sixty_ninety["30"] == "Understand VP priorities"
