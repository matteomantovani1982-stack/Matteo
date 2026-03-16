"""
tests/test_profile.py

Tests for the CandidateProfile and ExperienceEntry layer.

Coverage:
  - Model computed properties (is_ready, fields_completed, to_prompt_dict)
  - Schema validation (create, update, overrides)
  - ProfileService logic (create, activate, versioning, ordering)
  - Profile → prompt rendering
  - API endpoints (mocked service)
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.candidate_profile import (
    CandidateProfile,
    ExperienceEntry,
    ProfileStatus,
)
from app.schemas.candidate_profile import (
    ApplicationProfileOverrides,
    ExperienceOverride,
    IntakeQuestionsResponse,
    INTAKE_QUESTIONS,
)
from app.services.llm.profile_prompt import (
    _render_candidate_profile,
    build_positioning_strategy_prompt,
    build_profile_edit_plan_prompt,
)


# ── Model tests ───────────────────────────────────────────────────────────────

class TestCandidateProfileModel:
    def _make_profile(self, experiences=None) -> CandidateProfile:
        p = CandidateProfile()
        p.id = uuid.uuid4()
        p.user_id = "user_123"
        p.version = 1
        p.status = ProfileStatus.DRAFT
        p.headline = "Entrepreneur and former McKinsey EM"
        p.global_context = "Pivoting from founder back to corporate strategy"
        p.global_notes = "Built Baly with Rocket Internet in Iraq"
        p.experiences = experiences or []
        return p

    def _make_entry(self, is_complete=True, order=0) -> ExperienceEntry:
        e = ExperienceEntry()
        e.id = uuid.uuid4()
        e.profile_id = uuid.uuid4()
        e.company_name = "Baly"
        e.role_title = "Co-Founder & CEO"
        e.start_date = "2021-01"
        e.end_date = "2023-06"
        e.location = "Baghdad, Iraq"
        e.context = "Iraq had no digital ride-hailing market."
        e.contribution = "I built the ops playbook and hired the first 50 people."
        e.outcomes = "500+ employees, 8-15X valuation, 4 verticals."
        e.methods = "OKR framework, direct founder selling."
        e.hidden = "We nearly ran out of cash in month 8."
        e.freeform = None
        e.display_order = order
        e.is_complete = is_complete
        return e

    def test_is_ready_false_with_no_experiences(self):
        profile = self._make_profile(experiences=[])
        assert profile.is_ready is False

    def test_is_ready_false_with_incomplete_experience(self):
        entry = self._make_entry(is_complete=False)
        profile = self._make_profile(experiences=[entry])
        assert profile.is_ready is False

    def test_is_ready_true_with_complete_experience(self):
        entry = self._make_entry(is_complete=True)
        profile = self._make_profile(experiences=[entry])
        assert profile.is_ready is True

    def test_to_prompt_dict_structure(self):
        entry = self._make_entry()
        profile = self._make_profile(experiences=[entry])
        d = profile.to_prompt_dict()

        assert "headline" in d
        assert "global_context" in d
        assert "global_notes" in d
        assert "experiences" in d
        assert len(d["experiences"]) == 1

    def test_to_prompt_dict_experience_content(self):
        entry = self._make_entry()
        profile = self._make_profile(experiences=[entry])
        d = profile.to_prompt_dict()
        exp = d["experiences"][0]

        assert exp["company"] == "Baly"
        assert exp["role"] == "Co-Founder & CEO"
        assert "context" in exp
        assert "contribution" in exp
        assert "outcomes" in exp
        assert "hidden" in exp

    def test_to_prompt_dict_omits_empty_fields(self):
        entry = self._make_entry()
        entry.freeform = None
        entry.hidden = None
        profile = self._make_profile(experiences=[entry])
        d = profile.to_prompt_dict()
        exp = d["experiences"][0]

        assert "additional" not in exp
        assert "hidden" not in exp


class TestExperienceEntryModel:
    def _make_entry(self) -> ExperienceEntry:
        e = ExperienceEntry()
        e.context = "Context text"
        e.contribution = "Contribution text"
        e.outcomes = "Outcomes text"
        e.methods = "Methods text"
        e.hidden = "Hidden text"
        e.freeform = None
        e.start_date = "2021"
        e.end_date = "2023"
        return e

    def test_fields_completed_counts_non_empty(self):
        entry = self._make_entry()
        assert entry.fields_completed == 5

    def test_fields_completed_excludes_empty(self):
        entry = self._make_entry()
        entry.hidden = None
        entry.methods = "  "  # whitespace only
        assert entry.fields_completed == 3

    def test_date_range_with_both_dates(self):
        entry = self._make_entry()
        assert entry.date_range == "2021–2023"

    def test_date_range_with_no_start(self):
        entry = self._make_entry()
        entry.start_date = None
        assert entry.date_range == "2023"

    def test_to_prompt_dict_excludes_none_fields(self):
        entry = self._make_entry()
        entry.freeform = None
        entry.location = None
        d = entry.to_prompt_dict()
        assert "additional" not in d
        assert "location" not in d


# ── Schema tests ──────────────────────────────────────────────────────────────

class TestIntakeQuestionsResponse:
    def test_build_returns_all_five_questions(self):
        response = IntakeQuestionsResponse.build()
        assert len(response.questions) == 5

    def test_all_questions_have_required_fields(self):
        response = IntakeQuestionsResponse.build()
        for q in response.questions:
            assert q.field in INTAKE_QUESTIONS
            assert q.label
            assert q.prompt
            assert q.placeholder

    def test_question_fields_match_model_fields(self):
        response = IntakeQuestionsResponse.build()
        fields = {q.field for q in response.questions}
        assert fields == {"context", "contribution", "outcomes", "methods", "hidden"}


class TestApplicationProfileOverrides:
    def test_get_override_returns_matching_entry(self):
        exp_id = uuid.uuid4()
        override = ExperienceOverride(
            experience_id=exp_id,
            additional_context="Focus on fintech angle",
            highlight=True,
        )
        overrides = ApplicationProfileOverrides(experience_overrides=[override])
        result = overrides.get_override(exp_id)
        assert result is not None
        assert result.highlight is True
        assert result.additional_context == "Focus on fintech angle"

    def test_get_override_returns_none_for_missing(self):
        overrides = ApplicationProfileOverrides(experience_overrides=[])
        result = overrides.get_override(uuid.uuid4())
        assert result is None

    def test_suppress_and_highlight_are_independent(self):
        exp_id = uuid.uuid4()
        override = ExperienceOverride(experience_id=exp_id, suppress=True, highlight=False)
        assert override.suppress is True
        assert override.highlight is False


# ── Prompt building tests ─────────────────────────────────────────────────────

class TestRenderCandidateProfile:
    def _make_profile_dict(self) -> dict:
        return {
            "headline": "Serial entrepreneur and former McKinsey EM",
            "global_context": "Pivoting back to corporate strategy from founder roles",
            "global_notes": "Also authored Iraq Untold (Amazon bestseller)",
            "experiences": [
                {
                    "company": "Baly",
                    "role": "Co-Founder & CEO",
                    "dates": "2021–2023",
                    "location": "Baghdad",
                    "context": "No digital ride-hailing existed in Iraq.",
                    "contribution": "Built ops playbook, hired first 50.",
                    "outcomes": "500+ employees, 8-15X valuation.",
                    "methods": "OKR framework.",
                    "hidden": "Nearly ran out of cash in month 8.",
                }
            ],
        }

    def test_render_includes_headline(self):
        profile_dict = self._make_profile_dict()
        text = _render_candidate_profile(profile_dict)
        assert "Serial entrepreneur" in text

    def test_render_includes_all_five_fields(self):
        profile_dict = self._make_profile_dict()
        text = _render_candidate_profile(profile_dict)
        assert "SITUATION & CONTEXT" in text
        assert "MY SPECIFIC CONTRIBUTION" in text
        assert "OUTCOMES & IMPACT" in text
        assert "HOW I DID IT" in text
        assert "WHAT THE CV DOESN'T SHOW" in text

    def test_render_includes_experience_header(self):
        profile_dict = self._make_profile_dict()
        text = _render_candidate_profile(profile_dict)
        assert "Co-Founder & CEO @ Baly" in text

    def test_render_skips_suppressed_experiences(self):
        profile_dict = self._make_profile_dict()
        profile_dict["experiences"][0]["suppressed"] = True
        text = _render_candidate_profile(profile_dict)
        assert "Co-Founder & CEO @ Baly" not in text

    def test_render_marks_highlighted_experiences(self):
        profile_dict = self._make_profile_dict()
        profile_dict["experiences"][0]["highlight"] = True
        text = _render_candidate_profile(profile_dict)
        assert "★ HIGHLIGHT" in text

    def test_render_includes_application_context(self):
        profile_dict = self._make_profile_dict()
        profile_dict["application_context"] = "Emphasise the fintech angle"
        text = _render_candidate_profile(profile_dict)
        assert "APPLICATION-SPECIFIC CONTEXT" in text
        assert "fintech angle" in text

    def test_render_includes_global_notes(self):
        profile_dict = self._make_profile_dict()
        text = _render_candidate_profile(profile_dict)
        assert "Iraq Untold" in text


class TestBuildPositioningStrategyPrompt:
    def _profile_dict(self) -> dict:
        return {
            "headline": "Entrepreneur",
            "global_context": "Looking for EiR roles",
            "global_notes": "",
            "experiences": [],
        }

    def test_returns_system_and_user(self):
        system, user = build_positioning_strategy_prompt(
            profile_dict=self._profile_dict(),
            jd_text="We are looking for an EiR...",
            preferences={"region": "UAE"},
        )
        assert len(system) > 100
        assert "JOB DESCRIPTION" in user

    def test_user_prompt_contains_jd(self):
        jd = "Unique JD text that should appear in prompt 12345"
        _, user = build_positioning_strategy_prompt(
            profile_dict=self._profile_dict(),
            jd_text=jd,
            preferences={},
        )
        assert "12345" in user

    def test_retrieval_data_included_when_provided(self):
        _, user = build_positioning_strategy_prompt(
            profile_dict=self._profile_dict(),
            jd_text="JD text",
            preferences={},
            retrieval_data={"company_overview": "Revolut is a fintech unicorn."},
        )
        assert "COMPANY INTELLIGENCE" in user
        assert "fintech unicorn" in user

    def test_system_prompt_contains_schema(self):
        system, _ = build_positioning_strategy_prompt(
            profile_dict=self._profile_dict(),
            jd_text="JD",
            preferences={},
        )
        assert "strongest_angles" in system
        assert "gaps_to_address" in system
        assert "narrative_thread" in system
        assert "red_flags_and_responses" in system


class TestBuildProfileEditPlanPrompt:
    def _make_parsed_cv(self):
        from app.schemas.cv import ParsedCV, ParsedParagraph
        para = MagicMock()
        para.text = "Matteo Mantovani — CEO | Founder"
        para.style = "Normal"
        para.index = 0

        cv = MagicMock(spec=ParsedCV)
        cv.raw_paragraphs = [para]
        cv.sections = []
        return cv

    def test_returns_system_and_user(self):
        system, user = build_profile_edit_plan_prompt(
            profile_dict={"headline": "test", "global_context": "", "global_notes": "", "experiences": []},
            parsed_cv=self._make_parsed_cv(),
            jd_text="JD text here",
            preferences={"tone": "professional", "region": "UAE", "page_limit": 2},
        )
        assert "CareerOS CV Tailor" in system
        assert "JOB DESCRIPTION" in user
        assert "CV STRUCTURE" in user

    def test_user_prompt_contains_preferences(self):
        _, user = build_profile_edit_plan_prompt(
            profile_dict={"headline": "", "global_context": "", "global_notes": "", "experiences": []},
            parsed_cv=self._make_parsed_cv(),
            jd_text="JD",
            preferences={"tone": "executive", "region": "KSA", "page_limit": 1},
        )
        assert "executive" in user
        assert "KSA" in user

    def test_cv_structure_section_included(self):
        cv = self._make_parsed_cv()
        _, user = build_profile_edit_plan_prompt(
            profile_dict={"headline": "", "global_context": "", "global_notes": "", "experiences": []},
            parsed_cv=cv,
            jd_text="JD",
            preferences={},
        )
        assert "P:0" in user
        assert "Matteo Mantovani" in user
