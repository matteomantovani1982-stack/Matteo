"""
app/services/llm/schemas.py

Pydantic models for structured LLM outputs.

These are the schemas Claude must produce JSON matching.
They are validated by ClaudeClient.call_structured() before
being stored in JobRun.edit_plan and JobRun.reports.

Two top-level outputs per run:
  1. EditPlan   — instructions for the DOCX renderer
  2. ReportPack — company intelligence, salary, networking, strategy

Design principle: Every field Claude might skip must be Optional.
Validation is lenient — we prefer partial output over hard failure.
"""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


# ── Edit Plan schemas ───────────────────────────────────────────────────────
# These tell the DOCX renderer exactly what to change.
# Operations are index-based — the renderer matches by paragraph index.

class EditOperation(StrEnum):
    REPLACE_TEXT = "replace_text"      # Replace full paragraph text
    REPLACE_RUN  = "replace_run"       # Replace text of a specific run
    INSERT_AFTER = "insert_after"      # Insert new paragraph after index
    DELETE       = "delete"            # Remove paragraph at index
    RESTYLE      = "restyle"           # Change paragraph style only


class ParagraphEdit(BaseModel):
    """
    A single edit instruction targeting one paragraph by index.

    paragraph_index: zero-based index into raw_paragraphs list from ParsedCV.
    operation:       what to do (replace, insert, delete, restyle).
    new_text:        required for replace_text, insert_after.
    run_index:       required for replace_run (which run within the paragraph).
    style:           required for restyle; optional for insert_after.
    rationale:       Claude's explanation — useful for debugging and review UI.
    """
    paragraph_index: int = Field(..., ge=0)
    operation: EditOperation
    new_text: str | None = None
    run_index: int | None = None
    style: str | None = None
    rationale: str = Field(default="", max_length=2000)

    def validate_for_operation(self) -> None:
        """Raise ValueError if required fields are missing for the operation."""
        if self.operation in (EditOperation.REPLACE_TEXT, EditOperation.INSERT_AFTER):
            if self.new_text is None:
                raise ValueError(f"{self.operation} requires new_text")
        if self.operation == EditOperation.REPLACE_RUN:
            if self.new_text is None or self.run_index is None:
                raise ValueError("replace_run requires new_text and run_index")


class HeadlineSummaryEdit(BaseModel):
    """
    Special edit for the CV headline and summary block.
    Treated separately from paragraph edits for clarity.
    """
    new_headline: str | None = Field(
        default=None,
        max_length=200,
        description="New headline / title line (e.g. 'Senior Strategy Executive')",
    )
    new_summary: str | None = Field(
        default=None,
        max_length=1500,
        description="New professional summary paragraph",
    )
    rationale: str = Field(default="", max_length=2000)


class EditPlan(BaseModel):
    """
    Complete set of edit instructions for the DOCX renderer.

    Stored in JobRun.edit_plan.
    Applied by app/services/rendering/docx_renderer.py.

    Fields:
        headline_summary: Optional new headline and summary
        paragraph_edits:  List of targeted paragraph edits
        keyword_additions: Keywords to weave in (renderer decides placement)
        sections_to_add:  New sections to append (e.g. 'Key Achievements')
        positioning_note: Claude's explanation of the overall tailoring strategy
        keyword_match_score: 0-100 score of CV-JD alignment
    """
    headline_summary: HeadlineSummaryEdit | None = None
    paragraph_edits: list[ParagraphEdit] = Field(default_factory=list)
    keyword_additions: list[str] = Field(
        default_factory=list,
        description="JD keywords to add if not already present",
    )
    sections_to_add: list[dict[str, Any]] = Field(
        default_factory=list,
        description="New sections: [{heading, content, insert_after_index}]",
    )
    positioning_note: str = Field(
        default="",
        max_length=1000,
        description="Claude's overall tailoring rationale",
    )
    keyword_match_score: int = Field(
        default=0,
        ge=0,
        le=100,
        description="Estimated CV-JD keyword alignment score (0-100)",
    )


# ── Report Pack schemas ─────────────────────────────────────────────────────
# These are the intelligence reports shown to the user.

class SalaryBand(BaseModel):
    """A single salary data point with source attribution."""
    title: str
    base_monthly_aed_low: int | None = None
    base_monthly_aed_high: int | None = None
    base_annual_aed_low: int | None = None
    base_annual_aed_high: int | None = None
    bonus_pct_low: int | None = None
    bonus_pct_high: int | None = None
    total_comp_note: str | None = None
    source: str = ""
    confidence: str = Field(
        default="low",
        description="low | medium | high",
    )


class CompanyIntelligence(BaseModel):
    """Company research summary for the intelligence report."""
    company_name: str
    hq_location: str = ""
    business_description: str = ""
    revenue_and_scale: str = ""
    recent_news: list[str] = Field(default_factory=list)
    strategic_priorities: list[str] = Field(default_factory=list)
    culture_signals: list[str] = Field(default_factory=list)
    competitor_landscape: str = ""
    hiring_signals: list[str] = Field(default_factory=list)
    red_flags: list[str] = Field(default_factory=list)


class RoleIntelligence(BaseModel):
    """Analysis of the specific role being applied to."""
    role_title: str
    seniority_level: str = ""
    reporting_line: str = ""
    what_they_really_want: str = ""
    hidden_requirements: list[str] = Field(default_factory=list)
    hiring_manager_worries: list[str] = Field(default_factory=list)
    keyword_match_gaps: list[str] = Field(default_factory=list)
    positioning_recommendation: str = ""


class NetworkingStrategy(BaseModel):
    """Networking plan for the specific role and company."""
    named_contacts: list[dict] = Field(
        default_factory=list,
        description="List of specific people to target. Each: {name, title, linkedin_url, why_relevant, outreach_message}",
    )
    target_contacts: list[str] = Field(
        default_factory=list,
        description="Job titles of people to target if specific names unknown",
    )
    known_network_asks: list[dict] = Field(
        default_factory=list,
        description="Asks for people candidate already knows. Each: {person, ask}",
    )
    warm_path_hypotheses: list[str] = Field(default_factory=list)
    linkedin_search_strings: list[str] = Field(default_factory=list)
    outreach_template_hiring_manager: str = ""
    outreach_template_alumni: str = ""
    outreach_template_recruiter: str = ""
    seven_day_action_plan: list[str] = Field(default_factory=list)


class ApplicationStrategy(BaseModel):
    """Tactical application advice for this specific role."""
    positioning_headline: str = ""
    cover_letter_angle: str = ""
    interview_prep_themes: list[str] = Field(default_factory=list)
    thirty_sixty_ninety: dict[str, str] = Field(
        default_factory=dict,
        description="Keys: '30', '60', '90'. Values: plan text.",
    )
    risks_to_address: list[str] = Field(default_factory=list)
    differentiators: list[str] = Field(default_factory=list)


class ReportPack(BaseModel):
    """
    Full intelligence pack delivered to the user.

    Stored in JobRun.reports.
    Sections correspond to the tabs in the CareerOS frontend.

    Fields:
        company:     Company research
        role:        Role analysis
        salary:      Salary data and ranges
        networking:  Network activation plan
        application: Application strategy
        exec_summary: 3-5 bullet executive summary for quick scan
    """
    company: CompanyIntelligence
    role: RoleIntelligence
    salary: list[SalaryBand] = Field(default_factory=list)
    networking: NetworkingStrategy
    application: ApplicationStrategy
    exec_summary: list[str] = Field(
        default_factory=list,
        max_length=6,
        description="3-5 top-level bullets for the executive summary card",
    )
