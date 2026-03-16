"""
app/services/llm/client.py

Claude API client for CareerOS.

Responsibilities:
- Single entry point for all Claude API calls
- Structured JSON output enforcement (prompt + parse)
- Retry with exponential backoff on rate limits / transient errors
- Token usage tracking (stored in JobStep.metadata_json)
- Cost estimation per call

Design constraints:
- Synchronous — runs inside Celery workers (no event loop)
- Never imports FastAPI, SQLAlchemy, or Celery
- Raises typed exceptions so task layer can decide retry vs fail

Two call modes:
  1. call_structured() — expects JSON response, parses and validates
  2. call_text()        — returns raw text (for future use / debugging)
"""

import json
import time
from typing import Any, TypeVar
from pydantic import BaseModel

import anthropic
from anthropic import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    RateLimitError,
)

from app.config import settings

import structlog

logger = structlog.get_logger(__name__)

T = TypeVar("T", bound=BaseModel)

# ── Pricing constants (USD per million tokens) ─────────────────────────────
# Update when Anthropic changes pricing
PRICING: dict[str, dict[str, float]] = {
    "claude-opus-4-6": {
        "input_per_million": 15.00,
        "output_per_million": 75.00,
    },
    "claude-sonnet-4-6": {
        "input_per_million": 3.00,
        "output_per_million": 15.00,
    },
    "claude-haiku-4-5-20251001": {
        "input_per_million": 0.80,
        "output_per_million": 4.00,
    },
}

# ── Retry configuration ─────────────────────────────────────────────────────
MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 5.0
MAX_BACKOFF_SECONDS = 60.0


def _estimate_cost_usd(
    model: str,
    input_tokens: int,
    output_tokens: int,
) -> float:
    """Estimate USD cost for a call. Returns 0.0 if model not in pricing table."""
    pricing = PRICING.get(model)
    if not pricing:
        return 0.0
    input_cost = (input_tokens / 1_000_000) * pricing["input_per_million"]
    output_cost = (output_tokens / 1_000_000) * pricing["output_per_million"]
    return round(input_cost + output_cost, 6)


def _strip_json_fences(text: str) -> str:
    """
    Strip markdown code fences from LLM output.
    Claude sometimes wraps JSON in ```json ... ``` even when instructed not to.
    """
    text = text.strip()
    if text.startswith("```"):
        # Remove opening fence (```json or ```)
        first_newline = text.find("\n")
        if first_newline != -1:
            text = text[first_newline + 1:]
        # Remove closing fence
        if text.endswith("```"):
            text = text[:-3].rstrip()
    return text.strip()


# ── Demo mode — realistic fake responses (no API credits needed) ───────────

_DEMO_EDIT_PLAN = {
    "keyword_match_score": 82,
    "headline_summary": {
        "new_headline": "Commercial Director | MENA Growth Operator | P&L Owner",
        "new_summary": "Results-driven commercial operator with 8+ years scaling operations across MENA. Track record of building high-performing teams and driving 3-5x revenue growth. Experienced in full P&L ownership, enterprise sales, and market expansion from Series A through Series B+.",
        "rationale": "Headline updated to mirror JD seniority signals. Summary leads with P&L ownership which is the primary hiring criterion."
    },
    "paragraph_edits": [
        {
            "paragraph_index": 3,
            "operation": "replace_text",
            "new_text": "Led regional expansion into 6 new GCC markets, generating $12M ARR and 200+ enterprise clients within 18 months of launch.",
            "rationale": "Added quantified outcomes — original lacked metrics."
        },
        {
            "paragraph_index": 5,
            "operation": "replace_text",
            "new_text": "Built and scaled 45-person cross-functional team across product, sales and operations — maintained 95% retention in year one.",
            "rationale": "Retention metric added to demonstrate team-building quality."
        }
    ],
    "keyword_additions": ["P&L ownership", "go-to-market strategy", "Series B scaling", "OKR framework", "ARR growth"],
    "sections_to_add": [],
    "positioning_note": "Strong match at 82%. Lead with commercial impact and P&L ownership throughout. The JD signals they want an operator who has owned revenue targets, not just managed teams. Add SaaS-specific language (ARR, NRR, churn) where possible.",
}

_DEMO_POSITIONING = {
    "positioning_headline": "The operator who builds the machine, then runs it.",
    "positioning_narrative": "You are not a consultant. You have owned P&L, hired teams, and stayed long enough to see the results. In a market full of project managers, you have built businesses. That is rare — especially for a company moving from Series A scrappiness to Series B scale.",
    "key_differentiators": [
        "MENA-native operator who understands commercial and cultural nuance that outside hires miss",
        "P&L owner, not just budget manager — accountable for outcomes, not activities",
        "Builder at scale — grew teams from 5 to 50+ without losing the culture",
    ],
    "angle_for_this_role": "Position yourself as the person who turns their Series B funding into a repeatable commercial engine. They have the product. They need the operator.",
    "what_to_emphasise_in_interview": [
        "The expansion strategy — show you have a clear 90-day plan",
        "Retention metrics — demonstrate you understand unit economics",
        "Team building philosophy — they are scaling fast and need someone who hires well",
    ],
}

_DEMO_REPORT_PACK = {
    "company": {
        "company_name": "Acme Corp",
        "hq_location": "Dubai, UAE",
        "business_description": "High-growth Series B enterprise SaaS company serving the GCC market. Founded 2019, raised $45M to date with strong product-market fit and 120% NRR.",
        "revenue_and_scale": "Est. $20M ARR, 150 employees, expanding rapidly across MENA.",
        "recent_news": ["Closed $30M Series B led by STV", "Expanding into Saudi Arabia Q1 2026", "Hiring aggressively across commercial and product teams"],
        "strategic_priorities": ["GCC market expansion", "Enterprise sales motion", "Product-led growth layer"],
        "culture_signals": ["Fast-paced execution culture", "Founders remain hands-on", "Quarterly OKRs reviewed by full leadership"],
        "competitor_landscape": "Competes with regional SaaS players and global entrants adapting to MENA compliance requirements.",
        "hiring_signals": ["3 senior commercial roles open", "New Saudi entity registered"],
        "red_flags": ["High sales team turnover based on LinkedIn data", "Three CFOs in four years"]
    },
    "role": {
        "role_title": "Commercial Director",
        "seniority_level": "Director / VP",
        "reporting_line": "Reports to CEO",
        "what_they_really_want": "An operator who has built and scaled a commercial team in the GCC and can own the full revenue motion — not just manage it.",
        "hidden_requirements": ["Experience hiring in Saudi Arabia", "Existing network in enterprise procurement", "Comfort with founder-led culture"],
        "hiring_manager_worries": ["Will they stay once they land?", "Can they operate without a big team around them?"],
        "keyword_match_gaps": ["SaaS metrics fluency", "ARR ownership", "Channel partnerships"],
        "positioning_recommendation": "Lead with P&L ownership and MENA team-building. Show you understand the Series B to C scaling challenge."
    },
    "salary": [
        {
            "title": "Commercial Director — UAE",
            "base_monthly_aed_low": 40000,
            "base_monthly_aed_high": 55000,
            "base_annual_aed_low": 480000,
            "base_annual_aed_high": 660000,
            "bonus_pct_low": 20,
            "bonus_pct_high": 40,
            "total_comp_note": "Equity (0.1–0.5%) typical at this stage. Push for equity if base is at lower end.",
            "source": "Market benchmarks + LinkedIn Salary + GCC compensation surveys",
            "confidence": "medium"
        }
    ],
    "networking": {
        "target_contacts": ["VP Commercial", "Head of Talent", "CEO / Co-founder", "Chief of Staff"],
        "warm_path_hypotheses": ["INSEAD / LBS alumni network", "Former colleagues now at the company", "Shared investors or advisors"],
        "linkedin_search_strings": ["Commercial Director Acme Corp", "VP Sales Dubai SaaS", "Head of Revenue MENA"],
        "outreach_template_hiring_manager": "Hi [Name], I have been following Acme Corp's growth — the Series B and Saudi expansion caught my attention. I have spent the last 8 years building commercial teams across the GCC and would love 15 minutes to explore whether there is a fit. Happy to share context on what I have been working on.",
        "outreach_template_alumni": "Hi [Name], I saw you joined Acme Corp — congrats on the move. I am exploring a similar step and would love to hear your perspective on the culture and what success looks like there. 15 minutes?",
        "outreach_template_recruiter": "Hi [Name], I am actively looking at senior commercial roles in GCC SaaS. I saw the Acme Corp opening and believe my background is a strong match. Can we connect?",
        "seven_day_action_plan": [
            "Day 1: Apply via portal and connect with Head of Talent on LinkedIn",
            "Day 2: Research the Saudi expansion announcement in depth",
            "Day 3: Send outreach to VP Commercial",
            "Day 4: Follow up with recruiter if no response",
            "Day 5: Identify and connect with 2 alumni at the company",
            "Day 6: Prepare your 90-day plan for the GCC expansion",
            "Day 7: Follow up on all outstanding outreach"
        ]
    },
    "application": {
        "positioning_headline": "The operator who builds the machine, then runs it.",
        "cover_letter_angle": "Open with the Saudi expansion. Show you understand what they are trying to build and that you have done it before. Keep it to 3 paragraphs — founders do not read long letters.",
        "interview_prep_themes": ["90-day commercial plan", "Team building in a new market", "How you have handled underperforming salespeople", "Your view on PLG vs enterprise sales"],
        "thirty_sixty_ninety": {
            "30": "Listen, map the current commercial motion, identify the 3 biggest gaps, and earn trust with the team.",
            "60": "Close your first enterprise deal independently. Hire one senior AE for the Saudi market.",
            "90": "Present a revised commercial strategy to the founders with pipeline data to back it up."
        },
        "risks_to_address": ["Perceived as too senior / expensive", "No direct SaaS background — reframe as advantage"],
        "differentiators": ["MENA-native operator with regional network", "P&L owner not just budget manager", "Track record of building teams that stay"]
    },
    "exec_summary": [
        "Strong match — 82% keyword alignment with room to add SaaS-specific language",
        "Company is at a critical Series B-to-C inflection point — high upside, high execution risk",
        "Salary range AED 480–660K base + 20–40% bonus + equity upside",
        "Key move: reference the Saudi expansion in your cover letter and interview",
        "Top networking target: VP Commercial — likely the hiring manager or has direct influence"
    ]
}



def _extract_from_prompt(prompt: str, hints: list[str], default: str) -> str:
    """Pull a value out of the prompt text using simple keyword scanning."""
    import re
    for hint in hints:
        m = re.search(hint + r"[:\s]+([^\n]{3,80})", prompt, re.IGNORECASE)
        if m:
            return m.group(1).strip().rstrip(".,")
    return default


def _build_demo_report_pack(user_prompt: str) -> dict:
    """Build a demo ReportPack that references the actual JD content."""
    import re

    # Try to extract company name
    company = _extract_from_prompt(user_prompt, ["company", "employer", "organisation", "organization"], "the company")
    # Try to extract role title
    role = _extract_from_prompt(user_prompt, ["role", "position", "title", "job title", "applying for"], "the role")
    # Extract a couple of sentences from the JD for context
    jd_snippet = user_prompt[:800].replace("\n", " ").strip()

    return {
        "company": {
            "company_name": company[:80],
            "hq_location": "As per JD",
            "business_description": f"Based on the job description provided: {jd_snippet[:300]}",
            "revenue_and_scale": "Review the JD and company website for scale details.",
            "recent_news": [
                "Enable real AI (add Anthropic credits) for live company research",
                "Demo mode — company intel will be sourced from web in production",
            ],
            "strategic_priorities": [
                "Priorities will be extracted from the JD in production",
                "Enable real AI mode for full intelligence report",
            ],
            "culture_signals": ["Demo mode — culture signals will be researched in production"],
            "competitor_landscape": "Competitor analysis available in production mode.",
            "hiring_signals": ["This is a demo — real hiring signals sourced from web in production"],
            "red_flags": [],
        },
        "role": {
            "role_title": role[:80],
            "seniority_level": "See JD for seniority details",
            "reporting_line": "See JD",
            "what_they_really_want": f"Based on this JD: {jd_snippet[:400]}",
            "hidden_requirements": [
                "Demo mode — hidden requirements surfaced by AI in production",
                "Enable Anthropic API credits for full analysis",
            ],
            "hiring_manager_worries": ["Demo mode — add credits for real insight"],
            "keyword_match_gaps": ["Demo mode — real gap analysis requires AI"],
            "positioning_recommendation": "Add Anthropic API credits to get real positioning recommendations tailored to this exact JD.",
        },
        "salary": [
            {
                "title": role[:80],
                "base_annual_aed_low": 360000,
                "base_annual_aed_high": 600000,
                "bonus_pct_low": 15,
                "bonus_pct_high": 30,
                "total_comp_note": "Demo figures — real salary benchmarks sourced from market data in production.",
                "source": "Demo mode",
                "confidence": "low",
            }
        ],
        "networking": {
            "target_contacts": ["Hiring Manager", "Head of Talent", "Team Lead", "CEO / Founder"],
            "warm_path_hypotheses": ["Alumni network", "Shared connections on LinkedIn", "Industry events"],
            "linkedin_search_strings": [f"{role} {company}", f"Hiring {role}"],
            "outreach_template_hiring_manager": f"Hi [Name], I came across the {role} opening at {company} and believe my background is a strong match. I would love 15 minutes to discuss — happy to share context on what I have been working on.",
            "outreach_template_alumni": "Hi [Name], I saw you work at [Company] — I am exploring a similar move and would value your perspective. 15 minutes?",
            "outreach_template_recruiter": f"Hi [Name], I am actively looking at {role} roles and believe I am a strong fit for {company}. Can we connect?",
            "seven_day_action_plan": [
                "Day 1: Apply via the portal and connect with the recruiter on LinkedIn",
                "Day 2: Research the company — latest news, funding, leadership team",
                "Day 3: Identify and reach out to the hiring manager",
                "Day 4: Connect with 2-3 employees at the company",
                "Day 5: Follow up with recruiter if no response",
                "Day 6: Prepare your 30/60/90 day plan for the role",
                "Day 7: Follow up on all outstanding outreach",
            ],
        },
        "application": {
            "positioning_headline": f"The right candidate for {role[:50]}",
            "cover_letter_angle": f"Open by referencing what drew you to {company} specifically. Show you understand their current priorities. Keep it to 3 paragraphs — decision makers do not read long letters.",
            "interview_prep_themes": [
                "Your experience relevant to this role",
                "Why this company at this stage",
                "Your 30/60/90 day plan",
                "Specific examples with measurable outcomes",
            ],
            "thirty_sixty_ninety": {
                "30": "Listen, learn the team, understand current priorities, and identify quick wins.",
                "60": "Begin executing on your first deliverable. Build trust with key stakeholders.",
                "90": "Present a plan for the next quarter with clear metrics and priorities.",
            },
            "risks_to_address": ["Demo mode — real risk analysis available with Anthropic API credits"],
            "differentiators": ["Demo mode — your specific differentiators will be surfaced by AI in production"],
        },
        "exec_summary": [
            f"Demo mode — this is placeholder data for the {role} application",
            "Add Anthropic API credits to get real AI-powered intelligence",
            "The CV tailoring and DOCX download are fully functional",
            "Company intel, salary and contacts will be sourced from the web in production",
        ],
    }


def _build_demo_positioning(user_prompt: str) -> dict:
    """Build demo positioning that references actual JD content."""
    role = _extract_from_prompt(user_prompt, ["role", "position", "title", "applying for"], "this role")
    company = _extract_from_prompt(user_prompt, ["company", "employer", "organisation"], "the company")
    return {
        "positioning_headline": f"The right candidate for {role[:60]}",
        "positioning_narrative": f"Demo mode — your real positioning narrative will be crafted by AI based on your CV and the {role} JD at {company}. Add Anthropic API credits to unlock this.",
        "key_differentiators": [
            "Demo mode — your real differentiators will be surfaced in production",
            "Based on your CV vs the JD requirements",
            "Add API credits for personalised analysis",
        ],
        "angle_for_this_role": f"Add Anthropic API credits to get a real positioning angle for {role} at {company}.",
        "what_to_emphasise_in_interview": [
            "Demo mode — real interview emphasis points available in production",
            "Will be tailored to this specific JD",
            "Add API credits to unlock",
        ],
    }


def _make_demo_result(data: dict) -> "LLMCallResult":
    import json as _j
    return LLMCallResult(
        content=_j.dumps(data),
        model="demo-mode",
        input_tokens=0,
        output_tokens=0,
        duration_seconds=0.0,
    )


class LLMCallResult:
    """
    Result of a single Claude API call.
    Contains the parsed output plus usage metadata for cost tracking.
    """

    def __init__(
        self,
        content: str,
        input_tokens: int,
        output_tokens: int,
        model: str,
        duration_seconds: float,
    ) -> None:
        self.content = content
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.model = model
        self.duration_seconds = duration_seconds
        self.cost_usd = _estimate_cost_usd(model, input_tokens, output_tokens)

    def to_metadata(self) -> dict:
        """Serialise to dict for storage in JobStep.metadata_json."""
        return {
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.input_tokens + self.output_tokens,
            "cost_usd": self.cost_usd,
            "duration_seconds": self.duration_seconds,
        }


class ClaudeClient:
    """
    Synchronous Claude API client.

    Instantiate once per Celery task — do not share across tasks.
    Thread-safe (each task creates its own client instance).
    """

    def __init__(
        self,
        model: str | None = None,
        max_tokens: int | None = None,
    ) -> None:
        self._client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        self.model = model or settings.claude_model
        self.max_tokens = max_tokens or settings.claude_max_tokens

    def call_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        schema: type[T],
        temperature: float = 0.2,
    ) -> tuple[T, LLMCallResult]:
        """
        Call Claude and parse the response as a Pydantic model.

        Instructs Claude to return ONLY valid JSON matching the schema.
        Retries on rate limits and transient API errors.

        Args:
            system_prompt: System-level instructions (persona, format rules)
            user_prompt:   The actual request content (CV + JD + instructions)
            schema:        Pydantic model class to parse the JSON into
            temperature:   Sampling temperature (lower = more deterministic)

        Returns:
            Tuple of (parsed_schema_instance, LLMCallResult with metadata)

        Raises:
            ValueError:    if the response cannot be parsed as valid JSON
                           or doesn't match the schema (after retries)
            RuntimeError:  if max retries exceeded on API errors
        """
        raw_result = self._call_with_retry(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
        )

        # Parse and validate JSON
        try:
            cleaned = _strip_json_fences(raw_result.content)
            data = json.loads(cleaned)
            parsed = schema.model_validate(data)
        except json.JSONDecodeError as e:
            logger.error(
                "llm_json_parse_error",
                error=str(e),
                raw_content=raw_result.content[:500],
            )
            raise ValueError(
                f"Claude returned invalid JSON: {e}. "
                f"Raw content (first 200 chars): {raw_result.content[:200]}"
            ) from e
        except Exception as e:
            logger.error(
                "llm_schema_validation_error",
                schema=schema.__name__,
                error=str(e),
            )
            raise ValueError(
                f"Claude response didn't match schema {schema.__name__}: {e}"
            ) from e

        logger.info(
            "llm_call_structured_complete",
            model=raw_result.model,
            schema=schema.__name__,
            tokens=raw_result.input_tokens + raw_result.output_tokens,
            cost_usd=raw_result.cost_usd,
        )

        return parsed, raw_result

    def call_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7,
    ) -> tuple[str, LLMCallResult]:
        """
        Call Claude and return raw text response.
        Used for free-form outputs (e.g. cover letter drafts in future).
        """
        result = self._call_with_retry(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
        )
        return result.content, result


    # Alias for call_text — used when expecting raw JSON (e.g. PositioningStrategy)
    call_raw = call_text

    _DEMO_RESPONSES: dict[str, dict] = {
        "EditPlan": _DEMO_EDIT_PLAN,
        "PositioningStrategy": _DEMO_POSITIONING,
        "ReportPack": _DEMO_REPORT_PACK,
        "default": _DEMO_REPORT_PACK,
    }

    def _demo_response(self, schema_name: str) -> "LLMCallResult":
        """Return realistic fake data for demo/testing without hitting Claude API."""
        import json as _json

        fake_content = self._DEMO_RESPONSES.get(schema_name, self._DEMO_RESPONSES["default"])
        return LLMCallResult(
            content=_json.dumps(fake_content) if isinstance(fake_content, dict) else fake_content,
            model="demo-mode",
            input_tokens=0,
            output_tokens=0,
            duration_seconds=0.0,
        )

    def _call_with_retry(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
    ) -> LLMCallResult:
        """
        Core API call with exponential backoff retry.

        Retries on:
          - RateLimitError (429) — back off and retry
          - APIConnectionError  — transient network issue
          - APITimeoutError     — request timed out

        Does NOT retry on:
          - 400 Bad Request   — prompt issue, won't fix itself
          - 401 Unauthorized  — bad API key
          - 4xx other than 429
        """
        last_exception: Exception | None = None

        # Demo mode — skip real API call, generate context-aware fake data
        if settings.demo_mode:
            if "CV Tailor" in system_prompt:
                return _make_demo_result(_DEMO_EDIT_PLAN)
            elif "Intelligence Analyst" in system_prompt:
                return _make_demo_result(_build_demo_report_pack(user_prompt))
            elif "CareerOS Intelligence" in system_prompt:
                return _make_demo_result(_build_demo_positioning(user_prompt))
            else:
                return _make_demo_result(_build_demo_report_pack(user_prompt))

        for attempt in range(MAX_RETRIES + 1):
            try:
                start_time = time.monotonic()

                response = self._client.messages.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    temperature=temperature,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                )

                duration = time.monotonic() - start_time

                # Extract text content from response
                content = ""
                for block in response.content:
                    if hasattr(block, "text"):
                        content += block.text

                return LLMCallResult(
                    content=content,
                    input_tokens=response.usage.input_tokens,
                    output_tokens=response.usage.output_tokens,
                    model=response.model,
                    duration_seconds=duration,
                )

            except RateLimitError as e:
                last_exception = e
                if attempt >= MAX_RETRIES:
                    break
                backoff = min(
                    BASE_BACKOFF_SECONDS * (2 ** attempt),
                    MAX_BACKOFF_SECONDS,
                )
                logger.warning(
                    "llm_rate_limit",
                    attempt=attempt + 1,
                    backoff_seconds=backoff,
                )
                time.sleep(backoff)

            except (APIConnectionError, APITimeoutError) as e:
                last_exception = e
                if attempt >= MAX_RETRIES:
                    break
                backoff = min(
                    BASE_BACKOFF_SECONDS * (2 ** attempt),
                    MAX_BACKOFF_SECONDS,
                )
                logger.warning(
                    "llm_transient_error",
                    error_type=type(e).__name__,
                    attempt=attempt + 1,
                    backoff_seconds=backoff,
                )
                time.sleep(backoff)

            except APIStatusError as e:
                # 4xx errors other than 429 — don't retry
                logger.error(
                    "llm_api_status_error",
                    http_status=e.status_code,
                    error=str(e),
                )
                raise RuntimeError(
                    f"Claude API error {e.status_code}: {e.message}"
                ) from e

        # Max retries exceeded
        raise RuntimeError(
            f"Claude API call failed after {MAX_RETRIES} retries. "
            f"Last error: {last_exception}"
        ) from last_exception
