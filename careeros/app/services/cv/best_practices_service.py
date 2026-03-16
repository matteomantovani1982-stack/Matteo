"""
app/services/cv/best_practices_service.py

CV Best Practices Feedback — practical, plain-English suggestions.

Not a formal audit. Not scores. Just a senior colleague reading your CV
and telling you what to fix before you apply anywhere.

Each suggestion is:
  - Specific (references the actual weak text where possible)
  - Actionable (tells you exactly how to fix it)
  - Prioritised (high / medium / low)
  - Categorised (grammar, formatting, impact, structure, length, ats)

Runs automatically after parse_cv completes, stored on cv.quality_feedback
alongside the quality score. Surfaced in the UI before job run creation.

Design: one LLM call, medium depth (~500 tokens out), temperature 0.1.
"""

import json
import re

import structlog

from app.schemas.cv import ParsedCV

logger = structlog.get_logger(__name__)


# ── Prompt ────────────────────────────────────────────────────────────────────

BEST_PRACTICES_SYSTEM = """\
You are a senior CV coach with 20 years of experience helping professionals
land roles at top companies. You give honest, direct, practical feedback.

You do NOT give scores or grades. You give specific, actionable suggestions.

For each issue you find, you:
  1. Name it clearly (one short sentence)
  2. Quote or reference the specific offending text if possible
  3. Tell them exactly how to fix it

Focus on what actually matters to recruiters and hiring managers:
  - IMPACT: Are achievements quantified? Do bullets start with strong action verbs?
    Weak: "Responsible for managing the team"
    Strong: "Led 12-person team that shipped 3 products in 6 months"
  - GRAMMAR: Tense consistency (past tense for past roles), no typos, no passive voice
  - FORMATTING: Consistent date formats, consistent capitalisation, no walls of text
  - STRUCTURE: Missing sections (summary, skills)? Poor section order?
  - LENGTH: Too long (>2 pages for <15 years experience)? Too thin?
  - ATS: Generic section headers? Keyword-poor bullets?

Return ONLY valid JSON. No markdown fences. No preamble.

Schema:
{
  "suggestions": [
    {
      "priority": "high|medium|low",
      "category": "impact|grammar|formatting|structure|length|ats",
      "title": "Short description of the issue",
      "detail": "Specific actionable advice. Quote the weak text if possible.",
      "example_fix": "Optional: show what a better version looks like"
    }
  ],
  "top_strength": "One sentence: the best thing about this CV as-is",
  "summary": "2-3 sentences: overall assessment and single most important action"
}

Return 3–8 suggestions maximum. Quality over quantity. Only flag things that genuinely matter.
"""


def build_best_practices_prompt(parsed_cv: ParsedCV) -> str:
    """Build the user prompt — the CV text for Claude to review."""
    sections = []
    for section in (parsed_cv.sections or []):
        heading = section.get("heading", "")
        paras = section.get("paragraphs", [])
        text = "\n".join(
            p.get("text", "") for p in paras
            if p.get("text", "").strip()
        )
        if text.strip():
            sections.append(f"## {heading}\n{text}")

    cv_text = "\n\n".join(sections)
    cv_text = cv_text[:4000]  # ~1000 tokens — enough to review thoroughly

    return f"Please review this CV and give me your honest feedback:\n\n{cv_text}"


# ── Service ───────────────────────────────────────────────────────────────────

class BestPracticesService:
    """
    Generates practical best-practices feedback for an uploaded CV.
    Runs after parse_cv, results stored in cv.quality_feedback["suggestions"].
    """

    def analyse(self, parsed_cv: ParsedCV) -> dict:
        """
        Analyse the CV and return structured best-practices feedback.

        Returns:
            {
              "suggestions": [...],
              "top_strength": "...",
              "summary": "..."
            }

        On failure: returns empty suggestions with error noted.
        Safe to call — never raises, always returns a usable dict.
        """
        from app.services.llm.client import ClaudeClient

        client = ClaudeClient(max_tokens=600)
        user_prompt = build_best_practices_prompt(parsed_cv)

        try:
            raw, result = client.call_text(
                system_prompt=BEST_PRACTICES_SYSTEM,
                user_prompt=user_prompt,
                temperature=0.1,
            )

            text = re.sub(r"^```(?:json)?\s*", "", raw.strip())
            text = re.sub(r"\s*```$", "", text).strip()
            feedback = json.loads(text)

            suggestions = feedback.get("suggestions", [])
            # Validate and clamp
            for s in suggestions:
                if s.get("priority") not in ("high", "medium", "low"):
                    s["priority"] = "medium"
                if s.get("category") not in ("impact", "grammar", "formatting", "structure", "length", "ats"):
                    s["category"] = "impact"

            logger.info(
                "best_practices_analysed",
                suggestions=len(suggestions),
                tokens=result.input_tokens + result.output_tokens,
            )

            return {
                "suggestions": suggestions,
                "top_strength": feedback.get("top_strength", ""),
                "summary": feedback.get("summary", ""),
            }

        except Exception as e:
            logger.warning("best_practices_failed_non_fatal", error=str(e))
            return {
                "suggestions": [],
                "top_strength": "",
                "summary": "",
                "error": str(e),
            }
