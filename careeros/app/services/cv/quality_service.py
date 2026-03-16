"""
app/services/cv/quality_service.py

CV Quality Scorer — fast LLM pass after parsing that scores the uploaded CV
and tells the user whether it's worth editing or should be rebuilt.

Score bands:
  0–39   POOR     — rebuild strongly recommended
  40–69  WEAK     — rebuild suggested, edit possible
  70–84  GOOD     — edit mode, minor improvements
  85–100 STRONG   — edit mode, mostly fine

Called by the parse_cv Celery task after successful parsing.
Result stored in CV.quality_score + CV.quality_feedback.

Design: uses a lightweight prompt with low max_tokens — this is a fast
classification call, not a deep analysis. Target: <5s, <200 tokens out.
"""

import json
import re

import structlog

from app.schemas.cv import ParsedCV

logger = structlog.get_logger(__name__)


# ── Thresholds ────────────────────────────────────────────────────────────────

QUALITY_POOR      = 40   # < 40: always offer rebuild
QUALITY_WEAK      = 70   # 40–69: suggest rebuild
QUALITY_GOOD      = 85   # 70–84: edit mode
# >= 85: strong, edit mode only


def get_verdict(score: int) -> str:
    if score < QUALITY_POOR:
        return "poor"
    if score < QUALITY_WEAK:
        return "weak"
    if score < QUALITY_GOOD:
        return "good"
    return "strong"


def should_recommend_rebuild(score: int) -> bool:
    return score < QUALITY_WEAK


# ── Prompt ────────────────────────────────────────────────────────────────────

CV_QUALITY_SYSTEM = """\
You are a senior CV reviewer and hiring expert.
Your job is to evaluate CV quality quickly and objectively.

Score the CV 0–100 on these weighted criteria:
  Structure (25pts):    Clear sections, logical order, consistent formatting
  Content depth (25pts): Specific achievements with numbers, not just duties
  Bullet quality (20pts): Action verbs, measurable outcomes, brevity
  Completeness (15pts): Contact info, dates, no suspicious gaps
  Language (15pts):     Clear, professional, no typos or vague filler words

Return ONLY valid JSON, no markdown fences, no preamble:
{
  "score": <integer 0-100>,
  "verdict": "<poor|weak|good|strong>",
  "top_issues": ["<issue 1>", "<issue 2>", "<issue 3 max>"],
  "recommendation": "<one sentence: what the user should do>",
  "rebuild_recommended": <true|false>
}
"""

CV_QUALITY_REBUILD_THRESHOLD = QUALITY_WEAK


def build_quality_prompt(parsed_cv: ParsedCV) -> str:
    """Build the user prompt for the quality check — just the CV text."""
    sections = []
    for section in (parsed_cv.sections or []):
        heading = section.get("heading", "")
        paras = section.get("paragraphs", [])
        text = "\n".join(p.get("text", "") for p in paras if p.get("text", "").strip())
        if text.strip():
            sections.append(f"## {heading}\n{text}")

    cv_text = "\n\n".join(sections)
    # Limit to 3000 chars — enough to judge quality without burning tokens
    cv_text = cv_text[:3000]

    return f"Please evaluate this CV:\n\n{cv_text}"


# ── Service ───────────────────────────────────────────────────────────────────

class CVQualityService:
    """
    Runs a fast quality check on a parsed CV.
    Returns score + structured feedback.
    """

    def score(self, parsed_cv: ParsedCV) -> dict:
        """
        Score the CV. Returns quality_feedback dict.
        Uses a small Claude call — synchronous, called from Celery worker.

        Returns:
            {
              "score": int,
              "verdict": str,
              "top_issues": list[str],
              "recommendation": str,
              "rebuild_recommended": bool,
            }

        On failure: returns a safe default (score=60, no recommendation).
        """
        from app.services.llm.client import ClaudeClient

        client = ClaudeClient(max_tokens=300)
        user_prompt = build_quality_prompt(parsed_cv)

        try:
            raw, result = client.call_text(
                system_prompt=CV_QUALITY_SYSTEM,
                user_prompt=user_prompt,
                temperature=0.1,
            )

            # Strip markdown fences if present
            text = re.sub(r"^```(?:json)?\s*", "", raw.strip())
            text = re.sub(r"\s*```$", "", text).strip()
            feedback = json.loads(text)

            # Validate required fields
            score = int(feedback.get("score", 60))
            score = max(0, min(100, score))  # Clamp to 0–100
            feedback["score"] = score
            feedback["verdict"] = get_verdict(score)
            feedback["rebuild_recommended"] = should_recommend_rebuild(score)

            logger.info(
                "cv_quality_scored",
                score=score,
                verdict=feedback["verdict"],
                rebuild_recommended=feedback["rebuild_recommended"],
                tokens=result.input_tokens + result.output_tokens,
            )
            return feedback

        except Exception as e:
            logger.warning("cv_quality_score_failed", error=str(e))
            # Safe fallback — don't fail the parse task over a quality check
            return {
                "score": 60,
                "verdict": "good",
                "top_issues": [],
                "recommendation": "CV uploaded successfully.",
                "rebuild_recommended": False,
                "error": str(e),
            }
