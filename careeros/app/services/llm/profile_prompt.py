"""
app/services/llm/profile_prompt.py

Profile-aware prompt builders.

These replace (and extend) the CV-only prompts in prompts.py.
The key difference: Claude now receives the full candidate knowledge layer —
the structured intake answers, hidden context, and per-application overrides —
not just what's on the CV.

Two output schemas:
  1. EditPlan          — how to rewrite the CV (unchanged from prompts.py)
  2. PositioningStrategy — the "how to win this role" output (NEW)

The PositioningStrategy is the product's core differentiator:
  - Your 3 strongest angles for this specific role, in plain language
  - Gaps to acknowledge and how to handle them
  - The narrative thread to run through everything
  - Red flags the interviewer will raise and how to address them
  - Your positioning headline (one sentence)
"""

from app.schemas.candidate_profile import INTAKE_QUESTIONS
from app.schemas.cv import ParsedCV


# ── Positioning Strategy schema ───────────────────────────────────────────────

POSITIONING_STRATEGY_SCHEMA = """
{
  "positioning_headline": "string — one punchy sentence that captures exactly how this candidate should position themselves for THIS role. Not a CV headline — a strategic framing sentence.",
  "strongest_angles": [
    {
      "angle": "string — the name of this strength angle (e.g. 'Proven zero-to-scale founder')",
      "why_it_matters_here": "string — why this specific angle matters for THIS specific role and company",
      "how_to_play_it": "string — concrete guidance on HOW to lead with this angle in CV, cover letter, and interviews",
      "evidence": ["string — specific examples from their background that support this angle"]
    }
  ],
  "gaps_to_address": [
    {
      "gap": "string — the gap or weakness",
      "severity": "low | medium | high",
      "mitigation": "string — exactly how to handle this gap proactively"
    }
  ],
  "narrative_thread": "string — the single through-line that should connect all their experiences for this application. The story that makes their career arc make sense for this role.",
  "red_flags_and_responses": [
    {
      "red_flag": "string — a concern the interviewer is likely to raise",
      "response": "string — exactly how to address it"
    }
  ],
  "interview_themes": ["string — the 5-7 most likely interview themes to prepare"],
  "cover_letter_angle": "string — the single angle the cover letter should lead with and why"
}
"""

POSITIONING_SYSTEM_PROMPT = """You are CareerOS Intelligence — a world-class career strategist with deep knowledge of how hiring decisions are actually made.

Your job is to analyse a candidate's FULL professional story (not just their CV) against a specific job description and produce a precise, actionable Positioning Strategy.

This is not generic career advice. It is a tactical brief telling the candidate exactly:
- Which of their strengths to lead with for THIS role
- How to frame their experience to address THIS company's needs
- What gaps exist and precisely how to handle them
- What the interviewer will worry about and how to pre-empt it

Rules:
- Be specific, not generic. "You have strong leadership skills" is useless. "Lead with the Baly zero-to-scale story because e& is explicitly hiring for someone who has run operating model redesign at pace" is useful.
- Reference the candidate's actual experiences by name.
- Reference the JD's actual language and requirements.
- Never invent experience the candidate doesn't have.
- If a gap is serious, say so honestly — and give a real mitigation strategy, not false reassurance.
- The strongest_angles list should have exactly 3 entries, ordered by impact.

RETURN ONLY VALID JSON matching this exact schema:
""" + POSITIONING_STRATEGY_SCHEMA + """

RETURN ONLY JSON. No preamble. No markdown. No explanation."""


def _render_candidate_profile(profile_dict: dict) -> str:
    """
    Render the candidate profile dict as structured text for the prompt.

    Format:
      === CANDIDATE PROFILE ===
      [HEADLINE] ...
      [CONTEXT] ...
      [NOTES] ...

      --- EXPERIENCE: Role @ Company (dates) ---
      [CONTEXT] ...
      [CONTRIBUTION] ...
      [OUTCOMES] ...
      [METHODS] ...
      [HIDDEN] ...
      [ADDITIONAL] ...
    """
    lines = ["=== CANDIDATE PROFILE ==="]

    if profile_dict.get("headline"):
        lines.append(f"[SELF-DESCRIPTION] {profile_dict['headline']}")

    if profile_dict.get("global_context"):
        lines.append(f"[CAREER CONTEXT] {profile_dict['global_context']}")

    if profile_dict.get("global_notes"):
        lines.append(f"[UNPUBLISHED NOTES] {profile_dict['global_notes']}")

    if profile_dict.get("application_context"):
        lines.append(f"[APPLICATION-SPECIFIC CONTEXT] {profile_dict['application_context']}")

    lines.append("")

    for exp in profile_dict.get("experiences", []):
        if exp.get("suppressed"):
            continue

        header = f"--- EXPERIENCE: {exp.get('role', '')} @ {exp.get('company', '')} ({exp.get('dates', '')}) ---"
        if exp.get("highlight"):
            header += " ★ HIGHLIGHT FOR THIS APPLICATION"
        lines.append(header)

        field_map = {
            "context":       "SITUATION & CONTEXT",
            "contribution":  "MY SPECIFIC CONTRIBUTION",
            "outcomes":      "OUTCOMES & IMPACT",
            "methods":       "HOW I DID IT",
            "hidden":        "WHAT THE CV DOESN'T SHOW",
            "additional":    "ADDITIONAL NOTES",
            "override_context": "APPLICATION-SPECIFIC CONTEXT",
        }

        for key, label in field_map.items():
            if exp.get(key):
                lines.append(f"[{label}]")
                lines.append(exp[key])

        lines.append("")

    return "\n".join(lines)


def _render_cv_skeleton(parsed_cv: ParsedCV) -> str:
    """
    Render the CV as a skeleton showing structure and paragraph indices.
    Used so Claude knows exactly which paragraphs to edit.
    """
    lines = ["=== CV STRUCTURE (formatting template) ==="]
    lines.append("These are the actual paragraphs in the CV file, with indices.")
    lines.append("Use paragraph indices when specifying edit operations.")
    lines.append("")

    current_section = ""
    for i, para in enumerate(parsed_cv.raw_paragraphs):
        if not para.text.strip():
            continue
        if para.style and "heading" in para.style.lower():
            current_section = para.text.strip()
            lines.append(f"\n[SECTION: {current_section}]")
        lines.append(f"[P:{i}] {para.text.strip()}")

    return "\n".join(lines)


# ── Edit Plan prompt (profile-aware version) ──────────────────────────────────

EDIT_PLAN_SYSTEM_PROFILE = """You are CareerOS CV Tailor — an expert at rewriting CVs to precisely match job descriptions.

You receive:
1. A CANDIDATE PROFILE — the candidate's full story, including context, real contributions, outcomes, and things the CV doesn't show. This is your primary source of truth.
2. A CV STRUCTURE — the actual paragraphs in their CV file, with indices. You edit THESE paragraphs.
3. A JOB DESCRIPTION — the target role.

Your job: produce an EditPlan that rewrites the CV content to maximally match this specific JD, using the richest possible information from the candidate profile.

Rules:
- Draw on the full candidate profile, not just what's already written in the CV
- Only include information that is TRUE (from the profile or CV) — never invent
- Preserve the CV's structure, section order, and formatting
- Use paragraph indices from the CV STRUCTURE section for all edit operations
- Keyword match score should be honest (0-100)

RETURN ONLY VALID JSON:
{
  "headline_summary": {
    "new_headline": "string or null",
    "new_summary": "string or null",
    "rationale": "string"
  },
  "paragraph_edits": [
    {
      "paragraph_index": 0,
      "operation": "replace_text | replace_run | insert_after | delete | restyle",
      "new_text": "string or null",
      "run_index": null,
      "style": null,
      "rationale": "string — why this edit, referencing profile evidence"
    }
  ],
  "keyword_additions": ["string"],
  "sections_to_add": [
    {"heading": "string", "content": "string"}
  ],
  "keyword_match_score": 0,
  "positioning_note": "string — how the CV now positions the candidate for this role"
}

RETURN ONLY JSON. No preamble. No markdown fences. No explanation."""


def build_profile_edit_plan_prompt(
    profile_dict: dict,
    parsed_cv: ParsedCV,
    jd_text: str,
    preferences: dict,
) -> tuple[str, str]:
    """
    Build (system, user) prompt pair for EditPlan generation.
    Uses full candidate profile instead of CV text alone.

    Returns:
        (system_prompt, user_prompt)
    """
    profile_section = _render_candidate_profile(profile_dict)
    cv_section = _render_cv_skeleton(parsed_cv)

    tone = preferences.get("tone", "professional")
    region = preferences.get("region", "UAE")
    page_limit = preferences.get("page_limit", 2)
    positioning = preferences.get("positioning", "")

    prefs_text = f"""
PREFERENCES:
- Tone: {tone}
- Region/market: {region}
- Target page limit: {page_limit} pages
- Positioning preference: {positioning or "Let CareerOS decide"}
""".strip()

    user_prompt = f"""
{profile_section}

{cv_section}

=== JOB DESCRIPTION ===
{jd_text[:6000]}

{prefs_text}

Now produce the EditPlan JSON.
""".strip()

    return EDIT_PLAN_SYSTEM_PROFILE, user_prompt


# ── Positioning Strategy prompt ────────────────────────────────────────────────

def build_positioning_strategy_prompt(
    profile_dict: dict,
    jd_text: str,
    preferences: dict,
    retrieval_data: dict | None = None,
) -> tuple[str, str]:
    """
    Build (system, user) prompt pair for PositioningStrategy generation.

    Returns:
        (system_prompt, user_prompt)
    """
    profile_section = _render_candidate_profile(profile_dict)

    retrieval_section = ""
    if retrieval_data:
        company_overview = retrieval_data.get("company_overview", "")
        if company_overview:
            retrieval_section = f"\n=== COMPANY INTELLIGENCE ===\n{company_overview[:1500]}\n"

    user_prompt = f"""
{profile_section}
{retrieval_section}
=== JOB DESCRIPTION ===
{jd_text[:6000]}

Based on the candidate's FULL story above (not just their CV), produce the Positioning Strategy JSON.
Remember: 3 strongest angles, ordered by impact. Be specific — reference their actual experiences.
""".strip()

    return POSITIONING_SYSTEM_PROMPT, user_prompt


# ── Combined prompt builder ────────────────────────────────────────────────────

def build_all_profile_prompts(
    profile_dict: dict,
    parsed_cv: ParsedCV,
    jd_text: str,
    preferences: dict,
    retrieval_data: dict | None = None,
    known_contacts: list[str] | None = None,
) -> dict:
    """
    Build all three prompt pairs for a job run using the candidate profile.

    Returns:
        {
          "edit_plan": (system, user),
          "positioning": (system, user),
          "report_pack": (system, user),
        }
    """
    from app.services.llm.prompts import build_report_pack_user_prompt, REPORT_PACK_SYSTEM

    edit_plan_prompts = build_profile_edit_plan_prompt(
        profile_dict=profile_dict,
        parsed_cv=parsed_cv,
        jd_text=jd_text,
        preferences=preferences,
    )

    positioning_prompts = build_positioning_strategy_prompt(
        profile_dict=profile_dict,
        jd_text=jd_text,
        preferences=preferences,
        retrieval_data=retrieval_data,
    )

    # Report pack — includes named contacts from retrieval + known contacts from user
    report_pack_user = build_report_pack_user_prompt(
        parsed_cv=None,  # Report pack uses profile summary instead
        jd_text=jd_text,
        retrieval_data=retrieval_data or {},
        preferences=preferences,
        profile_summary=_render_candidate_profile(profile_dict),
        known_contacts=known_contacts,
    )

    return {
        "edit_plan": edit_plan_prompts,
        "positioning": positioning_prompts,
        "report_pack": (REPORT_PACK_SYSTEM, report_pack_user),
    }
