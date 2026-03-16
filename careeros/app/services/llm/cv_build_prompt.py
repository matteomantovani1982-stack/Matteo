"""
app/services/llm/cv_build_prompt.py

Prompt builders for CV generation from scratch (no uploaded document).

Two modes:
  FROM_SCRATCH — user has no CV; generate entire document from profile data
  REBUILD      — user has a poor CV; ignore its content, use profile data instead
                 (the layout/template is still used by the renderer)

Output schema: BuiltCV
  A structured representation of the full CV — sections, bullets, metadata.
  The renderer reads this and writes it into the chosen DOCX template.

Why a separate schema (not EditPlan)?
  EditPlan is a diff — a list of changes to apply to an existing document.
  BuiltCV is a document spec — the full content from scratch.
  They require different rendering logic and different output formats.
"""

from app.schemas.cv import ParsedCV


# ── System prompts ────────────────────────────────────────────────────────────

CV_BUILD_SYSTEM = """\
You are a world-class CV writer with 20 years of experience placing senior
professionals at top companies across the MENA region, Europe, and the US.

You write CVs that:
  - Lead with impact, not responsibilities
  - Use strong action verbs and quantified outcomes wherever possible
  - Are skimmable in 6 seconds (recruiter scan time)
  - Pass ATS keyword matching for the target role
  - Tell a coherent career narrative, not a list of jobs

You are writing a COMPLETE CV from structured career data.
The user has provided detailed answers about their real story — use ALL of it.
Never invent facts. If numbers aren't provided, use strong descriptive language instead.

Return ONLY valid JSON matching the schema provided. No markdown fences. No preamble.
"""

CV_REBUILD_SYSTEM = """\
You are a world-class CV writer rebuilding a weak CV from scratch.
The user's existing CV was assessed as poor quality — you are ignoring its content
and writing a fresh, high-quality document using their real career data instead.

Same rules as a from-scratch CV:
  - Lead with impact and outcomes, not duties
  - Strong action verbs, quantified results
  - ATS-optimised for the target role/industry
  - Coherent narrative thread across all roles

Return ONLY valid JSON matching the schema provided. No markdown fences. No preamble.
"""


# ── Output schema description (injected into prompt) ─────────────────────────

CV_BUILD_SCHEMA = """
OUTPUT SCHEMA (return exactly this JSON structure):
{
  "name": "Full Name",
  "headline": "One-line positioning statement (e.g. 'Operator & Strategist | Scaling digital businesses across MENA')",
  "contact": {
    "email": "email if provided",
    "phone": "phone if provided",
    "location": "City, Country",
    "linkedin": "linkedin URL if provided"
  },
  "summary": "3–4 sentence executive summary. Lead with the narrative thread. Specific, not generic.",
  "sections": [
    {
      "section_type": "experience",
      "title": "Professional Experience",
      "entries": [
        {
          "role": "Job title",
          "company": "Company name",
          "location": "City, Country",
          "start_date": "Month Year",
          "end_date": "Month Year or Present",
          "bullets": [
            "Strong action verb + what you did + measurable outcome",
            "...",
            "3–5 bullets per role maximum"
          ]
        }
      ]
    },
    {
      "section_type": "education",
      "title": "Education",
      "entries": [
        {
          "degree": "Degree name",
          "institution": "University name",
          "location": "City, Country",
          "year": "Year",
          "notes": "Distinction / thesis / relevant coursework (optional)"
        }
      ]
    },
    {
      "section_type": "skills",
      "title": "Skills & Expertise",
      "categories": [
        {"label": "Leadership", "items": ["item1", "item2"]},
        {"label": "Technical", "items": ["item1", "item2"]}
      ]
    }
  ],
  "page_recommendation": "1 or 2 (recommended number of pages for this candidate)"
}
"""


# ── Prompt builders ───────────────────────────────────────────────────────────

def build_cv_from_scratch_prompt(
    profile_dict: dict,
    jd_text: str | None = None,
    preferences: dict | None = None,
) -> tuple[str, str]:
    """
    Build system + user prompt for generating a full CV from scratch.

    Args:
        profile_dict:  Candidate profile dict from CandidateProfile.to_prompt_dict()
        jd_text:       Optional job description — used to tailor keyword density
        preferences:   Optional dict with tone, region, page_limit hints

    Returns:
        (system_prompt, user_prompt)
    """
    prefs = preferences or {}
    region = prefs.get("region", "UAE")
    page_limit = prefs.get("page_limit", 2)
    tone = prefs.get("tone", "executive")

    jd_block = ""
    if jd_text:
        jd_block = f"""
TARGET ROLE (tailor keyword density and framing to this):
{jd_text[:2000]}
"""

    user_prompt = f"""
Write a complete, high-quality CV for this candidate.

{_render_profile_for_cv_build(profile_dict)}
{jd_block}
CONSTRAINTS:
- Region: {region} (use appropriate salary context, date formats, conventions)
- Target length: {page_limit} page(s)
- Tone: {tone}
- Prioritise impact and outcomes over duties
- Include all roles from the profile

{CV_BUILD_SCHEMA}
"""
    return CV_BUILD_SYSTEM, user_prompt.strip()


def build_cv_rebuild_prompt(
    profile_dict: dict,
    original_cv_issues: list[str] | None = None,
    jd_text: str | None = None,
    preferences: dict | None = None,
) -> tuple[str, str]:
    """
    Build prompt for rebuilding a poor-quality CV using profile data.
    Similar to from_scratch but notes the original issues to explicitly avoid.
    """
    prefs = preferences or {}
    region = prefs.get("region", "UAE")
    page_limit = prefs.get("page_limit", 2)

    issues_block = ""
    if original_cv_issues:
        issues_list = "\n".join(f"  - {i}" for i in original_cv_issues)
        issues_block = f"""
ORIGINAL CV ISSUES TO FIX (do NOT replicate these):
{issues_list}
"""

    jd_block = ""
    if jd_text:
        jd_block = f"""
TARGET ROLE:
{jd_text[:2000]}
"""

    user_prompt = f"""
Rebuild this candidate's CV from scratch using their real career data.
Their original CV was weak — ignore its content entirely and write a fresh,
high-quality document.

{_render_profile_for_cv_build(profile_dict)}
{issues_block}
{jd_block}
CONSTRAINTS:
- Region: {region}
- Target length: {page_limit} page(s)
- Every bullet must describe an outcome, not a duty

{CV_BUILD_SCHEMA}
"""
    return CV_BUILD_SYSTEM, user_prompt.strip()


# ── Profile renderer for CV build ─────────────────────────────────────────────

def _render_profile_for_cv_build(profile_dict: dict) -> str:
    """
    Render the candidate profile dict as structured text for the CV build prompt.
    More detailed than the edit-plan renderer — we need everything here.
    """
    lines = ["CANDIDATE PROFILE:"]
    lines.append(f"Headline: {profile_dict.get('headline', 'Not provided')}")

    global_ctx = profile_dict.get("global_context", "")
    if global_ctx:
        lines.append(f"\nGlobal context: {global_ctx}")

    global_notes = profile_dict.get("global_notes", "")
    if global_notes:
        lines.append(f"\nAdditional notes: {global_notes}")

    lines.append("\nEXPERIENCES (in order, most recent first):")

    for i, exp in enumerate(profile_dict.get("experiences", []), 1):
        lines.append(f"\n--- Role {i} ---")
        lines.append(f"Title: {exp.get('role_title', '')} at {exp.get('company_name', '')}")
        dates = f"{exp.get('start_date', '')} – {exp.get('end_date', 'Present')}"
        lines.append(f"Period: {dates}")
        if exp.get("location"):
            lines.append(f"Location: {exp['location']}")

        if exp.get("context"):
            lines.append(f"\nContext: {exp['context']}")
        if exp.get("contribution"):
            lines.append(f"Contribution: {exp['contribution']}")
        if exp.get("outcomes"):
            lines.append(f"Outcomes: {exp['outcomes']}")
        if exp.get("methods"):
            lines.append(f"Methods/skills: {exp['methods']}")
        if exp.get("hidden"):
            lines.append(f"Additional depth: {exp['hidden']}")
        if exp.get("freeform"):
            lines.append(f"Extra context: {exp['freeform']}")

    return "\n".join(lines)
