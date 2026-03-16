"""
app/services/llm/prompts.py

Prompt templates for all Claude API calls in CareerOS.

Design principles:
  1. System prompts define WHAT Claude is and its output contract
  2. User prompts inject all dynamic data (CV, JD, company data, preferences)
  3. JSON output schema is always embedded in the system prompt
  4. "Return ONLY valid JSON" is explicit and repeated
  5. Each prompt is a pure function: inputs → prompt strings

Two main prompt pairs:
  A. edit_plan_prompts()   → produces EditPlan JSON
  B. report_pack_prompts() → produces ReportPack JSON

Keeping prompts here (not inline in tasks) means:
  - Easy to iterate and A/B test without touching task logic
  - Can add prompt versioning later
  - Readable and reviewable as a standalone file
"""

import json
from app.schemas.cv import ParsedCV


# ── Shared formatting instructions ─────────────────────────────────────────

_JSON_ONLY_INSTRUCTION = """
CRITICAL OUTPUT RULE:
You must return ONLY valid JSON. No preamble. No explanation. No markdown fences.
Your entire response must be parseable by json.loads().
If you cannot complete a field, use null or an empty string — never omit required keys.
""".strip()


# ── A. Edit Plan prompts ────────────────────────────────────────────────────

EDIT_PLAN_SYSTEM = """
You are CareerOS CV Tailor, an expert in executive-level CV optimisation.

Your task: analyse a candidate's CV and a job description, then produce a precise
JSON edit plan that tells a downstream DOCX renderer exactly what to change.

PRINCIPLES:
1. Preserve the candidate's authentic voice and truthful content — never fabricate
2. Optimise for keyword match with the JD without keyword stuffing
3. Reframe existing achievements in language that resonates with the role
4. Respect the candidate's page limit preference
5. Keep formatting instructions minimal — the renderer handles layout

EDIT OPERATION TYPES:
- replace_text: Replace entire paragraph text (paragraph_index required)
- replace_run:  Replace text of a specific run (paragraph_index + run_index required)
- insert_after: Insert new paragraph after given index
- delete:       Remove paragraph at index
- restyle:      Change paragraph style name only

POSITIONING MODES (use preference.positioning):
- auto:             Infer best positioning from CV + JD
- specialist:       Deep expertise in one domain
- strategic_leader: Broad C-suite / transformation framing
- generalist:       Versatile across functions
- career_pivot:     Acknowledge change, use prior experience as asset

OUTPUT FORMAT — return ONLY this JSON structure:
{
  "headline_summary": {
    "new_headline": "string or null",
    "new_summary": "string or null",
    "rationale": "string"
  },
  "paragraph_edits": [
    {
      "paragraph_index": 0,
      "operation": "replace_text",
      "new_text": "string",
      "run_index": null,
      "style": null,
      "rationale": "string"
    }
  ],
  "keyword_additions": ["keyword1", "keyword2"],
  "sections_to_add": [],
  "positioning_note": "string",
  "keyword_match_score": 75
}

""" + _JSON_ONLY_INSTRUCTION


def build_edit_plan_user_prompt(
    parsed_cv: ParsedCV,
    jd_text: str,
    preferences: dict,
) -> str:
    """
    Build the user prompt for edit plan generation.

    Args:
        parsed_cv:   Structured CV from the parser
        jd_text:     Job description full text
        preferences: JobRun.preferences dict (tone, region, page_limit, etc.)
    """
    # Render CV as structured text for the prompt
    cv_text = _render_cv_for_prompt(parsed_cv)

    # Extract key preferences
    tone = preferences.get("tone", "executive")
    region = preferences.get("region", "UAE")
    page_limit = preferences.get("page_limit", 2)
    positioning = preferences.get("positioning", "auto")
    career_notes = preferences.get("career_notes") or ""

    page_instruction = (
        f"Target length: {page_limit} page(s)."
        if page_limit > 0
        else "Target length: match original."
    )

    career_notes_block = (
        f"\nADDITIONAL CANDIDATE CONTEXT (use to strengthen edits):\n{career_notes}"
        if career_notes
        else ""
    )

    return f"""
CANDIDATE CV (structured):
{cv_text}

JOB DESCRIPTION:
{jd_text[:8000]}

PREFERENCES:
- Tone: {tone} (ats = keyword-dense; executive = polished C-suite language; human = conversational)
- Region: {region}
- {page_instruction}
- Positioning: {positioning}
{career_notes_block}

TASK:
1. Identify the top 15 keywords/phrases from the JD
2. Analyse which are present in the CV and which are missing
3. Generate AGGRESSIVE edit instructions:
   - Rewrite the headline completely to mirror the exact role title and seniority
   - Rewrite the summary from scratch — lead with the ONE insight that makes this candidate perfect for THIS role. Incorporate top 5 JD keywords. 3-4 punchy sentences.
   - Rewrite EVERY work experience: for each job, rewrite at least 3-5 bullet points to (a) use JD language and keywords, (b) lead with strong action verbs, (c) include metrics wherever the original hints at scale, (d) connect the achievement to what this role needs
   - Do NOT leave any experience section unchanged — every section must have rewrites
   - Remove bullets irrelevant to this role
4. Set keyword_match_score to your estimated CV-JD alignment (0-100) AFTER edits
5. Write a positioning_note explaining your overall strategy in 2-3 sentences

Be aggressive. The goal is a CV that reads like it was written FOR this specific role.
Use paragraph_index values that match the CV structure above.

Return ONLY the JSON edit plan.
""".strip()


def _render_cv_for_prompt(parsed_cv: ParsedCV) -> str:
    """
    Render a ParsedCV as readable structured text for the LLM prompt.

    Format:
      [SECTION: Experience]
      [P:0] McKinsey & Company | Engagement Manager
      [P:1]   • Led 8-figure cost transformation for Gulf telco...

    Why include paragraph index? The LLM needs it to write accurate edit operations.
    """
    lines = []
    lines.append(f"[TOTAL PARAGRAPHS: {parsed_cv.total_paragraphs}]")
    lines.append(f"[TOTAL WORDS: {parsed_cv.total_words}]")
    lines.append("")

    for section in parsed_cv.sections:
        lines.append(f"[SECTION: {section.heading}]")
        for para in section.paragraphs:
            if para.is_empty:
                lines.append(f"[P:{para.index}] <empty>")
            else:
                # Indent non-heading paragraphs for readability
                indent = "  " if para.style.lower() == "normal" else ""
                lines.append(f"[P:{para.index}] {indent}{para.text[:300]}")
        lines.append("")

    return "\n".join(lines)


# ── B. Report Pack prompts ──────────────────────────────────────────────────

REPORT_PACK_SYSTEM = """
You are CareerOS Intelligence Analyst, an expert in executive job search strategy
with deep knowledge of the UAE/GCC market.

Your task: produce a comprehensive Application Intelligence Pack as structured JSON.

You will receive:
1. Candidate CV summary
2. Job description
3. Company and salary research data (from web search)
4. Candidate preferences

INTELLIGENCE PACK SECTIONS:

COMPANY INTELLIGENCE:
- Business description, scale, revenue, ownership
- Strategic priorities and recent news
- Culture signals inferred from JD language and public data
- Competitor landscape
- Hiring signals (why are they hiring now?)
- Red flags worth investigating

ROLE INTELLIGENCE:
- What they REALLY want (beyond the stated JD)
- Hidden requirements not explicitly stated
- Hiring manager's likely worries about this candidate
- Keyword gaps between CV and JD
- Positioning recommendation

SALARY INTELLIGENCE:
- Market ranges for this role/level in this location
- Compensation structure typical for this company type
- Negotiation leverage points

NETWORKING STRATEGY:
- Named contacts found at this company (from search data — use these real people)
- For each named contact: why they matter, personalised connection request message
- Warm path hypotheses (alumni, mutual connections, ecosystem overlaps)
- LinkedIn search strings (ready to copy-paste) for finding more contacts
- Outreach message templates tailored to each contact type
- If the candidate has named people they know at the company, write specific warm intro asks
- 7-day action plan

APPLICATION STRATEGY:
- Cover letter angle (the ONE insight that differentiates this candidate)
- Interview process: likely stages for this company/role type (e.g. recruiter screen → case study → panel → CEO), who interviews at each stage
- For each stage: the type of questions to expect (behavioural / business case / technical / situational / culture)
- Question bank: 4-6 actual likely questions per category, written specifically for THIS role and THIS candidate's background
- For behavioural questions: suggest which story from the candidate's profile to use and the key points to hit
- For business cases: describe the type of case likely given (growth strategy, market entry, operational problem) and how to frame the answer
- Killer questions to ask THEM — 3-5 smart questions that signal deep preparation
- 30/60/90 day plan for the role
- Risks to address proactively
- Key differentiators to lead with

OUTPUT FORMAT — return ONLY this JSON structure:
{
  "company": {
    "company_name": "string",
    "hq_location": "string",
    "business_description": "string",
    "revenue_and_scale": "string",
    "recent_news": ["string"],
    "strategic_priorities": ["string"],
    "culture_signals": ["string"],
    "competitor_landscape": "string",
    "hiring_signals": ["string"],
    "red_flags": ["string"]
  },
  "role": {
    "role_title": "string",
    "seniority_level": "string",
    "reporting_line": "string",
    "what_they_really_want": "string",
    "hidden_requirements": ["string"],
    "hiring_manager_worries": ["string"],
    "keyword_match_gaps": ["string"],
    "positioning_recommendation": "string"
  },
  "salary": [
    {
      "title": "string",
      "base_monthly_aed_low": null,
      "base_monthly_aed_high": null,
      "base_annual_aed_low": null,
      "base_annual_aed_high": null,
      "bonus_pct_low": null,
      "bonus_pct_high": null,
      "total_comp_note": "string",
      "source": "string",
      "confidence": "low|medium|high"
    }
  ],
  "networking": {
    "named_contacts": [
      {
        "name": "string",
        "title": "string",
        "linkedin_url": "string or null",
        "why_relevant": "string",
        "outreach_message": "string — personalised LinkedIn connection request (300 chars max)"
      }
    ],
    "known_network_asks": [
      {
        "person": "string — name from candidate\'s known contacts",
        "ask": "string — exactly what to say to this person"
      }
    ],
    "warm_path_hypotheses": ["string"],
    "linkedin_search_strings": ["string"],
    "outreach_template_hiring_manager": "string",
    "outreach_template_alumni": "string",
    "outreach_template_recruiter": "string",
    "seven_day_action_plan": ["string"]
  },
  "application": {
    "positioning_headline": "string",
    "cover_letter_angle": "string",
    "interview_process": [
      {
        "stage": "string — e.g. Recruiter Screen, Case Study, Panel Interview, CEO/Founder Interview",
        "format": "string — phone / video / in-person / written",
        "who": "string — who likely conducts this stage",
        "duration": "string — estimated duration",
        "what_to_expect": "string — what this stage is really assessing"
      }
    ],
    "question_bank": {
      "behavioural": [
        {
          "question": "string — the actual question",
          "why_they_ask": "string — what they are assessing",
          "your_story": "string — which experience from THIS candidate's background to use",
          "key_points": ["string — specific points to land in the answer"]
        }
      ],
      "business_case": [
        {
          "question": "string — the actual case prompt or scenario",
          "case_type": "string — e.g. market sizing, growth strategy, operational problem",
          "how_to_frame": "string — recommended structure and approach for this candidate",
          "watch_out": "string — common mistake to avoid"
        }
      ],
      "situational": [
        {
          "question": "string — hypothetical scenario question",
          "what_they_want": "string — the underlying quality being assessed",
          "suggested_answer_angle": "string — the angle this candidate should take"
        }
      ],
      "culture_and_motivation": [
        {
          "question": "string",
          "ideal_answer_angle": "string — how to answer authentically and compellingly for this role"
        }
      ]
    },
    "questions_to_ask_them": [
      {
        "question": "string — the actual question to ask",
        "why_powerful": "string — why this question signals depth"
      }
    ],
    "thirty_sixty_ninety": {"30": "string", "60": "string", "90": "string"},
    "risks_to_address": ["string"],
    "differentiators": ["string"]
  },
  "exec_summary": ["bullet 1", "bullet 2", "bullet 3"]
}

""" + _JSON_ONLY_INSTRUCTION


def build_report_pack_user_prompt(
    parsed_cv: "ParsedCV | None",
    jd_text: str,
    retrieval_data: dict,
    preferences: dict,
    profile_summary: str | None = None,
    known_contacts: list[str] | None = None,
) -> str:
    """
    Build the user prompt for intelligence report generation.
    Accepts either a ParsedCV or a pre-rendered profile_summary string.

    known_contacts: optional list of people the candidate knows at the company,
    e.g. ["Ahmed Al-Rashidi (former colleague)", "Sarah Johnson (MBA classmate)"]
    """
    if profile_summary:
        cv_summary = profile_summary
    else:
        cv_summary = _render_cv_summary_for_prompt(parsed_cv)
    region = preferences.get("region", "UAE")

    # Format retrieval data for the prompt
    retrieval_block = _format_retrieval_data(retrieval_data)

    # Format known contacts block
    known_contacts_block = ""
    if known_contacts:
        kc_list = "\n".join(f"  - {c}" for c in known_contacts)
        known_contacts_block = f"""
CANDIDATE'S KNOWN CONTACTS AT THIS COMPANY:
{kc_list}
(Write a specific warm-intro ask for each of these people in networking.known_network_asks)
"""

    return f"""
CANDIDATE CV SUMMARY:
{cv_summary}

JOB DESCRIPTION:
{jd_text[:6000]}

RESEARCH DATA (from web search — includes named contacts found at the company):
{retrieval_block}
{known_contacts_block}
PREFERENCES:
- Region: {region}

TASK:
Produce a comprehensive Application Intelligence Pack for this specific
candidate applying to this specific role.

Be specific, not generic. Every insight should be actionable.
Named contacts in the research data are REAL people — use them in the networking section.
Salary figures should be in AED for UAE roles (or local currency for other regions).
Outreach messages should be ≤300 characters, ready to copy-paste into LinkedIn.

Return ONLY the JSON intelligence pack.
""".strip()


def _render_cv_summary_for_prompt(parsed_cv: ParsedCV) -> str:
    """
    Render a condensed CV summary for the report pack prompt.
    Less detail than the edit plan prompt — we need context, not every paragraph.
    Target: ~500 words.
    """
    lines = []

    for section in parsed_cv.sections:
        if section.heading in ("Preamble", "Document"):
            # Just include the text
            for para in section.paragraphs[:5]:
                if not para.is_empty:
                    lines.append(para.text)
        else:
            lines.append(f"\n{section.heading.upper()}")
            for para in section.paragraphs[:8]:  # Max 8 per section
                if not para.is_empty:
                    lines.append(f"  {para.text[:200]}")

    return "\n".join(lines)[:4000]  # Hard cap at 4000 chars for prompt budget


def _format_retrieval_data(retrieval_data: dict) -> str:
    """
    Format retrieval data dict as readable text for the prompt.
    Handles missing keys gracefully — retrieval may be partial.
    """
    if not retrieval_data:
        return "No retrieval data available. Use your training knowledge."

    sections = []

    if company_data := retrieval_data.get("company_overview"):
        sections.append(f"COMPANY OVERVIEW:\n{company_data}")

    if salary_data := retrieval_data.get("salary_data"):
        sections.append(f"SALARY DATA:\n{salary_data}")

    if news := retrieval_data.get("news"):
        if isinstance(news, list):
            news_text = "\n".join(f"- {item}" for item in news[:5])
        else:
            news_text = str(news)
        sections.append(f"RECENT NEWS:\n{news_text}")

    if competitors := retrieval_data.get("competitors"):
        sections.append(f"COMPETITOR LANDSCAPE:\n{competitors}")

    if contacts := retrieval_data.get("contacts"):
        if isinstance(contacts, list) and contacts:
            contact_lines = []
            for c in contacts[:6]:
                name = c.get("name", "")
                title = c.get("title", "")
                url = c.get("linkedin_url", "")
                relevance = c.get("relevance", "")
                outreach = c.get("suggested_outreach", "")
                line = f"  - {name} | {title}"
                if url:
                    line += f" | {url}"
                if relevance:
                    line += f"\n    Why relevant: {relevance}"
                if outreach:
                    line += f"\n    Suggested opener: {outreach}"
                contact_lines.append(line)
            sections.append(f"NAMED CONTACTS FOUND AT COMPANY:\n" + "\n".join(contact_lines))

    return "\n\n".join(sections) if sections else "No retrieval data available."


# ── C. Utility: build both prompts in one call ──────────────────────────────

def build_all_prompts(
    parsed_cv: ParsedCV,
    jd_text: str,
    retrieval_data: dict,
    preferences: dict,
) -> dict[str, tuple[str, str]]:
    """
    Build all prompt pairs needed for a full run.

    Returns a dict:
    {
        "edit_plan":   (system_prompt, user_prompt),
        "report_pack": (system_prompt, user_prompt),
    }

    Useful for the run_llm task which calls both in sequence.
    """
    return {
        "edit_plan": (
            EDIT_PLAN_SYSTEM,
            build_edit_plan_user_prompt(parsed_cv, jd_text, preferences),
        ),
        "report_pack": (
            REPORT_PACK_SYSTEM,
            build_report_pack_user_prompt(parsed_cv, jd_text, retrieval_data, preferences),
        ),
    }
