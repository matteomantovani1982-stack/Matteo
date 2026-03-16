"""
app/api/routes/profile_import.py

Profile auto-fill from CV or LinkedIn URL.

POST /api/v1/profiles/{profile_id}/import-cv
  — Takes a cv_id already uploaded, extracts text, sends to Claude,
    returns structured profile data (name, headline, experiences, skills).

POST /api/v1/profiles/{profile_id}/import-linkedin
  — Takes a LinkedIn URL, scrapes public profile via Serper,
    returns structured profile data.

POST /api/v1/profiles/{profile_id}/apply-import
  — Applies the extracted data to the profile (upserts experiences, sets headline etc.)
"""

import json
import re
import uuid
from functools import partial

import httpx
import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.dependencies import CurrentUserId, DB

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["Profile Import"])

TIMEOUT = 15.0


# ── Schemas ───────────────────────────────────────────────────────────────────

class ImportCVRequest(BaseModel):
    cv_id: str


class ImportLinkedInRequest(BaseModel):
    linkedin_url: str
    paste_text: str = ""  # Optional: user can paste raw LinkedIn profile text


def _coerce_str(v: any) -> str:
    """Accept str or list — join lists with newline."""
    if isinstance(v, list):
        return "\n".join(str(i) for i in v)
    return str(v) if v is not None else ""

class ImportedExperience(BaseModel):
    model_config = {"extra": "ignore"}
    role_title: str = ""
    company_name: str = ""
    start_date: str = ""
    end_date: str = ""
    location: str = ""
    context: str = ""
    contribution: str = ""
    outcomes: str = ""
    methods: str = ""
    hidden: str = ""
    freeform: str = ""

    @classmethod
    def model_validate(cls, obj: any, **kwargs):
        if isinstance(obj, dict):
            for field in ("context","contribution","outcomes","methods","hidden","freeform","role_title","company_name","location"):
                if field in obj and isinstance(obj[field], list):
                    obj[field] = "\n".join(str(i) for i in obj[field])
        return super().model_validate(obj, **kwargs)


class ImportedProfile(BaseModel):
    model_config = {"extra": "ignore"}
    full_name: str = ""
    headline: str = ""
    location: str = ""
    email: str = ""
    phone: str = ""
    nationality: str = ""
    linkedin_url: str = ""
    summary: str = ""
    skills: list[str] = []
    languages: list[str] = []
    education: list[dict] = []
    experiences: list[ImportedExperience] = []
    raw_source: str = ""  # "cv" | "linkedin"


class ApplyImportRequest(BaseModel):
    imported: ImportedProfile
    overwrite_existing: bool = False


# ── CV text extraction ────────────────────────────────────────────────────────

def _cv_to_text(parsed_content: dict) -> str:
    """
    Convert parsed CV JSONB into plain text for Claude.
    Uses sections if available, falls back to raw_paragraphs.
    Preserves ALL text — no truncation here.
    """
    lines = []

    sections = parsed_content.get("sections", [])
    if sections:
        for section in sections:
            heading = section.get("heading", "")
            if heading:
                lines.append(f"\n=== {heading.upper()} ===")
            for para in section.get("paragraphs", []):
                text = para.get("text", "").strip()
                if not text:
                    continue
                # Detect bullet-like paragraphs by style or leading chars
                style = para.get("style", "")
                is_bullet = (
                    "list" in style.lower() or
                    "bullet" in style.lower() or
                    text.startswith(("•", "-", "–", "·", "*", "◦", "▪"))
                )
                lines.append(f"  • {text}" if is_bullet else text)
    else:
        # Fallback: use raw flat paragraphs
        for para in parsed_content.get("raw_paragraphs", []):
            text = para.get("text", "").strip()
            if text:
                lines.append(text)

    result = "\n".join(lines)
    # Log how much text we got
    import logging
    logging.getLogger(__name__).info(f"cv_to_text extracted {len(result)} chars from {len(sections)} sections")
    return result


def _extract_profile_with_claude(cv_text: str) -> ImportedProfile:
    """Send CV text to Claude, get back structured profile JSON."""
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="Claude API key not configured")

    prompt = f"""You are an expert CV parser and career analyst. Your job is to extract EVERYTHING from this CV — leave nothing out.

CV TEXT (full document):
{cv_text[:14000]}

Return ONLY a valid JSON object. No markdown fences, no preamble, no explanation. Just the JSON.

{{
  "full_name": "Full legal name",
  "headline": "Craft a sharp 1-line professional headline — e.g. 'CFO | Series A–C Fintech | MENA & Europe | $500M+ P&L'",
  "location": "Current city and country",
  "email": "email address or empty string",
  "phone": "phone number or empty string",
  "linkedin_url": "LinkedIn URL or empty string",
  "nationality": "nationality if mentioned",
  "summary": "Write a 3–5 sentence professional summary capturing who they are, what they've built, and what makes them distinctive. Infer from the whole CV if no summary section exists.",
  "skills": ["Every technical skill, tool, methodology, and competency mentioned — aim for 15-30 items"],
  "languages": ["Language (proficiency level)", "..."],
  "education": [
    {{"institution": "University/school name", "degree": "BSc/MBA/etc", "field": "subject", "year": "graduation year", "notes": "any honours, thesis, or notable detail"}}
  ],
  "experiences": [
    {{
      "role_title": "Exact job title as written",
      "company_name": "Exact company name",
      "start_date": "YYYY-MM or YYYY",
      "end_date": "YYYY-MM or Present",
      "location": "City, Country",
      "context": "DETAILED: What was the company stage, size, revenue, team size, sector? What was the situation/challenge when they joined? What did they inherit?",
      "contribution": "DETAILED: What did THIS PERSON specifically own and drive — not the team, not the company. Their exact decisions, initiatives, workstreams. Be specific.",
      "outcomes": "DETAILED: Quantified results — revenue grown, cost saved, headcount scaled, valuations achieved, time to market, customer numbers. Extract every number from bullet points.",
      "methods": "DETAILED: How did they do it — frameworks, tools, partners, approaches, management style.",
      "hidden": "What is impressive about this role that a plain CV reading might miss? Infer from context, scale, complexity.",
      "freeform": "Any other relevant detail: board relationships, equity, awards, press mentions, context about why they left."
    }}
  ]
}}

CRITICAL RULES:
- Extract EVERY job listed, even short stints and early-career roles
- For outcomes: pull out EVERY number, percentage, and metric mentioned anywhere in the CV for that role
- For skills: include sector knowledge (e.g. 'MENA market entry'), functional expertise (e.g. 'P&L management'), and tools/frameworks
- For summary: write it as if you're a headhunter briefing a client — make it compelling
- If a field genuinely has no data, use empty string — do NOT omit the field
- Reverse chronological order for experiences (most recent first)"""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key, timeout=60.0)
        logger.info("claude_cv_call_start", key_len=len(settings.anthropic_api_key or ""))
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
        )
        logger.info("claude_cv_call_done")
        raw = resp.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        data = json.loads(raw)

        experiences = [ImportedExperience(**e) for e in data.get("experiences", [])]
        return ImportedProfile(
            full_name=data.get("full_name", ""),
            headline=data.get("headline", ""),
            location=data.get("location", ""),
            email=data.get("email", ""),
            phone=data.get("phone", ""),
            nationality=data.get("nationality", ""),
            linkedin_url=data.get("linkedin_url", ""),
            summary=data.get("summary", ""),
            skills=data.get("skills", []),
            languages=data.get("languages", []),
            education=data.get("education", []),
            experiences=experiences,
            raw_source="cv",
        )
    except json.JSONDecodeError as e:
        logger.error("cv_parse_json_error", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to parse Claude response as JSON")
    except Exception as e:
        import traceback as _tb
        logger.error("cv_parse_error", error=str(e), tb=_tb.format_exc()[-500:])
        raise HTTPException(status_code=500, detail=f"CV extraction failed: {str(e)}")


# ── LinkedIn scraping ─────────────────────────────────────────────────────────

def _scrape_linkedin_via_serper(linkedin_url: str) -> str:
    """
    Multi-source LinkedIn profile scraper.
    1. Try to fetch the LinkedIn page directly (often works for public profiles)
    2. Use Serper Google search for snippets
    3. Use Serper News for any press mentions
    Combines all sources for maximum coverage.
    """
    profile_slug = linkedin_url.rstrip("/").split("/in/")[-1].split("/")[0].split("?")[0]
    text_parts = [f"LinkedIn Profile: {linkedin_url}", f"Profile slug: {profile_slug}"]

    # Source 1: Direct HTTP fetch of LinkedIn page (works for some public profiles)
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
        r = httpx.get(linkedin_url, headers=headers, timeout=8.0, follow_redirects=True)
        if r.status_code == 200 and len(r.text) > 500:
            import re as _re
            html = r.text
            # Strip scripts/styles
            html = _re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=_re.DOTALL|_re.IGNORECASE)
            # Extract text
            text = _re.sub(r"<[^>]+>", " ", html)
            text = _re.sub(r"[ \t]+", " ", text)
            text = _re.sub(r"\n{3,}", "\n\n", text)
            text_parts.append("=== LINKEDIN PAGE CONTENT ===")
            text_parts.append(text.strip()[:5000])
    except Exception as e:
        logger.debug("linkedin_direct_fetch_failed", error=str(e))

    if not settings.serper_api_key:
        return "\n".join(text_parts)

    # Source 2: Google search for LinkedIn profile
    queries = [
        f'site:linkedin.com "{profile_slug}"',
        f'linkedin.com/in/{profile_slug} experience background',
        f'"{profile_slug.replace("-", " ")}" linkedin executive career',
    ]
    try:
        for q in queries:
            r = httpx.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": settings.serper_api_key, "Content-Type": "application/json"},
                json={"q": q, "num": 8},
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            for item in r.json().get("organic", []):
                title = item.get("title", "")
                snippet = item.get("snippet", "")
                if title or snippet:
                    text_parts.append(f"Search result: {title}")
                    text_parts.append(snippet)
    except Exception as e:
        logger.warning("linkedin_serper_error", error=str(e))

    # Source 3: News mentions of the person
    try:
        name_guess = profile_slug.replace("-", " ").title()
        news_q = f'"{name_guess}" executive career appointment'
        r = httpx.post(
            "https://google.serper.dev/news",
            headers={"X-API-KEY": settings.serper_api_key, "Content-Type": "application/json"},
            json={"q": news_q, "num": 5},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        for item in r.json().get("news", []):
            text_parts.append(f"News: {item.get('title', '')} — {item.get('snippet', '')}")
    except Exception as e:
        logger.debug("linkedin_news_error", error=str(e))

    result = "\n".join(text_parts)
    logger.info("linkedin_scrape_done", chars=len(result), sources=len(text_parts))
    return result


def _extract_profile_from_linkedin(linkedin_url: str, scraped_text: str) -> ImportedProfile:
    """Send scraped LinkedIn data to Claude for structuring."""
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="Claude API key not configured")

    prompt = f"""You are an expert LinkedIn profile analyst and career researcher. Extract EVERYTHING from this LinkedIn profile data.

LINKEDIN URL: {linkedin_url}
PROFILE DATA:
{scraped_text[:10000]}

The data above may come from: direct page scrape, Google search snippets, news articles, and other public sources.
Extract the most complete profile possible from all available data.

Return ONLY a valid JSON object. No markdown, no preamble. Just JSON.

{{
  "full_name": "Full name",
  "headline": "Craft a sharp 1-line professional headline from their experience — e.g. 'COO | MENA Scale-ups | $200M P&L'",
  "location": "City, Country",
  "email": "",
  "phone": "",
  "linkedin_url": "{linkedin_url}",
  "nationality": "",
  "summary": "Write a 3–5 sentence professional summary from what you know. Make it compelling for a senior executive audience.",
  "skills": ["List every professional skill, domain expertise, tool or competency you can infer — aim for 15-20"],
  "languages": ["Language (level)"],
  "education": [{{"institution": "", "degree": "", "field": "", "year": "", "notes": ""}}],
  "experiences": [
    {{
      "role_title": "Exact title",
      "company_name": "Company name",
      "start_date": "YYYY-MM or YYYY",
      "end_date": "YYYY-MM or Present",
      "location": "City, Country",
      "context": "Company stage, size, sector context when they joined",
      "contribution": "What they specifically owned and drove in this role",
      "outcomes": "Any numbers, metrics, achievements mentioned or inferable",
      "methods": "How they operated — management style, frameworks, approaches",
      "hidden": "What is impressive or notable about this role that stands out",
      "freeform": "Additional context from LinkedIn description, any notable detail"
    }}
  ]
}}

RULES:
- Extract ALL positions listed, even brief ones
- Infer skills from their roles and sectors even if not explicitly listed
- For experiences with LinkedIn descriptions, put the full description in freeform
- If data is limited, still construct the best possible profile from what's available
- Reverse chronological order"""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key, timeout=60.0)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        data = json.loads(raw)
        experiences = [ImportedExperience(**e) for e in data.get("experiences", [])]
        return ImportedProfile(
            full_name=data.get("full_name", ""),
            headline=data.get("headline", ""),
            location=data.get("location", ""),
            email=data.get("email", ""),
            phone=data.get("phone", ""),
            nationality=data.get("nationality", ""),
            linkedin_url=linkedin_url,
            summary=data.get("summary", ""),
            skills=data.get("skills", []),
            languages=data.get("languages", []),
            education=data.get("education", []),
            experiences=experiences,
            raw_source="linkedin",
        )
    except Exception as e:
        logger.error("linkedin_parse_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"LinkedIn extraction failed: {str(e)}")



def _extract_text_from_bytes(file_bytes: bytes, filename: str) -> str:
    """Extract plain text from a DOCX or PDF file bytes."""
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""

    if ext == "docx":
        import io
        from docx import Document
        from docx.text.paragraph import Paragraph as _Para
        doc = Document(io.BytesIO(file_bytes))
        # Walk full XML to catch text in tables, text boxes, etc.
        seen, paras = set(), []
        for child in doc.element.body.iter():
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "p":
                try:
                    p = _Para(child, doc)
                    pid = id(p._element)
                    if pid not in seen:
                        seen.add(pid)
                        paras.append(p)
                except Exception:
                    pass
        lines = [p.text.strip() for p in paras if p.text.strip()]
        return "\n".join(lines)

    elif ext == "pdf":
        try:
            import pdfminer.high_level as _pdf
            import io
            text = _pdf.extract_text(io.BytesIO(file_bytes))
            return text or ""
        except ImportError:
            pass
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            return "\n".join(page.get_text() for page in doc)
        except ImportError:
            pass
        raise HTTPException(status_code=400, detail="PDF parsing not available — please upload a .docx file")

    raise HTTPException(status_code=400, detail=f"Unsupported file type: .{ext}. Please upload .docx or .pdf")


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/api/v1/profiles/{profile_id}/import-cv")
async def import_from_cv(
    profile_id: uuid.UUID,
    payload: ImportCVRequest,
    db: DB,
    current_user_id: CurrentUserId,
) -> ImportedProfile:
    """Extract profile data from an uploaded CV using Claude.
    Reads raw file bytes directly from S3 — no need to wait for Celery parsing.
    Works immediately after upload.
    """
    import asyncio
    from sqlalchemy import select
    from app.models.cv import CV
    from app.config import settings as _s

    result = await db.execute(
        select(CV).where(CV.id == uuid.UUID(payload.cv_id), CV.user_id == str(current_user_id))
    )
    cv = result.scalar_one_or_none()
    if not cv:
        raise HTTPException(status_code=404, detail="CV not found")

    logger.info("import_cv_start", cv_id=payload.cv_id)

    # Path 1: Celery already parsed it — use stored structured text (fast)
    cv_text = ""
    if cv.parsed_content:
        cv_text = _cv_to_text(cv.parsed_content)

    # Path 2: Not yet parsed — read raw bytes from S3 and extract text directly
    if not cv_text.strip():
        try:
            import boto3
            s3 = boto3.client(
                "s3",
                endpoint_url=_s.s3_endpoint_url,
                aws_access_key_id=_s.s3_access_key_id,
                aws_secret_access_key=_s.s3_secret_access_key,
                region_name=_s.s3_region,
            )
            loop = asyncio.get_running_loop()
            obj = await loop.run_in_executor(
                None, lambda: s3.get_object(Bucket=cv.s3_bucket, Key=cv.s3_key)
            )
            file_bytes = obj["Body"].read()
            cv_text = _extract_text_from_bytes(file_bytes, cv.original_filename)
            logger.info("import_cv_read_from_s3_direct", bytes=len(file_bytes), text_chars=len(cv_text))
        except HTTPException:
            raise
        except Exception as e:
            logger.error("import_cv_s3_read_failed", error=str(e))
            raise HTTPException(status_code=500, detail=f"Could not read CV file: {str(e)}")

    if not cv_text.strip():
        raise HTTPException(status_code=400, detail="Could not extract text from CV — ensure it is a valid .docx or .pdf")

    loop = asyncio.get_running_loop()
    imported = await loop.run_in_executor(None, partial(_extract_profile_with_claude, cv_text))
    logger.info("import_cv_done", experiences=len(imported.experiences))
    return imported


@router.post("/api/v1/profiles/{profile_id}/import-linkedin")
async def import_from_linkedin(
    profile_id: uuid.UUID,
    payload: ImportLinkedInRequest,
    db: DB,
    current_user_id: CurrentUserId,
) -> ImportedProfile:
    """Extract profile data from a LinkedIn URL."""
    import asyncio

    has_url = "linkedin.com" in payload.linkedin_url
    has_paste = len(payload.paste_text.strip()) > 50
    if not has_url and not has_paste:
        raise HTTPException(status_code=400, detail="Provide a LinkedIn URL or paste your profile text")

    logger.info("import_linkedin_start", url=payload.linkedin_url)

    loop = asyncio.get_running_loop()
    # If user pasted their profile text, use that (much richer than scraping)
    if payload.paste_text and len(payload.paste_text.strip()) > 100:
        source_text = f"LinkedIn URL: {payload.linkedin_url}\n\n=== PASTED PROFILE TEXT ===\n{payload.paste_text[:8000]}"
        logger.info("import_linkedin_using_paste", chars=len(payload.paste_text))
    else:
        source_text = await loop.run_in_executor(
            None, partial(_scrape_linkedin_via_serper, payload.linkedin_url)
        )

    imported = await loop.run_in_executor(
        None, partial(_extract_profile_from_linkedin, payload.linkedin_url, source_text)
    )

    logger.info("import_linkedin_done", experiences=len(imported.experiences))
    return imported


@router.post("/api/v1/profiles/{profile_id}/apply-import")
async def apply_import(
    profile_id: uuid.UUID,
    payload: ApplyImportRequest,
    db: DB,
    current_user_id: CurrentUserId,
) -> dict:
    """
    Apply imported profile data to an existing profile.
    Upserts experiences, updates headline/summary/skills.
    """
    from sqlalchemy import select
    from app.models.candidate_profile import CandidateProfile, ExperienceEntry
    from app.models.user import User

    imp = payload.imported

    # Load profile
    result = await db.execute(
        select(CandidateProfile).where(
            CandidateProfile.id == profile_id,
            CandidateProfile.user_id == str(current_user_id),
        )
    )
    profile = result.scalar_one_or_none()
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")

    # Update profile-level fields
    if imp.headline:
        profile.headline = imp.headline
    if imp.location:
        profile.location = imp.location

    # Mark profile as active once we have real data
    from app.models.candidate_profile import ProfileStatus
    if imp.experiences or imp.headline:
        profile.status = ProfileStatus.ACTIVE

    # Store skills, languages, education, summary in global_context
    import json as _json
    try:
        ctx = _json.loads(profile.global_context or "{}")
    except Exception:
        ctx = {}

    if imp.summary:
        ctx["summary"] = imp.summary
    if imp.skills:
        ctx["skills"] = imp.skills
    if imp.languages:
        ctx["languages"] = imp.languages
    if imp.education:
        ctx["education"] = imp.education
    if imp.full_name:
        ctx["full_name"] = imp.full_name
    if imp.linkedin_url:
        ctx["linkedin_url"] = imp.linkedin_url
    if imp.email:
        ctx["email"] = imp.email
    if imp.phone:
        ctx["phone"] = imp.phone
    if imp.nationality:
        ctx["nationality"] = imp.nationality
    if imp.education:
        ctx["education"] = [e if isinstance(e, dict) else e.model_dump() for e in imp.education]

    profile.global_context = _json.dumps(ctx)

    # Update user full_name if we have it
    if imp.full_name:
        user_result = await db.execute(
            select(User).where(User.id == uuid.UUID(current_user_id))
        )
        user = user_result.scalar_one_or_none()
        if user and not user.full_name:
            user.full_name = imp.full_name

    # Upsert experiences
    experiences_added = 0
    if payload.overwrite_existing:
        # Delete existing experiences
        from sqlalchemy import delete
        await db.execute(
            delete(ExperienceEntry).where(ExperienceEntry.profile_id == profile_id)
        )

    for i, exp in enumerate(imp.experiences):
        entry = ExperienceEntry(
            id=uuid.uuid4(),
            profile_id=profile_id,
            company_name=exp.company_name,
            role_title=exp.role_title,
            start_date=exp.start_date or None,
            end_date=exp.end_date or None,
            location=exp.location or None,
            context=exp.context or None,
            contribution=exp.contribution or None,
            outcomes=exp.outcomes or None,
            methods=exp.methods or None,
            hidden=exp.hidden or None,
            freeform=exp.freeform or None,
            display_order=i,
        )
        db.add(entry)
        experiences_added += 1

    await db.commit()

    logger.info("apply_import_done", experiences_added=experiences_added)
    return {
        "success": True,
        "experiences_added": experiences_added,
        "headline_set": bool(imp.headline),
        "profile_id": str(profile_id),
    }
