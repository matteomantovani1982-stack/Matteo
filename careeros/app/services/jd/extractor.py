"""
app/services/jd/extractor.py

Job Description extractor — fetches a URL and extracts clean JD text.

Handles:
  - LinkedIn job postings (/jobs/view/...)
  - Company careers pages (Greenhouse, Lever, Workday, etc.)
  - Bayt, GulfTalent, Indeed, and other job boards
  - Any arbitrary URL (generic fallback)

Pipeline:
  1. Fetch the URL (httpx, with browser-like headers to avoid 403s)
  2. Strip HTML — remove nav, footer, scripts, ads, cookie banners
  3. LLM pass — extract clean JD text from the remaining noise
  4. Return extracted text

The LLM step is important: even after HTML stripping, job board pages
contain a lot of noise (related jobs, company blurbs, salary widgets).
Claude reads the page and returns only the actual job description.

Fallback: if LLM extraction fails, returns the raw stripped text.
If fetch fails entirely, raises JDExtractionError with a user-friendly message.
"""

import re
from typing import Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)

# Request timeout — job boards can be slow
FETCH_TIMEOUT = 25.0

# Max chars to send to LLM — enough to cover any JD
LLM_INPUT_MAX = 8000

# Browser-like headers to avoid 403s on LinkedIn, etc.
FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# Extraction system prompt
JD_EXTRACT_SYSTEM = """\
You are extracting a job description from a web page.

The page content may include navigation menus, related jobs, company descriptions,
cookie notices, salary widgets, and other noise.

Extract ONLY the actual job posting content:
  - Job title
  - Company name and location
  - Role summary / about the role
  - Responsibilities / what you will do
  - Requirements / qualifications
  - Nice to have / preferred skills
  - Benefits (if listed)

Return ONLY the clean job description text. No JSON. No headers like "Here is the JD:".
Preserve the original structure (headings, bullet points) using plain text.
If you cannot find a clear job description on this page, return exactly: NO_JD_FOUND
"""


class JDExtractionError(Exception):
    """Raised when JD cannot be fetched or extracted."""
    def __init__(self, message: str, url: str):
        self.url = url
        super().__init__(message)


class JDExtractor:
    """
    Fetches a URL and extracts the job description text.

    Usage:
        extractor = JDExtractor()
        jd_text = await extractor.extract(url)
    """

    async def extract(self, url: str) -> str:
        """
        Fetch URL and extract clean JD text.

        Returns:
            Clean job description as plain text.

        Raises:
            JDExtractionError if fetch fails or no JD found on page.
        """
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        logger.info("jd_extract_start", url=url)

        # Step 1: Fetch
        raw_html = await self._fetch(url)

        # Step 2: Strip HTML
        stripped = _strip_html(raw_html)
        logger.info("jd_html_stripped", chars=len(stripped))

        # Step 3: LLM extraction
        jd_text = await self._extract_with_llm(stripped, url)

        logger.info("jd_extracted", chars=len(jd_text), url=url)
        # Normalize whitespace in returned text
        jd_text = "\n".join(
            line.strip() for line in jd_text.splitlines()
        )
        import re
        jd_text = re.sub(r"\n{3,}", "\n\n", jd_text).strip()
        return jd_text

    async def _fetch(self, url: str) -> str:
        """Fetch URL, return HTML string."""
        try:
            async with httpx.AsyncClient(
                headers=FETCH_HEADERS,
                timeout=FETCH_TIMEOUT,
                follow_redirects=True,
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.text

        except httpx.TimeoutException:
            raise JDExtractionError(
                f"The page took too long to load ({FETCH_TIMEOUT}s). "
                "Try copying and pasting the job description manually.",
                url=url,
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise JDExtractionError(
                    "This page blocked automatic access (403 Forbidden). "
                    "LinkedIn job postings often require login — "
                    "please copy and paste the job description manually.",
                    url=url,
                )
            raise JDExtractionError(
                f"Could not load this page (HTTP {e.response.status_code}). "
                "Please copy and paste the job description manually.",
                url=url,
            )
        except httpx.RequestError as e:
            raise JDExtractionError(
                f"Could not reach this URL ({e}). "
                "Check the URL is correct, or paste the job description manually.",
                url=url,
            )

    async def _extract_with_llm(self, stripped_text: str, url: str) -> str:
        """Use Claude to extract clean JD text from stripped page content."""
        from app.services.llm.client import ClaudeClient

        # Truncate to LLM input limit
        input_text = stripped_text[:LLM_INPUT_MAX]

        # Skip demo mode for JD extraction - use real Claude or return stripped text
        from app.config import settings as _settings
        if _settings.demo_mode or not _settings.anthropic_api_key:
            # No LLM available — return stripped text directly
            if len(stripped_text) < 200:
                raise JDExtractionError(
                    "Could not extract enough text from this page. "
                    "Please copy and paste the job description manually.",
                    url=url,
                )
            return stripped_text[:4000]

        client = ClaudeClient(max_tokens=2000)
        try:
            import anthropic as _anthropic
            _client = _anthropic.Anthropic(api_key=_settings.anthropic_api_key)
            _resp = _client.messages.create(
                model=_settings.claude_model,
                max_tokens=2000,
                system=JD_EXTRACT_SYSTEM,
                messages=[{"role": "user", "content": f"Extract the job description from this page:\n\n{input_text}"}],
            )
            raw = _resp.content[0].text.strip()

            if raw == "NO_JD_FOUND":
                raise JDExtractionError(
                    "No job description found on this page. "
                    "Please copy and paste the job description manually.",
                    url=url,
                )

            return raw

        except JDExtractionError:
            raise
        except Exception as e:
            logger.warning("jd_llm_extract_failed_using_stripped", error=str(e))
            if len(stripped_text) < 200:
                raise JDExtractionError(
                    "Could not extract job description from this page. "
                    "Please copy and paste the text manually.",
                    url=url,
                )
            return stripped_text[:4000]


# ── HTML stripping ─────────────────────────────────────────────────────────────

# Tags whose content should be removed entirely (not just the tag)
_REMOVE_CONTENT_TAGS = re.compile(
    r"<(script|style|nav|footer|header|aside|noscript|iframe|svg|form)"
    r"[^>]*>.*?</\1>",
    re.DOTALL | re.IGNORECASE,
)

# All remaining HTML tags
_HTML_TAGS = re.compile(r"<[^>]+>")

# Multiple whitespace/newlines
_WHITESPACE = re.compile(r"\n{3,}")
_SPACES = re.compile(r" {2,}")

# Common noise patterns on job board pages
_NOISE_PATTERNS = [
    re.compile(r"Apply now.*?(?=\n)", re.IGNORECASE),
    re.compile(r"Save job.*?(?=\n)", re.IGNORECASE),
    re.compile(r"Share.*?(?=\n)", re.IGNORECASE),
    re.compile(r"Report.*?(?=\n)", re.IGNORECASE),
    re.compile(r"Cookie.*?(?=\n)", re.IGNORECASE),
    re.compile(r"\d+ applicants", re.IGNORECASE),
]


def _strip_html(html: str) -> str:
    """
    Strip HTML to plain text, removing scripts, styles, nav, footer.
    Preserves line breaks for structure.
    """
    # Decode common HTML entities
    text = html
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&nbsp;", " ").replace("&quot;", '"').replace("&#39;", "'")
    text = text.replace("&mdash;", "—").replace("&ndash;", "–").replace("&bull;", "•")

    # Remove blocks with content (scripts, nav, etc.)
    text = _REMOVE_CONTENT_TAGS.sub("", text)

    # Convert block elements to newlines before stripping tags
    for tag in ("</p>", "</div>", "</li>", "</h1>", "</h2>", "</h3>",
                "</h4>", "</tr>", "</br>", "<br>", "<br/>"):
        text = text.replace(tag, "\n")

    # Strip remaining tags
    text = _HTML_TAGS.sub("", text)

    # Remove noise patterns
    for pattern in _NOISE_PATTERNS:
        text = pattern.sub("", text)

    # Clean up whitespace
    text = _WHITESPACE.sub("\n\n", text)
    text = _SPACES.sub(" ", text)

    return text.strip()
