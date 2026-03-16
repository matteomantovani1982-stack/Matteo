"""
app/services/retrieval/web_search.py

Retrieval service: fetches company intelligence, salary data, and news
to enrich the LLM prompt before the Claude API call.

Architecture:
  - Synchronous (runs inside Celery worker)
  - Modular: each retrieval function is independent and can fail gracefully
  - Returns a RetrievalResult with structured data + source citations
  - Falls back to empty data on any error — never blocks the LLM call

Two retrieval backends supported:
  1. Serper API  (Google search via serper.dev) — when SERPER_API_KEY is set
  2. Stub/mock   — returns empty data, used in tests and when key is missing

Design principle: Retrieval enriches the prompt but is NOT required.
If retrieval fails entirely, the LLM uses its training knowledge.
The RetrievalResult.partial_failure flag signals degraded quality.
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from app.config import settings

import structlog

logger = structlog.get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
SERPER_API_URL = "https://google.serper.dev/search"
REQUEST_TIMEOUT_SECONDS = 10.0
MAX_RESULTS_PER_QUERY = 5


# ── Result container ─────────────────────────────────────────────────────────

@dataclass
class RetrievalResult:
    """
    Structured output from the retrieval service.
    Stored in JobRun.retrieval_data.
    Passed to build_report_pack_user_prompt().
    """
    company_overview: str = ""
    salary_data: str = ""
    news: list[str] = field(default_factory=list)
    competitors: str = ""
    contacts: list[dict] = field(default_factory=list)   # Sprint J: named contacts
    sources: list[str] = field(default_factory=list)
    partial_failure: bool = False
    error_notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Serialise to dict for storage in JobRun.retrieval_data (JSONB)."""
        return {
            "company_overview": self.company_overview,
            "salary_data": self.salary_data,
            "news": self.news,
            "competitors": self.competitors,
            "contacts": self.contacts,
            "sources": self.sources,
            "partial_failure": self.partial_failure,
            "error_notes": self.error_notes,
        }

    @classmethod
    def empty(cls, reason: str = "") -> "RetrievalResult":
        """Return an empty result with a reason note."""
        return cls(
            partial_failure=True,
            error_notes=[reason] if reason else [],
        )


# ── Query builders ────────────────────────────────────────────────────────────

def _extract_company_name(jd_text: str) -> str:
    """
    Attempt to extract company name from the first 500 chars of JD.
    Heuristic: look for 'at CompanyName' or 'join CompanyName'.
    Falls back to first capitalised phrase.

    This is a best-effort extraction — not perfect.
    The LLM will have the full JD anyway.
    """
    import re
    # Pattern: "at [Company]" or "join [Company]" near the start
    patterns = [
        r"(?:at|join|for)\s+([A-Z][A-Za-z0-9&\s]{2,40}?)(?:\s+as|\s+in|\.|,)",
        r"^([A-Z][A-Za-z0-9&\s]{2,30}?)\s+(?:is|are)\s+(?:looking|hiring|seeking)",
    ]
    for pattern in patterns:
        match = re.search(pattern, jd_text[:500])
        if match:
            return match.group(1).strip()

    # Fallback: first line of JD that looks like a company name
    first_line = jd_text.split("\n")[0].strip()
    if len(first_line) < 60 and first_line[0].isupper():
        return first_line

    return ""


def _build_queries(
    jd_text: str,
    company_name: str,
    role_title: str,
    region: str,
) -> dict[str, str]:
    """
    Build targeted search queries for each data type we want to retrieve.

    Returns a dict mapping data type → search query string.
    """
    queries: dict[str, str] = {}

    if company_name:
        queries["company_overview"] = (
            f"{company_name} company overview revenue strategy 2024 2025"
        )
        queries["news"] = (
            f"{company_name} news 2024 2025 expansion hiring"
        )
        queries["competitors"] = (
            f"{company_name} competitors market position {region}"
        )

    if role_title and region:
        queries["salary_data"] = (
            f"{role_title} salary {region} 2024 AED compensation"
        )
    elif role_title:
        queries["salary_data"] = (
            f"{role_title} salary compensation range 2024"
        )

    return queries


# ── Serper API client ─────────────────────────────────────────────────────────

class SerperClient:
    """
    Thin wrapper around the Serper.dev Google Search API.
    https://serper.dev/

    Serper returns the top Google results as JSON — much cheaper and
    more reliable than scraping.
    """

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS)

    def search(self, query: str, num_results: int = MAX_RESULTS_PER_QUERY) -> list[dict]:
        """
        Execute a search query and return organic results.

        Returns list of result dicts with keys: title, link, snippet.
        Returns empty list on any error (never raises).
        """
        try:
            response = self._client.post(
                SERPER_API_URL,
                headers={
                    "X-API-KEY": self._api_key,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": num_results},
            )
            response.raise_for_status()
            data = response.json()
            return data.get("organic", [])

        except httpx.TimeoutException:
            logger.warning("serper_timeout", query=query)
            return []
        except httpx.HTTPStatusError as e:
            logger.warning(
                "serper_http_error",
                http_status=e.response.status_code,
                query=query,
            )
            return []
        except Exception as e:
            logger.warning("serper_error", error=str(e), query=query)
            return []

    def close(self) -> None:
        self._client.close()


def _format_results_as_text(results: list[dict], max_chars: int = 1500) -> str:
    """
    Format Serper results as readable text for the LLM prompt.

    Each result: "TITLE (SOURCE)\nSNIPPET\n"
    Truncated to max_chars to control prompt size.
    """
    if not results:
        return ""

    lines = []
    for r in results:
        title = r.get("title", "")
        snippet = r.get("snippet", "")
        link = r.get("link", "")

        # Extract domain as source attribution
        source = ""
        if link:
            try:
                from urllib.parse import urlparse
                source = urlparse(link).netloc.replace("www.", "")
            except Exception:
                pass

        lines.append(f"{title} ({source})")
        if snippet:
            lines.append(snippet)
        lines.append("")

    text = "\n".join(lines)
    return text[:max_chars]


def _extract_sources(results: list[dict]) -> list[str]:
    """Extract clean domain names from search results for attribution."""
    sources = []
    for r in results:
        link = r.get("link", "")
        if link:
            try:
                from urllib.parse import urlparse
                domain = urlparse(link).netloc.replace("www.", "")
                if domain and domain not in sources:
                    sources.append(domain)
            except Exception:
                pass
    return sources[:5]


# ── Main retrieval service ────────────────────────────────────────────────────

class RetrievalService:
    """
    Orchestrates web retrieval for a job run.

    Executes multiple targeted search queries in sequence,
    formats the results for the LLM prompt, and returns a RetrievalResult.

    Graceful degradation: each query failure is noted but doesn't
    abort the entire retrieval. Partial data is better than none.
    """

    def __init__(self, serper_client: SerperClient | None = None) -> None:
        """
        Accept an injected SerperClient for testability.
        If not provided, creates one from settings (or returns empty if no key).
        """
        self._serper = serper_client

        if self._serper is None and settings.serper_api_key:
            self._serper = SerperClient(api_key=settings.serper_api_key)

    def retrieve(
        self,
        jd_text: str,
        role_title: str = "",
        region: str = "UAE",
    ) -> RetrievalResult:
        """
        Execute all retrieval queries for a job run.

        Args:
            jd_text:    Full job description text
            role_title: Extracted or provided role title for salary search
            region:     Target region for salary data (default: UAE)

        Returns:
            RetrievalResult with all data or partial data if some queries failed
        """
        if self._serper is None:
            logger.info(
                "retrieval_skipped",
                reason="No SERPER_API_KEY configured — using LLM training knowledge only",
            )
            return RetrievalResult.empty(
                "No search API configured. LLM will use training knowledge."
            )

        company_name = _extract_company_name(jd_text)
        queries = _build_queries(
            jd_text=jd_text,
            company_name=company_name,
            role_title=role_title,
            region=region,
        )

        logger.info(
            "retrieval_start",
            company=company_name,
            queries=list(queries.keys()),
        )

        result = RetrievalResult()
        all_sources: list[str] = []

        # ── Company overview ───────────────────────────────────────────────
        if query := queries.get("company_overview"):
            try:
                results = self._serper.search(query)
                result.company_overview = _format_results_as_text(results)
                all_sources.extend(_extract_sources(results))
            except Exception as e:
                result.partial_failure = True
                result.error_notes.append(f"company_overview failed: {e}")

        # ── Recent news ────────────────────────────────────────────────────
        if query := queries.get("news"):
            try:
                results = self._serper.search(query)
                result.news = [
                    f"{r.get('title', '')} — {r.get('snippet', '')[:200]}"
                    for r in results
                    if r.get("title")
                ][:5]
                all_sources.extend(_extract_sources(results))
            except Exception as e:
                result.partial_failure = True
                result.error_notes.append(f"news failed: {e}")

        # ── Salary data ────────────────────────────────────────────────────
        if query := queries.get("salary_data"):
            try:
                results = self._serper.search(query)
                result.salary_data = _format_results_as_text(results, max_chars=1000)
                all_sources.extend(_extract_sources(results))
            except Exception as e:
                result.partial_failure = True
                result.error_notes.append(f"salary_data failed: {e}")

        # ── Competitor landscape ───────────────────────────────────────────
        if query := queries.get("competitors"):
            try:
                results = self._serper.search(query)
                result.competitors = _format_results_as_text(results, max_chars=800)
                all_sources.extend(_extract_sources(results))
            except Exception as e:
                result.partial_failure = True
                result.error_notes.append(f"competitors failed: {e}")

        # ── Contact search (Sprint J) ──────────────────────────────────────
        if company_name:
            try:
                from app.services.retrieval.contact_search import ContactSearchService
                contact_svc = ContactSearchService(serper_client=self._serper)
                contacts = contact_svc.find_contacts(
                    company_name=company_name,
                    role_title=role_title,
                    region=region,
                )
                result.contacts = [c.to_dict() for c in contacts]
                logger.info("contact_search_done", found=len(contacts))
            except Exception as e:
                result.error_notes.append(f"contact_search failed: {e}")
                logger.warning("contact_search_failed_non_fatal", error=str(e))

        # Deduplicate sources
        seen: set[str] = set()
        result.sources = [
            s for s in all_sources
            if not (s in seen or seen.add(s))  # type: ignore[func-returns-value]
        ]

        logger.info(
            "retrieval_complete",
            company=company_name,
            sources=len(result.sources),
            partial_failure=result.partial_failure,
        )

        return result

    def close(self) -> None:
        if self._serper:
            self._serper.close()
