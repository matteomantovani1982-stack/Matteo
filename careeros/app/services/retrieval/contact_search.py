"""
app/services/retrieval/contact_search.py

Contact Search — finds real named people at target companies using Serper.

Strategy:
  For each target role (hiring manager, team lead, relevant VP), we search:
    "{company} {role_title} linkedin"
    "{company} {role_title} site:linkedin.com"

  Google indexes public LinkedIn profiles. We extract names, titles, and
  LinkedIn URLs from the search results without ever touching the LinkedIn API.

  This is public information — the same thing a recruiter does manually.

Limitations:
  - Only finds people with public LinkedIn profiles indexed by Google
  - LinkedIn login walls block direct access (handled gracefully)
  - Results may be outdated if someone has changed roles
  - Accuracy varies by company size and LinkedIn activity

Output: a list of ContactResult objects, deduplicated by name.
Each contact includes:
  - name (extracted from search result titles)
  - title (their stated role)
  - company (confirmed match)
  - linkedin_url (if detectable)
  - relevance (why they matter for this application)
  - suggested_outreach (personalised opener based on what we know)

Called from RetrievalService.retrieve() when company name is found.
Results stored in RetrievalResult.contacts and passed to the LLM.
"""

import re
from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger(__name__)

# Max contacts to search for — more than this adds noise and burns Serper credits
MAX_CONTACTS_TO_FIND = 6

# Roles worth targeting for networking — ordered by priority
PRIORITY_ROLES = [
    "Chief of Staff",
    "Head of Talent",
    "VP Talent Acquisition",
    "Head of HR",
    "Head of Strategy",
    "VP Strategy",
    "Chief Strategy Officer",
    "Managing Director",
    "VP Operations",
    "Head of Operations",
]


@dataclass
class ContactResult:
    """A named person found at the target company."""
    name: str
    title: str
    company: str
    linkedin_url: str | None
    source_snippet: str       # Raw snippet from search result
    relevance: str            # Why this person matters
    suggested_outreach: str   # Personalised opening line

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "title": self.title,
            "company": self.company,
            "linkedin_url": self.linkedin_url,
            "relevance": self.relevance,
            "suggested_outreach": self.suggested_outreach,
        }


# ── Name extraction from LinkedIn search result titles ───────────────────────

# LinkedIn Google results typically look like:
#   "Sarah Johnson - Head of Talent - e& Enterprise | LinkedIn"
#   "Ahmed Al-Rashidi | VP Strategy at Tamara | LinkedIn"
#   "e& Enterprise: Overview | LinkedIn"  (company page — skip)

_LINKEDIN_PERSON_TITLE = re.compile(
    r"^([A-Z][a-zA-Z\s\-\.\']{2,40})\s*[\|\-–]\s*(.+?)\s*[\|\-–]",
    re.UNICODE,
)

# Company page patterns to skip
_COMPANY_PAGE_PATTERNS = [
    "overview", "linkedin.com/company", "employees", "followers",
    "see who you know", "jobs at",
]

def _extract_person_from_result(result: dict) -> tuple[str, str, str | None] | None:
    """
    Extract (name, title, linkedin_url) from a Serper search result.
    Returns None if the result is a company page or doesn't match.
    """
    title_text = result.get("title", "")
    link = result.get("link", "")
    snippet = result.get("snippet", "")

    # Skip company pages
    combined = (title_text + " " + link + " " + snippet).lower()
    if any(p in combined for p in _COMPANY_PAGE_PATTERNS):
        return None

    # Must be a LinkedIn URL to be trustworthy
    if "linkedin.com/in/" not in link:
        return None

    match = _LINKEDIN_PERSON_TITLE.match(title_text)
    if not match:
        return None

    name = match.group(1).strip()
    role = match.group(2).strip()

    # Filter out obvious non-people (long strings, all caps, etc.)
    if len(name) > 50 or len(name) < 4:
        return None
    if name.isupper():
        return None

    return name, role, link


# ── Query builders ────────────────────────────────────────────────────────────

def _build_contact_queries(
    company_name: str,
    role_title: str,
    region: str = "UAE",
) -> list[tuple[str, str]]:
    """
    Build search queries to find named contacts at the company.

    Returns list of (query_string, relevance_label) tuples.
    relevance_label is used to populate ContactResult.relevance.
    """
    queries = []

    # 1. Hiring-adjacent roles — most likely to be in the decision chain
    queries.append((
        f'"{company_name}" "head of talent" OR "VP talent" OR "talent acquisition" linkedin',
        "Talent / recruiting — likely involved in the hiring process",
    ))

    # 2. The role's likely peer / manager
    # e.g. if role is "VP Strategy" → search "Chief Strategy Officer" and "MD Strategy"
    role_upper = role_title.title()
    queries.append((
        f'"{company_name}" {role_upper} linkedin site:linkedin.com',
        f"Direct peer or manager for a {role_title} role",
    ))

    # 3. Senior leadership at the company
    queries.append((
        f'"{company_name}" "chief of staff" OR "managing director" OR "VP" {region} linkedin',
        "Senior leadership — warm intro path",
    ))

    return queries[:3]  # Never more than 3 queries — Serper credits


# ── Contact search service ────────────────────────────────────────────────────

class ContactSearchService:
    """
    Searches for named contacts at a target company using Serper.

    Used by RetrievalService to enrich the networking section
    with real people instead of generic role titles.
    """

    def __init__(self, serper_client) -> None:
        self._serper = serper_client

    def find_contacts(
        self,
        company_name: str,
        role_title: str,
        region: str = "UAE",
        candidate_background: str = "",
    ) -> list[ContactResult]:
        """
        Search for named contacts at the company.

        Args:
            company_name:         Target company
            role_title:           Role being applied for
            region:               Geographic region
            candidate_background: Brief candidate summary for personalised outreach

        Returns:
            List of ContactResult objects, deduplicated by name, max MAX_CONTACTS_TO_FIND.
        """
        if not company_name:
            return []

        queries = _build_contact_queries(company_name, role_title, region)
        found: dict[str, ContactResult] = {}  # name → ContactResult, deduped

        for query, relevance in queries:
            if len(found) >= MAX_CONTACTS_TO_FIND:
                break

            try:
                results = self._serper.search(query, num_results=5)
                logger.info(
                    "contact_search_query",
                    query=query,
                    results=len(results),
                )

                for result in results:
                    if len(found) >= MAX_CONTACTS_TO_FIND:
                        break

                    extracted = _extract_person_from_result(result)
                    if extracted is None:
                        continue

                    name, title, linkedin_url = extracted

                    # Skip if already found this person
                    if name in found:
                        continue

                    # Confirm company match — check snippet contains company name
                    snippet = result.get("snippet", "")
                    if company_name.lower() not in (snippet + result.get("title", "")).lower():
                        continue

                    outreach = _build_outreach_opener(
                        contact_name=name,
                        contact_title=title,
                        company_name=company_name,
                        role_title=role_title,
                        candidate_background=candidate_background,
                    )

                    found[name] = ContactResult(
                        name=name,
                        title=title,
                        company=company_name,
                        linkedin_url=linkedin_url,
                        source_snippet=snippet[:300],
                        relevance=relevance,
                        suggested_outreach=outreach,
                    )

            except Exception as e:
                logger.warning("contact_search_query_failed", query=query, error=str(e))
                continue

        contacts = list(found.values())
        logger.info(
            "contact_search_complete",
            company=company_name,
            found=len(contacts),
        )
        return contacts


def _build_outreach_opener(
    contact_name: str,
    contact_title: str,
    company_name: str,
    role_title: str,
    candidate_background: str = "",
) -> str:
    """
    Generate a personalised opening line for a connection request.
    Short, specific, non-generic. References the person's role and context.
    """
    first_name = contact_name.split()[0] if contact_name else "Hi"

    # Tailor by contact role type
    title_lower = contact_title.lower()

    if any(t in title_lower for t in ["talent", "recruit", "hr", "people"]):
        return (
            f"Hi {first_name}, I came across your profile while researching {company_name}. "
            f"I'm exploring the {role_title} opportunity and would love to connect — "
            f"happy to share my background if useful."
        )
    elif any(t in title_lower for t in ["chief of staff", "coo", "operations"]):
        return (
            f"Hi {first_name}, I'm a fellow operator with a background in "
            f"{'MENA digital businesses' if not candidate_background else candidate_background[:60]}. "
            f"Exploring the {role_title} role at {company_name} — would value a brief connection."
        )
    else:
        return (
            f"Hi {first_name}, I've been following {company_name}'s work and am interested "
            f"in the {role_title} opportunity. Your experience in {contact_title.split(' at ')[0]} "
            f"is directly relevant — would be great to connect."
        )
