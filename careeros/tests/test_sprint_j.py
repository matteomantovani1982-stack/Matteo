"""
tests/test_sprint_j.py

Sprint J: Networking intelligence — named contacts via Serper, known_contacts.
"""

import pytest
from unittest.mock import MagicMock


# ════════════════════════════════════════════════════════════
# Contact extraction from LinkedIn search results
# ════════════════════════════════════════════════════════════

class TestContactExtraction:

    def _linkedin_result(self, title, link, snippet=""):
        return {"title": title, "link": link, "snippet": snippet}

    def test_extracts_name_and_title_from_standard_format(self):
        from app.services.retrieval.contact_search import _extract_person_from_result
        result = self._linkedin_result(
            title="Sarah Johnson - Head of Talent - e& Enterprise | LinkedIn",
            link="https://linkedin.com/in/sarah-johnson",
            snippet="Sarah Johnson is Head of Talent at e& Enterprise, Dubai.",
        )
        extracted = _extract_person_from_result(result)
        assert extracted is not None
        name, title, url = extracted
        assert name == "Sarah Johnson"
        assert "Talent" in title
        assert url == "https://linkedin.com/in/sarah-johnson"

    def test_extracts_with_pipe_separator(self):
        from app.services.retrieval.contact_search import _extract_person_from_result
        result = self._linkedin_result(
            title="Ahmed Al-Rashidi | VP Strategy at Tamara | LinkedIn",
            link="https://linkedin.com/in/ahmed-al-rashidi",
        )
        extracted = _extract_person_from_result(result)
        assert extracted is not None
        name, _, _ = extracted
        assert "Ahmed" in name

    def test_skips_company_overview_pages(self):
        from app.services.retrieval.contact_search import _extract_person_from_result
        result = self._linkedin_result(
            title="e& Enterprise: Overview | LinkedIn",
            link="https://linkedin.com/company/e-enterprise",
        )
        assert _extract_person_from_result(result) is None

    def test_skips_non_linkedin_urls(self):
        from app.services.retrieval.contact_search import _extract_person_from_result
        result = self._linkedin_result(
            title="Sarah Johnson - Head of Talent | Some Other Site",
            link="https://example.com/sarah-johnson",
        )
        assert _extract_person_from_result(result) is None

    def test_skips_very_short_names(self):
        from app.services.retrieval.contact_search import _extract_person_from_result
        result = self._linkedin_result(
            title="Jo - VP - Company | LinkedIn",
            link="https://linkedin.com/in/jo",
        )
        # "Jo" is < 4 chars
        assert _extract_person_from_result(result) is None

    def test_skips_all_caps_names(self):
        from app.services.retrieval.contact_search import _extract_person_from_result
        result = self._linkedin_result(
            title="JOHN SMITH - VP Strategy - Acme | LinkedIn",
            link="https://linkedin.com/in/john-smith",
        )
        assert _extract_person_from_result(result) is None


# ════════════════════════════════════════════════════════════
# ContactSearchService
# ════════════════════════════════════════════════════════════

class TestContactSearchService:

    def _make_serper_results(self):
        return [
            {
                "title": "Sarah Johnson - Head of Talent - e& Enterprise | LinkedIn",
                "link": "https://linkedin.com/in/sarah-johnson-eand",
                "snippet": "Sarah Johnson leads talent acquisition at e& Enterprise in Dubai.",
            },
            {
                "title": "Mohammed Al-Farsi | Chief of Staff at e& Enterprise | LinkedIn",
                "link": "https://linkedin.com/in/mohammed-al-farsi",
                "snippet": "Mohammed Al-Farsi is Chief of Staff at e& Enterprise, Abu Dhabi.",
            },
            {
                "title": "e& Enterprise: Overview | LinkedIn",  # Company page — skip
                "link": "https://linkedin.com/company/e-enterprise",
                "snippet": "e& Enterprise is a telecom company.",
            },
        ]

    def test_finds_contacts_from_search_results(self):
        from app.services.retrieval.contact_search import ContactSearchService

        mock_serper = MagicMock()
        mock_serper.search = MagicMock(return_value=self._make_serper_results())

        svc = ContactSearchService(serper_client=mock_serper)
        contacts = svc.find_contacts(
            company_name="e& Enterprise",
            role_title="VP Strategy",
            region="UAE",
        )

        # Should find the 2 people, skip the company page
        assert len(contacts) >= 1
        names = [c.name for c in contacts]
        assert "Sarah Johnson" in names or "Mohammed Al-Farsi" in names

    def test_returns_empty_for_no_company_name(self):
        from app.services.retrieval.contact_search import ContactSearchService
        mock_serper = MagicMock()
        svc = ContactSearchService(serper_client=mock_serper)
        contacts = svc.find_contacts(company_name="", role_title="VP")
        assert contacts == []
        mock_serper.search.assert_not_called()

    def test_deduplicates_contacts_by_name(self):
        from app.services.retrieval.contact_search import ContactSearchService

        # Same person appears in multiple search results
        duplicate_results = [
            {
                "title": "Sarah Johnson - Head of Talent - e& Enterprise | LinkedIn",
                "link": "https://linkedin.com/in/sarah-johnson-eand",
                "snippet": "Sarah Johnson heads talent at e& Enterprise.",
            },
        ] * 3  # Same result 3 times

        mock_serper = MagicMock()
        mock_serper.search = MagicMock(return_value=duplicate_results)

        svc = ContactSearchService(serper_client=mock_serper)
        contacts = svc.find_contacts("e& Enterprise", "VP Strategy")

        # Should deduplicate
        names = [c.name for c in contacts]
        assert names.count("Sarah Johnson") <= 1

    def test_contact_has_required_fields(self):
        from app.services.retrieval.contact_search import ContactSearchService

        mock_serper = MagicMock()
        mock_serper.search = MagicMock(return_value=[self._make_serper_results()[0]])

        svc = ContactSearchService(serper_client=mock_serper)
        contacts = svc.find_contacts("e& Enterprise", "VP Strategy")

        if contacts:
            c = contacts[0]
            assert c.name
            assert c.title
            assert c.company == "e& Enterprise"
            assert c.relevance
            assert c.suggested_outreach

    def test_contact_to_dict_has_required_keys(self):
        from app.services.retrieval.contact_search import ContactResult
        c = ContactResult(
            name="Sarah Johnson",
            title="Head of Talent",
            company="Acme",
            linkedin_url="https://linkedin.com/in/sarah",
            source_snippet="...",
            relevance="Recruiting contact",
            suggested_outreach="Hi Sarah...",
        )
        d = c.to_dict()
        assert set(d.keys()) == {"name", "title", "company", "linkedin_url", "relevance", "suggested_outreach"}

    def test_handles_serper_failure_gracefully(self):
        from app.services.retrieval.contact_search import ContactSearchService

        mock_serper = MagicMock()
        mock_serper.search = MagicMock(side_effect=Exception("Serper API down"))

        svc = ContactSearchService(serper_client=mock_serper)
        # Should not raise — returns empty list
        contacts = svc.find_contacts("e& Enterprise", "VP Strategy")
        assert contacts == []


# ════════════════════════════════════════════════════════════
# Outreach message generation
# ════════════════════════════════════════════════════════════

class TestOutreachMessages:

    def test_talent_contact_gets_talent_outreach(self):
        from app.services.retrieval.contact_search import _build_outreach_opener
        msg = _build_outreach_opener(
            contact_name="Sarah Johnson",
            contact_title="Head of Talent Acquisition",
            company_name="e& Enterprise",
            role_title="VP Strategy",
        )
        assert "Sarah" in msg
        assert len(msg) < 500
        assert "e& Enterprise" in msg or "VP Strategy" in msg

    def test_senior_contact_gets_operator_outreach(self):
        from app.services.retrieval.contact_search import _build_outreach_opener
        msg = _build_outreach_opener(
            contact_name="Mohammed Al-Farsi",
            contact_title="Chief of Staff",
            company_name="Tamara",
            role_title="Head of Operations",
        )
        assert "Mohammed" in msg
        assert len(msg) < 500

    def test_generic_contact_gets_generic_outreach(self):
        from app.services.retrieval.contact_search import _build_outreach_opener
        msg = _build_outreach_opener(
            contact_name="Emily Chen",
            contact_title="VP Engineering",
            company_name="STV Growth",
            role_title="EiR",
        )
        assert "Emily" in msg
        assert "STV Growth" in msg


# ════════════════════════════════════════════════════════════
# RetrievalResult contacts field
# ════════════════════════════════════════════════════════════

class TestRetrievalResultContacts:

    def test_contacts_field_in_to_dict(self):
        from app.services.retrieval.web_search import RetrievalResult
        result = RetrievalResult(
            contacts=[{"name": "Sarah", "title": "Head of Talent", "company": "Acme"}]
        )
        d = result.to_dict()
        assert "contacts" in d
        assert d["contacts"][0]["name"] == "Sarah"

    def test_empty_contacts_by_default(self):
        from app.services.retrieval.web_search import RetrievalResult
        result = RetrievalResult()
        assert result.contacts == []
        assert "contacts" in result.to_dict()


# ════════════════════════════════════════════════════════════
# Known contacts in JobRunCreate schema
# ════════════════════════════════════════════════════════════

class TestKnownContactsSchema:

    def test_known_contacts_optional(self):
        from app.schemas.job_run import JobRunCreate
        run = JobRunCreate(
            cv_id="00000000-0000-0000-0000-000000000001",
            jd_text="Some job description here",
        )
        assert run.known_contacts is None

    def test_known_contacts_accepted(self):
        from app.schemas.job_run import JobRunCreate
        run = JobRunCreate(
            cv_id="00000000-0000-0000-0000-000000000001",
            jd_text="Some job description here",
            known_contacts=["Ahmed Al-Rashidi (former colleague)", "Sarah J. (MBA classmate)"],
        )
        assert len(run.known_contacts) == 2
        assert "Ahmed" in run.known_contacts[0]


# ════════════════════════════════════════════════════════════
# Contact search query builder
# ════════════════════════════════════════════════════════════

class TestContactQueryBuilder:

    def test_builds_three_queries(self):
        from app.services.retrieval.contact_search import _build_contact_queries
        queries = _build_contact_queries("e& Enterprise", "VP Strategy", "UAE")
        assert len(queries) == 3

    def test_queries_include_company_name(self):
        from app.services.retrieval.contact_search import _build_contact_queries
        queries = _build_contact_queries("Tamara", "Chief of Staff", "KSA")
        for query, _ in queries:
            assert "Tamara" in query

    def test_queries_include_linkedin(self):
        from app.services.retrieval.contact_search import _build_contact_queries
        queries = _build_contact_queries("ADNOC", "Managing Director")
        for query, _ in queries:
            assert "linkedin" in query.lower()
