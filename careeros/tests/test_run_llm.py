"""
tests/test_run_llm.py

Tests for the retrieval service and run_llm Celery task.

All external dependencies mocked:
  - Serper API (httpx)
  - Anthropic API (ClaudeClient)
  - DB (get_sync_db)
  - Celery task dispatch
"""

import json
import uuid
from unittest.mock import MagicMock, patch, call

import pytest

from app.services.retrieval.web_search import (
    RetrievalService,
    RetrievalResult,
    SerperClient,
    _extract_company_name,
    _build_queries,
    _format_results_as_text,
    _strip_json_fences,
)


# ── RetrievalResult tests ────────────────────────────────────────────────────

class TestRetrievalResult:
    def test_to_dict_contains_all_keys(self):
        result = RetrievalResult(
            company_overview="Revolut is a fintech.",
            salary_data="AED 45K-65K/month.",
            news=["Revolut launches UAE."],
            competitors="Wio, Liv, Zand",
            sources=["techcrunch.com"],
        )
        d = result.to_dict()
        assert "company_overview" in d
        assert "salary_data" in d
        assert "news" in d
        assert "sources" in d
        assert "partial_failure" in d

    def test_empty_factory(self):
        result = RetrievalResult.empty("no API key")
        assert result.partial_failure is True
        assert "no API key" in result.error_notes[0]
        assert result.company_overview == ""


# ── Company name extraction ──────────────────────────────────────────────────

class TestExtractCompanyName:
    def test_extracts_at_pattern(self):
        jd = "We are looking for a Senior Director at Revolut to lead..."
        name = _extract_company_name(jd)
        assert "Revolut" in name

    def test_extracts_join_pattern(self):
        jd = "Join e& as our next VP Strategy..."
        name = _extract_company_name(jd)
        assert "e&" in name

    def test_empty_jd_returns_empty(self):
        name = _extract_company_name("")
        assert name == ""

    def test_no_company_pattern_returns_something_or_empty(self):
        jd = "general job description with no clear company signal"
        name = _extract_company_name(jd)
        # Should not raise — returns empty string or best guess
        assert isinstance(name, str)


class TestBuildQueries:
    def test_all_query_types_built_with_company(self):
        queries = _build_queries(
            jd_text="Join Revolut as EiR",
            company_name="Revolut",
            role_title="Entrepreneur in Residence",
            region="UAE",
        )
        assert "company_overview" in queries
        assert "news" in queries
        assert "salary_data" in queries
        assert "competitors" in queries

    def test_no_company_skips_company_queries(self):
        queries = _build_queries(
            jd_text="generic jd",
            company_name="",
            role_title="Director",
            region="UAE",
        )
        assert "company_overview" not in queries
        assert "salary_data" in queries  # Still builds salary query

    def test_queries_contain_region(self):
        queries = _build_queries(
            jd_text="test",
            company_name="ACME",
            role_title="VP",
            region="KSA",
        )
        assert "KSA" in queries["salary_data"]


class TestFormatResults:
    def test_formats_results_with_title_and_snippet(self):
        results = [
            {"title": "Revolut raises $800M", "snippet": "Fintech unicorn...", "link": "https://techcrunch.com/article"},
        ]
        text = _format_results_as_text(results)
        assert "Revolut raises $800M" in text
        assert "Fintech unicorn" in text

    def test_empty_results_returns_empty_string(self):
        assert _format_results_as_text([]) == ""

    def test_truncated_to_max_chars(self):
        results = [
            {"title": "T" * 100, "snippet": "S" * 500, "link": "https://example.com"}
            for _ in range(20)
        ]
        text = _format_results_as_text(results, max_chars=200)
        assert len(text) <= 200


# ── SerperClient tests ───────────────────────────────────────────────────────

class TestSerperClient:
    def _make_client(self) -> SerperClient:
        return SerperClient(api_key="test-key")

    @patch("httpx.Client.post")
    def test_returns_organic_results(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "organic": [
                {"title": "Result 1", "snippet": "Snippet 1", "link": "https://a.com"},
                {"title": "Result 2", "snippet": "Snippet 2", "link": "https://b.com"},
            ]
        }
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        client = self._make_client()
        results = client.search("Revolut UAE")

        assert len(results) == 2
        assert results[0]["title"] == "Result 1"

    @patch("httpx.Client.post")
    def test_timeout_returns_empty_list(self, mock_post):
        import httpx
        mock_post.side_effect = httpx.TimeoutException("timeout")

        client = self._make_client()
        results = client.search("some query")

        assert results == []  # Never raises

    @patch("httpx.Client.post")
    def test_http_error_returns_empty_list(self, mock_post):
        import httpx
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_post.side_effect = httpx.HTTPStatusError(
            "Forbidden",
            request=MagicMock(),
            response=mock_response,
        )

        client = self._make_client()
        results = client.search("some query")

        assert results == []


# ── RetrievalService integration tests ──────────────────────────────────────

class TestRetrievalService:
    def _make_mock_serper(self, results: list[dict] | None = None) -> MagicMock:
        mock = MagicMock(spec=SerperClient)
        mock.search.return_value = results or [
            {"title": "Test result", "snippet": "Test snippet", "link": "https://example.com"}
        ]
        return mock

    def test_retrieve_with_serper_returns_result(self):
        mock_serper = self._make_mock_serper()
        service = RetrievalService(serper_client=mock_serper)

        result = service.retrieve(
            jd_text="Join Revolut as Entrepreneur in Residence in UAE",
            role_title="Entrepreneur in Residence",
            region="UAE",
        )

        assert isinstance(result, RetrievalResult)
        assert mock_serper.search.call_count >= 1

    def test_retrieve_without_serper_returns_empty(self):
        """No API key configured → graceful empty result."""
        service = RetrievalService(serper_client=None)
        # Ensure settings has no key for this test
        with patch.object(service, "_serper", None):
            result = service.retrieve(jd_text="test jd")

        assert result.partial_failure is True

    def test_serper_failure_returns_partial_result(self):
        """If one query fails, others should still succeed."""
        mock_serper = MagicMock(spec=SerperClient)

        call_count = [0]
        def side_effect(query, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Network error on second query")
            return [{"title": "OK", "snippet": "ok", "link": "https://ok.com"}]

        mock_serper.search.side_effect = side_effect

        service = RetrievalService(serper_client=mock_serper)
        result = service.retrieve(
            jd_text="Join Revolut as EiR in UAE",
            role_title="EiR",
            region="UAE",
        )

        # Should have partial failure noted but not crash
        assert result.partial_failure is True
        assert len(result.error_notes) >= 1


# ── run_llm task tests ───────────────────────────────────────────────────────

class TestRunLLMTask:
    """
    Tests for the run_llm Celery task.
    All external deps mocked.
    """

    def _make_mock_job_run(self):
        from app.models.job_run import JobRunStatus
        job_run = MagicMock()
        job_run.id = uuid.uuid4()
        job_run.cv_id = uuid.uuid4()
        job_run.status = JobRunStatus.RETRIEVING
        job_run.jd_text = "We are looking for an EiR at Revolut UAE."
        job_run.jd_url = None
        job_run.preferences = {"tone": "human", "region": "UAE"}
        job_run.retrieval_data = None
        job_run.edit_plan = None
        job_run.reports = None
        job_run.celery_task_id = None
        job_run.is_terminal = False
        return job_run

    def _make_mock_cv(self):
        from app.models.cv import CVStatus
        from app.schemas.cv import ParsedCV

        cv = MagicMock()
        cv.id = uuid.uuid4()
        cv.status = CVStatus.PARSED
        cv.parsed_content = ParsedCV(
            total_paragraphs=5,
            total_words=100,
            sections=[],
            raw_paragraphs=[],
        ).model_dump()
        return cv

    def test_invalid_uuid_raises(self):
        from app.workers.tasks.run_llm import run_llm_task
        with pytest.raises(ValueError):
            run_llm_task("not-a-uuid")

    @patch("app.workers.tasks.run_llm.render_docx_task")
    @patch("app.workers.tasks.run_llm._run_llm_calls")
    @patch("app.workers.tasks.run_llm._run_retrieval")
    @patch("app.workers.tasks.run_llm._complete_job_step")
    @patch("app.workers.tasks.run_llm._create_job_step")
    @patch("app.workers.tasks.run_llm.get_sync_db")
    def test_successful_run_dispatches_render(
        self,
        mock_get_db,
        mock_create_step,
        mock_complete_step,
        mock_retrieval,
        mock_llm_calls,
        mock_render_task,
    ):
        """Happy path: both LLM calls succeed, render_docx task dispatched."""
        job_run = self._make_mock_job_run()
        cv = self._make_mock_cv()

        mock_db = MagicMock()
        mock_db.get.side_effect = lambda model, id: (
            job_run if model.__name__ == "JobRun" else cv
        )
        mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)

        mock_create_step.return_value = uuid.uuid4()
        mock_retrieval.return_value = {"sources": [], "partial_failure": False}
        mock_llm_calls.return_value = (
            {"keyword_match_score": 78, "paragraph_edits": []},
            {"company": {"company_name": "Revolut"}, "exec_summary": []},
            {"total_tokens": 5000, "total_cost_usd": 0.15},
        )

        from app.workers.tasks.run_llm import run_llm_task
        result = run_llm_task(str(job_run.id))

        assert result["status"] == "llm_complete_rendering_dispatched"
        assert result["keyword_match_score"] == 78
        mock_render_task.delay.assert_called_once_with(str(job_run.id))

    @patch("app.workers.tasks.run_llm._mark_run_failed")
    @patch("app.workers.tasks.run_llm._fail_job_step")
    @patch("app.workers.tasks.run_llm._complete_job_step")
    @patch("app.workers.tasks.run_llm._create_job_step")
    @patch("app.workers.tasks.run_llm._run_retrieval")
    @patch("app.workers.tasks.run_llm.get_sync_db")
    def test_retrieval_failure_continues_with_empty_data(
        self,
        mock_get_db,
        mock_retrieval,
        mock_create_step,
        mock_complete_step,
        mock_fail_step,
        mock_mark_failed,
    ):
        """Retrieval failure should not abort the whole run — LLM still called."""
        job_run = self._make_mock_job_run()
        cv = self._make_mock_cv()

        mock_db = MagicMock()
        mock_db.get.side_effect = lambda model, id: (
            job_run if model.__name__ == "JobRun" else cv
        )
        mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_db)
        mock_get_db.return_value.__exit__ = MagicMock(return_value=False)

        mock_create_step.return_value = uuid.uuid4()
        mock_retrieval.side_effect = Exception("Serper API down")

        # Patch LLM calls to succeed after retrieval failure
        with patch("app.workers.tasks.run_llm._run_llm_calls") as mock_llm, \
             patch("app.workers.tasks.run_llm.render_docx_task") as mock_render:

            mock_llm.return_value = (
                {"keyword_match_score": 70, "paragraph_edits": []},
                {"company": {}, "exec_summary": []},
                {"total_tokens": 4000, "total_cost_usd": 0.10},
            )

            from app.workers.tasks.run_llm import run_llm_task
            result = run_llm_task(str(job_run.id))

            # LLM should still have been called even though retrieval failed
            mock_llm.assert_called_once()
            # render_docx should have been dispatched
            mock_render.delay.assert_called_once()
            # run should not be marked as failed
            mock_mark_failed.assert_not_called()
