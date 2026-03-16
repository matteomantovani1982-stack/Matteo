"""
tests/test_sprint_h_i.py

Sprint H: TemplateRenderer
Sprint I: BestPracticesService, JDExtractor
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ════════════════════════════════════════════════════════════
# SPRINT H — TemplateRenderer
# ════════════════════════════════════════════════════════════

class TestTemplateRenderer:

    def _make_built_cv(self, include_all_sections=True):
        cv = {
            "name": "Matteo Mantovani",
            "headline": "Operator & Strategist | MENA",
            "contact": {
                "email": "m@example.com",
                "phone": "+971 50 000 0000",
                "location": "Dubai, UAE",
                "linkedin": "linkedin.com/in/matteo",
            },
            "summary": "Zero-to-scale operator with 10 years building digital businesses across MENA.",
            "sections": [],
        }
        if include_all_sections:
            cv["sections"] = [
                {
                    "section_type": "experience",
                    "title": "Professional Experience",
                    "entries": [
                        {
                            "role": "Co-Founder & CEO",
                            "company": "Baly",
                            "location": "Baghdad, Iraq",
                            "start_date": "Jan 2021",
                            "end_date": "Dec 2023",
                            "bullets": [
                                "Built Iraq's first on-demand delivery platform from zero to 500k orders/month.",
                                "Raised $10M Series A; led all functions across product, ops, and growth.",
                                "Grew team from 3 to 500 in 18 months across 4 cities.",
                            ],
                        }
                    ],
                },
                {
                    "section_type": "education",
                    "title": "Education",
                    "entries": [
                        {
                            "degree": "BA Economics",
                            "institution": "Bocconi University",
                            "location": "Milan, Italy",
                            "year": "2012",
                        }
                    ],
                },
                {
                    "section_type": "skills",
                    "title": "Skills",
                    "categories": [
                        {"label": "Leadership", "items": ["P&L ownership", "Team building", "OKRs"]},
                        {"label": "Technical", "items": ["SQL", "Python basics", "Excel"]},
                    ],
                },
            ]
        return cv

    def test_render_generated_returns_bytes(self):
        from app.services.rendering.template_renderer import TemplateRenderer
        renderer = TemplateRenderer()
        result = renderer.render(built_cv=self._make_built_cv(), template_bytes=None)
        assert isinstance(result.docx_bytes, bytes)
        assert len(result.docx_bytes) > 1000
        assert result.mode == "generated"

    def test_render_generated_counts_sections(self):
        from app.services.rendering.template_renderer import TemplateRenderer
        renderer = TemplateRenderer()
        result = renderer.render(built_cv=self._make_built_cv(), template_bytes=None)
        assert result.sections_written >= 3  # experience + education + skills

    def test_render_generated_counts_bullets(self):
        from app.services.rendering.template_renderer import TemplateRenderer
        renderer = TemplateRenderer()
        result = renderer.render(built_cv=self._make_built_cv(), template_bytes=None)
        assert result.bullets_written == 3

    def test_render_generated_no_sections_still_works(self):
        from app.services.rendering.template_renderer import TemplateRenderer
        renderer = TemplateRenderer()
        cv = self._make_built_cv(include_all_sections=False)
        result = renderer.render(built_cv=cv, template_bytes=None)
        assert isinstance(result.docx_bytes, bytes)
        assert result.mode == "generated"

    def test_render_falls_back_to_generated_on_bad_template(self):
        from app.services.rendering.template_renderer import TemplateRenderer
        renderer = TemplateRenderer()
        bad_bytes = b"this is not a valid docx file"
        result = renderer.render(
            built_cv=self._make_built_cv(),
            template_bytes=bad_bytes,
            template_slug="modern",
        )
        assert result.mode == "generated"
        assert any("Template rendering failed" in w for w in result.warnings)

    def test_format_contact_line(self):
        from app.services.rendering.template_renderer import _format_contact_line
        contact = {
            "email": "test@example.com",
            "phone": "+971 50 000",
            "location": "Dubai, UAE",
        }
        line = _format_contact_line(contact)
        assert "test@example.com" in line
        assert "Dubai, UAE" in line
        assert "·" in line

    def test_format_contact_line_partial(self):
        from app.services.rendering.template_renderer import _format_contact_line
        line = _format_contact_line({"email": "a@b.com"})
        assert line == "a@b.com"

    def test_format_contact_line_empty(self):
        from app.services.rendering.template_renderer import _format_contact_line
        assert _format_contact_line({}) == ""

    def test_render_result_metadata(self):
        from app.services.rendering.template_renderer import TemplateRenderer
        renderer = TemplateRenderer()
        result = renderer.render(built_cv=self._make_built_cv())
        meta = result.to_metadata()
        assert "sections_written" in meta
        assert "bullets_written" in meta
        assert "mode" in meta
        assert "warnings" in meta


# ════════════════════════════════════════════════════════════
# SPRINT I — BestPracticesService
# ════════════════════════════════════════════════════════════

class TestBestPracticesService:

    def _make_parsed_cv(self):
        mock = MagicMock()
        mock.sections = [
            {
                "heading": "Experience",
                "paragraphs": [
                    {"text": "Responsible for managing team operations.", "runs": [], "style": ""},
                    {"text": "Was involved in various projects.", "runs": [], "style": ""},
                ],
            }
        ]
        return mock

    def _make_response(self, suggestions=None):
        return json.dumps({
            "suggestions": suggestions or [
                {
                    "priority": "high",
                    "category": "impact",
                    "title": "Bullets describe duties, not achievements",
                    "detail": "'Responsible for managing team operations' — rewrite with outcome.",
                    "example_fix": "Led 8-person ops team, reducing delivery time by 30%.",
                },
                {
                    "priority": "medium",
                    "category": "grammar",
                    "title": "Passive voice throughout",
                    "detail": "'Was involved in' — use active voice and name your contribution.",
                    "example_fix": "Delivered X by doing Y.",
                },
            ],
            "top_strength": "Clear career progression from consulting to operator.",
            "summary": "Strong profile but bullets need to lead with outcomes, not duties.",
        })

    def test_analyse_returns_suggestions(self):
        from app.services.cv.best_practices_service import BestPracticesService
        svc = BestPracticesService()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.input_tokens = 300
        mock_result.output_tokens = 200
        mock_client.call_text = MagicMock(return_value=(self._make_response(), mock_result))

        with patch("app.services.cv.best_practices_service.ClaudeClient", return_value=mock_client):
            result = svc.analyse(self._make_parsed_cv())

        assert len(result["suggestions"]) == 2
        assert result["suggestions"][0]["priority"] == "high"
        assert result["suggestions"][0]["category"] == "impact"
        assert result["top_strength"] != ""
        assert result["summary"] != ""

    def test_analyse_validates_priority_field(self):
        from app.services.cv.best_practices_service import BestPracticesService
        svc = BestPracticesService()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.input_tokens = 300
        mock_result.output_tokens = 200
        bad_suggestions = [{"priority": "URGENT", "category": "impact", "title": "X", "detail": "Y"}]
        mock_client.call_text = MagicMock(return_value=(self._make_response(bad_suggestions), mock_result))

        with patch("app.services.cv.best_practices_service.ClaudeClient", return_value=mock_client):
            result = svc.analyse(self._make_parsed_cv())

        # Invalid priority clamped to "medium"
        assert result["suggestions"][0]["priority"] == "medium"

    def test_analyse_returns_safe_default_on_failure(self):
        from app.services.cv.best_practices_service import BestPracticesService
        svc = BestPracticesService()
        mock_client = MagicMock()
        mock_client.call_text = MagicMock(side_effect=Exception("LLM timeout"))

        with patch("app.services.cv.best_practices_service.ClaudeClient", return_value=mock_client):
            result = svc.analyse(self._make_parsed_cv())

        assert result["suggestions"] == []
        assert "error" in result

    def test_analyse_strips_markdown_fences(self):
        from app.services.cv.best_practices_service import BestPracticesService
        svc = BestPracticesService()
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.input_tokens = 300
        mock_result.output_tokens = 200
        wrapped = "```json\n" + self._make_response() + "\n```"
        mock_client.call_text = MagicMock(return_value=(wrapped, mock_result))

        with patch("app.services.cv.best_practices_service.ClaudeClient", return_value=mock_client):
            result = svc.analyse(self._make_parsed_cv())

        assert len(result["suggestions"]) == 2


# ════════════════════════════════════════════════════════════
# SPRINT I — JDExtractor
# ════════════════════════════════════════════════════════════

class TestJDExtractor:

    SAMPLE_HTML = """
    <html>
    <head><title>VP Strategy - e& Enterprise</title></head>
    <body>
    <nav><a href="/">Home</a><a href="/jobs">Jobs</a></nav>
    <script>var x = 1;</script>
    <div class="job-posting">
      <h1>VP Strategy &amp; Operations</h1>
      <p>e&amp; Enterprise is looking for a VP Strategy.</p>
      <h2>About the role</h2>
      <p>Lead strategic planning across our digital ventures portfolio.</p>
      <h2>Requirements</h2>
      <ul>
        <li>10+ years strategy or operations experience</li>
        <li>MENA market knowledge</li>
        <li>MBA preferred</li>
      </ul>
    </div>
    <footer>Apply now &copy; 2026</footer>
    </body>
    </html>
    """

    def test_strip_html_removes_script_tags(self):
        from app.services.jd.extractor import _strip_html
        result = _strip_html(self.SAMPLE_HTML)
        assert "var x = 1" not in result
        assert "VP Strategy" in result

    def test_strip_html_removes_nav_footer(self):
        from app.services.jd.extractor import _strip_html
        result = _strip_html(self.SAMPLE_HTML)
        assert "Home</a>" not in result

    def test_strip_html_decodes_entities(self):
        from app.services.jd.extractor import _strip_html
        result = _strip_html(self.SAMPLE_HTML)
        assert "&amp;" not in result
        assert "e& Enterprise" in result

    def test_strip_html_preserves_content(self):
        from app.services.jd.extractor import _strip_html
        result = _strip_html(self.SAMPLE_HTML)
        assert "10+ years" in result
        assert "MENA market knowledge" in result

    @pytest.mark.asyncio
    async def test_extract_success(self):
        from app.services.jd.extractor import JDExtractor

        mock_response = MagicMock()
        mock_response.text = self.SAMPLE_HTML
        mock_response.raise_for_status = MagicMock()

        mock_client_instance = AsyncMock()
        mock_client_instance.get = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        extracted_jd = "VP Strategy & Operations\n\nLead strategic planning..."
        mock_llm_client = MagicMock()
        mock_llm_result = MagicMock()
        mock_llm_result.input_tokens = 200
        mock_llm_result.output_tokens = 100
        mock_llm_client.call_text = MagicMock(return_value=(extracted_jd, mock_llm_result))

        with patch("app.services.jd.extractor.httpx.AsyncClient", return_value=mock_client_instance):
            with patch("app.services.jd.extractor.ClaudeClient", return_value=mock_llm_client):
                extractor = JDExtractor()
                result = await extractor.extract("https://example.com/jobs/123")

        assert "VP Strategy" in result
        assert len(result) > 10

    @pytest.mark.asyncio
    async def test_extract_raises_on_403(self):
        import httpx
        from app.services.jd.extractor import JDExtractor, JDExtractionError

        mock_response = MagicMock()
        mock_response.status_code = 403
        error = httpx.HTTPStatusError("403", request=MagicMock(), response=mock_response)

        mock_client_instance = AsyncMock()
        mock_client_instance.get = AsyncMock(side_effect=error)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        with patch("app.services.jd.extractor.httpx.AsyncClient", return_value=mock_client_instance):
            extractor = JDExtractor()
            with pytest.raises(JDExtractionError) as exc_info:
                await extractor.extract("https://linkedin.com/jobs/view/123")

        assert "403" in str(exc_info.value) or "blocked" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_extract_raises_when_no_jd_found(self):
        from app.services.jd.extractor import JDExtractor, JDExtractionError

        mock_response = MagicMock()
        mock_response.text = "<html><body>Page not found</body></html>"
        mock_response.raise_for_status = MagicMock()

        mock_client_instance = AsyncMock()
        mock_client_instance.get = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_llm_client = MagicMock()
        mock_llm_result = MagicMock()
        mock_llm_result.input_tokens = 50
        mock_llm_result.output_tokens = 10
        mock_llm_client.call_text = MagicMock(return_value=("NO_JD_FOUND", mock_llm_result))

        with patch("app.services.jd.extractor.httpx.AsyncClient", return_value=mock_client_instance):
            with patch("app.services.jd.extractor.ClaudeClient", return_value=mock_llm_client):
                extractor = JDExtractor()
                with pytest.raises(JDExtractionError) as exc_info:
                    await extractor.extract("https://example.com/not-a-job")

        assert "No job description found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_extract_prepends_https_if_missing(self):
        from app.services.jd.extractor import JDExtractor

        mock_response = MagicMock()
        mock_response.text = self.SAMPLE_HTML
        mock_response.raise_for_status = MagicMock()

        calls = []
        mock_client_instance = AsyncMock()
        async def capture_get(url, **kwargs):
            calls.append(url)
            return mock_response
        mock_client_instance.get = capture_get
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        mock_llm_client = MagicMock()
        mock_llm_result = MagicMock()
        mock_llm_result.input_tokens = 200
        mock_llm_result.output_tokens = 100
        mock_llm_client.call_text = MagicMock(return_value=("VP Strategy role", mock_llm_result))

        with patch("app.services.jd.extractor.httpx.AsyncClient", return_value=mock_client_instance):
            with patch("app.services.jd.extractor.ClaudeClient", return_value=mock_llm_client):
                extractor = JDExtractor()
                await extractor.extract("example.com/jobs/123")  # No https://

        assert calls[0].startswith("https://")
