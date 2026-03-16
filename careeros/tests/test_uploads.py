"""
tests/test_uploads.py

Tests for the CV upload endpoint and storage service.

Strategy:
- Mock S3StorageService so tests never touch real AWS/MinIO
- Mock Celery task dispatch so tests don't need a running broker
- Use an in-memory SQLite DB for fast, isolated DB tests
- Test validation logic without any infrastructure

Run with: pytest tests/test_uploads.py -v
"""

import io
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient

from app.main import app
from app.models.cv import CVStatus
from app.services.ingest.storage import (
    MAX_FILE_SIZE_BYTES,
    validate_cv_file,
    build_cv_s3_key,
    compute_md5,
)
from app.api.middleware.error_handler import ValidationError


# ── Fixtures ────────────────────────────────────────────────────────────────

VALID_DOCX_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)
VALID_PDF_CONTENT_TYPE = "application/pdf"

# Minimal valid DOCX bytes (just needs to not be empty for upload tests)
FAKE_DOCX_BYTES = b"PK" + b"\x00" * 100  # DOCX is a ZIP — starts with PK


def make_fake_cv_response():
    """Build a fake CVUploadResponse for mocking service layer."""
    return {
        "id": str(uuid.uuid4()),
        "status": CVStatus.UPLOADED,
        "original_filename": "test_cv.docx",
        "file_size_bytes": len(FAKE_DOCX_BYTES),
        "mime_type": VALID_DOCX_CONTENT_TYPE,
        "created_at": datetime.now(UTC).isoformat(),
    }


# ── Storage utility tests (no infrastructure needed) ───────────────────────

class TestValidateCVFile:
    """Unit tests for file validation — pure logic, no I/O."""

    def test_valid_docx_passes(self):
        validate_cv_file(
            filename="my_cv.docx",
            content_type=VALID_DOCX_CONTENT_TYPE,
            file_size=1024,
        )  # Should not raise

    def test_valid_pdf_passes(self):
        validate_cv_file(
            filename="my_cv.pdf",
            content_type=VALID_PDF_CONTENT_TYPE,
            file_size=1024,
        )  # Should not raise

    def test_invalid_extension_raises(self):
        with pytest.raises(ValidationError, match="Unsupported file type"):
            validate_cv_file(
                filename="cv.txt",
                content_type="text/plain",
                file_size=100,
            )

    def test_invalid_mime_type_raises(self):
        with pytest.raises(ValidationError, match="Unsupported MIME type"):
            validate_cv_file(
                filename="cv.docx",
                content_type="application/octet-stream",
                file_size=100,
            )

    def test_file_too_large_raises(self):
        with pytest.raises(ValidationError, match="too large"):
            validate_cv_file(
                filename="cv.docx",
                content_type=VALID_DOCX_CONTENT_TYPE,
                file_size=MAX_FILE_SIZE_BYTES + 1,
            )

    def test_empty_file_raises(self):
        with pytest.raises(ValidationError, match="empty"):
            validate_cv_file(
                filename="cv.docx",
                content_type=VALID_DOCX_CONTENT_TYPE,
                file_size=0,
            )

    def test_exact_max_size_passes(self):
        validate_cv_file(
            filename="cv.docx",
            content_type=VALID_DOCX_CONTENT_TYPE,
            file_size=MAX_FILE_SIZE_BYTES,
        )  # Should not raise


class TestS3KeyBuilders:
    """Unit tests for S3 key path builders."""

    def test_cv_key_structure(self):
        user_id = "user_123"
        cv_id = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
        filename = "my resume.docx"

        key = build_cv_s3_key(user_id, cv_id, filename)

        assert key.startswith("cvs/user_123/")
        assert str(cv_id) in key
        assert "my resume.docx" in key

    def test_cv_key_prevents_path_traversal(self):
        """Filenames with path separators must be sanitised."""
        key = build_cv_s3_key(
            user_id="user_123",
            cv_id=uuid.uuid4(),
            filename="../../../etc/passwd",
        )
        # Path.name strips the directory components
        assert "etc/passwd" not in key
        assert key.endswith("passwd")

    def test_md5_is_deterministic(self):
        data = b"hello world"
        assert compute_md5(data) == compute_md5(data)
        assert compute_md5(data) != compute_md5(b"different")


# ── S3StorageService unit tests ─────────────────────────────────────────────

class TestS3StorageService:
    """Tests for S3 operations with a mocked boto3 client."""

    def _make_service(self):
        from app.services.ingest.storage import S3StorageService
        mock_client = MagicMock()
        return S3StorageService(client=mock_client), mock_client

    def test_upload_bytes_calls_upload_fileobj(self):
        service, mock_client = self._make_service()
        service.upload_bytes(
            data=b"fake docx content",
            s3_key="cvs/user/123/cv.docx",
            content_type=VALID_DOCX_CONTENT_TYPE,
        )
        mock_client.upload_fileobj.assert_called_once()

    def test_upload_failure_raises_storage_error(self):
        from botocore.exceptions import ClientError
        from app.api.middleware.error_handler import StorageError

        service, mock_client = self._make_service()
        mock_client.upload_fileobj.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "Internal Error"}},
            "upload_fileobj",
        )

        with pytest.raises(StorageError, match="Failed to upload"):
            service.upload_bytes(
                data=b"data",
                s3_key="cvs/test/cv.docx",
                content_type=VALID_DOCX_CONTENT_TYPE,
            )

    def test_generate_presigned_url_returns_string(self):
        service, mock_client = self._make_service()
        mock_client.generate_presigned_url.return_value = (
            "https://s3.amazonaws.com/bucket/key?X-Amz-Signature=abc"
        )

        url = service.generate_presigned_url("outputs/user/run/output.docx")

        assert url.startswith("https://")
        mock_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": service.bucket, "Key": "outputs/user/run/output.docx"},
            ExpiresIn=3600,
        )

    def test_object_exists_returns_true_when_found(self):
        service, mock_client = self._make_service()
        mock_client.head_object.return_value = {"ContentLength": 1024}

        assert service.object_exists("cvs/user/cv.docx") is True

    def test_object_exists_returns_false_when_404(self):
        from botocore.exceptions import ClientError

        service, mock_client = self._make_service()
        mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "head_object",
        )

        assert service.object_exists("cvs/missing.docx") is False


# ── API endpoint integration tests ──────────────────────────────────────────

class TestUploadEndpoint:
    """
    Integration tests for POST /api/v1/cvs.

    S3 and Celery are mocked — we test the HTTP layer + service wiring.
    DB session is also mocked to avoid needing a real PostgreSQL instance.
    """

    def _get_client(self) -> TestClient:
        return TestClient(app, raise_server_exceptions=True)

    @patch("app.services.ingest.cv_service.CVService.upload_cv")
    def test_upload_valid_docx_returns_202(self, mock_upload_cv):
        """Valid DOCX upload returns 202 with CV id."""
        from app.schemas.cv import CVUploadResponse

        fake_response = CVUploadResponse(**make_fake_cv_response())
        mock_upload_cv.return_value = fake_response

        client = self._get_client()
        response = client.post(
            "/api/v1/cvs",
            files={
                "file": ("test_cv.docx", FAKE_DOCX_BYTES, VALID_DOCX_CONTENT_TYPE)
            },
            headers={"X-User-Id": "test_user_123"},
        )

        assert response.status_code == 202
        data = response.json()
        assert "id" in data
        assert data["status"] == CVStatus.UPLOADED

    def test_upload_missing_user_id_returns_422(self):
        """Request without X-User-Id header should fail validation."""
        client = self._get_client()
        response = client.post(
            "/api/v1/cvs",
            files={
                "file": ("test_cv.docx", FAKE_DOCX_BYTES, VALID_DOCX_CONTENT_TYPE)
            },
            # No X-User-Id header
        )
        assert response.status_code == 422

    def test_upload_no_file_returns_422(self):
        """Request without a file should fail validation."""
        client = self._get_client()
        response = client.post(
            "/api/v1/cvs",
            headers={"X-User-Id": "test_user_123"},
        )
        assert response.status_code == 422

    @patch("app.services.ingest.cv_service.CVService.get_cv_status")
    def test_get_cv_status_returns_200(self, mock_get_status):
        """GET /api/v1/cvs/{id} returns status for known CV."""
        from app.schemas.cv import CVStatusResponse

        cv_id = uuid.uuid4()
        mock_get_status.return_value = CVStatusResponse(
            id=cv_id,
            status=CVStatus.PARSED,
            parsed_section_count=4,
            parsed_word_count=850,
        )

        client = self._get_client()
        response = client.get(
            f"/api/v1/cvs/{cv_id}",
            headers={"X-User-Id": "test_user_123"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == CVStatus.PARSED
        assert data["parsed_section_count"] == 4

    @patch("app.services.ingest.cv_service.CVService.get_cv_status")
    def test_get_cv_status_not_found_returns_404(self, mock_get_status):
        """GET /api/v1/cvs/{id} returns 404 for unknown CV."""
        from app.api.middleware.error_handler import NotFoundError

        mock_get_status.side_effect = NotFoundError(
            resource="CV",
            resource_id=str(uuid.uuid4()),
        )

        client = self._get_client()
        response = client.get(
            f"/api/v1/cvs/{uuid.uuid4()}",
            headers={"X-User-Id": "test_user_123"},
        )

        assert response.status_code == 404
        assert "not found" in response.json()["error"].lower()
