"""
app/services/ingest/storage.py

S3 storage service for CV files and rendered DOCX outputs.

Responsibilities:
- Upload raw CV files to S3
- Generate pre-signed download URLs for outputs
- Build consistent S3 key paths
- Handle S3 errors with typed exceptions

This module is intentionally synchronous (boto3 is sync).
Called from Celery workers (sync context) and from the upload
route via run_in_executor to avoid blocking the event loop.

Never import FastAPI or SQLAlchemy here — pure storage logic only.
"""

import hashlib
import uuid
from datetime import datetime, UTC
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from app.api.middleware.error_handler import StorageError
from app.config import settings

import structlog

logger = structlog.get_logger(__name__)


# ── S3 key path builders ────────────────────────────────────────────────────

def build_cv_s3_key(user_id: str, cv_id: uuid.UUID, filename: str) -> str:
    """
    Builds a deterministic S3 key for a raw CV upload.

    Pattern: cvs/{user_id}/{cv_id}/{original_filename}

    Example: cvs/user_abc123/550e8400-e29b-41d4-a716/cv_matteo.docx

    Why include cv_id: ensures uniqueness even if same user uploads
    the same filename twice.
    """
    safe_filename = Path(filename).name  # strip any path traversal attempts
    return f"cvs/{user_id}/{cv_id}/{safe_filename}"


def build_output_s3_key(user_id: str, job_run_id: uuid.UUID, filename: str) -> str:
    """
    Builds S3 key for a rendered output DOCX.

    Pattern: outputs/{user_id}/{job_run_id}/{filename}
    """
    return f"outputs/{user_id}/{job_run_id}/{filename}"


# ── Storage client ──────────────────────────────────────────────────────────

class S3StorageService:
    """
    Thin wrapper around boto3 S3 client.

    Instantiate once per request (injected via dependency).
    All methods raise StorageError on failure — never raw boto3 exceptions.
    """

    def __init__(self, client=None) -> None:
        """
        Accept an injected boto3 client, or create one from settings.
        Accepting an injected client makes this testable without AWS.
        """
        if client is not None:
            self._client = client
        else:
            kwargs: dict = {
                "region_name": settings.s3_region,
                "aws_access_key_id": settings.s3_access_key_id,
                "aws_secret_access_key": settings.s3_secret_access_key,
            }
            if settings.s3_endpoint_url:
                kwargs["endpoint_url"] = settings.s3_endpoint_url
            self._client = boto3.client("s3", **kwargs)

        self.bucket = settings.s3_bucket_name

    def upload_fileobj(
        self,
        file_obj,
        s3_key: str,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """
        Upload a file-like object to S3.

        Args:
            file_obj:     Any file-like object with a .read() method
            s3_key:       Destination S3 key (path within bucket)
            content_type: MIME type stored as S3 object metadata
            metadata:     Optional extra key/value metadata dict

        Returns:
            The s3_key on success (for storing in DB)

        Raises:
            StorageError: on any S3 or network failure
        """
        extra_args: dict = {
            "ContentType": content_type,
        }
        if metadata:
            extra_args["Metadata"] = metadata

        try:
            logger.info(
                "s3_upload_start",
                extra={"bucket": self.bucket, "key": s3_key},
            )
            self._client.upload_fileobj(
                file_obj,
                self.bucket,
                s3_key,
                ExtraArgs=extra_args,
            )
            logger.info(
                "s3_upload_complete",
                extra={"bucket": self.bucket, "key": s3_key},
            )
            return s3_key

        except (BotoCoreError, ClientError) as e:
            logger.error(
                "s3_upload_failed",
                extra={"bucket": self.bucket, "key": s3_key, "error": str(e)},
            )
            raise StorageError(f"Failed to upload file to S3: {e}") from e

    def upload_bytes(
        self,
        data: bytes,
        s3_key: str,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """
        Upload raw bytes to S3.
        Convenience wrapper around upload_fileobj for in-memory data.
        """
        import io
        return self.upload_fileobj(
            io.BytesIO(data),
            s3_key,
            content_type,
            metadata,
        )

    def download_bytes(self, s3_key: str) -> bytes:
        """
        Download an S3 object and return its content as bytes.

        Used by the DOCX parser to read the original CV from S3.

        Raises:
            StorageError: if the object doesn't exist or download fails
        """
        try:
            logger.info(
                "s3_download_start",
                extra={"bucket": self.bucket, "key": s3_key},
            )
            response = self._client.get_object(Bucket=self.bucket, Key=s3_key)
            data = response["Body"].read()
            logger.info(
                "s3_download_complete",
                extra={"bucket": self.bucket, "key": s3_key, "bytes": len(data)},
            )
            return data

        except self._client.exceptions.NoSuchKey:
            raise StorageError(f"File not found in S3: {s3_key}")
        except (BotoCoreError, ClientError) as e:
            logger.error(
                "s3_download_failed",
                extra={"bucket": self.bucket, "key": s3_key, "error": str(e)},
            )
            raise StorageError(f"Failed to download file from S3: {e}") from e

    def generate_presigned_url(
        self,
        s3_key: str,
        expires_in_seconds: int = 3600,
    ) -> str:
        """
        Generate a pre-signed download URL for an S3 object.

        Used to give the client a time-limited download link for
        the rendered output DOCX without exposing S3 credentials.

        Args:
            s3_key:             The object to generate a URL for
            expires_in_seconds: URL validity (default: 1 hour)

        Returns:
            HTTPS pre-signed URL string

        Raises:
            StorageError: on generation failure
        """
        try:
            url = self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": s3_key},
                ExpiresIn=expires_in_seconds,
            )
            # MinIO generates URLs with its internal Docker hostname (e.g. minio:9000)
            # Replace with the public URL so the browser can reach it
            if settings.s3_endpoint_url:
                import re as _re
                internal = settings.s3_endpoint_url.replace("http://", "").replace("https://", "")
                public_s3_url = getattr(settings, "s3_public_url", None) or "http://localhost:9000"
                url = _re.sub(
                    r"https?://" + _re.escape(internal),
                    public_s3_url,
                    url,
                )
            return url
        except (BotoCoreError, ClientError) as e:
            raise StorageError(
                f"Failed to generate pre-signed URL for {s3_key}: {e}"
            ) from e

    def delete_object(self, s3_key: str) -> None:
        """
        Delete an object from S3.
        Used for cleanup of failed uploads or expired outputs.
        Silent if the object does not exist.
        """
        try:
            self._client.delete_object(Bucket=self.bucket, Key=s3_key)
        except (BotoCoreError, ClientError) as e:
            logger.warning(
                "s3_delete_failed",
                extra={"bucket": self.bucket, "key": s3_key, "error": str(e)},
            )

    def object_exists(self, s3_key: str) -> bool:
        """
        Check whether an object exists in S3 without downloading it.
        Uses head_object — efficient, no data transfer.
        """
        try:
            self._client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise StorageError(f"S3 head_object failed for {s3_key}: {e}") from e


# ── File validation ─────────────────────────────────────────────────────────

ALLOWED_MIME_TYPES: set[str] = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",  # .docx
    "application/pdf",  # .pdf (LibreOffice conversion — Sprint 2+)
}

ALLOWED_EXTENSIONS: set[str] = {".docx", ".pdf"}

MAX_FILE_SIZE_BYTES: int = 10 * 1024 * 1024  # 10 MB


def validate_cv_file(
    filename: str,
    content_type: str,
    file_size: int,
) -> None:
    """
    Validate a CV upload before touching S3.
    Validates by file extension only — browsers send inconsistent MIME types
    for .docx files (octet-stream, zip, etc.) so we cannot rely on content_type.

    Raises:
        app.api.middleware.error_handler.ValidationError: on any violation
    """
    from app.api.middleware.error_handler import ValidationError

    ext = Path(filename).suffix.lower()

    if ext not in ALLOWED_EXTENSIONS:
        raise ValidationError(
            f"Unsupported file type '{ext}'. "
            f"Please upload a .docx or .pdf file."
        )

    if file_size > MAX_FILE_SIZE_BYTES:
        max_mb = MAX_FILE_SIZE_BYTES // (1024 * 1024)
        raise ValidationError(
            f"File too large ({file_size / 1024 / 1024:.1f} MB). "
            f"Maximum allowed size is {max_mb} MB."
        )

    if file_size == 0:
        raise ValidationError("Uploaded file is empty.")


def compute_md5(data: bytes) -> str:
    """Compute MD5 hex digest of file bytes — used for deduplication."""
    return hashlib.md5(data).hexdigest()
