"""
tests/test_sprint_l.py

Sprint L: Email verification and password reset.
"""

import time
import uuid
import pytest
from unittest.mock import MagicMock, patch


# ════════════════════════════════════════════════════════════
# Token generation and validation
# ════════════════════════════════════════════════════════════

class TestTokens:

    def test_generate_verify_token_returns_string(self):
        from app.services.email.tokens import generate_token
        token = generate_token(uuid.uuid4(), "verify")
        assert isinstance(token, str)
        assert "." in token

    def test_generate_reset_token_returns_string(self):
        from app.services.email.tokens import generate_token
        token = generate_token(uuid.uuid4(), "reset")
        assert isinstance(token, str)

    def test_validate_returns_user_id(self):
        from app.services.email.tokens import generate_token, validate_token
        uid = uuid.uuid4()
        token = generate_token(uid, "verify")
        result = validate_token(token, "verify")
        assert result == str(uid)

    def test_validate_reset_token(self):
        from app.services.email.tokens import generate_token, validate_token
        uid = uuid.uuid4()
        token = generate_token(uid, "reset")
        result = validate_token(token, "reset")
        assert result == str(uid)

    def test_wrong_type_raises(self):
        from app.services.email.tokens import generate_token, validate_token, TokenError
        uid = uuid.uuid4()
        token = generate_token(uid, "verify")
        with pytest.raises(TokenError, match="Wrong token type"):
            validate_token(token, "reset")

    def test_tampered_token_raises(self):
        from app.services.email.tokens import generate_token, validate_token, TokenError
        uid = uuid.uuid4()
        token = generate_token(uid, "verify")
        # Corrupt the signature
        parts = token.split(".")
        bad_token = parts[0] + ".deadbeef" + parts[1][8:]
        with pytest.raises(TokenError):
            validate_token(bad_token, "verify")

    def test_malformed_token_raises(self):
        from app.services.email.tokens import validate_token, TokenError
        with pytest.raises(TokenError):
            validate_token("not-a-valid-token", "verify")

    def test_expired_token_raises(self):
        from app.services.email.tokens import generate_token, validate_token, TokenError
        import app.services.email.tokens as tok_module
        uid = uuid.uuid4()
        # Patch expiry to -1 seconds (already expired)
        original = tok_module.EXPIRY_SECONDS.copy()
        tok_module.EXPIRY_SECONDS["verify"] = -1
        try:
            token = generate_token(uid, "verify")
            with pytest.raises(TokenError, match="expired"):
                validate_token(token, "verify")
        finally:
            tok_module.EXPIRY_SECONDS.update(original)

    def test_token_with_string_uuid(self):
        from app.services.email.tokens import generate_token, validate_token
        uid = str(uuid.uuid4())
        token = generate_token(uid, "reset")
        result = validate_token(token, "reset")
        assert result == uid

    def test_different_tokens_each_call(self):
        from app.services.email.tokens import generate_token
        uid = uuid.uuid4()
        t1 = generate_token(uid, "verify")
        t2 = generate_token(uid, "verify")
        assert t1 != t2  # jti differs


# ════════════════════════════════════════════════════════════
# Email service
# ════════════════════════════════════════════════════════════

class TestEmailService:

    def test_send_verification_no_smtp_returns_true(self):
        """Without SMTP configured, console send returns True."""
        from app.services.email.service import EmailService
        svc = EmailService.__new__(EmailService)
        svc._host = None
        svc._port = 587
        svc._user = None
        svc._password = None
        svc._from_addr = "noreply@cvlab.co"
        svc._from_name = "CVLab"
        svc._use_tls = True
        svc._use_ssl = False
        svc._base_url = "http://localhost:3000"
        svc._configured = False

        result = svc.send_verification_email(
            to_email="test@example.com",
            to_name="Test User",
            token="test-token-123",
        )
        assert result is True

    def test_verification_url_includes_token(self):
        from app.services.email.service import EmailService
        svc = EmailService.__new__(EmailService)
        svc._configured = False
        svc._base_url = "https://cvlab.co"
        svc._from_addr = "noreply@cvlab.co"
        svc._from_name = "CVLab"
        svc._use_tls = True
        svc._use_ssl = False
        svc._host = None
        svc._user = None
        svc._password = None
        svc._port = 587

        captured = []
        original_console = svc._console_send
        def capture(msg):
            captured.append(msg)
        svc._console_send = capture

        svc.send_verification_email("u@example.com", "User", "mytoken123")
        assert len(captured) == 1
        assert "mytoken123" in captured[0].text_body
        assert "https://cvlab.co/verify-email" in captured[0].text_body

    def test_reset_url_includes_token(self):
        from app.services.email.service import EmailService, EmailMessage
        svc = EmailService.__new__(EmailService)
        svc._configured = False
        svc._base_url = "https://cvlab.co"
        svc._from_addr = "noreply@cvlab.co"
        svc._from_name = "CVLab"
        svc._use_tls = True
        svc._use_ssl = False
        svc._host = None
        svc._user = None
        svc._password = None
        svc._port = 587

        captured = []
        svc._console_send = captured.append

        svc.send_password_reset_email("u@example.com", "User", "resettoken")
        assert "resettoken" in captured[0].text_body
        assert "reset-password" in captured[0].text_body

    def test_welcome_email_content(self):
        from app.services.email.service import EmailService
        svc = EmailService.__new__(EmailService)
        svc._configured = False
        svc._base_url = "https://cvlab.co"
        svc._from_addr = "noreply@cvlab.co"
        svc._from_name = "CVLab"
        svc._use_tls = True
        svc._use_ssl = False
        svc._host = None
        svc._user = None
        svc._password = None
        svc._port = 587

        captured = []
        svc._console_send = captured.append

        svc.send_welcome_email("u@example.com", "Matteo")
        assert "Matteo" in captured[0].text_body
        assert "CVLab" in captured[0].text_body

    def test_smtp_failure_falls_back_to_console(self):
        from app.services.email.service import EmailService
        svc = EmailService.__new__(EmailService)
        svc._configured = True  # Pretend SMTP is configured
        svc._base_url = "http://localhost:3000"
        svc._from_addr = "noreply@cvlab.co"
        svc._from_name = "CVLab"
        svc._use_tls = True
        svc._use_ssl = False
        svc._host = "smtp.example.com"
        svc._port = 587
        svc._user = "user"
        svc._password = "pass"

        console_called = []
        svc._console_send = lambda m: console_called.append(m)
        svc._smtp_send = MagicMock(side_effect=Exception("Connection refused"))

        result = svc.send_verification_email("u@example.com", "User", "tok")
        # Should return False (SMTP failed) but console fallback triggered
        assert result is False
        assert len(console_called) == 1

    def test_html_body_contains_button(self):
        from app.services.email.service import _verification_html
        html = _verification_html("Matteo", "https://cvlab.co/verify?token=abc")
        assert "Verify email address" in html
        assert "https://cvlab.co/verify?token=abc" in html
        assert "background:#2563EB" in html

    def test_html_wraps_in_full_doc(self):
        from app.services.email.service import _reset_html
        html = _reset_html("User", "https://cvlab.co/reset")
        assert "<!DOCTYPE html>" in html
        assert "<html" in html
        assert "CVLab" in html


# ════════════════════════════════════════════════════════════
# Config additions
# ════════════════════════════════════════════════════════════

class TestEmailConfig:

    def test_smtp_fields_have_defaults(self):
        from app.config import settings
        # These should all have defaults — no error accessing them
        assert hasattr(settings, "smtp_host")
        assert hasattr(settings, "smtp_port")
        assert hasattr(settings, "smtp_from")
        assert hasattr(settings, "app_base_url")
        assert settings.smtp_port == 587
        assert settings.smtp_from == "noreply@cvlab.co"
        assert "localhost" in settings.app_base_url

    def test_smtp_host_defaults_none(self):
        from app.config import settings
        assert settings.smtp_host is None  # unconfigured by default


# ════════════════════════════════════════════════════════════
# Token edge cases
# ════════════════════════════════════════════════════════════

class TestTokenEdgeCases:

    def test_empty_string_raises(self):
        from app.services.email.tokens import validate_token, TokenError
        with pytest.raises(TokenError):
            validate_token("", "verify")

    def test_single_segment_raises(self):
        from app.services.email.tokens import validate_token, TokenError
        with pytest.raises(TokenError):
            validate_token("onlyone", "verify")

    def test_verify_token_different_from_reset(self):
        from app.services.email.tokens import generate_token
        uid = uuid.uuid4()
        v = generate_token(uid, "verify")
        r = generate_token(uid, "reset")
        assert v != r
