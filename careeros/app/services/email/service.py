"""
app/services/email/service.py

Email delivery service for CVLab.

Design:
  - SMTP via Python stdlib smtplib — no third-party dep required
  - In development (no SMTP config): prints emails to console/logs
  - In production: connects to any SMTP relay (SendGrid, Mailgun,
    AWS SES, Postmark, or plain SMTP server)
  - All templates are plain-text + HTML (inline-styled for email clients)
  - Never raises — logs failures, returns bool success flag
  - All sends are synchronous (called from Celery tasks or background)

Environment variables (all optional — omit for console mode):
  SMTP_HOST         SMTP server hostname
  SMTP_PORT         Port (default 587 for STARTTLS, 465 for SSL)
  SMTP_USER         SMTP username / API key
  SMTP_PASSWORD     SMTP password
  SMTP_FROM         Sender address  (default: noreply@cvlab.co)
  SMTP_FROM_NAME    Sender name     (default: CVLab)
  SMTP_USE_TLS      "true" for STARTTLS on port 587 (default)
  SMTP_USE_SSL      "true" for direct SSL on port 465
  APP_BASE_URL      Used in email links (default: http://localhost:3000)
"""

import structlog
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dataclasses import dataclass

from app.config import settings

logger = structlog.get_logger(__name__)


@dataclass
class EmailMessage:
    to_email: str
    to_name: str | None
    subject: str
    text_body: str
    html_body: str


class EmailService:
    """
    Sends transactional emails.
    Falls back to console output when SMTP is not configured.
    """

    def __init__(self) -> None:
        self._host = getattr(settings, "smtp_host", None)
        self._port = int(getattr(settings, "smtp_port", 587))
        self._user = getattr(settings, "smtp_user", None)
        self._password = getattr(settings, "smtp_password", None)
        self._from_addr = getattr(settings, "smtp_from", "noreply@cvlab.co")
        self._from_name = getattr(settings, "smtp_from_name", "CVLab")
        self._use_tls = str(getattr(settings, "smtp_use_tls", "true")).lower() == "true"
        self._use_ssl = str(getattr(settings, "smtp_use_ssl", "false")).lower() == "true"
        self._base_url = getattr(settings, "app_base_url", "http://localhost:3000")
        self._configured = bool(self._host and self._user)

    # ── Public send methods ───────────────────────────────────────────────────

    def send_verification_email(self, to_email: str, to_name: str | None, token: str) -> bool:
        """Send email verification link."""
        verify_url = f"{self._base_url}/verify-email?token={token}"
        display_name = to_name or to_email.split("@")[0]

        msg = EmailMessage(
            to_email=to_email,
            to_name=to_name,
            subject="Verify your CVLab email address",
            text_body=_verification_text(display_name, verify_url),
            html_body=_verification_html(display_name, verify_url),
        )
        return self._send(msg)

    def send_password_reset_email(self, to_email: str, to_name: str | None, token: str) -> bool:
        """Send password reset link."""
        reset_url = f"{self._base_url}/reset-password?token={token}"
        display_name = to_name or to_email.split("@")[0]

        msg = EmailMessage(
            to_email=to_email,
            to_name=to_name,
            subject="Reset your CVLab password",
            text_body=_reset_text(display_name, reset_url),
            html_body=_reset_html(display_name, reset_url),
        )
        return self._send(msg)

    def send_welcome_email(self, to_email: str, to_name: str | None) -> bool:
        """Send welcome email after successful verification."""
        display_name = to_name or to_email.split("@")[0]
        dashboard_url = f"{self._base_url}/"

        msg = EmailMessage(
            to_email=to_email,
            to_name=to_name,
            subject="Welcome to CVLab",
            text_body=_welcome_text(display_name, dashboard_url),
            html_body=_welcome_html(display_name, dashboard_url),
        )
        return self._send(msg)

    # ── Core send ─────────────────────────────────────────────────────────────

    def _send(self, msg: EmailMessage) -> bool:
        if not self._configured:
            self._console_send(msg)
            return True

        try:
            mime = self._build_mime(msg)
            self._smtp_send(mime, msg.to_email)
            logger.info("email_sent", to=msg.to_email, subject=msg.subject)
            return True
        except Exception as e:
            logger.error("email_send_failed", to=msg.to_email, error=str(e))
            # Fall back to console so the token is not lost in dev/staging
            self._console_send(msg)
            return False

    def _build_mime(self, msg: EmailMessage) -> MIMEMultipart:
        mime = MIMEMultipart("alternative")
        mime["Subject"] = msg.subject
        mime["From"] = f"{self._from_name} <{self._from_addr}>"
        to_header = f"{msg.to_name} <{msg.to_email}>" if msg.to_name else msg.to_email
        mime["To"] = to_header

        mime.attach(MIMEText(msg.text_body, "plain", "utf-8"))
        mime.attach(MIMEText(msg.html_body, "html", "utf-8"))
        return mime

    def _smtp_send(self, mime: MIMEMultipart, to_email: str) -> None:
        if self._use_ssl:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(self._host, self._port, context=ctx) as server:
                if self._user:
                    server.login(self._user, self._password or "")
                server.sendmail(self._from_addr, [to_email], mime.as_string())
        else:
            with smtplib.SMTP(self._host, self._port) as server:
                server.ehlo()
                if self._use_tls:
                    server.starttls(context=ssl.create_default_context())
                    server.ehlo()
                if self._user:
                    server.login(self._user, self._password or "")
                server.sendmail(self._from_addr, [to_email], mime.as_string())

    def _console_send(self, msg: EmailMessage) -> None:
        """Print email to console — used in dev / when SMTP fails."""
        separator = "─" * 60
        logger.info(
            "email_console_send\n"
            f"{separator}\n"
            f"TO:      {msg.to_email}\n"
            f"SUBJECT: {msg.subject}\n"
            f"{separator}\n"
            f"{msg.text_body}\n"
            f"{separator}"
        )


# ── Singleton ─────────────────────────────────────────────────────────────────

_email_service: EmailService | None = None


def get_email_service() -> EmailService:
    global _email_service
    if _email_service is None:
        _email_service = EmailService()
    return _email_service


# ── Email templates ───────────────────────────────────────────────────────────
# Plain-text versions first (always preferred by spam filters).
# HTML versions use inline styles for maximum email client compatibility.

def _verification_text(name: str, url: str) -> str:
    return f"""Hi {name},

Welcome to CVLab. Please verify your email address by clicking the link below:

{url}

This link expires in 24 hours.

If you didn't create a CVLab account, you can safely ignore this email.

– The CVLab team
"""


def _verification_html(name: str, url: str) -> str:
    return _wrap_html(
        f"""
        <p style="margin:0 0 16px">Hi {name},</p>
        <p style="margin:0 0 24px">Welcome to CVLab. Please verify your email address to get started.</p>
        <a href="{url}" style="{_btn_style()}">Verify email address</a>
        <p style="margin:24px 0 0;font-size:13px;color:#6B6B62">
          This link expires in 24 hours. If you didn't create a CVLab account, you can safely ignore this email.
        </p>
        """,
        title="Verify your email",
    )


def _reset_text(name: str, url: str) -> str:
    return f"""Hi {name},

We received a request to reset your CVLab password. Click the link below to choose a new one:

{url}

This link expires in 1 hour. If you didn't request a password reset, you can safely ignore this email — your password won't change.

– The CVLab team
"""


def _reset_html(name: str, url: str) -> str:
    return _wrap_html(
        f"""
        <p style="margin:0 0 16px">Hi {name},</p>
        <p style="margin:0 0 24px">We received a request to reset your CVLab password.</p>
        <a href="{url}" style="{_btn_style()}">Reset password</a>
        <p style="margin:24px 0 0;font-size:13px;color:#6B6B62">
          This link expires in 1 hour. If you didn't request a password reset, you can safely ignore this email — your password won't change.
        </p>
        """,
        title="Reset your password",
    )


def _welcome_text(name: str, dashboard_url: str) -> str:
    return f"""Hi {name},

Your CVLab account is verified and ready to go.

Start your first application here:
{dashboard_url}

CVLab generates a full intelligence pack for every job you apply to:
- Tailored CV (DOCX, same layout)
- Positioning strategy and strongest angles
- Named contacts at the company + outreach messages
- Company intelligence and salary benchmarks
- 7-day action plan

– The CVLab team
"""


def _welcome_html(name: str, dashboard_url: str) -> str:
    return _wrap_html(
        f"""
        <p style="margin:0 0 16px">Hi {name},</p>
        <p style="margin:0 0 16px">Your CVLab account is verified and ready to go.</p>
        <a href="{dashboard_url}" style="{_btn_style()}">Go to dashboard →</a>
        <div style="margin:28px 0 0;padding:20px;background:#F6F6F3;border-radius:8px">
          <p style="margin:0 0 10px;font-weight:600;font-size:13px">What CVLab generates for every application:</p>
          <ul style="margin:0;padding-left:18px;font-size:13px;color:#6B6B62;line-height:1.8">
            <li>Tailored CV (DOCX, same layout)</li>
            <li>Positioning strategy and strongest angles</li>
            <li>Named contacts + ready-to-send outreach messages</li>
            <li>Company intelligence and salary benchmarks</li>
            <li>7-day action plan</li>
          </ul>
        </div>
        """,
        title="Welcome to CVLab",
    )


def _btn_style() -> str:
    return (
        "display:inline-block;background:#2563EB;color:#FFFFFF;font-family:'Helvetica Neue',Arial,sans-serif;"
        "font-size:14px;font-weight:600;padding:12px 24px;border-radius:7px;text-decoration:none;"
    )


def _wrap_html(body: str, title: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{title}</title>
</head>
<body style="margin:0;padding:0;background:#F6F6F3;font-family:'Helvetica Neue',Arial,sans-serif;font-size:15px;line-height:1.6;color:#1A1A17">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#F6F6F3;padding:40px 0">
    <tr>
      <td align="center">
        <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%">

          <!-- Logo -->
          <tr>
            <td style="padding:0 0 24px">
              <span style="font-size:20px;font-weight:700;color:#1A1A17;letter-spacing:-0.02em">
                CV<span style="color:#2563EB">Lab</span>
              </span>
            </td>
          </tr>

          <!-- Card -->
          <tr>
            <td style="background:#FFFFFF;border:1px solid #E4E4DE;border-radius:12px;padding:36px 40px">
              {body}
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:20px 0 0;font-size:12px;color:#A8A89E;text-align:center">
              CVLab · You're receiving this because you signed up for an account.
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
