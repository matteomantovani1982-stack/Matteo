"""
app/services/auth/tokens.py

JWT token creation and verification — stdlib only (no python-jose needed).

Two token types:
  ACCESS  — short-lived (30 min), contains user_id + email
  REFRESH — long-lived (30 days), contains user_id + jti (unique ID)
             Stored hash in DB — token rotation on each use

JWT implementation:
  Standard HS256 JWT using hmac + hashlib.
  Header.Payload.Signature — base64url encoded.

Format matches RFC 7519 so any standard JWT library can verify these
tokens if we migrate to python-jose or PyJWT later.
"""

import base64
import hashlib
import hmac
import json
import secrets
import time
import uuid
from datetime import UTC, datetime, timedelta

from app.config import settings


# ── Token lifetimes ───────────────────────────────────────────────────────────
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 30
TOKEN_TYPE_ACCESS = "access"
TOKEN_TYPE_REFRESH = "refresh"


# ── Base64url helpers ─────────────────────────────────────────────────────────

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    # Add padding
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


# ── JWT core ──────────────────────────────────────────────────────────────────

def _sign(header_b64: str, payload_b64: str, secret: str) -> str:
    msg = f"{header_b64}.{payload_b64}".encode("ascii")
    sig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest()
    return _b64url_encode(sig)


def _create_token(payload: dict, secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    sig = _sign(header_b64, payload_b64, secret)
    return f"{header_b64}.{payload_b64}.{sig}"


def _verify_token(token: str, secret: str) -> dict:
    """
    Verify a JWT token.
    Raises ValueError with a descriptive message on any failure.
    Returns the payload dict on success.
    """
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("Malformed token")

        header_b64, payload_b64, sig = parts

        # Verify signature
        expected_sig = _sign(header_b64, payload_b64, secret)
        if not hmac.compare_digest(sig, expected_sig):
            raise ValueError("Invalid signature")

        # Decode payload
        payload = json.loads(_b64url_decode(payload_b64))

        # Check expiry
        exp = payload.get("exp")
        if exp is None:
            raise ValueError("Token has no expiry")
        if time.time() > exp:
            raise ValueError("Token has expired")

        return payload

    except (ValueError, KeyError, json.JSONDecodeError) as e:
        raise ValueError(str(e)) from e


# ── Public API ────────────────────────────────────────────────────────────────

def create_access_token(user_id: uuid.UUID, email: str) -> str:
    """Create a short-lived access token (30 minutes)."""
    now = time.time()
    payload = {
        "sub": str(user_id),
        "email": email,
        "type": TOKEN_TYPE_ACCESS,
        "iat": int(now),
        "exp": int(now + ACCESS_TOKEN_EXPIRE_MINUTES * 60),
        "jti": secrets.token_hex(16),
    }
    return _create_token(payload, settings.secret_key)


def create_refresh_token(user_id: uuid.UUID) -> tuple[str, str]:
    """
    Create a long-lived refresh token (30 days).

    Returns:
        (token_string, jti) — jti is the unique token ID used for rotation.
        Store SHA-256 hash of the token string in the DB, not the token itself.
    """
    now = time.time()
    jti = secrets.token_hex(32)
    payload = {
        "sub": str(user_id),
        "type": TOKEN_TYPE_REFRESH,
        "iat": int(now),
        "exp": int(now + REFRESH_TOKEN_EXPIRE_DAYS * 86400),
        "jti": jti,
    }
    token = _create_token(payload, settings.secret_key)
    return token, jti


def verify_access_token(token: str) -> dict:
    """
    Verify an access token.
    Returns payload dict with 'sub' (user_id) and 'email'.
    Raises ValueError on invalid/expired token.
    """
    payload = _verify_token(token, settings.secret_key)
    if payload.get("type") != TOKEN_TYPE_ACCESS:
        raise ValueError("Not an access token")
    return payload


def verify_refresh_token(token: str) -> dict:
    """
    Verify a refresh token.
    Returns payload dict with 'sub' (user_id) and 'jti'.
    Raises ValueError on invalid/expired token.
    """
    payload = _verify_token(token, settings.secret_key)
    if payload.get("type") != TOKEN_TYPE_REFRESH:
        raise ValueError("Not a refresh token")
    return payload


def hash_token(token: str) -> str:
    """SHA-256 hash of a token for DB storage. Never store tokens in plaintext."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
