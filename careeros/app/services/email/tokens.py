"""
app/services/email/tokens.py

Generates and validates signed, time-limited tokens for:
  - Email verification  (24h expiry)
  - Password reset      (1h expiry)

Design:
  - Pure stdlib: hmac + hashlib + base64 + json — zero extra deps
  - Token format: base64url( JSON payload ) + "." + HMAC-SHA256 signature
  - Payload: { "sub": user_id, "type": "verify"|"reset", "exp": unix_timestamp }
  - Signed with SECRET_KEY — forgery requires the secret
  - Stateless — no DB lookup needed to validate structure/signature/expiry
  - DB still checked: user.is_verified prevents replay of old verify tokens
"""

import base64
import hashlib
import hmac
import json
import time
import uuid
from typing import Literal

from app.config import settings

TokenType = Literal["verify", "reset"]

EXPIRY_SECONDS: dict[TokenType, int] = {
    "verify": 24 * 3600,   # 24 hours
    "reset":  1  * 3600,   # 1 hour
}


class TokenError(Exception):
    """Raised when a token is invalid, expired, or tampered."""
    pass


def _sign(payload_b64: str) -> str:
    """Return HMAC-SHA256 hex digest of the payload."""
    return hmac.new(
        settings.secret_key.encode(),
        payload_b64.encode(),
        hashlib.sha256,
    ).hexdigest()


def generate_token(user_id: str | uuid.UUID, token_type: TokenType) -> str:
    """
    Generate a signed token for the given user and purpose.

    Returns an opaque URL-safe string suitable for embedding in links.
    """
    expiry = int(time.time()) + EXPIRY_SECONDS[token_type]
    payload = {
        "sub": str(user_id),
        "type": token_type,
        "exp": expiry,
        "jti": str(uuid.uuid4()),  # unique per token — prevents trivial replay
    }
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode()
    sig = _sign(payload_b64)
    return f"{payload_b64}.{sig}"


def validate_token(token: str, expected_type: TokenType) -> str:
    """
    Validate a token and return the user_id (str) it was issued for.

    Raises TokenError on any failure:
      - Malformed structure
      - Invalid signature (tampered)
      - Wrong token type
      - Expired
    """
    try:
        parts = token.split(".")
        if len(parts) != 2:
            raise TokenError("Malformed token")

        payload_b64, sig = parts

        # Verify signature — constant-time comparison
        expected_sig = _sign(payload_b64)
        if not hmac.compare_digest(sig, expected_sig):
            raise TokenError("Invalid token signature")

        # Decode payload
        # Restore padding
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes)

    except TokenError:
        raise
    except Exception:
        raise TokenError("Malformed token")

    # Type check
    if payload.get("type") != expected_type:
        raise TokenError(f"Wrong token type: expected {expected_type}")

    # Expiry check
    if int(time.time()) > payload.get("exp", 0):
        raise TokenError("Token has expired")

    user_id = payload.get("sub")
    if not user_id:
        raise TokenError("Missing subject in token")

    return user_id
