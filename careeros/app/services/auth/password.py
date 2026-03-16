"""
app/services/auth/password.py

Password hashing using Python stdlib only (no passlib/bcrypt needed).

Algorithm: PBKDF2-HMAC-SHA256
  - 600,000 iterations (OWASP 2023 recommendation)
  - 32-byte random salt per password
  - Stored format: "pbkdf2:sha256:{iterations}${salt_hex}${hash_hex}"

This format is compatible with passlib's pbkdf2_sha256 scheme,
so migration to passlib later is non-breaking.
"""

import hashlib
import hmac
import secrets

ITERATIONS = 600_000
SALT_BYTES = 32
ALGORITHM = "sha256"
PREFIX = f"pbkdf2:{ALGORITHM}:{ITERATIONS}"


def hash_password(plaintext: str) -> str:
    """
    Hash a plaintext password. Returns a storable string.
    Each call produces a different hash (different salt).
    """
    salt = secrets.token_hex(SALT_BYTES)
    dk = hashlib.pbkdf2_hmac(
        ALGORITHM,
        plaintext.encode("utf-8"),
        salt.encode("utf-8"),
        ITERATIONS,
    )
    return f"{PREFIX}${salt}${dk.hex()}"


def verify_password(plaintext: str, stored_hash: str) -> bool:
    """
    Verify a plaintext password against a stored hash.
    Constant-time comparison to prevent timing attacks.
    Returns True if the password matches.
    """
    try:
        prefix, salt, expected_hex = stored_hash.split("$", 2)
        parts = prefix.split(":")
        algorithm = parts[1]
        iterations = int(parts[2])
    except (ValueError, IndexError):
        return False

    try:
        dk = hashlib.pbkdf2_hmac(
            algorithm,
            plaintext.encode("utf-8"),
            salt.encode("utf-8"),
            iterations,
        )
        return hmac.compare_digest(dk.hex(), expected_hex)
    except Exception:
        return False
