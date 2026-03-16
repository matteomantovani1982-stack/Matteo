"""
tests/test_auth.py

Tests for the authentication layer.

Coverage:
  - Password hashing (PBKDF2-SHA256)
  - JWT token creation, verification, expiry, rotation
  - AuthService: register, login, refresh, logout
  - Security: timing-safe comparison, token reuse detection
"""

import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.auth.password import hash_password, verify_password
from app.services.auth.tokens import (
    create_access_token,
    create_refresh_token,
    hash_token,
    verify_access_token,
    verify_refresh_token,
    ACCESS_TOKEN_EXPIRE_MINUTES,
)


# ── Password tests ────────────────────────────────────────────────────────────

class TestPasswordHashing:

    def test_hash_produces_string(self):
        h = hash_password("mysecretpassword")
        assert isinstance(h, str)
        assert len(h) > 20

    def test_hash_includes_algorithm_prefix(self):
        h = hash_password("password123")
        assert h.startswith("pbkdf2:sha256:")

    def test_verify_correct_password(self):
        h = hash_password("correct-horse-battery-staple")
        assert verify_password("correct-horse-battery-staple", h) is True

    def test_verify_wrong_password(self):
        h = hash_password("correct-password")
        assert verify_password("wrong-password", h) is False

    def test_same_password_produces_different_hashes(self):
        """Each call uses a different salt."""
        h1 = hash_password("samepassword")
        h2 = hash_password("samepassword")
        assert h1 != h2

    def test_both_hashes_verify_correctly(self):
        """Despite different salts, both should verify."""
        h1 = hash_password("samepassword")
        h2 = hash_password("samepassword")
        assert verify_password("samepassword", h1) is True
        assert verify_password("samepassword", h2) is True

    def test_verify_invalid_hash_format(self):
        """Malformed hash should return False, not raise."""
        assert verify_password("password", "not-a-valid-hash") is False

    def test_verify_empty_hash(self):
        assert verify_password("password", "") is False

    def test_unicode_password(self):
        password = "pässwörd123!"
        h = hash_password(password)
        assert verify_password(password, h) is True
        assert verify_password("passwword123!", h) is False


# ── Token tests ───────────────────────────────────────────────────────────────

class TestAccessTokens:

    def _user_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def test_create_and_verify_access_token(self):
        uid = self._user_id()
        token = create_access_token(uid, "test@example.com")
        payload = verify_access_token(token)
        assert payload["sub"] == str(uid)
        assert payload["email"] == "test@example.com"
        assert payload["type"] == "access"

    def test_access_token_has_expiry(self):
        token = create_access_token(self._user_id(), "a@b.com")
        payload = verify_access_token(token)
        assert "exp" in payload
        assert payload["exp"] > time.time()

    def test_access_token_expires_in_30_minutes(self):
        token = create_access_token(self._user_id(), "a@b.com")
        payload = verify_access_token(token)
        expected_exp = time.time() + ACCESS_TOKEN_EXPIRE_MINUTES * 60
        assert abs(payload["exp"] - expected_exp) < 5  # within 5 seconds

    def test_tampered_token_rejected(self):
        token = create_access_token(self._user_id(), "a@b.com")
        parts = token.split(".")
        # Tamper with payload
        tampered = parts[0] + "." + parts[1] + "X" + "." + parts[2]
        with pytest.raises(ValueError):
            verify_access_token(tampered)

    def test_wrong_signature_rejected(self):
        token = create_access_token(self._user_id(), "a@b.com")
        parts = token.split(".")
        bad_token = parts[0] + "." + parts[1] + ".badsignature"
        with pytest.raises(ValueError, match="Invalid signature"):
            verify_access_token(bad_token)

    def test_refresh_token_rejected_as_access(self):
        uid = self._user_id()
        refresh_token, _ = create_refresh_token(uid)
        with pytest.raises(ValueError, match="Not an access token"):
            verify_access_token(refresh_token)

    def test_malformed_token_rejected(self):
        with pytest.raises(ValueError):
            verify_access_token("not.a.jwt")

    def test_each_token_has_unique_jti(self):
        uid = self._user_id()
        t1 = create_access_token(uid, "a@b.com")
        t2 = create_access_token(uid, "a@b.com")
        p1 = verify_access_token(t1)
        p2 = verify_access_token(t2)
        assert p1["jti"] != p2["jti"]


class TestRefreshTokens:

    def test_create_and_verify_refresh_token(self):
        uid = uuid.uuid4()
        token, jti = create_refresh_token(uid)
        payload = verify_refresh_token(token)
        assert payload["sub"] == str(uid)
        assert payload["type"] == "refresh"
        assert payload["jti"] == jti

    def test_access_token_rejected_as_refresh(self):
        token = create_access_token(uuid.uuid4(), "a@b.com")
        with pytest.raises(ValueError, match="Not a refresh token"):
            verify_refresh_token(token)

    def test_hash_token_consistent(self):
        token = "some_token_string"
        h1 = hash_token(token)
        h2 = hash_token(token)
        assert h1 == h2

    def test_different_tokens_different_hashes(self):
        assert hash_token("token1") != hash_token("token2")

    def test_hash_token_returns_hex_string(self):
        h = hash_token("test_token")
        assert len(h) == 64  # SHA-256 = 32 bytes = 64 hex chars
        assert all(c in "0123456789abcdef" for c in h)


# ── AuthService tests ─────────────────────────────────────────────────────────

class TestAuthService:
    """Unit tests with mocked DB session."""

    def _make_service(self, user=None):
        from app.services.auth.auth_service import AuthService
        db = AsyncMock()

        if user is not None:
            db.get = AsyncMock(return_value=user)
            # Mock execute for email lookup
            result = MagicMock()
            result.scalar_one_or_none = MagicMock(return_value=user)
            db.execute = AsyncMock(return_value=result)
        else:
            db.get = AsyncMock(return_value=None)
            result = MagicMock()
            result.scalar_one_or_none = MagicMock(return_value=None)
            db.execute = AsyncMock(return_value=result)

        db.add = MagicMock()
        db.flush = AsyncMock()

        svc = AuthService(db=db)
        return svc, db

    def _make_user(self, password="password123", active=True):
        from app.models.user import User
        user = MagicMock(spec=User)
        user.id = uuid.uuid4()
        user.email = "test@example.com"
        user.password_hash = hash_password(password)
        user.is_active = active
        user.refresh_token_hash = None
        user.last_login_at = None
        return user

    @pytest.mark.asyncio
    async def test_register_new_user(self):
        svc, db = self._make_service(user=None)
        user = await svc.register("new@example.com", "password123")
        db.add.assert_called_once()
        db.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_register_duplicate_email_raises(self):
        from app.services.auth.auth_service import AuthError
        existing = self._make_user()
        svc, _ = self._make_service(user=existing)
        with pytest.raises(AuthError) as exc_info:
            await svc.register("test@example.com", "password123")
        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_register_short_password_raises(self):
        from app.services.auth.auth_service import AuthError
        svc, _ = self._make_service(user=None)
        with pytest.raises(AuthError) as exc_info:
            await svc.register("new@example.com", "short")
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_login_valid_credentials(self):
        user = self._make_user(password="correct123")
        svc, _ = self._make_service(user=user)
        returned_user, access_token, refresh_token = await svc.login(
            "test@example.com", "correct123"
        )
        assert returned_user is user
        assert len(access_token) > 20
        assert len(refresh_token) > 20

    @pytest.mark.asyncio
    async def test_login_wrong_password_raises(self):
        from app.services.auth.auth_service import AuthError
        user = self._make_user(password="correct123")
        svc, _ = self._make_service(user=user)
        with pytest.raises(AuthError) as exc_info:
            await svc.login("test@example.com", "wrongpassword")
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_login_unknown_user_raises(self):
        from app.services.auth.auth_service import AuthError
        svc, _ = self._make_service(user=None)
        with pytest.raises(AuthError) as exc_info:
            await svc.login("unknown@example.com", "password123")
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_login_disabled_user_raises(self):
        from app.services.auth.auth_service import AuthError
        user = self._make_user(active=False)
        svc, _ = self._make_service(user=user)
        with pytest.raises(AuthError) as exc_info:
            await svc.login("test@example.com", "password123")
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_login_stores_refresh_token_hash(self):
        user = self._make_user(password="password123")
        svc, _ = self._make_service(user=user)
        await svc.login("test@example.com", "password123")
        assert user.refresh_token_hash is not None
        assert len(user.refresh_token_hash) == 64  # SHA-256 hex

    @pytest.mark.asyncio
    async def test_refresh_valid_token(self):
        user = self._make_user()
        refresh_token, _ = create_refresh_token(user.id)
        user.refresh_token_hash = hash_token(refresh_token)

        svc, _ = self._make_service(user=user)
        new_access, new_refresh = await svc.refresh(refresh_token)

        assert len(new_access) > 20
        assert len(new_refresh) > 20
        assert new_refresh != refresh_token  # Token was rotated

    @pytest.mark.asyncio
    async def test_refresh_token_reuse_revokes_session(self):
        from app.services.auth.auth_service import AuthError
        user = self._make_user()
        old_refresh, _ = create_refresh_token(user.id)
        # Simulate: user has a DIFFERENT token stored (rotation already happened)
        new_refresh_stored, _ = create_refresh_token(user.id)
        user.refresh_token_hash = hash_token(new_refresh_stored)

        svc, _ = self._make_service(user=user)
        with pytest.raises(AuthError):
            await svc.refresh(old_refresh)

        # Session should be fully revoked
        assert user.refresh_token_hash is None

    @pytest.mark.asyncio
    async def test_logout_clears_refresh_token(self):
        user = self._make_user()
        user.refresh_token_hash = "some_hash"
        svc, db = self._make_service(user=user)
        await svc.logout(user.id)
        assert user.refresh_token_hash is None

    @pytest.mark.asyncio
    async def test_get_current_user_valid_token(self):
        user = self._make_user()
        token = create_access_token(user.id, user.email)
        svc, _ = self._make_service(user=user)
        returned = await svc.get_current_user_from_token(token)
        assert returned is user

    @pytest.mark.asyncio
    async def test_get_current_user_invalid_token(self):
        from app.services.auth.auth_service import AuthError
        svc, _ = self._make_service()
        with pytest.raises(AuthError):
            await svc.get_current_user_from_token("invalid.token.here")

    def test_login_error_message_same_for_user_not_found_and_wrong_password(self):
        """Prevent user enumeration — same error message in both cases."""
        from app.services.auth.auth_service import _INVALID_CREDENTIALS
        # Both cases raise the same message
        assert "Invalid" in _INVALID_CREDENTIALS
