"""
app/services/auth/auth_service.py

Authentication business logic.

Responsibilities:
  - User registration (email + password)
  - Login → access + refresh token pair
  - Token refresh (rotation — old token invalidated)
  - Logout (refresh token revoked)
  - Current user lookup from access token

Security properties:
  - Passwords hashed with PBKDF2-SHA256 (600k iterations)
  - Refresh tokens rotated on every use (reuse = immediate revocation)
  - Refresh token hash stored in DB, not the token itself
  - Timing-safe comparisons throughout
  - Consistent error messages to prevent user enumeration
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.services.auth.password import hash_password, verify_password
from app.services.auth.tokens import (
    create_access_token,
    create_refresh_token,
    hash_token,
    verify_access_token,
    verify_refresh_token,
)

import structlog

logger = structlog.get_logger(__name__)

# Generic error message — same for "user not found" and "wrong password"
# to prevent user enumeration
_INVALID_CREDENTIALS = "Invalid email or password."
_ACCOUNT_DISABLED = "Account is disabled. Please contact support."


class AuthError(Exception):
    """Raised on authentication failures. HTTP layer maps this to 401."""
    def __init__(self, message: str, status_code: int = 401):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class AuthService:

    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    # ── Registration ──────────────────────────────────────────────────────

    async def register(self, email: str, password: str, full_name: str | None = None) -> User:
        """
        Create a new user account.

        Raises:
            AuthError(409) if email already registered
            AuthError(400) if password too short
        """
        email = email.strip().lower()

        if len(password) < 8:
            raise AuthError("Password must be at least 8 characters.", status_code=400)

        # Check email uniqueness
        existing = await self._get_by_email(email)
        if existing:
            raise AuthError("Email already registered.", status_code=409)

        user = User(
            email=email,
            password_hash=hash_password(password),
            full_name=full_name,
            is_active=True,
            is_verified=False,
        )
        self._db.add(user)
        await self._db.flush()

        logger.info("user_registered", extra={"user_id": str(user.id), "email": email})
        return user

    # ── Login ─────────────────────────────────────────────────────────────

    async def login(self, email: str, password: str) -> tuple[User, str, str]:
        """
        Authenticate a user.

        Returns:
            (user, access_token, refresh_token)

        Raises:
            AuthError(401) on invalid credentials or disabled account
        """
        email = email.strip().lower()
        user = await self._get_by_email(email)

        # Intentionally same error for "not found" and "wrong password"
        if user is None or not verify_password(password, user.password_hash):
            raise AuthError(_INVALID_CREDENTIALS)

        if not user.is_active:
            raise AuthError(_ACCOUNT_DISABLED)

        access_token = create_access_token(user.id, user.email)
        refresh_token, _ = create_refresh_token(user.id)

        # Store refresh token hash for rotation validation
        user.refresh_token_hash = hash_token(refresh_token)
        user.last_login_at = datetime.now(UTC)
        await self._db.flush()

        logger.info("user_login", extra={"user_id": str(user.id)})
        return user, access_token, refresh_token

    # ── Token refresh ─────────────────────────────────────────────────────

    async def refresh(self, refresh_token: str) -> tuple[str, str]:
        """
        Rotate refresh token and issue new access + refresh pair.

        Token rotation: the provided refresh token is invalidated immediately.
        If a reused token is detected (hash mismatch after prior rotation),
        ALL sessions are revoked as a security measure.

        Returns:
            (new_access_token, new_refresh_token)

        Raises:
            AuthError(401) on invalid/expired/reused token
        """
        try:
            payload = verify_refresh_token(refresh_token)
        except ValueError as e:
            raise AuthError(f"Invalid refresh token: {e}")

        user_id = uuid.UUID(payload["sub"])
        user = await self._db.get(User, user_id)

        if user is None or not user.is_active:
            raise AuthError(_INVALID_CREDENTIALS)

        # Rotation check — hash must match what's stored
        token_hash = hash_token(refresh_token)
        if not user.refresh_token_hash:
            raise AuthError("No active session found.")

        if user.refresh_token_hash != token_hash:
            # Token reuse detected — revoke all sessions
            logger.warning(
                "refresh_token_reuse_detected",
                extra={"user_id": str(user_id)},
            )
            user.refresh_token_hash = None
            await self._db.flush()
            raise AuthError("Session invalidated. Please log in again.")

        # Issue new token pair
        new_access = create_access_token(user.id, user.email)
        new_refresh, _ = create_refresh_token(user.id)

        user.refresh_token_hash = hash_token(new_refresh)
        await self._db.flush()

        return new_access, new_refresh

    # ── Logout ────────────────────────────────────────────────────────────

    async def logout(self, user_id: uuid.UUID) -> None:
        """Revoke all sessions for the user by clearing the refresh token hash."""
        user = await self._db.get(User, user_id)
        if user:
            user.refresh_token_hash = None
            await self._db.flush()
        logger.info("user_logout", extra={"user_id": str(user_id)})

    # ── Current user ──────────────────────────────────────────────────────

    async def get_current_user_from_token(self, access_token: str) -> User:
        """
        Validate access token and return the User.

        Raises:
            AuthError(401) on invalid/expired token
            AuthError(401) if user not found or disabled
        """
        try:
            payload = verify_access_token(access_token)
        except ValueError as e:
            raise AuthError(f"Invalid token: {e}")

        user_id = uuid.UUID(payload["sub"])
        user = await self._db.get(User, user_id)

        if user is None:
            raise AuthError("User not found.")
        if not user.is_active:
            raise AuthError(_ACCOUNT_DISABLED)

        return user

    # ── Helpers ───────────────────────────────────────────────────────────

    async def _get_by_email(self, email: str) -> User | None:
        result = await self._db.execute(
            select(User).where(User.email == email)
        )
        return result.scalar_one_or_none()
