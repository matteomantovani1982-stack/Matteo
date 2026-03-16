"""
app/models/user.py

User account model.

Security design:
  - Passwords stored as PBKDF2-HMAC-SHA256 with per-user salt
  - No plaintext passwords ever stored or logged
  - email stored lowercase, unique index enforced at DB level
  - is_active flag for soft disable without data deletion
  - refresh_token_hash stored for token rotation (one active session per user)
    In production, extend to a separate refresh_tokens table for multi-device.
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDMixin


class User(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Lowercase email — unique identifier",
    )

    # PBKDF2-HMAC-SHA256: "pbkdf2:sha256:{iterations}${salt}${hash}"
    password_hash: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
        comment="PBKDF2-HMAC-SHA256 hash with embedded salt and iterations",
    )

    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        comment="False = account disabled, all requests return 401",
    )

    is_verified: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
        comment="Email verification flag — not enforced in MVP, reserved for Sprint E",
    )

    # Hashed refresh token for rotation — None means no active session
    refresh_token_hash: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
        comment="SHA-256 hash of the current refresh token — rotated on each use",
    )

    last_login_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email} active={self.is_active}>"
