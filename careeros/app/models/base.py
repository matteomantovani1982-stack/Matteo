"""
app/models/base.py

SQLAlchemy declarative base and shared mixins.
Every model in CareerOS inherits from Base and TimestampMixin.

Rules:
- UUIDs as primary keys (no sequential integers — safe for distributed systems)
- created_at / updated_at on every table automatically
- __repr__ on every model for readable debug output
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import DateTime, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """
    Shared declarative base for all ORM models.
    Alembic's env.py imports this to detect schema changes.
    """
    pass


class UUIDMixin:
    """
    Adds a UUID primary key to any model.
    Uses PostgreSQL's native UUID type for storage efficiency.
    Default generated server-side so it works even if Python
    somehow skips the default (e.g. bulk inserts via raw SQL).
    """
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=func.gen_random_uuid(),
        index=True,
    )


class TimestampMixin:
    """
    Adds created_at and updated_at to any model.
    updated_at is automatically refreshed on every UPDATE via onupdate.
    Both columns are timezone-aware (stored as UTC in PostgreSQL TIMESTAMPTZ).
    """
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=lambda: datetime.now(UTC),
        nullable=False,
    )
