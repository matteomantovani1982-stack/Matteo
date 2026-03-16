"""
app/models/subscription.py

Subscription and usage tracking for CareerOS B2C billing.

Design:
  One Subscription per user — tracks Stripe state.
  One UsageRecord per job_run completed — immutable audit trail.
  Quota enforcement reads UsageRecord count within billing period.

Plans (defined in app/services/billing/plans.py):
  FREE     — 3 packs/month, no company intel, no positioning strategy
  STARTER  — 10 packs/month, full output
  PRO      — 30 packs/month, full output + priority queue
  UNLIMITED — no pack limit, full output + priority queue

Stripe integration:
  Subscription.stripe_customer_id   — Stripe Customer object
  Subscription.stripe_subscription_id — Stripe Subscription object (null for FREE)
  Subscription.stripe_price_id       — current price/plan identifier
  Status mirrors Stripe subscription statuses where applicable.
"""

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDMixin


class PlanTier(StrEnum):
    FREE      = "free"
    STARTER   = "starter"
    PRO       = "pro"
    UNLIMITED = "unlimited"


class SubscriptionStatus(StrEnum):
    """Mirrors Stripe subscription statuses + local states."""
    ACTIVE             = "active"
    TRIALING           = "trialing"
    PAST_DUE           = "past_due"
    CANCELED           = "canceled"
    UNPAID             = "unpaid"
    INCOMPLETE         = "incomplete"
    INCOMPLETE_EXPIRED = "incomplete_expired"
    FREE               = "free"           # Local-only: free tier, no Stripe sub


class Subscription(Base, UUIDMixin, TimestampMixin):
    """
    One subscription per user.
    Created automatically on registration (FREE tier).
    Updated via Stripe webhooks when user upgrades/downgrades/cancels.
    """
    __tablename__ = "subscriptions"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )

    plan_tier: Mapped[PlanTier] = mapped_column(
        String(20),
        nullable=False,
        default=PlanTier.FREE,
        index=True,
    )

    status: Mapped[SubscriptionStatus] = mapped_column(
        String(30),
        nullable=False,
        default=SubscriptionStatus.FREE,
        index=True,
    )

    # ── Stripe IDs ─────────────────────────────────────────────────────────
    stripe_customer_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
        index=True,
        comment="Stripe Customer ID — cus_xxxx",
    )
    stripe_subscription_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
        index=True,
        comment="Stripe Subscription ID — sub_xxxx. Null for FREE tier.",
    )
    stripe_price_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        comment="Stripe Price ID — price_xxxx. Used to determine plan tier.",
    )

    # ── Billing period ─────────────────────────────────────────────────────
    current_period_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Start of current billing period — from Stripe webhook",
    )
    current_period_end: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="End of current billing period — quota resets at this time",
    )

    # ── Cancellation ───────────────────────────────────────────────────────
    cancel_at_period_end: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="If True, subscription cancels at period end (user requested cancel)",
    )
    canceled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # ── Raw Stripe event cache ─────────────────────────────────────────────
    stripe_metadata: Mapped[dict | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="Last Stripe event payload — for debugging and reconciliation",
    )

    # ── Relationships ──────────────────────────────────────────────────────
    usage_records: Mapped[list["UsageRecord"]] = relationship(
        "UsageRecord",
        back_populates="subscription",
        cascade="all, delete-orphan",
        lazy="noload",
    )

    def __repr__(self) -> str:
        return (
            f"<Subscription user={self.user_id} "
            f"plan={self.plan_tier} status={self.status}>"
        )

    @property
    def is_active(self) -> bool:
        return self.status in (
            SubscriptionStatus.ACTIVE,
            SubscriptionStatus.TRIALING,
            SubscriptionStatus.FREE,
        )

    @property
    def is_paid(self) -> bool:
        return self.plan_tier != PlanTier.FREE and self.is_active


class UsageRecord(Base, UUIDMixin, TimestampMixin):
    """
    Immutable record of one completed Application Intelligence Pack.

    Written when a job_run reaches COMPLETED status.
    Used to enforce quota limits within the current billing period.
    Never deleted — provides full audit trail of usage.
    """
    __tablename__ = "usage_records"

    subscription_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        index=True,
        comment="Denormalised for fast quota queries without join",
    )

    job_run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        unique=True,
        index=True,
        comment="One record per job_run — idempotent via unique constraint",
    )

    plan_tier_at_time: Mapped[PlanTier] = mapped_column(
        String(20),
        nullable=False,
        comment="Plan tier when this pack was generated — for historical analysis",
    )

    # Billing period this usage belongs to (snapshot from subscription at time of use)
    billing_period_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    billing_period_end: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # ── Relationship ───────────────────────────────────────────────────────
    subscription: Mapped["Subscription"] = relationship(
        "Subscription",
        back_populates="usage_records",
    )

    def __repr__(self) -> str:
        return (
            f"<UsageRecord user={self.user_id} "
            f"job_run={self.job_run_id} plan={self.plan_tier_at_time}>"
        )
