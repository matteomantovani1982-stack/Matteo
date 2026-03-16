"""
app/services/billing/billing_service.py

Billing business logic — the layer between routes/webhooks and the DB.

Responsibilities:
  - Provision FREE subscription on user registration
  - Stripe customer creation (lazy — on first upgrade attempt)
  - Checkout session creation (upgrade flow)
  - Billing portal session creation (manage/cancel)
  - Handle Stripe webhooks (subscription lifecycle events)
  - Quota enforcement (can this user create a job run right now?)
  - Record usage (write UsageRecord on job_run completion)
  - Return current subscription + usage state for the UI
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.subscription import (
    PlanTier,
    Subscription,
    SubscriptionStatus,
    UsageRecord,
)
from app.services.billing.plans import PLANS, PRICE_TO_TIER, get_plan

import structlog

logger = structlog.get_logger(__name__)


class QuotaExceededError(Exception):
    """Raised when a user has hit their monthly pack limit."""
    def __init__(self, plan_tier: PlanTier, used: int, limit: int):
        self.plan_tier = plan_tier
        self.used = used
        self.limit = limit
        super().__init__(
            f"Monthly quota exceeded: {used}/{limit} packs used on {plan_tier} plan. "
            f"Upgrade to generate more packs."
        )


class BillingService:

    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    # ── Provisioning ──────────────────────────────────────────────────────

    async def provision_free_subscription(self, user_id: uuid.UUID) -> Subscription:
        """
        Create a FREE tier subscription for a newly registered user.
        Called automatically in the registration flow.
        """
        sub = Subscription(
            user_id=user_id,
            plan_tier=PlanTier.FREE,
            status=SubscriptionStatus.FREE,
        )
        self._db.add(sub)
        await self._db.flush()
        logger.info("free_subscription_provisioned", extra={"user_id": str(user_id)})
        return sub

    # ── Checkout / upgrade ────────────────────────────────────────────────

    async def create_checkout_session(
        self,
        user_id: uuid.UUID,
        email: str,
        full_name: str | None,
        price_id: str,
        success_url: str,
        cancel_url: str,
    ) -> str:
        """
        Create a Stripe Checkout Session URL.
        Lazily creates a Stripe Customer if the user doesn't have one yet.
        Returns the session URL to redirect the user to.
        """
        from app.services.billing.stripe_client import StripeClient

        sub = await self._get_or_create_subscription(user_id)

        async with StripeClient() as stripe:
            # Lazily create Stripe customer
            if not sub.stripe_customer_id:
                customer = await stripe.create_customer(
                    email=email,
                    user_id=str(user_id),
                    name=full_name,
                )
                sub.stripe_customer_id = customer["id"]
                await self._db.flush()

            session = await stripe.create_checkout_session(
                customer_id=sub.stripe_customer_id,
                price_id=price_id,
                success_url=success_url,
                cancel_url=cancel_url,
                user_id=str(user_id),
            )

        logger.info(
            "checkout_session_created",
            extra={"user_id": str(user_id), "price_id": price_id},
        )
        return session["url"]

    # ── Billing portal ────────────────────────────────────────────────────

    async def create_portal_session(
        self,
        user_id: uuid.UUID,
        return_url: str,
    ) -> str:
        """
        Create a Stripe Billing Portal session URL.
        Requires the user to already have a Stripe customer ID.
        """
        from app.services.billing.stripe_client import StripeClient

        sub = await self._get_or_create_subscription(user_id)
        if not sub.stripe_customer_id:
            raise ValueError("No Stripe customer found. Complete a purchase first.")

        async with StripeClient() as stripe:
            session = await stripe.create_billing_portal_session(
                customer_id=sub.stripe_customer_id,
                return_url=return_url,
            )
        return session["url"]

    # ── Webhook handling ──────────────────────────────────────────────────

    async def handle_webhook_event(self, event: dict) -> str:
        """
        Process a verified Stripe webhook event.
        Returns a string describing what was done (for logging).

        Handled events:
          checkout.session.completed        — subscription created after payment
          customer.subscription.updated     — plan change, renewal
          customer.subscription.deleted     — cancellation
          invoice.payment_failed            — mark past_due
        """
        event_type = event.get("type", "")
        data = event.get("data", {}).get("object", {})

        if event_type == "checkout.session.completed":
            return await self._handle_checkout_completed(data)

        elif event_type in ("customer.subscription.updated", "customer.subscription.created"):
            return await self._handle_subscription_updated(data)

        elif event_type == "customer.subscription.deleted":
            return await self._handle_subscription_deleted(data)

        elif event_type == "invoice.payment_failed":
            return await self._handle_payment_failed(data)

        else:
            return f"unhandled_event:{event_type}"

    async def _handle_checkout_completed(self, session: dict) -> str:
        """checkout.session.completed — subscription is live."""
        subscription_id = session.get("subscription")
        customer_id = session.get("customer")
        user_id_str = session.get("metadata", {}).get("user_id")

        if not subscription_id or not user_id_str:
            return "checkout_completed_missing_data"

        # Fetch full subscription from Stripe to get price info
        from app.services.billing.stripe_client import StripeClient
        async with StripeClient() as stripe:
            stripe_sub = await stripe.get_subscription(subscription_id)

        price_id = stripe_sub.get("items", {}).get("data", [{}])[0].get("price", {}).get("id")
        plan = PRICE_TO_TIER.get(price_id, PlanTier.FREE) if price_id else PlanTier.FREE

        user_id = uuid.UUID(user_id_str)
        sub = await self._get_or_create_subscription(user_id)
        self._update_subscription_from_stripe(sub, stripe_sub, PlanTier(plan))
        sub.stripe_customer_id = customer_id
        await self._db.flush()

        logger.info("subscription_activated", extra={"user_id": user_id_str, "plan": plan})
        return f"subscription_activated:{plan}"

    async def _handle_subscription_updated(self, stripe_sub: dict) -> str:
        """customer.subscription.updated — covers renewals, upgrades, downgrades."""
        sub = await self._find_by_stripe_subscription(stripe_sub["id"])
        if not sub:
            # Could be a new subscription from checkout (handled by checkout.session.completed)
            return "subscription_not_found_in_db"

        price_id = stripe_sub.get("items", {}).get("data", [{}])[0].get("price", {}).get("id")
        tier = PlanTier(PRICE_TO_TIER.get(price_id, PlanTier.FREE)) if price_id else sub.plan_tier

        self._update_subscription_from_stripe(sub, stripe_sub, tier)
        await self._db.flush()

        logger.info("subscription_updated", extra={"sub_id": stripe_sub["id"], "plan": tier})
        return f"subscription_updated:{tier}"

    async def _handle_subscription_deleted(self, stripe_sub: dict) -> str:
        """customer.subscription.deleted — subscription fully canceled."""
        sub = await self._find_by_stripe_subscription(stripe_sub["id"])
        if not sub:
            return "subscription_not_found"

        sub.status = SubscriptionStatus.CANCELED
        sub.plan_tier = PlanTier.FREE
        sub.canceled_at = datetime.now(UTC)
        sub.stripe_metadata = stripe_sub
        await self._db.flush()

        logger.info("subscription_canceled", extra={"sub_id": stripe_sub["id"]})
        return "subscription_canceled"

    async def _handle_payment_failed(self, invoice: dict) -> str:
        """invoice.payment_failed — mark past_due."""
        sub_id = invoice.get("subscription")
        if not sub_id:
            return "payment_failed_no_subscription"

        sub = await self._find_by_stripe_subscription(sub_id)
        if not sub:
            return "subscription_not_found"

        sub.status = SubscriptionStatus.PAST_DUE
        await self._db.flush()

        logger.warning("payment_failed", extra={"sub_id": sub_id})
        return "subscription_past_due"

    def _update_subscription_from_stripe(
        self,
        sub: Subscription,
        stripe_sub: dict,
        tier: PlanTier,
    ) -> None:
        """Apply Stripe subscription fields to our DB record."""
        sub.plan_tier = tier
        sub.stripe_subscription_id = stripe_sub.get("id")
        raw_status = stripe_sub.get("status", "active")
        try:
            sub.status = SubscriptionStatus(raw_status)
        except ValueError:
            logger.warning("unknown_stripe_status", status=raw_status)
            sub.status = SubscriptionStatus.PAST_DUE
        sub.cancel_at_period_end = stripe_sub.get("cancel_at_period_end", False)

        period_start = stripe_sub.get("current_period_start")
        period_end = stripe_sub.get("current_period_end")
        if period_start:
            sub.current_period_start = datetime.fromtimestamp(period_start, UTC)
        if period_end:
            sub.current_period_end = datetime.fromtimestamp(period_end, UTC)

        sub.stripe_metadata = stripe_sub

    # ── Quota enforcement ─────────────────────────────────────────────────

    async def check_quota(self, user_id: uuid.UUID) -> dict:
        """
        Check if user can create a new job run.

        Returns:
            {
              "allowed": bool,
              "plan_tier": str,
              "used_this_period": int,
              "limit": int | None,
              "remaining": int | None,
            }

        Raises QuotaExceededError if not allowed.
        """
        sub = await self._get_or_create_subscription(user_id)
        plan = get_plan(sub.plan_tier)
        used = await self._count_usage_this_period(sub)

        remaining = plan.quota_remaining(used)
        over = plan.is_over_quota(used)

        result = {
            "allowed": not over,
            "plan_tier": sub.plan_tier,
            "used_this_period": used,
            "limit": plan.packs_per_month,
            "remaining": remaining,
        }

        if over:
            raise QuotaExceededError(
                plan_tier=sub.plan_tier,
                used=used,
                limit=plan.packs_per_month,
            )

        return result

    async def record_usage(self, user_id: uuid.UUID, job_run_id: uuid.UUID) -> UsageRecord:
        """
        Record that a pack was completed. Idempotent — safe to call multiple times
        for the same job_run (unique constraint on job_run_id).
        """
        sub = await self._get_or_create_subscription(user_id)

        # Check for existing record (idempotency)
        existing = await self._db.execute(
            select(UsageRecord).where(UsageRecord.job_run_id == job_run_id)
        )
        existing_record = existing.scalar_one_or_none()
        if existing_record:
            logger.info("usage_already_recorded", extra={"job_run_id": str(job_run_id)})
            return existing_record

        record = UsageRecord(
            subscription_id=sub.id,
            user_id=user_id,
            job_run_id=job_run_id,
            plan_tier_at_time=sub.plan_tier,
            billing_period_start=sub.current_period_start,
            billing_period_end=sub.current_period_end,
        )
        self._db.add(record)
        await self._db.flush()

        logger.info(
            "usage_recorded",
            extra={"user_id": str(user_id), "job_run_id": str(job_run_id)},
        )
        return record

    # ── Status ────────────────────────────────────────────────────────────

    async def get_subscription_status(self, user_id: uuid.UUID) -> dict:
        """
        Full subscription + usage state for the UI dashboard.
        """
        sub = await self._get_or_create_subscription(user_id)
        plan = get_plan(sub.plan_tier)
        used = await self._count_usage_this_period(sub)

        return {
            "plan_tier": sub.plan_tier,
            "plan_display_name": plan.display_name,
            "status": sub.status,
            "is_active": sub.is_active,
            "is_paid": sub.is_paid,
            "packs_per_month": plan.packs_per_month,
            "used_this_period": used,
            "remaining": plan.quota_remaining(used),
            "features": {
                "company_intel": plan.company_intel,
                "salary_data": plan.salary_data,
                "networking": plan.networking,
                "positioning": plan.positioning,
                "priority_queue": plan.priority_queue,
            },
            "current_period_start": sub.current_period_start,
            "current_period_end": sub.current_period_end,
            "cancel_at_period_end": sub.cancel_at_period_end,
            "stripe_customer_id": sub.stripe_customer_id,
        }

    # ── Private helpers ───────────────────────────────────────────────────

    async def _get_or_create_subscription(self, user_id: uuid.UUID) -> Subscription:
        result = await self._db.execute(
            select(Subscription).where(Subscription.user_id == user_id)
        )
        sub = result.scalar_one_or_none()
        if sub is None:
            sub = await self.provision_free_subscription(user_id)
        return sub

    async def _find_by_stripe_subscription(self, stripe_sub_id: str) -> Subscription | None:
        result = await self._db.execute(
            select(Subscription).where(
                Subscription.stripe_subscription_id == stripe_sub_id
            )
        )
        return result.scalar_one_or_none()

    async def _count_usage_this_period(self, sub: Subscription) -> int:
        """
        Count packs used in the current billing period.
        For FREE tier (no Stripe period), uses calendar month.
        """
        now = datetime.now(UTC)

        if sub.current_period_start and sub.current_period_end:
            # Paid plan: use Stripe billing period
            period_start = sub.current_period_start
            period_end = sub.current_period_end
        else:
            # Free plan: use calendar month
            period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            period_end = now

        result = await self._db.execute(
            select(func.count(UsageRecord.id)).where(
                UsageRecord.subscription_id == sub.id,
                UsageRecord.created_at >= period_start,
                UsageRecord.created_at <= period_end,
            )
        )
        return result.scalar_one() or 0
