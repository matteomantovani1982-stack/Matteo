"""
tests/test_billing.py

Tests for billing: plan logic, quota enforcement, Stripe webhook handling.
"""

import hashlib
import hmac
import json
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.subscription import PlanTier, SubscriptionStatus
from app.services.billing.plans import (
    PLANS,
    Plan,
    get_plan,
    get_plan_by_price_id,
)
from app.services.billing.stripe_client import StripeClient, StripeError


# ── Plan logic tests ──────────────────────────────────────────────────────────

class TestPlanDefinitions:

    def test_all_four_plans_defined(self):
        assert PlanTier.FREE in PLANS
        assert PlanTier.STARTER in PLANS
        assert PlanTier.PRO in PLANS
        assert PlanTier.UNLIMITED in PLANS

    def test_free_plan_has_no_positioning(self):
        plan = get_plan(PlanTier.FREE)
        assert plan.positioning is False

    def test_free_plan_has_no_company_intel(self):
        plan = get_plan(PlanTier.FREE)
        assert plan.company_intel is False

    def test_paid_plans_have_all_features(self):
        for tier in [PlanTier.STARTER, PlanTier.PRO, PlanTier.UNLIMITED]:
            plan = get_plan(tier)
            assert plan.positioning is True
            assert plan.company_intel is True
            assert plan.salary_data is True
            assert plan.networking is True

    def test_priority_queue_only_on_pro_and_unlimited(self):
        assert get_plan(PlanTier.FREE).priority_queue is False
        assert get_plan(PlanTier.STARTER).priority_queue is False
        assert get_plan(PlanTier.PRO).priority_queue is True
        assert get_plan(PlanTier.UNLIMITED).priority_queue is True

    def test_unlimited_has_no_pack_limit(self):
        plan = get_plan(PlanTier.UNLIMITED)
        assert plan.packs_per_month is None

    def test_free_plan_pack_limit(self):
        plan = get_plan(PlanTier.FREE)
        assert plan.packs_per_month == 3

    def test_quota_remaining_counts_correctly(self):
        plan = get_plan(PlanTier.FREE)  # 3 packs/month
        assert plan.quota_remaining(0) == 3
        assert plan.quota_remaining(2) == 1
        assert plan.quota_remaining(3) == 0
        assert plan.quota_remaining(5) == 0  # Never negative

    def test_quota_remaining_unlimited_returns_none(self):
        plan = get_plan(PlanTier.UNLIMITED)
        assert plan.quota_remaining(9999) is None

    def test_is_over_quota_free(self):
        plan = get_plan(PlanTier.FREE)
        assert plan.is_over_quota(0) is False
        assert plan.is_over_quota(2) is False
        assert plan.is_over_quota(3) is True
        assert plan.is_over_quota(10) is True

    def test_is_over_quota_unlimited_never_true(self):
        plan = get_plan(PlanTier.UNLIMITED)
        assert plan.is_over_quota(10000) is False

    def test_get_plan_by_price_id_returns_correct_plan(self):
        starter = get_plan(PlanTier.STARTER)
        found = get_plan_by_price_id(starter.stripe_price_id_monthly)
        assert found is not None
        assert found.tier == PlanTier.STARTER

    def test_get_plan_by_price_id_unknown_returns_none(self):
        assert get_plan_by_price_id("price_nonexistent_xxx") is None

    def test_plans_have_increasing_price(self):
        free = get_plan(PlanTier.FREE).price_monthly_usd
        starter = get_plan(PlanTier.STARTER).price_monthly_usd
        pro = get_plan(PlanTier.PRO).price_monthly_usd
        unlimited = get_plan(PlanTier.UNLIMITED).price_monthly_usd
        assert free < starter < pro < unlimited


# ── Stripe webhook tests ──────────────────────────────────────────────────────

class TestStripeWebhookVerification:

    def _make_signature(self, payload: bytes, secret: str, timestamp: int = None) -> str:
        """Helper: generate a valid Stripe-Signature header."""
        t = timestamp or int(time.time())
        signed = f"{t}.{payload.decode('utf-8')}"
        sig = hmac.new(secret.encode(), signed.encode(), hashlib.sha256).hexdigest()
        return f"t={t},v1={sig}"

    def _make_event(self, event_type: str = "checkout.session.completed") -> dict:
        return {
            "id": "evt_test_123",
            "type": event_type,
            "data": {"object": {"id": "cs_test_123"}},
        }

    def test_valid_signature_returns_event(self):
        secret = "whsec_test_secret"
        payload = json.dumps(self._make_event()).encode()
        sig = self._make_signature(payload, secret)
        event = StripeClient.construct_event(payload, sig, secret)
        assert event["type"] == "checkout.session.completed"

    def test_wrong_secret_raises_error(self):
        payload = json.dumps(self._make_event()).encode()
        sig = self._make_signature(payload, "correct_secret")
        with pytest.raises(StripeError, match="signature verification failed"):
            StripeClient.construct_event(payload, sig, "wrong_secret")

    def test_tampered_payload_raises_error(self):
        secret = "test_secret"
        payload = json.dumps(self._make_event()).encode()
        sig = self._make_signature(payload, secret)
        tampered = payload + b"extra"
        with pytest.raises(StripeError):
            StripeClient.construct_event(tampered, sig, secret)

    def test_expired_timestamp_raises_error(self):
        secret = "test_secret"
        payload = json.dumps(self._make_event()).encode()
        # 10 minutes ago — beyond the 5 minute tolerance
        old_timestamp = int(time.time()) - 601
        sig = self._make_signature(payload, secret, timestamp=old_timestamp)
        with pytest.raises(StripeError, match="too old"):
            StripeClient.construct_event(payload, sig, secret)

    def test_missing_signature_header_raises_error(self):
        payload = json.dumps(self._make_event()).encode()
        with pytest.raises(StripeError):
            StripeClient.construct_event(payload, "", "secret")

    def test_malformed_signature_header_raises_error(self):
        payload = json.dumps(self._make_event()).encode()
        with pytest.raises(StripeError):
            StripeClient.construct_event(payload, "not_a_valid_header", "secret")

    def test_invalid_json_payload_raises_error(self):
        secret = "test_secret"
        payload = b"not valid json {"
        sig = self._make_signature(payload, secret)
        with pytest.raises(StripeError, match="Invalid JSON"):
            StripeClient.construct_event(payload, sig, secret)

    def test_timestamp_within_tolerance_accepted(self):
        """4 minutes 50 seconds old — within 5 min window."""
        secret = "test_secret"
        payload = json.dumps(self._make_event()).encode()
        recent_timestamp = int(time.time()) - 290
        sig = self._make_signature(payload, secret, timestamp=recent_timestamp)
        event = StripeClient.construct_event(payload, sig, secret)
        assert event is not None


# ── BillingService tests ──────────────────────────────────────────────────────

class TestBillingService:

    def _make_service(self, subscription=None):
        from app.services.billing.billing_service import BillingService
        db = AsyncMock()

        result = MagicMock()
        result.scalar_one_or_none = MagicMock(return_value=subscription)
        result.scalar_one = MagicMock(return_value=0)
        db.execute = AsyncMock(return_value=result)
        db.get = AsyncMock(return_value=subscription)
        db.add = MagicMock()
        db.flush = AsyncMock()

        return BillingService(db=db), db

    def _make_subscription(self, tier=PlanTier.FREE, status=SubscriptionStatus.FREE):
        from app.models.subscription import Subscription
        sub = MagicMock(spec=Subscription)
        sub.id = uuid.uuid4()
        sub.user_id = uuid.uuid4()
        sub.plan_tier = tier
        sub.status = status
        sub.stripe_customer_id = None
        sub.stripe_subscription_id = None
        sub.current_period_start = None
        sub.current_period_end = None
        sub.is_active = True
        sub.is_paid = tier != PlanTier.FREE
        return sub

    @pytest.mark.asyncio
    async def test_provision_free_subscription(self):
        svc, db = self._make_service()
        user_id = uuid.uuid4()
        sub = await svc.provision_free_subscription(user_id)
        db.add.assert_called_once()
        db.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_quota_allowed_when_under_limit(self):
        sub = self._make_subscription(tier=PlanTier.FREE)
        svc, db = self._make_service(subscription=sub)

        # Mock usage count = 1 (under FREE limit of 3)
        result = MagicMock()
        result.scalar_one_or_none = MagicMock(return_value=sub)
        result.scalar_one = MagicMock(return_value=1)
        db.execute = AsyncMock(return_value=result)

        quota = await svc.check_quota(sub.user_id)
        assert quota["allowed"] is True
        assert quota["used_this_period"] == 1
        assert quota["remaining"] == 2

    @pytest.mark.asyncio
    async def test_check_quota_raises_when_over_limit(self):
        from app.services.billing.billing_service import QuotaExceededError
        sub = self._make_subscription(tier=PlanTier.FREE)
        svc, db = self._make_service(subscription=sub)

        # Mock usage count = 3 (at FREE limit)
        result = MagicMock()
        result.scalar_one_or_none = MagicMock(return_value=sub)
        result.scalar_one = MagicMock(return_value=3)
        db.execute = AsyncMock(return_value=result)

        with pytest.raises(QuotaExceededError) as exc_info:
            await svc.check_quota(sub.user_id)
        assert exc_info.value.limit == 3
        assert exc_info.value.used == 3

    @pytest.mark.asyncio
    async def test_check_quota_unlimited_always_allowed(self):
        sub = self._make_subscription(tier=PlanTier.UNLIMITED)
        svc, db = self._make_service(subscription=sub)

        # Even with 9999 packs used
        result = MagicMock()
        result.scalar_one_or_none = MagicMock(return_value=sub)
        result.scalar_one = MagicMock(return_value=9999)
        db.execute = AsyncMock(return_value=result)

        quota = await svc.check_quota(sub.user_id)
        assert quota["allowed"] is True
        assert quota["remaining"] is None

    @pytest.mark.asyncio
    async def test_get_subscription_status_structure(self):
        sub = self._make_subscription(tier=PlanTier.STARTER)
        svc, db = self._make_service(subscription=sub)

        result = MagicMock()
        result.scalar_one_or_none = MagicMock(return_value=sub)
        result.scalar_one = MagicMock(return_value=5)
        db.execute = AsyncMock(return_value=result)

        status = await svc.get_subscription_status(sub.user_id)

        assert "plan_tier" in status
        assert "used_this_period" in status
        assert "remaining" in status
        assert "features" in status
        assert "positioning" in status["features"]
        assert status["features"]["positioning"] is True


# ── QuotaExceededError tests ──────────────────────────────────────────────────

class TestQuotaExceededError:

    def test_error_message_includes_plan_and_counts(self):
        from app.services.billing.billing_service import QuotaExceededError
        err = QuotaExceededError(PlanTier.FREE, used=3, limit=3)
        assert "3/3" in str(err)
        assert "free" in str(err)

    def test_error_stores_attributes(self):
        from app.services.billing.billing_service import QuotaExceededError
        err = QuotaExceededError(PlanTier.STARTER, used=10, limit=10)
        assert err.plan_tier == PlanTier.STARTER
        assert err.used == 10
        assert err.limit == 10
