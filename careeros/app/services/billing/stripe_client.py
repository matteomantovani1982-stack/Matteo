"""
app/services/billing/stripe_client.py

Stripe API client built on httpx (already in deps — no stripe SDK needed).

Covers:
  - Create customer
  - Create checkout session (hosted payment page)
  - Create billing portal session (manage subscription)
  - Retrieve subscription
  - Construct + verify webhook events (HMAC-SHA256)

All methods are async — called from FastAPI route handlers.
Webhook verification is sync-safe (pure crypto, no I/O).
"""

import hashlib
import hmac
import json
import time
from typing import Any

import httpx

from app.config import settings

STRIPE_API_BASE = "https://api.stripe.com/v1"
STRIPE_API_VERSION = "2024-04-10"


class StripeError(Exception):
    """Raised on Stripe API errors. Contains HTTP status and error message."""
    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class StripeClient:

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=STRIPE_API_BASE,
            auth=(settings.stripe_secret_key, ""),
            headers={
                "Stripe-Version": STRIPE_API_VERSION,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=30.0,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self._client.aclose()

    async def close(self):
        await self._client.aclose()

    # ── Customer ──────────────────────────────────────────────────────────

    async def create_customer(self, email: str, user_id: str, name: str | None = None) -> dict:
        """Create a Stripe Customer. Returns customer object."""
        data: dict[str, str] = {
            "email": email,
            "metadata[user_id]": user_id,
        }
        if name:
            data["name"] = name
        return await self._post("/customers", data)

    async def get_customer(self, customer_id: str) -> dict:
        return await self._get(f"/customers/{customer_id}")

    # ── Checkout ──────────────────────────────────────────────────────────

    async def create_checkout_session(
        self,
        customer_id: str,
        price_id: str,
        success_url: str,
        cancel_url: str,
        user_id: str,
    ) -> dict:
        """
        Create a Stripe Checkout Session for subscription purchase.
        Returns session object with .url for redirect.
        """
        data = {
            "customer": customer_id,
            "mode": "subscription",
            "line_items[0][price]": price_id,
            "line_items[0][quantity]": "1",
            "success_url": success_url,
            "cancel_url": cancel_url,
            "metadata[user_id]": user_id,
            "allow_promotion_codes": "true",
            "billing_address_collection": "auto",
            "subscription_data[metadata][user_id]": user_id,
        }
        return await self._post("/checkout/sessions", data)

    # ── Billing portal ────────────────────────────────────────────────────

    async def create_billing_portal_session(
        self,
        customer_id: str,
        return_url: str,
    ) -> dict:
        """
        Create a Stripe Billing Portal session.
        User is redirected here to manage/cancel their subscription.
        """
        data = {
            "customer": customer_id,
            "return_url": return_url,
        }
        return await self._post("/billing_portal/sessions", data)

    # ── Subscription ──────────────────────────────────────────────────────

    async def get_subscription(self, subscription_id: str) -> dict:
        return await self._get(f"/subscriptions/{subscription_id}")

    async def cancel_subscription(self, subscription_id: str, at_period_end: bool = True) -> dict:
        """Cancel a subscription. Defaults to cancel at period end."""
        data = {"cancel_at_period_end": "true" if at_period_end else "false"}
        return await self._post(f"/subscriptions/{subscription_id}", data)

    # ── Webhook verification ──────────────────────────────────────────────

    @staticmethod
    def construct_event(payload: bytes, sig_header: str, webhook_secret: str) -> dict:
        """
        Verify Stripe webhook signature and return the event dict.

        Stripe signs webhooks with HMAC-SHA256.
        The signature header format: "t=timestamp,v1=signature,v1=signature2"

        Raises StripeError if:
          - Signature header is malformed
          - Signature doesn't match
          - Timestamp is more than 5 minutes old (replay attack protection)
        """
        try:
            parts = {k: v for k, v in (p.split("=", 1) for p in sig_header.split(","))}
            timestamp = parts.get("t")
            signatures = [v for k, v in parts.items() if k == "v1"]
        except (ValueError, AttributeError):
            raise StripeError("Invalid Stripe-Signature header format", 400)

        if not timestamp or not signatures:
            raise StripeError("Missing timestamp or signature in Stripe-Signature", 400)

        # Replay attack protection: reject events older than 5 minutes
        try:
            event_age = int(time.time()) - int(timestamp)
        except ValueError:
            raise StripeError("Invalid timestamp in Stripe-Signature", 400)

        if event_age > 300:
            raise StripeError(f"Webhook timestamp too old: {event_age}s", 400)

        # Compute expected signature
        signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
        expected = hmac.new(
            webhook_secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        # Constant-time comparison against all provided signatures
        if not any(hmac.compare_digest(expected, sig) for sig in signatures):
            raise StripeError("Webhook signature verification failed", 400)

        try:
            return json.loads(payload)
        except json.JSONDecodeError as e:
            raise StripeError(f"Invalid JSON in webhook payload: {e}", 400)

    # ── HTTP helpers ──────────────────────────────────────────────────────

    async def _post(self, path: str, data: dict) -> dict:
        response = await self._client.post(path, data=data)
        return self._handle_response(response)

    async def _get(self, path: str) -> dict:
        response = await self._client.get(path)
        return self._handle_response(response)

    @staticmethod
    def _handle_response(response: httpx.Response) -> dict:
        body = response.json()
        if response.status_code >= 400:
            error = body.get("error", {})
            raise StripeError(
                message=error.get("message", f"Stripe error {response.status_code}"),
                status_code=response.status_code,
            )
        return body
