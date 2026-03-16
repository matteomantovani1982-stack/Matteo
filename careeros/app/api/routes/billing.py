"""
app/api/routes/billing.py

Billing endpoints.

Routes:
  GET  /api/v1/billing/status               — current plan + usage
  GET  /api/v1/billing/plans                — all available plans
  POST /api/v1/billing/checkout             — create Stripe checkout session
  POST /api/v1/billing/portal               — create Stripe billing portal session
  POST /api/v1/billing/webhook              — Stripe webhook receiver (no auth)
"""

import structlog

from fastapi import APIRouter, Header, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.config import settings
from app.dependencies import DB, CurrentUser
from app.models.subscription import PlanTier
from app.services.billing.billing_service import BillingService, QuotaExceededError
from app.services.billing.plans import PLANS

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/v1/billing", tags=["Billing"])


# ── Request / response models ─────────────────────────────────────────────────

class CheckoutRequest(BaseModel):
    price_id: str = Field(..., description="Stripe Price ID to subscribe to")
    success_url: str = Field(..., description="URL to redirect after successful payment")
    cancel_url: str = Field(..., description="URL to redirect if user cancels checkout")


class PortalRequest(BaseModel):
    return_url: str = Field(..., description="URL to return to after managing subscription")


class CheckoutResponse(BaseModel):
    checkout_url: str


class PortalResponse(BaseModel):
    portal_url: str


class PlanFeatures(BaseModel):
    company_intel: bool
    salary_data: bool
    networking: bool
    positioning: bool
    priority_queue: bool


class PlanResponse(BaseModel):
    tier: str
    display_name: str
    packs_per_month: int | None
    price_monthly_usd: float
    features: PlanFeatures


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get(
    "/plans",
    response_model=list[PlanResponse],
    summary="Get all available plans",
    description="Returns plan definitions including quotas, pricing, and features.",
)
async def list_plans() -> list[PlanResponse]:
    return [
        PlanResponse(
            tier=plan.tier,
            display_name=plan.display_name,
            packs_per_month=plan.packs_per_month,
            price_monthly_usd=plan.price_monthly_usd,
            features=PlanFeatures(
                company_intel=plan.company_intel,
                salary_data=plan.salary_data,
                networking=plan.networking,
                positioning=plan.positioning,
                priority_queue=plan.priority_queue,
            ),
        )
        for plan in PLANS.values()
    ]


@router.get(
    "/status",
    summary="Get current subscription and usage",
)
async def get_billing_status(db: DB, current_user: CurrentUser) -> dict:
    svc = BillingService(db=db)
    return await svc.get_subscription_status(current_user.id)


@router.post(
    "/checkout",
    response_model=CheckoutResponse,
    summary="Create Stripe checkout session",
    description=(
        "Returns a Stripe-hosted checkout URL. "
        "Redirect the user to this URL to complete payment. "
        "On success, Stripe redirects to success_url and fires webhook."
    ),
)
async def create_checkout(
    payload: CheckoutRequest,
    db: DB,
    current_user: CurrentUser,
) -> CheckoutResponse:
    svc = BillingService(db=db)
    try:
        from app.services.billing.stripe_client import StripeError
        url = await svc.create_checkout_session(
            user_id=current_user.id,
            email=current_user.email,
            full_name=current_user.full_name,
            price_id=payload.price_id,
            success_url=payload.success_url,
            cancel_url=payload.cancel_url,
        )
        await db.commit()
    except StripeError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)

    return CheckoutResponse(checkout_url=url)


@router.post(
    "/portal",
    response_model=PortalResponse,
    summary="Create Stripe billing portal session",
    description=(
        "Returns a Stripe-hosted portal URL. "
        "Redirect the user here to manage or cancel their subscription."
    ),
)
async def create_portal(
    payload: PortalRequest,
    db: DB,
    current_user: CurrentUser,
) -> PortalResponse:
    svc = BillingService(db=db)
    try:
        from app.services.billing.stripe_client import StripeError
        url = await svc.create_portal_session(
            user_id=current_user.id,
            return_url=payload.return_url,
        )
    except (StripeError, ValueError) as e:
        msg = getattr(e, "user_message", None) or str(e)
        raise HTTPException(status_code=400, detail=msg)

    return PortalResponse(portal_url=url)


@router.post(
    "/webhook",
    status_code=status.HTTP_200_OK,
    summary="Stripe webhook receiver",
    description=(
        "Receives Stripe webhook events. "
        "Signature verified via HMAC-SHA256 before processing. "
        "This endpoint must NOT require authentication."
    ),
    include_in_schema=False,  # Hide from public docs
)
async def stripe_webhook(
    request: Request,
    db: DB,
    stripe_signature: str | None = Header(default=None, alias="Stripe-Signature"),
) -> dict:
    if not stripe_signature:
        raise HTTPException(status_code=400, detail="Missing Stripe-Signature header")

    body = await request.body()

    from app.services.billing.stripe_client import StripeClient, StripeError
    try:
        event = StripeClient.construct_event(
            payload=body,
            sig_header=stripe_signature,
            webhook_secret=settings.stripe_webhook_secret,
        )
    except StripeError as e:
        logger.warning("webhook_signature_failed", extra={"error": e.message})
        raise HTTPException(status_code=400, detail=e.message)

    svc = BillingService(db=db)
    try:
        result = await svc.handle_webhook_event(event)
        await db.commit()
        logger.info("webhook_processed", extra={"type": event.get("type"), "result": result})
    except Exception as e:
        logger.error("webhook_processing_failed", extra={"error": str(e)})
        # Return 200 anyway — Stripe will retry on 4xx/5xx, not on 200
        # Log the error for manual investigation
        return {"status": "error", "detail": str(e)}

    return {"status": "ok", "result": result}


@router.post(
    "/dev/grant-credits",
    summary="DEV ONLY — grant free packs for testing",
    include_in_schema=True,
)
async def dev_grant_credits(
    db: DB,
    current_user: CurrentUser,
) -> dict:
    """Grant Pro plan for 30 days — dev/demo use only."""
    from app.models.subscription import Subscription, PlanTier
    from sqlalchemy import select
    from datetime import UTC, datetime, timedelta

    result = await db.execute(
        select(Subscription).where(Subscription.user_id == current_user.id)
    )
    sub = result.scalar_one_or_none()

    now = datetime.now(UTC)
    if sub:
        sub.plan_tier = PlanTier.PRO
        sub.current_period_start = now
        sub.current_period_end = now + timedelta(days=30)
    else:
        db.add(Subscription(
            user_id=current_user.id,
            plan_tier=PlanTier.PRO,
            current_period_start=now,
            current_period_end=now + timedelta(days=30),
        ))
    await db.commit()
    return {"status": "ok", "message": "Pro plan granted for 30 days — unlimited packs"}
