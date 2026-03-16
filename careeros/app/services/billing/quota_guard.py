"""
app/services/billing/quota_guard.py

FastAPI dependency for quota enforcement.

Usage in job creation route:
    async def create_job_run(
        payload: JobRunCreate,
        db: DB,
        current_user: CurrentUser,
        _quota: QuotaCheck,      ← raises 402 if over limit
    ): ...

Two guards:
  QuotaCheck  — enforces pack quota (raises 402 if exceeded)
  FeatureCheck — enforces feature access per plan (raises 403 if not on plan)

The quota check runs BEFORE the job run is created.
Usage is recorded AFTER the job run completes (in render_docx task).
"""

from typing import Annotated

from fastapi import Depends, HTTPException, status

from app.dependencies import CurrentUser, DB
from app.models.subscription import PlanTier
from app.services.billing.billing_service import BillingService, QuotaExceededError
from app.services.billing.plans import get_plan


async def check_pack_quota(db: DB, current_user: CurrentUser) -> dict:
    """
    Dependency: verify user has quota remaining for a new job run.

    Raises:
        402 Payment Required — quota exhausted for current plan
    """
    svc = BillingService(db=db)
    try:
        quota_info = await svc.check_quota(current_user.id)
    except QuotaExceededError as e:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"Monthly quota exceeded: {e.used}/{e.limit} packs used on {e.plan_tier} plan. "
                f"Upgrade to generate more packs."
            ),
            headers={"X-Quota-Used": str(e.used), "X-Quota-Limit": str(e.limit)},
        )
    return quota_info


# Type alias — use this in route signatures
QuotaCheck = Annotated[dict, Depends(check_pack_quota)]


async def check_feature_positioning(db: DB, current_user: CurrentUser) -> None:
    """
    Dependency: verify user's plan includes PositioningStrategy output.
    FREE plan excludes positioning.
    """
    svc = BillingService(db=db)
    status_data = await svc.get_subscription_status(current_user.id)
    plan = get_plan(PlanTier(status_data["plan_tier"]))

    if not plan.positioning:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "feature_not_available",
                "message": (
                    "Positioning Strategy is not available on the Free plan. "
                    "Upgrade to Starter or higher."
                ),
                "feature": "positioning",
                "upgrade_url": "/billing/checkout",
            },
        )


PositioningFeature = Annotated[None, Depends(check_feature_positioning)]


async def get_plan_features(db: DB, current_user: CurrentUser) -> dict:
    """
    Dependency: returns the feature flags for the current user's plan.
    Use to conditionally include/exclude outputs in a job run.
    """
    svc = BillingService(db=db)
    status_data = await svc.get_subscription_status(current_user.id)
    plan = get_plan(PlanTier(status_data["plan_tier"]))
    return {
        "company_intel": plan.company_intel,
        "salary_data": plan.salary_data,
        "networking": plan.networking,
        "positioning": plan.positioning,
        "priority_queue": plan.priority_queue,
        "plan_tier": plan.tier,
    }


PlanFeatures = Annotated[dict, Depends(get_plan_features)]
