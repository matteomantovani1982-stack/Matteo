"""
app/services/billing/plans.py

Plan definitions — single source of truth for pricing, quotas, and features.

Changing a plan: update PLANS dict only. Everything else reads from here.
Adding a plan: add to PLANS and PlanTier enum in subscription.py.

Feature flags per plan:
  packs_per_month   — max Application Intelligence Packs per billing period
                      (None = unlimited)
  company_intel     — include company intelligence report
  salary_data       — include salary benchmarks
  networking        — include networking activation plan
  positioning       — include PositioningStrategy ("how to win this role")
  priority_queue    — job runs processed before FREE/STARTER
  pdf_support       — accept PDF CV uploads (Sprint 8, future)
"""

from dataclasses import dataclass, field
from app.models.subscription import PlanTier


@dataclass(frozen=True)
class Plan:
    tier: PlanTier
    display_name: str
    packs_per_month: int | None      # None = unlimited
    price_monthly_usd: float         # 0 = free
    stripe_price_id_monthly: str     # env-specific — set in config
    stripe_price_id_annual: str | None

    # Feature flags
    company_intel: bool = True
    salary_data: bool = True
    networking: bool = True
    positioning: bool = True
    priority_queue: bool = False
    pdf_support: bool = False

    def has_feature(self, feature: str) -> bool:
        return getattr(self, feature, False)

    def quota_remaining(self, used_this_period: int) -> int | None:
        """
        Returns packs remaining this period, or None if unlimited.
        Returns 0 if quota exhausted.
        """
        if self.packs_per_month is None:
            return None
        return max(0, self.packs_per_month - used_this_period)

    def is_over_quota(self, used_this_period: int) -> bool:
        if self.packs_per_month is None:
            return False
        return used_this_period >= self.packs_per_month


PLANS: dict[PlanTier, Plan] = {
    PlanTier.FREE: Plan(
        tier=PlanTier.FREE,
        display_name="Free",
        packs_per_month=20,
        price_monthly_usd=0.0,
        stripe_price_id_monthly="",        # No Stripe price for free
        stripe_price_id_annual=None,
        company_intel=True,
        salary_data=True,
        networking=True,
        positioning=True,
        priority_queue=False,
    ),
    PlanTier.STARTER: Plan(
        tier=PlanTier.STARTER,
        display_name="Starter",
        packs_per_month=10,
        price_monthly_usd=19.0,
        stripe_price_id_monthly="price_starter_monthly",   # Override in config
        stripe_price_id_annual="price_starter_annual",
        company_intel=True,
        salary_data=True,
        networking=True,
        positioning=True,
        priority_queue=False,
    ),
    PlanTier.PRO: Plan(
        tier=PlanTier.PRO,
        display_name="Pro",
        packs_per_month=30,
        price_monthly_usd=49.0,
        stripe_price_id_monthly="price_pro_monthly",
        stripe_price_id_annual="price_pro_annual",
        company_intel=True,
        salary_data=True,
        networking=True,
        positioning=True,
        priority_queue=True,
    ),
    PlanTier.UNLIMITED: Plan(
        tier=PlanTier.UNLIMITED,
        display_name="Unlimited",
        packs_per_month=None,
        price_monthly_usd=99.0,
        stripe_price_id_monthly="price_unlimited_monthly",
        stripe_price_id_annual="price_unlimited_annual",
        company_intel=True,
        salary_data=True,
        networking=True,
        positioning=True,
        priority_queue=True,
    ),
}

# Reverse lookup: Stripe price_id → PlanTier
# Populated from PLANS dict — single source of truth
PRICE_TO_TIER: dict[str, PlanTier] = {}
for _plan in PLANS.values():
    if _plan.stripe_price_id_monthly:
        PRICE_TO_TIER[_plan.stripe_price_id_monthly] = _plan.tier
    if _plan.stripe_price_id_annual:
        PRICE_TO_TIER[_plan.stripe_price_id_annual] = _plan.tier


def get_plan(tier: PlanTier) -> Plan:
    return PLANS[tier]


def get_plan_by_price_id(price_id: str) -> Plan | None:
    tier = PRICE_TO_TIER.get(price_id)
    return PLANS.get(tier) if tier else None
