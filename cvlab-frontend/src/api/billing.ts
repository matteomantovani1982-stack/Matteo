import { api } from './client'

export interface BillingStatus {
  plan_tier: string
  plan_display_name: string
  status: string
  is_active: boolean
  is_paid: boolean
  packs_per_month: number | null
  used_this_period: number
  remaining: number | null
  features: {
    company_intel: boolean
    salary_data: boolean
    networking: boolean
    positioning: boolean
    priority_queue: boolean
  }
  current_period_start: string | null
  current_period_end: string | null
  cancel_at_period_end: boolean
  stripe_customer_id: string | null
}

export interface Plan {
  tier: string
  display_name: string
  packs_per_month: number | null
  price_monthly_usd: number
  features: {
    company_intel: boolean
    salary_data: boolean
    networking: boolean
    positioning: boolean
    priority_queue: boolean
  }
}

export const billingApi = {
  status: async (): Promise<BillingStatus> => {
    const res = await api.get<BillingStatus>('/api/v1/billing/status')
    return res.data
  },

  plans: async (): Promise<Plan[]> => {
    const res = await api.get<Plan[]>('/api/v1/billing/plans')
    return res.data
  },

  checkout: async (tier: string, returnUrl: string): Promise<{ checkout_url: string }> => {
    const res = await api.post('/api/v1/billing/checkout', { tier, return_url: returnUrl })
    return res.data
  },

  portal: async (returnUrl: string): Promise<{ portal_url: string }> => {
    const res = await api.post('/api/v1/billing/portal', { return_url: returnUrl })
    return res.data
  },
}
