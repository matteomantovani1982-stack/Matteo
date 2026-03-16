import { useQuery, useMutation } from '@tanstack/react-query'
import { billingApi } from '../api/billing'
import { Button } from '../components/ui/Button'
import s from './Billing.module.css'

const PLANS = [
  {
    tier: 'free',
    name: 'Free',
    price: 0,
    period: null,
    description: 'Try before you commit',
    cta: null,
    sections: [
      {
        label: 'What you get',
        items: [
          { text: '2 application packs total', included: true },
          { text: 'Upload your CV', included: true },
          { text: 'Paste a job description', included: true },
          { text: 'Tailored CV (DOCX download)', included: true },
          { text: 'Cover letter', included: false },
          { text: 'Positioning strategy', included: false },
          { text: 'Company intelligence', included: false },
          { text: 'Salary benchmarks', included: false },
          { text: 'Recruiter & hiring manager finder', included: false },
          { text: 'Outreach messages', included: false },
          { text: 'Interview prep pack', included: false },
        ],
      },
    ],
  },
  {
    tier: 'sprint',
    name: 'Job Search Sprint',
    price: 49,
    period: '30 days',
    description: 'Everything you need to land a job',
    highlight: true,
    cta: 'Start my Sprint — $49',
    sections: [
      {
        label: 'Application engine',
        items: [
          { text: 'Unlimited applications for 30 days', included: true },
          { text: 'Tailored CV per application (DOCX)', included: true },
          { text: 'Cover letter, tailored to each JD', included: true },
          { text: 'Positioning strategy — how to win this role', included: true },
        ],
      },
      {
        label: 'Intelligence reports',
        items: [
          { text: 'Company intelligence — culture, priorities, red flags', included: true },
          { text: 'Salary benchmarks for the role & region', included: true },
          { text: '7-day action plan', included: true },
        ],
      },
      {
        label: 'Recruiter access',
        items: [
          { text: 'Hiring manager identified by name', included: true },
          { text: 'Recruiter identified by name', included: true },
          { text: 'Referral connections surfaced', included: true },
          { text: 'LinkedIn outreach messages written for you', included: true },
          { text: 'Warm intro request drafted', included: true },
        ],
      },
      {
        label: 'Interview prep',
        items: [
          { text: 'Likely interview questions for this role', included: true },
          { text: 'Suggested answers based on your CV', included: true },
          { text: 'Salary negotiation guidance', included: true },
        ],
      },
    ],
  },
  {
    tier: 'interview',
    name: 'Interview Pack',
    price: 39,
    period: 'one-time',
    description: 'Once you land the interview',
    cta: 'Get Interview Pack — $39',
    sections: [
      {
        label: 'Before the interview',
        items: [
          { text: 'Deep company briefing (10+ sources)', included: true },
          { text: 'Interviewer background research', included: true },
          { text: 'Culture & values analysis', included: true },
          { text: 'Recent news & priorities', included: true },
        ],
      },
      {
        label: 'In the interview',
        items: [
          { text: 'Likely questions for this specific role', included: true },
          { text: 'Suggested answers from your CV & profile', included: true },
          { text: 'Case study framework if relevant', included: true },
          { text: 'Questions to ask the interviewer', included: true },
        ],
      },
      {
        label: 'After the interview',
        items: [
          { text: 'Thank-you note drafted', included: true },
          { text: 'Salary negotiation script', included: true },
          { text: 'Offer evaluation checklist', included: true },
        ],
      },
    ],
  },
]

export default function Billing() {
  const { data: status, isLoading } = useQuery({
    queryKey: ['billing-status'],
    queryFn: billingApi.status,
    retry: false,
  })

  const portalMut = useMutation({
    mutationFn: () => billingApi.portal(window.location.href),
    onSuccess: (data) => { window.location.href = data.portal_url },
  })

  const checkoutMut = useMutation({
    mutationFn: (tier: string) => billingApi.checkout(tier, window.location.href),
    onSuccess: (data) => { window.location.href = data.checkout_url },
  })

  const devGrantMut = useMutation({
    mutationFn: () => import('../api/client').then(m => m.api.post('/api/v1/billing/dev/grant-credits')),
    onSuccess: () => window.location.reload(),
  })

  const currentTier = status?.plan_tier?.toLowerCase() ?? 'free'
  const used = status?.used_this_period ?? 0
  const total = status?.packs_per_month ?? 2
  const pct = total ? Math.min(100, Math.round((used / total) * 100)) : 0

  return (
    <div className={s.page}>
      <div className={s.header}>
        <h1 className={s.title}>Plans & Billing</h1>
        <p className={s.subtitle}>
          "If this gets me a $120k job, $49 is nothing." — that's how to think about this.
        </p>
      </div>

      {/* Current usage */}
      {!isLoading && status && (
        <div className={s.usageCard}>
          <div className={s.usageTop}>
            <div>
              <span className={s.planBadge}>{status.plan_display_name ?? 'Free'}</span>
              {status.is_paid && <span className={s.activeTag}>Active</span>}
            </div>
            {status.is_paid && (
              <Button variant="ghost" loading={portalMut.isPending} onClick={() => portalMut.mutate()}>
                Manage subscription
              </Button>
            )}
          </div>
          <div className={s.usageLabel}>
            {total === null ? `${used} packs used · Unlimited` : `${used} of ${total} packs used`}
          </div>
          <div className={s.usageTrack}>
            <div className={s.usageFill} style={{ width: `${pct}%`, background: pct >= 80 ? '#dc2626' : '#2563eb' }} />
          </div>
          {status.current_period_end && (
            <div className={s.periodNote}>
              {status.cancel_at_period_end ? 'Cancels' : 'Renews'}{' '}
              {new Date(status.current_period_end).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}
            </div>
          )}
        </div>
      )}

      {/* Plan cards */}
      <div className={s.plansGrid}>
        {PLANS.map(plan => {
          const isCurrent = plan.tier === currentTier
          return (
            <div key={plan.tier} className={`${s.planCard} ${plan.highlight ? s.planHighlight : ''} ${isCurrent ? s.planCurrent : ''}`}>
              {plan.highlight && <div className={s.popularBadge}>Most popular</div>}
              {isCurrent && <div className={s.currentBadge}>Your plan</div>}

              <div className={s.planTop}>
                <div className={s.planName}>{plan.name}</div>
                <div className={s.planDesc}>{plan.description}</div>
                <div className={s.planPrice}>
                  {plan.price === 0
                    ? <span className={s.priceZero}>Free</span>
                    : <><span className={s.priceAmt}>${plan.price}</span><span className={s.pricePer}>{plan.period === 'one-time' ? ' one-time' : ' / 30 days'}</span></>
                  }
                </div>
              </div>

              <div className={s.sections}>
                {plan.sections.map(section => (
                  <div key={section.label} className={s.section}>
                    <div className={s.sectionLabel}>{section.label}</div>
                    <ul className={s.featureList}>
                      {section.items.map(item => (
                        <li key={item.text} className={`${s.featureItem} ${!item.included ? s.featureOff : ''}`}>
                          <span className={item.included ? s.checkOn : s.checkOff}>
                            {item.included ? '✓' : '✕'}
                          </span>
                          {item.text}
                        </li>
                      ))}
                    </ul>
                  </div>
                ))}
              </div>

              <div className={s.planCta}>
                {isCurrent ? (
                  <div className={s.currentLabel}>✓ Current plan</div>
                ) : plan.cta ? (
                  <Button
                    variant="primary"
                    loading={checkoutMut.isPending}
                    onClick={() => checkoutMut.mutate(plan.tier)}
                    style={{ width: '100%', justifyContent: 'center' }}
                  >
                    {plan.cta}
                  </Button>
                ) : null}
              </div>
            </div>
          )
        })}
      </div>

      <p className={s.note}>Payments processed securely by Stripe · Cancel anytime · No hidden fees</p>
    </div>
  )
}
