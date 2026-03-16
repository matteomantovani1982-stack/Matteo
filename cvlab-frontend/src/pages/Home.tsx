import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '../store/auth'
import { useQuery } from '@tanstack/react-query'
import { profileApi } from '../api/profile'
import s from './Home.module.css'

export default function Home() {
  const navigate = useNavigate()
  const { user } = useAuthStore()
  const { data: usage } = useQuery({ queryKey: ['usage'], queryFn: profileApi.usage, staleTime: 60_000 })

  const firstName = user?.full_name?.split(' ')[0] ?? 'there'
  const hour = new Date().getHours()
  const greeting = hour < 12 ? 'Good morning' : hour < 17 ? 'Good afternoon' : 'Good evening'

  return (
    <div className={s.page}>

      {/* Hero */}
      <div className={s.hero}>
        <div className={s.heroEyebrow}>⚡ StealthRole</div>
        <h1 className={s.heroTitle}>{greeting}, {firstName}.</h1>
        <p className={s.heroSub}>
          Your next role — before the market knows you're looking.
        </p>
      </div>

      {/* What is StealthRole */}
      <div className={s.explainer}>
        <div className={s.explainerCard}>
          <div className={s.explainerIcon}>🕵️</div>
          <div className={s.explainerTitle}>Move in stealth</div>
          <div className={s.explainerText}>
            You're employed. You're good. But you're selectively exploring.
            StealthRole lets you research, prepare, and apply — without anyone knowing.
          </div>
        </div>
        <div className={s.explainerCard}>
          <div className={s.explainerIcon}>🎯</div>
          <div className={s.explainerTitle}>Beat the queue</div>
          <div className={s.explainerText}>
            Most applicants spend 20 minutes copy-pasting CVs. You arrive with a tailored CV,
            company intel, named contacts, and a salary benchmark — in 60 seconds.
          </div>
        </div>
        <div className={s.explainerCard}>
          <div className={s.explainerIcon}>📡</div>
          <div className={s.explainerTitle}>Find the hidden market</div>
          <div className={s.explainerText}>
            80% of senior roles are never posted. We monitor funding rounds, leadership changes,
            and expansion signals — and alert you before the job exists.
          </div>
        </div>
      </div>

      {/* Quick actions */}
      <div className={s.section}>
        <div className={s.sectionTitle}>What do you want to do?</div>
        <div className={s.actions}>

          <button className={s.actionCard} onClick={() => navigate('/applications/new')}>
            <div className={s.actionIcon}>🚀</div>
            <div className={s.actionContent}>
              <div className={s.actionTitle}>Generate Intelligence Pack</div>
              <div className={s.actionDesc}>
                Paste a job description → get a tailored CV, company intel, named contacts,
                salary data, and interview strategy. Takes 60 seconds.
              </div>
            </div>
            <div className={s.actionArrow}>→</div>
          </button>

          <button className={s.actionCard} onClick={() => navigate('/applications/new?mode=cv_only')}>
            <div className={s.actionIcon}>✏️</div>
            <div className={s.actionContent}>
              <div className={s.actionTitle}>Modify my CV for a role</div>
              <div className={s.actionDesc}>
                Have a JD or a few lines about a role? We'll tailor your CV to it — fast.
                No full intel pack, just the CV.
              </div>
            </div>
            <div className={s.actionArrow}>→</div>
          </button>

          <button className={s.actionCard} onClick={() => navigate('/dashboard')}>
            <div className={s.actionIcon}>🔍</div>
            <div className={s.actionContent}>
              <div className={s.actionTitle}>Scout jobs now</div>
              <div className={s.actionDesc}>
                Browse live scouted roles matched to your profile. Rate them, generate packs,
                apply directly from the card.
              </div>
            </div>
            <div className={s.actionArrow}>→</div>
          </button>

          <button className={s.actionCard} onClick={() => navigate('/applications')}>
            <div className={s.actionIcon}>📋</div>
            <div className={s.actionContent}>
              <div className={s.actionTitle}>Track my applications</div>
              <div className={s.actionDesc}>
                Kanban board across Watching → Applied → Interviewing → Offer.
                Notes, follow-ups, and status in one place.
              </div>
            </div>
            <div className={s.actionArrow}>→</div>
          </button>

          <button className={s.actionCard} onClick={() => navigate('/profile')}>
            <div className={s.actionIcon}>👤</div>
            <div className={s.actionContent}>
              <div className={s.actionTitle}>Set my job preferences</div>
              <div className={s.actionDesc}>
                Tell us your target regions, roles, seniority, sectors, and salary.
                The scout engine uses this to find your best matches automatically.
              </div>
            </div>
            <div className={s.actionArrow}>→</div>
          </button>

        </div>
      </div>

      {/* Usage strip */}
      {usage && (
        <div className={s.usageStrip}>
          <span className={s.usagePlan}>{usage.plan} plan</span>
          <span className={s.usageSep}>·</span>
          <span className={s.usageCount}>
            <strong>{usage.runs_used_this_month}</strong> of {usage.monthly_run_limit ?? '∞'} packs used this month
          </span>
          {usage.plan === 'free' && (
            <>
              <span className={s.usageSep}>·</span>
              <button className={s.upgradeLink} onClick={() => navigate('/billing')}>
                Upgrade for unlimited →
              </button>
            </>
          )}
        </div>
      )}

    </div>
  )
}
