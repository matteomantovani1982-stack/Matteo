import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { scoutApi } from '../api/scout'
import { profileApi } from '../api/profile'
import s from './Dashboard.module.css'

// ─── Types ────────────────────────────────────────────────────────────────────
interface Signal { signal_type: string; headline: string; detail: string; source_url: string; source_name: string; published_date: string }
interface OpportunityCard {
  id: string; company: string; sector: string; location: string; signals: Signal[]
  signal_summary: string; fit_score: number; fit_reasons: string[]; red_flags: string[]
  suggested_role: string; suggested_action: string; contact_name: string; contact_title: string
  salary_estimate: string; urgency: string; apply_url: string; is_posted: boolean; posted_title: string
}
interface LiveOpening {
  title: string; company: string; snippet: string; url: string; source: string
  date: string; status: 'current' | 'imminent' | 'strategic'; is_posted: boolean
}
interface ScoutData {
  opportunities: OpportunityCard[]; live_openings: LiveOpening[]
  signals_detected: number; is_demo: boolean; scored_by: string
}

// ─── Signal type icons/labels ──────────────────────────────────────────────
const SIG_META: Record<string, { icon: string; label: string }> = {
  funding:    { icon: '💰', label: 'Funding' },
  leadership: { icon: '👤', label: 'Leadership change' },
  expansion:  { icon: '🌍', label: 'Expansion' },
  velocity:   { icon: '📈', label: 'Hiring spike' },
  distress:   { icon: '⚠️', label: 'Restructure' },
}

// ─── Urgency config ────────────────────────────────────────────────────────
const URGENCY: Record<string, { color: string; label: string; dot: string }> = {
  high:   { color: '#ef4444', label: 'Act now', dot: s.dotRed   },
  medium: { color: '#f59e0b', label: 'This week', dot: s.dotAmber },
  low:    { color: '#6b7280', label: 'Monitor', dot: s.dotGrey  },
}

// ─── Opening status config ─────────────────────────────────────────────────
const STATUS_META = {
  current:   { bg: '#dcfce7', border: '#86efac', badge: '#16a34a', label: '🟢 Current vacancy',    sub: 'Posted now — apply directly' },
  imminent:  { bg: '#fefce8', border: '#fde047', badge: '#ca8a04', label: '🟡 Imminent opening',   sub: 'Signal detected — role likely forming' },
  strategic: { bg: '#fef2f2', border: '#fca5a5', badge: '#dc2626', label: '🔴 Strategic horizon',  sub: 'Early signal — position yourself now' },
}

// ─── OpportunityCard ─────────────────────────────────────────────────────────
function OppCard({ card, onGenerate }: { card: OpportunityCard; onGenerate: (c: OpportunityCard) => void }) {
  const [open, setOpen] = useState(false)
  const urg = URGENCY[card.urgency] || URGENCY.low
  const sigTypes = [...new Set(card.signals.map(s => s.signal_type))]

  return (
    <div className={s.oppCard} style={{ borderLeft: `4px solid ${urg.color}` }}>
      <div className={s.oppTop}>
        <div className={s.oppLeft}>
          <div className={s.oppCompany}>{card.company}</div>
          <div className={s.oppMeta}>{card.sector && <span>{card.sector}</span>}{card.location && <span>· {card.location}</span>}</div>
          <div className={s.oppSignalRow}>
            {sigTypes.map(t => <span key={t} className={s.sigBadge}>{SIG_META[t]?.icon} {SIG_META[t]?.label}</span>)}
          </div>
        </div>
        <div className={s.oppRight}>
          <div className={s.fitScore} style={{ color: card.fit_score >= 70 ? '#059669' : card.fit_score >= 50 ? '#d97706' : '#6b7280' }}>
            {card.fit_score}<span className={s.fitOf}>/100</span>
          </div>
          <div className={s.urgBadge} style={{ background: urg.color + '20', color: urg.color }}>{urg.label}</div>
        </div>
      </div>

      <div className={s.sigSummary}>{card.signal_summary}</div>

      {card.suggested_role && (
        <div className={s.oppAction}>
          <div className={s.oppActionRole}>Target role: <strong>{card.suggested_role}</strong></div>
          {card.salary_estimate && <div className={s.oppSalary}>Est. {card.salary_estimate}</div>}
          {card.contact_name && <span className={s.contactChip}>👤 {card.contact_name}{card.contact_title ? `, ${card.contact_title}` : ''}</span>}
        </div>
      )}

      {open && (
        <div className={s.oppDetail}>
          {card.fit_reasons.length > 0 && (
            <div className={s.oppSection}>
              <div className={s.oppSectionTitle}>✓ Why you fit</div>
              {card.fit_reasons.map((r,i) => <div key={i} className={s.oppBullet}>{r}</div>)}
            </div>
          )}
          {card.red_flags.length > 0 && (
            <div className={s.oppSection}>
              <div className={s.oppSectionTitle}>⚠ Watch out</div>
              {card.red_flags.map((r,i) => <div key={i} className={s.oppBullet} style={{color:'#dc2626'}}>{r}</div>)}
            </div>
          )}
          {card.suggested_action && (
            <div className={s.oppSection}>
              <div className={s.oppSectionTitle}>→ Suggested move</div>
              <div className={s.oppActionText}>{card.suggested_action}</div>
            </div>
          )}
          <div className={s.oppSources}>
            {card.signals.slice(0,3).map((sig,i) => (
              <a key={i} href={sig.source_url} target="_blank" rel="noreferrer" className={s.oppSource}>
                {SIG_META[sig.signal_type]?.icon} {sig.source_name} · {sig.headline.slice(0,60)}…
              </a>
            ))}
          </div>
        </div>
      )}

      <div className={s.oppFooter}>
        <button className={s.expandBtn} onClick={() => setOpen(o => !o)}>{open ? '▲ Less' : '▼ Details'}</button>
        <button className={s.genBtn} onClick={() => onGenerate(card)}>Generate pack →</button>
      </div>
    </div>
  )
}

// ─── LiveOpeningCard ──────────────────────────────────────────────────────────
function OpeningCard({ job, onGenerate }: { job: LiveOpening; onGenerate: (url: string, title: string) => void }) {
  const meta = STATUS_META[job.status] || STATUS_META.strategic

  return (
    <div className={s.openingCard} style={{ background: meta.bg, borderColor: meta.border }}>
      <div className={s.openingTop}>
        <div className={s.openingBadge} style={{ background: meta.badge }}>{meta.label}</div>
        <div className={s.openingSource}>{job.source}</div>
      </div>
      <div className={s.openingTitle}>{job.title}</div>
      {job.company && <div className={s.openingCompany}>{job.company}</div>}
      {job.snippet && <div className={s.openingSnippet}>{job.snippet}</div>}
      <div className={s.openingFooter}>
        <div className={s.openingSub}>{meta.sub}</div>
        <div className={s.openingBtns}>
          {job.url && <a href={job.url} target="_blank" rel="noreferrer" className={s.viewBtn}>View →</a>}
          <button className={s.genBtnSm} onClick={() => onGenerate(job.url, job.title)}>
            {job.is_posted ? 'Generate pack' : 'Research company'} →
          </button>
        </div>
      </div>
    </div>
  )
}

// ─── Main Dashboard ────────────────────────────────────────────────────────────
export default function Dashboard() {
  const navigate = useNavigate()
  const [tab, setTab] = useState<'signals' | 'openings'>('signals')

  const { data: profile } = useQuery({ queryKey: ['profile'], queryFn: profileApi.get })

  const { data, isLoading, error, refetch } = useQuery<ScoutData>({
    queryKey: ['scout-signals'],
    queryFn: scoutApi.getSignals,
    staleTime: 10 * 60 * 1000,
  })

  const handleGenerate = (card: OpportunityCard) => {
    sessionStorage.setItem('scout_job_url', card.apply_url || '')
    sessionStorage.setItem('scout_job_title', card.suggested_role || card.company)
    sessionStorage.setItem('scout_company', card.company)
    navigate(`/applications/new`)
  }

  const handleOpeningGenerate = (url: string, title: string) => {
    sessionStorage.setItem('scout_job_url', url || '')
    sessionStorage.setItem('scout_job_title', title || '')
    navigate('/applications/new')
  }

  const opportunities = data?.opportunities || []
  const liveOpenings  = data?.live_openings  || []
  const current   = liveOpenings.filter(j => j.status === 'current')
  const imminent  = liveOpenings.filter(j => j.status === 'imminent')
  const strategic = liveOpenings.filter(j => j.status === 'strategic')

  const prefComplete = (() => {
    try {
      const ctx = JSON.parse(profile?.global_context || '{}')
      const p = ctx.__preferences || {}
      return (p.regions?.length > 0) && (p.roles?.length > 0)
    } catch { return false }
  })()

  return (
    <div className={s.page}>
      {/* ── Top nav ── */}
      <header className={s.topbar}>
        <div className={s.topbarLeft}>
          <h1 className={s.pageTitle}>⚡ Job Scout</h1>
          {data && !data.is_demo && (
            <div className={s.engineBadge}>
              {data.signals_detected} signals · v{data.engine_version || '3.0'} · scored by {data.scored_by}
            </div>
          )}
        </div>
        <div className={s.tabBar}>
          <button className={[s.tab, tab==='signals'?s.tabActive:''].join(' ')} onClick={() => setTab('signals')}>
            🔮 Signal Intelligence
            {opportunities.length > 0 && <span className={s.tabBadge}>{opportunities.length}</span>}
          </button>
          <button className={[s.tab, tab==='openings'?s.tabActive:''].join(' ')} onClick={() => setTab('openings')}>
            📋 Live Openings
            {liveOpenings.length > 0 && <span className={s.tabBadge}>{liveOpenings.length}</span>}
          </button>
        </div>
        <button className={s.refreshBtn} onClick={() => refetch()} disabled={isLoading}>
          {isLoading ? '⏳' : '↻'} Refresh
        </button>
      </header>

      {/* ── No preferences warning ── */}
      {!prefComplete && (
        <div className={s.noPrefs}>
          <span>⚠ Set your target roles and regions in </span>
          <button className={s.noPrefsLink} onClick={() => navigate('/profile')}>My Profile → Job Preferences</button>
          <span> to get personalised results</span>
        </div>
      )}

      {/* ── Loading state ── */}
      {isLoading && (
        <div className={s.loading}>
          <div className={s.radar}><div className={s.radarRing}/><div className={s.radarRing}/><div className={s.radarRing}/><div className={s.radarDot}/></div>
          <div className={s.loadingText}>Scanning market signals across news, job boards & company announcements…</div>
          <div className={s.loadingSub}>This takes 15–30 seconds</div>
        </div>
      )}

      {/* ── Error ── */}
      {error && !isLoading && (
        <div className={s.errorBox}>
          <div className={s.errorTitle}>Scan failed</div>
          <div className={s.errorSub}>{(error as any)?.message || 'Could not reach signal engine'}</div>
          <button className={s.retryBtn} onClick={() => refetch()}>Try again</button>
        </div>
      )}

      {/* ── SIGNALS TAB ── */}
      {!isLoading && !error && tab === 'signals' && (
        <div className={s.content}>
          {data?.is_demo && (
            <div className={s.demoBanner}>⚡ Demo mode — set your preferences in Profile to see live signals</div>
          )}
          {opportunities.length === 0 ? (
            <div className={s.empty}>
              <div className={s.emptyIcon}>🔮</div>
              <div className={s.emptyTitle}>No signals detected yet</div>
              <div className={s.emptySub}>Set your target roles, regions and sectors in <button className={s.emptyLink} onClick={() => navigate('/profile')}>Job Preferences</button> and refresh.</div>
            </div>
          ) : (
            <div className={s.cards}>
              {opportunities.map(c => <OppCard key={c.id} card={c} onGenerate={handleGenerate} />)}
            </div>
          )}
        </div>
      )}

      {/* ── OPENINGS TAB ── */}
      {!isLoading && !error && tab === 'openings' && (
        <div className={s.content}>
          {liveOpenings.length === 0 ? (
            <div className={s.empty}>
              <div className={s.emptyIcon}>📋</div>
              <div className={s.emptyTitle}>No openings found</div>
              <div className={s.emptySub}>Set your target roles and regions in <button className={s.emptyLink} onClick={() => navigate('/profile')}>Job Preferences</button> and refresh.</div>
            </div>
          ) : (
            <>
              {current.length > 0 && (
                <div className={s.openingSection}>
                  <div className={s.openingSectionHeader}>
                    <div className={s.openingSectionDot} style={{background:'#16a34a'}}/>
                    <div>
                      <div className={s.openingSectionTitle}>Current Vacancies</div>
                      <div className={s.openingSectionSub}>Live posted roles — apply now</div>
                    </div>
                    <span className={s.openingSectionCount}>{current.length}</span>
                  </div>
                  <div className={s.openingGrid}>{current.map((j,i) => <OpeningCard key={i} job={j} onGenerate={handleOpeningGenerate}/>)}</div>
                </div>
              )}

              {imminent.length > 0 && (
                <div className={s.openingSection}>
                  <div className={s.openingSectionHeader}>
                    <div className={s.openingSectionDot} style={{background:'#ca8a04'}}/>
                    <div>
                      <div className={s.openingSectionTitle}>Imminent Openings</div>
                      <div className={s.openingSectionSub}>Strong signals — roles forming, position yourself now</div>
                    </div>
                    <span className={s.openingSectionCount}>{imminent.length}</span>
                  </div>
                  <div className={s.openingGrid}>{imminent.map((j,i) => <OpeningCard key={i} job={j} onGenerate={handleOpeningGenerate}/>)}</div>
                </div>
              )}

              {strategic.length > 0 && (
                <div className={s.openingSection}>
                  <div className={s.openingSectionHeader}>
                    <div className={s.openingSectionDot} style={{background:'#dc2626'}}/>
                    <div>
                      <div className={s.openingSectionTitle}>Strategic Horizon</div>
                      <div className={s.openingSectionSub}>Early signals — build relationships before the role exists</div>
                    </div>
                    <span className={s.openingSectionCount}>{strategic.length}</span>
                  </div>
                  <div className={s.openingGrid}>{strategic.map((j,i) => <OpeningCard key={i} job={j} onGenerate={handleOpeningGenerate}/>)}</div>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  )
}
