import { useState, useRef, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { jobsApi } from '../api/jobs'
import { Button } from '../components/ui/Button'
import { StatusPill } from '../components/ui/StatusPill'
import type { JobRunStatus, PositioningOutput, ReportPack, NetworkingOutput, ApplicationOutput } from '../types'
import s from './IntelPack.module.css'

const TERMINAL: JobRunStatus[] = ['completed', 'failed']

function sanitizeUrl(url: string): string {
  const trimmed = url.trim()
  const withProto = trimmed.startsWith('http') ? trimmed : `https://${trimmed}`
  try {
    const parsed = new URL(withProto)
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return '#'
    return parsed.href
  } catch {
    return '#'
  }
}
type Tab = 'strengths' | 'interview' | 'contacts' | 'company' | 'salary' | 'strategy'

export default function IntelPack() {
  const { runId } = useParams<{ runId: string }>()
  const navigate = useNavigate()
  const [tab, setTab] = useState<Tab>('strengths')
  const [copied, setCopied] = useState<string | null>(null)

  const { data: run, isLoading } = useQuery({
    queryKey: ['run', runId],
    queryFn: () => jobsApi.get(runId!),
    refetchInterval: (query) => {
      const data = query.state.data
      if (!data) return 3000
      return TERMINAL.includes(data.status) ? false : 3000
    },
    enabled: !!runId,
  })

  // Apply Now — open job posting directly
  const applyUrl = run?.jd_url || sessionStorage.getItem('pending_apply_url') || null

  const handleApplyNow = () => {
    if (!applyUrl) return
    window.open(applyUrl, '_blank')
    sessionStorage.removeItem('pending_apply_url')
  }

  const copyTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const handleCopy = (text: string, key: string) => {
    navigator.clipboard.writeText(text)
    setCopied(key)
    if (copyTimerRef.current) clearTimeout(copyTimerRef.current)
    copyTimerRef.current = setTimeout(() => setCopied(null), 1800)
  }

  useEffect(() => {
    return () => { if (copyTimerRef.current) clearTimeout(copyTimerRef.current) }
  }, [])

  const handleDownload = async () => {
    if (!runId) return
    const { url } = await jobsApi.downloadUrl(runId)
    window.open(url, '_blank')
  }

  if (isLoading) return <div className={s.loading}>Loading…</div>
  if (!run) return <div className={s.loading}>Not found.</div>

  const reports = run.reports as ReportPack | null
  const positioning = run.positioning as PositioningOutput | null

  const headline = reports?.application?.positioning_headline
    ?? positioning?.positioning_headline
    ?? null

  const roleTitle = reports?.role?.role_title ?? '—'
  const companyName = reports?.company?.company_name ?? '—'
  const titleStr = `${companyName} · ${roleTitle}`

  return (
    <div className={s.page}>
      <header className={s.topbar}>
        <div>
          <div className={s.pageTitle}>
            {companyName} <span className={s.pageSub}>· {roleTitle}</span>
          </div>
        </div>
        <div className={s.topbarRight}>
          <StatusPill status={run.status} />
          <Button variant="ghost" onClick={() => navigate('/applications')}>← All applications</Button>
          {run.status === 'completed' && (
            <>
              {applyUrl && (
                <Button variant="ghost" onClick={handleApplyNow}>
                  ↗ Apply Now
                </Button>
              )}
              <Button variant="primary" onClick={handleDownload}>↓ Download CV</Button>
            </>
          )}
        </div>
      </header>

      <div className={s.content}>

        {/* Processing state */}
        {run.status !== 'completed' && run.status !== 'failed' && (
          <div className={s.processingBanner}>
            <div className={s.processingSpinner} />
            <div>
              <div className={s.processingTitle}>Generating your intelligence pack…</div>
              <div className={s.processingSub}>
                {run.status === 'retrieving' && 'Researching the company and finding contacts…'}
                {run.status === 'llm_processing' && 'Analysing the role and building your strategy…'}
                {run.status === 'rendering' && 'Rendering your tailored CV…'}
                {run.status === 'queued' && 'Queued — starting shortly…'}
              </div>
            </div>
          </div>
        )}

        {run.status === 'failed' && (
          <div className={s.failedBanner}>
            Something went wrong during processing.
            {run.error_message && <div className={s.failedDetail}>{run.error_message}</div>}
          </div>
        )}

        {run.status === 'completed' && reports && (
          <>
            {/* Headline */}
            {headline && (
              <div className={s.headlineBanner}>
                <div className={s.headlineLabel}>Your positioning</div>
                <div className={s.headlineText}>{headline}</div>
                <div className={s.headlineScore}>
                  <span className={s.scoreBadge}>
                    Match score: {run.keyword_match_score != null ? `${run.keyword_match_score}/100` : '—'}
                  </span>
                  <span className={s.scoreNote}>{reports.role?.positioning_recommendation}</span>
                </div>
              </div>
            )}

            {/* Download row */}
            <div className={s.dlRow}>
              <div className={s.dlIcon}>DOC</div>
              <div className={s.dlInfo}>
                <div className={s.dlName}>{titleStr.replace(' · ', '_').replace(/\s/g, '_')}_CV.docx</div>
                <div className={s.dlMeta}>
                  Tailored CV · same layout, updated content ·{' '}
                  {run.completed_at ? new Date(run.completed_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : ''}
                </div>
              </div>
              <Button variant="primary" size="sm" onClick={handleDownload}>↓ Download</Button>
            </div>

            {/* Tabs */}
            <div className={s.tabs}>
              {([
                ['strengths',  'Strengths & Gaps'],
                ['contacts',   'Contacts'],
                ['company',    'Company Intel'],
                ['salary',     'Salary'],
                ['strategy',   'Strategy'],
                ['interview',  'Interview Prep'],
              ] as [Tab, string][]).map(([key, label]) => (
                <button
                  key={key}
                  className={[s.tab, tab === key ? s.tabActive : ''].join(' ')}
                  onClick={() => setTab(key)}
                >
                  {label}
                </button>
              ))}
            </div>

            {/* Tab content */}
            <div className={s.packLayout}>
              <div className={s.packMain}>

                {tab === 'strengths' && (
                  <StrengthsTab positioning={positioning} />
                )}
                {tab === 'interview' && (
                  <InterviewTab application={reports?.application} />
                )}
                {tab === 'contacts' && (
                  <ContactsTab
                    networking={reports?.networking}
                    copied={copied}
                    onCopy={handleCopy}
                  />
                )}
                {tab === 'company' && (
                  <CompanyTab company={reports?.company} />
                )}
                {tab === 'salary' && (
                  <SalaryTab salary={reports?.salary} />
                )}
                {tab === 'strategy' && (
                  <StrategyTab application={reports?.application} />
                )}
              </div>

              {/* Right sidebar */}
              <div className={s.packSidebar}>

                {positioning && (
                  <div className={s.sideCard}>
                    <div className={s.sideCardHeader}>Your narrative</div>
                    <div className={s.sideCardBody}>
                      <div className={s.narrative}>{positioning.narrative_thread}</div>
                    </div>
                  </div>
                )}

                {reports.networking?.named_contacts?.length > 0 && (
                  <div className={s.sideCard}>
                    <div className={s.sideCardHeader}>Named contacts</div>
                    <div className={s.sideCardBody}>
                      {reports.networking.named_contacts.slice(0, 3).map((c, i) => (
                        <div key={i} className={s.sideContact}>
                          <div className={s.sideContactName}>{c.name}</div>
                          <div className={s.sideContactTitle}>{c.title}</div>
                          {c.linkedin_url && (
                            <a
                              className={s.sideContactLink}
                              href={sanitizeUrl(c.linkedin_url)}
                              target="_blank"
                              rel="noreferrer"
                              style={{display:'inline-block',marginTop:4,padding:'2px 8px',background:'#0a66c2',color:'#fff',borderRadius:4,fontSize:11,fontWeight:600,textDecoration:'none'}}
                            >
                              LinkedIn →
                            </a>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {reports.application?.interview_process?.length > 0 && (
                  <div className={s.sideCard}>
                    <div className={s.sideCardHeader}>Interview process</div>
                    <div className={s.sideCardBody}>
                      {reports.application.interview_process.map((stage, i) => (
                        <div key={i} className={s.themeItem}>
                          <span className={s.stageNumSmall}>{i + 1}</span>
                          <span>{stage.stage}</span>
                        </div>
                      ))}
                      <button
                        className={s.sideTabLink}
                        onClick={() => setTab('interview')}
                      >
                        View full prep guide →
                      </button>
                    </div>
                  </div>
                )}

                {reports.networking?.seven_day_action_plan?.length > 0 && (
                  <div className={s.sideCard}>
                    <div className={s.sideCardHeader}>7-day action plan</div>
                    <div className={s.sideCardBody}>
                      {reports.networking.seven_day_action_plan.map((item, i) => (
                        <div key={i} className={s.planItem}>
                          <span className={s.planDay}>Day {i + 1}</span>
                          <span className={s.planText}>{item}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

// ── Tab components ─────────────────────────────────────────────────────────

function StrengthsTab({ positioning }: { positioning: PositioningOutput | null | undefined }) {
  if (!positioning) return <div style={{padding:24,color:'#888'}}>No data available</div>
  return (
    <div className={s.fadeIn}>
      {positioning.strongest_angles?.length > 0 && (
        <>
          <div className={s.tabSectionLabel}>Your strongest angles</div>
          <div className={s.angleList}>
            {positioning.strongest_angles.map((a: any, i: number) => (
              <div key={i} className={s.angleCard}>
                <div className={s.angleNum}>{i + 1}</div>
                <div>
                  <div className={s.angleTitle}>{a.title || a.angle}</div>
                  <div className={s.angleBody}>{a.explanation || a.why_it_matters_here}</div>
                  {a.how_to_play_it && <div className={s.angleBody} style={{marginTop:4,opacity:0.8}}>▶ {a.how_to_play_it}</div>}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {positioning.gaps_to_address?.length > 0 && (
        <>
          <div className={s.tabSectionLabel} style={{ marginTop: 24 }}>Gaps to address</div>
          <div className={s.gapList}>
            {positioning.gaps_to_address.map((g, i) => (
              <div key={i} className={s.gapCard}>
                <div className={[s.sevTag, s[`sev_${g.severity}`]].join(' ')}>
                  {g.severity.charAt(0).toUpperCase() + g.severity.slice(1)}
                </div>
                <div>
                  <div className={s.gapText}>{g.gap}</div>
                  <div className={s.gapFix}>{g.mitigation}</div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

function ContactsTab({ networking, copied, onCopy }: {
  networking: NetworkingOutput | null | undefined
  copied: string | null
  onCopy: (text: string, key: string) => void
}) {
  if (!networking) return <div style={{padding:24,color:'#888'}}>No contacts data available</div>
  const allContacts = [
    ...(networking.named_contacts ?? []),
  ]
  const targetContacts: string[] = (networking as any).target_contacts ?? []

  const hasContent = allContacts.length > 0 || targetContacts.length > 0 ||
    (networking.known_network_asks ?? []).length > 0 ||
    networking.outreach_template_hiring_manager || networking.seven_day_action_plan?.length > 0

  if (!hasContent) {
    return <div className={s.empty}>No contacts found for this company.</div>
  }

  return (
    <div className={s.fadeIn}>
      {allContacts.length > 0 && (
        <>
          <div className={s.tabSectionLabel}>People found at this company</div>
          <div className={s.contactList}>
            {allContacts.map((c, i) => (
              <div key={i} className={s.contactCard}>
                <div className={s.contactTop}>
                  <div className={[s.contactAvatar, s[`av${(i % 4) + 1}`]].join(' ')}>
                    {c.name.split(' ').map((n) => n[0]).join('').slice(0, 2)}
                  </div>
                  <div>
                    <div className={s.contactName}>{c.name}</div>
                    <div className={s.contactTitle}>{c.title}</div>
                    {c.linkedin_url && (
                      <a
                        className={s.contactLink}
                        href={sanitizeUrl(c.linkedin_url)}
                        target="_blank"
                        rel="noreferrer"
                        onClick={e => e.stopPropagation()}
                        style={{display:'inline-block',marginTop:4,padding:'3px 10px',background:'#0a66c2',color:'#fff',borderRadius:4,fontSize:12,fontWeight:600,textDecoration:'none'}}
                      >
                        View on LinkedIn →
                      </a>
                    )}
                  </div>
                </div>
                <div className={s.contactMsg}>{c.outreach_message}</div>
                <button
                  className={s.copyBtn}
                  onClick={() => onCopy(c.outreach_message, `contact-${i}`)}
                >
                  {copied === `contact-${i}` ? '✓ Copied!' : '⧉ Copy message'}
                </button>
              </div>
            ))}
          </div>
        </>
      )}

      {allContacts.length === 0 && targetContacts.length > 0 && (
        <>
          <div className={s.tabSectionLabel}>Who to target</div>
          <div className={s.contactList}>
            {targetContacts.map((title, i) => (
              <div key={i} className={s.contactCard}>
                <div className={s.contactTop}>
                  <div className={[s.contactAvatar, s[`av${(i % 4) + 1}`]].join(' ')}>
                    {title.split(' ').map((n: string) => n[0]).join('').slice(0, 2).toUpperCase()}
                  </div>
                  <div>
                    <div className={s.contactName}>{title}</div>
                    <div className={s.contactTitle}>Search on LinkedIn</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {networking.known_network_asks?.length > 0 && (
        <>
          <div className={s.tabSectionLabel} style={{ marginTop: 24 }}>Your warm intro asks</div>
          <div className={s.contactList}>
            {networking.known_network_asks.map((ask, i) => (
              <div key={i} className={s.contactCard}>
                <div className={s.contactName}>{ask.person}</div>
                <div className={s.contactMsg}>{ask.ask}</div>
                <button
                  className={s.copyBtn}
                  onClick={() => onCopy(ask.ask, `ask-${i}`)}
                >
                  {copied === `ask-${i}` ? '✓ Copied!' : '⧉ Copy message'}
                </button>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

function CompanyTab({ company }: { company: ReportPack['company'] | null | undefined }) {
  if (!company) return <div style={{padding:24,color:'#888'}}>No company data available</div>
  return (
    <div className={s.fadeIn}>
      <div className={s.companyDesc}>{company.business_description}</div>
      {company.recent_news?.length > 0 && (
        <Section title="Recent news">
          {company.recent_news.map((n, i) => <BulletItem key={i} text={n} />)}
        </Section>
      )}
      {company.strategic_priorities?.length > 0 && (
        <Section title="Strategic priorities">
          {company.strategic_priorities.map((p, i) => <BulletItem key={i} text={p} />)}
        </Section>
      )}
      {company.hiring_signals?.length > 0 && (
        <Section title="Why they're hiring now">
          {company.hiring_signals.map((h, i) => <BulletItem key={i} text={h} />)}
        </Section>
      )}
      {company.red_flags?.length > 0 && (
        <Section title="Red flags to investigate">
          {company.red_flags.map((r, i) => <BulletItem key={i} text={r} warning />)}
        </Section>
      )}
    </div>
  )
}

function SalaryTab({ salary }: { salary: ReportPack['salary'] | null | undefined }) {
  if (!salary) return <div style={{padding:24,color:'#888'}}>No salary data available</div>
  if (!salary?.length) return <div className={s.empty}>No salary data available.</div>
  return (
    <div className={s.fadeIn}>
      {salary.map((s_item, i) => (
        <div key={i} className={s.salaryCard}>
          <div className={s.salaryTitle}>{s_item.title}</div>
          {(s_item.base_annual_aed_low || s_item.base_annual_aed_high) && (
            <div className={s.salaryRange}>
              AED {s_item.base_annual_aed_low?.toLocaleString()} – {s_item.base_annual_aed_high?.toLocaleString()} / year
            </div>
          )}
          <div className={s.salaryNote}>{s_item.total_comp_note}</div>
          <div className={s.salaryMeta}>Source: {s_item.source} · Confidence: {s_item.confidence}</div>
        </div>
      ))}
    </div>
  )
}

function StrategyTab({ application }: { application: ReportPack['application'] | null | undefined }) {
  if (!application) return <div style={{padding:24,color:'#888'}}>No strategy data available</div>
  return (
    <div className={s.fadeIn}>
      {application.cover_letter_angle && (
        <Section title="Cover letter angle">
          <div className={s.coverAngle}>{application.cover_letter_angle}</div>
        </Section>
      )}
      {application.thirty_sixty_ninety && (
        <Section title="30 / 60 / 90 day plan">
          {(['30', '60', '90'] as const).map((k) => (
            <div key={k} className={s.planRow}>
              <span className={s.planKey}>Day {k}</span>
              <span>{application.thirty_sixty_ninety[k]}</span>
            </div>
          ))}
        </Section>
      )}
      {application.risks_to_address?.length > 0 && (
        <Section title="Risks to address proactively">
          {application.risks_to_address.map((r, i) => <BulletItem key={i} text={r} warning />)}
        </Section>
      )}
      {application.differentiators?.length > 0 && (
        <Section title="Lead with these differentiators">
          {application.differentiators.map((d, i) => <BulletItem key={i} text={d} />)}
        </Section>
      )}
    </div>
  )
}

function InterviewTab({ application }: { application: ApplicationOutput | null | undefined }) {
  if (!application) return <div style={{padding:24,color:'#888'}}>No interview data available</div>
  const [openQ, setOpenQ] = useState<string | null>(null)
  const toggle = (k: string) => setOpenQ(openQ === k ? null : k)

  const qbank = application.question_bank ?? {}
  const behavioural   = qbank.behavioural ?? []
  const businessCase  = qbank.business_case ?? []
  const situational   = qbank.situational ?? []
  const culture       = qbank.culture_and_motivation ?? []

  return (
    <div className={s.fadeIn}>

      {/* ── Process map ── */}
      {application.interview_process?.length > 0 && (
        <>
          <div className={s.tabSectionLabel}>Interview process</div>
          <div className={s.processMap}>
            {application.interview_process.map((stage, i) => (
              <div key={i} className={s.stageCard}>
                <div className={s.stageNum}>{i + 1}</div>
                <div className={s.stageBody}>
                  <div className={s.stageName}>{stage.stage}</div>
                  <div className={s.stageMeta}>
                    {[stage.format, stage.duration, stage.who].filter(Boolean).join(' · ')}
                  </div>
                  <div className={s.stageExpect}>{stage.what_to_expect}</div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {/* ── Behavioural questions ── */}
      {behavioural.length > 0 && (
        <>
          <div className={s.tabSectionLabel} style={{marginTop:28}}>
            Behavioural questions
            <span className={s.qCatNote}>Tell me about a time…</span>
          </div>
          <div className={s.qList}>
            {behavioural.map((q, i) => {
              const key = `beh-${i}`
              return (
                <div key={key} className={s.qCard}>
                  <button className={s.qHeader} onClick={() => toggle(key)}>
                    <span className={s.qTag + ' ' + s.qTagBeh}>B</span>
                    <span className={s.qText}>{q.question}</span>
                    <span className={s.qChevron}>{openQ === key ? '▴' : '▾'}</span>
                  </button>
                  {openQ === key && (
                    <div className={s.qBody}>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel}>Why they ask</span>
                        <span className={s.qRowVal}>{q.why_they_ask}</span>
                      </div>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel}>Use this story</span>
                        <span className={s.qRowVal} style={{color:'var(--blue)',fontWeight:500}}>{q.your_story}</span>
                      </div>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel}>Key points to land</span>
                        <ul className={s.keyPoints}>
                          {q.key_points?.map((pt, j) => (
                            <li key={j}>{pt}</li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </>
      )}

      {/* ── Business case questions ── */}
      {businessCase.length > 0 && (
        <>
          <div className={s.tabSectionLabel} style={{marginTop:28}}>
            Business case &amp; problem solving
            <span className={s.qCatNote}>How would you approach…</span>
          </div>
          <div className={s.qList}>
            {businessCase.map((q, i) => {
              const key = `case-${i}`
              return (
                <div key={key} className={s.qCard}>
                  <button className={s.qHeader} onClick={() => toggle(key)}>
                    <span className={s.qTag + ' ' + s.qTagCase}>C</span>
                    <span className={s.qText}>{q.question}</span>
                    <span className={s.qChevron}>{openQ === key ? '▴' : '▾'}</span>
                  </button>
                  {openQ === key && (
                    <div className={s.qBody}>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel}>Case type</span>
                        <span className={s.qRowVal}>{q.case_type}</span>
                      </div>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel}>How to frame it</span>
                        <span className={s.qRowVal}>{q.how_to_frame}</span>
                      </div>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel} style={{color:'var(--amber)'}}>Watch out for</span>
                        <span className={s.qRowVal}>{q.watch_out}</span>
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </>
      )}

      {/* ── Situational ── */}
      {situational.length > 0 && (
        <>
          <div className={s.tabSectionLabel} style={{marginTop:28}}>
            Situational questions
            <span className={s.qCatNote}>What would you do if…</span>
          </div>
          <div className={s.qList}>
            {situational.map((q, i) => {
              const key = `sit-${i}`
              return (
                <div key={key} className={s.qCard}>
                  <button className={s.qHeader} onClick={() => toggle(key)}>
                    <span className={s.qTag + ' ' + s.qTagSit}>S</span>
                    <span className={s.qText}>{q.question}</span>
                    <span className={s.qChevron}>{openQ === key ? '▴' : '▾'}</span>
                  </button>
                  {openQ === key && (
                    <div className={s.qBody}>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel}>What they want</span>
                        <span className={s.qRowVal}>{q.what_they_want}</span>
                      </div>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel}>Your angle</span>
                        <span className={s.qRowVal} style={{color:'var(--blue)',fontWeight:500}}>{q.suggested_answer_angle}</span>
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </>
      )}

      {/* ── Culture & motivation ── */}
      {culture.length > 0 && (
        <>
          <div className={s.tabSectionLabel} style={{marginTop:28}}>
            Culture &amp; motivation
            <span className={s.qCatNote}>Why us? Why this role?</span>
          </div>
          <div className={s.qList}>
            {culture.map((q, i) => {
              const key = `cult-${i}`
              return (
                <div key={key} className={s.qCard}>
                  <button className={s.qHeader} onClick={() => toggle(key)}>
                    <span className={s.qTag + ' ' + s.qTagCult}>M</span>
                    <span className={s.qText}>{q.question}</span>
                    <span className={s.qChevron}>{openQ === key ? '▴' : '▾'}</span>
                  </button>
                  {openQ === key && (
                    <div className={s.qBody}>
                      <div className={s.qRow}>
                        <span className={s.qRowLabel}>Ideal angle</span>
                        <span className={s.qRowVal}>{q.ideal_answer_angle}</span>
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </>
      )}

      {/* ── Questions to ask them ── */}
      {application.questions_to_ask_them?.length > 0 && (
        <>
          <div className={s.tabSectionLabel} style={{marginTop:28}}>
            Questions to ask them
            <span className={s.qCatNote}>Signal depth, not curiosity</span>
          </div>
          <div className={s.askList}>
            {application.questions_to_ask_them.map((q, i) => (
              <div key={i} className={s.askCard}>
                <div className={s.askQ}>"{q.question}"</div>
                <div className={s.askWhy}>{q.why_powerful}</div>
              </div>
            ))}
          </div>
        </>
      )}

    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className={s.sectionBlock}>
      <div className={s.sectionBlockTitle}>{title}</div>
      {children}
    </div>
  )
}

function BulletItem({ text, warning }: { text: string; warning?: boolean }) {
  return (
    <div className={s.bulletItem}>
      <span className={[s.bulletDot, warning ? s.bulletWarn : ''].join(' ')} />
      {text}
    </div>
  )
}
