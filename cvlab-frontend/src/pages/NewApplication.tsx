import { useState, useRef, useEffect, KeyboardEvent } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useNavigate } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import { jobsApi } from '../api/jobs'
import { cvsApi } from '../api/cvs'
import { profileApi } from '../api/profile'
import { Button } from '../components/ui/Button'
import type { CV } from '../types'
import s from './NewApplication.module.css'

export default function NewApplication() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()

  // Auto-fill from extension or URL params
  useEffect(() => {
    const urlParam = searchParams.get('url')
    const titleParam = searchParams.get('title')
    const companyParam = searchParams.get('company')
    const fromExt = searchParams.get('ext')

    if (urlParam) setJdUrl(urlParam)

    // Pre-fill from scout "Generate pack" button
    const scoutUrl = sessionStorage.getItem('scout_job_url')
    const scoutTitle = sessionStorage.getItem('scout_job_title')
    if (scoutUrl) { setJdUrl(scoutUrl); sessionStorage.removeItem('scout_job_url') }
    if (scoutTitle) { sessionStorage.removeItem('scout_job_title') }

    // If came from extension, try to get full JD text from chrome storage
    if (fromExt && typeof window !== 'undefined' && (window as any).chrome?.storage) {
      try {
        ;(window as any).chrome.storage.local.get(['pending_jd'], (result: any) => {
          try {
            if (result?.pending_jd?.jd_text) {
              setJdText(result.pending_jd.jd_text)
              setJdUrl(result.pending_jd.jd_url || urlParam || '')
              ;(window as any).chrome.storage.local.remove(['pending_jd'])
            }
          } catch { /* extension storage callback error — ignore */ }
        })
      } catch { /* chrome.storage not available — ignore */ }
    }
  }, [])

  // Step 1 — JD
  const [jdUrl, setJdUrl] = useState('')
  const [jdText, setJdText] = useState('')
  const [fetching, setFetching] = useState(false)
  const [fetchError, setFetchError] = useState('')

  // Step 2 — CV
  const [selectedCvId, setSelectedCvId] = useState<string | null>(null)
  const [uploadPct, setUploadPct] = useState<number | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  // Step 3 — Known contacts
  const [contacts, setContacts] = useState<string[]>([])
  const [contactInput, setContactInput] = useState('')

  // Region
  const [region, setRegion] = useState('UAE')

  // Check mode — cv_only = just tailor CV, no full intel pack
  const [mode, setMode] = useState<'full'|'cv_only'>(
    new URLSearchParams(window.location.search).get('mode') === 'cv_only' ? 'cv_only' : 'full'
  )

  const { data: cvs, isLoading: cvsLoading } = useQuery({ queryKey: ['cvs'], queryFn: cvsApi.list })

  // Auto-select first CV when list loads
  useEffect(() => {
    if (cvs && cvs.length > 0 && !selectedCvId) {
      setSelectedCvId(cvs[0].id)
    }
  }, [cvs])

  const [uploadError, setUploadError] = useState('')

  const uploadMut = useMutation({
    mutationFn: (file: File) => cvsApi.upload(file, setUploadPct),
    onSuccess: (cv) => { setSelectedCvId(cv.id); setUploadPct(null); setUploadError('') },
    onError: (err: any) => {
      setUploadPct(null)
      const detail = err?.response?.data?.detail || err?.response?.data?.error || ''
      const status = err?.response?.status
      if (status === 422) {
        setUploadError('Invalid file — please upload a .docx or .pdf CV.')
      } else if (status === 401) {
        setUploadError('Session expired — please sign in again.')
      } else {
        setUploadError(`Upload failed (${status || 'network error'}). Please try again.`)
      }
    },
  })

  const createMut = useMutation({
    mutationFn: jobsApi.create,
    onSuccess: (run) => navigate(`/applications/${run.id}`),
  })

  const handleFetch = async () => {
    if (!jdUrl.trim()) return
    setFetching(true); setFetchError('')
    try {
      const result = await jobsApi.extractJd(jdUrl.trim())
      // Clean up extracted text — normalize line breaks
      const cleaned = result.jd_text
        .replace(/\r\n/g, '\n')
        .replace(/\r/g, '\n')
        .replace(/[ \t]+/g, ' ')
        .replace(/\n{3,}/g, '\n\n')
        .trim()
      setJdText(cleaned)
      setFetchError('')
    } catch (err: any) {
      const detail = err?.response?.data?.detail || ''
      if (detail.toLowerCase().includes('linkedin') || detail.toLowerCase().includes('blocked') || err?.response?.status === 403) {
        setFetchError('LinkedIn blocks automated access. Please copy and paste the job description text below.')
      } else if (err?.response?.status === 422) {
        setFetchError('Could not extract job description from this URL. Try pasting the text below instead.')
      } else {
        setFetchError('Could not fetch this URL. Please paste the job description text below.')
      }
    } finally {
      setFetching(false)
    }
  }

  const addContact = () => {
    const v = contactInput.trim()
    if (v && contacts.length < 10) {
      setContacts([...contacts, v])
      setContactInput('')
    }
  }

  const handleContactKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); addContact() }
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) uploadMut.mutate(file)
  }

  const canSubmit = (jdText.trim() || jdUrl.trim()) && selectedCvId

  const { data: profile } = useQuery({ queryKey: ['profile'], queryFn: profileApi.get })

  const handleSubmit = () => {
    if (!canSubmit) return
    createMut.mutate({
      cv_id: selectedCvId!,
      jd_text: jdText.trim() || undefined,
      jd_url: !jdText.trim() ? jdUrl.trim() : undefined,
      known_contacts: contacts.length > 0 ? contacts : undefined,
      profile_id: profile?.id || undefined,
      preferences: { region, cv_only: mode === 'cv_only' },
    })
  }

  return (
    <div className={s.page}>
      <header className={s.topbar}>
        <div>
          <h1 className={s.pageTitle}>New Application</h1>
        </div>
        <div className={s.topbarRight}>
          <Button variant="ghost" onClick={() => navigate(-1)}>Cancel</Button>
          <Button
            variant="primary"
            disabled={!canSubmit}
            loading={createMut.isPending}
            onClick={handleSubmit}
          >
            Generate Pack →
          </Button>
        </div>
      </header>

      <div className={s.content}>
        <div className={s.layout}>
          {/* ── LEFT: steps ── */}
          <div className={s.steps}>

            {/* Step 1 */}
            <div className={s.card}>
              <div className={s.cardHeader}>
                <StepPill n={1} />
                <div>
                  <div className={s.cardTitle}>Job description</div>
                  <div className={s.cardSub}>Paste a link or copy-paste the text</div>
                </div>
              </div>
              <div className={s.cardBody}>
                <div className={s.urlRow}>
                  <input
                    className={s.input}
                    placeholder="🔗  Paste a LinkedIn, company careers page, or job board URL…"
                    value={jdUrl}
                    onChange={(e) => setJdUrl(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleFetch()}
                  />
                  <Button variant="ghost" loading={fetching} onClick={handleFetch}>
                    Fetch
                  </Button>
                </div>
                {fetchError && <div className={s.fetchError}>{fetchError}</div>}
                <div className={s.orDiv}>
                  <div className={s.orLine} /><span className={s.orText}>or paste the text</span><div className={s.orLine} />
                </div>
                <textarea
                  className={s.textarea}
                  placeholder="Paste the full job description here…"
                  value={jdText}
                  onChange={(e) => setJdText(e.target.value)}
                />
                {jdText && (
                  <div className={s.charCount}>{jdText.length.toLocaleString()} characters</div>
                )}
              </div>
            </div>

            {/* Step 2 */}
            <div className={s.card}>
              <div className={s.cardHeader}>
                <StepPill n={2} />
                <div>
                  <div className={s.cardTitle}>Which CV?</div>
                  <div className={s.cardSub}>Use an existing CV or generate one from your profile</div>
                </div>
              </div>
              <div className={s.cardBody}>
                {cvsLoading && (
                  <div className={s.cvsLoading}>Loading your CVs…</div>
                )}
                {!cvsLoading && (!cvs || cvs.length === 0) && (
                  <div className={s.cvsEmpty}>No CVs uploaded yet — upload one below</div>
                )}
                {cvs?.map((cv) => (
                  <CvOption
                    key={cv.id}
                    cv={cv}
                    selected={selectedCvId === cv.id}
                    onSelect={() => setSelectedCvId(cv.id)}
                  />
                ))}

                <div
                  className={s.uploadZone}
                  onClick={() => fileRef.current?.click()}
                >
                  {uploadPct !== null
                    ? `Uploading… ${uploadPct}%`
                    : '+ Upload a different CV (.docx or .pdf)'}
                  {uploadError && <div className={s.uploadError}>{uploadError}</div>}
                  <input
                    ref={fileRef}
                    type="file"
                    accept=".docx,.pdf"
                    style={{ display: 'none' }}
                    onChange={handleFileChange}
                  />
                </div>
              </div>
            </div>

            {/* Step 3 */}
            <div className={s.card}>
              <div className={s.cardHeader}>
                <StepPill n={3} />
                <div>
                  <div className={s.cardTitle}>
                    Anyone you know there?
                    <span className={s.optional}>Optional</span>
                  </div>
                  <div className={s.cardSub}>CVLab writes a specific warm-intro ask for each person</div>
                </div>
              </div>
              <div className={s.cardBody}>
                {contacts.length > 0 && (
                  <div className={s.chips}>
                    {contacts.map((c, i) => (
                      <span key={i} className={s.chip}>
                        {c}
                        <button className={s.chipRemove} onClick={() => setContacts(contacts.filter((_, j) => j !== i))}>✕</button>
                      </span>
                    ))}
                  </div>
                )}
                <input
                  className={s.input}
                  placeholder="Add a name, e.g. Sarah Johnson (MBA classmate) — press Enter to add"
                  value={contactInput}
                  onChange={(e) => setContactInput(e.target.value)}
                  onKeyDown={handleContactKey}
                  onBlur={addContact}
                />
              </div>
            </div>

            {createMut.isError && (
              <div className={s.error}>Something went wrong. Please try again.</div>
            )}
          </div>

          {/* ── RIGHT: context sidebar ── */}
          <div className={s.sidebar}>
            <div className={s.sideCard}>
              <div className={s.sideCardHeader}>Run settings</div>
              <div className={s.sideCardBody}>
                <div className={s.settingRow}>
                  <div className={s.settingLabel}>Region</div>
                  <select
                    className={s.select}
                    value={region}
                    onChange={(e) => setRegion(e.target.value)}
                  >
                    <optgroup label="GCC">
                      <option value="UAE">UAE (Dubai, Abu Dhabi)</option>
                      <option value="KSA">Saudi Arabia (Riyadh, Jeddah)</option>
                      <option value="Qatar">Qatar (Doha)</option>
                      <option value="Kuwait">Kuwait</option>
                      <option value="Bahrain">Bahrain</option>
                      <option value="Oman">Oman (Muscat)</option>
                    </optgroup>
                    <optgroup label="Wider MENA">
                      <option value="Egypt">Egypt (Cairo)</option>
                      <option value="Jordan">Jordan (Amman)</option>
                      <option value="Lebanon">Lebanon (Beirut)</option>
                      <option value="Iraq">Iraq</option>
                      <option value="Morocco">Morocco (Casablanca)</option>
                      <option value="Pakistan">Pakistan (Karachi, Lahore)</option>
                    </optgroup>
                    <optgroup label="Europe & UK">
                      <option value="UK">United Kingdom (London)</option>
                      <option value="EU">Europe (Paris, Amsterdam, Berlin)</option>
                    </optgroup>
                    <optgroup label="Americas">
                      <option value="US">USA (New York, San Francisco)</option>
                      <option value="Canada">Canada (Toronto)</option>
                    </optgroup>
                    <optgroup label="Other">
                      <option value="APAC">Asia Pacific</option>
                      <option value="Global">Global / Remote</option>
                    </optgroup>
                  </select>
                </div>

                <div className={s.settingRow}>
                  <div className={s.settingLabel}>What this generates</div>
                  <div className={s.checkList}>
                    {[
                      'Tailored CV (DOCX)',
                      'Positioning strategy',
                      'Company intelligence',
                      'Named contacts + outreach',
                      'Salary benchmarks',
                      '7-day action plan',
                    ].map((item) => (
                      <div key={item} className={s.checkItem}>
                        <span className={s.checkMark}>✓</span>
                        {item}
                      </div>
                    ))}
                  </div>
                </div>

                <div className={s.settingRow} style={{ borderBottom: 'none' }}>
                  <div className={s.settingLabel}>Cost</div>
                  <div className={s.cost}>1 <span>pack</span></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function StepPill({ n }: { n: number }) {
  return (
    <div className={s.stepPill}>
      <div className={s.stepNum}>{n}</div>
    </div>
  )
}

function CvOption({ cv, selected, onSelect }: { cv: CV; selected: boolean; onSelect: () => void }) {
  const score = cv.quality_score
  return (
    <div className={[s.cvOption, selected ? s.cvSelected : ''].join(' ')} onClick={onSelect}>
      <div className={[s.cvRadio, selected ? s.cvRadioOn : ''].join(' ')} />
      <div className={s.cvIcon}>DOC</div>
      <div className={s.cvMeta}>
        <div className={s.cvName}>{cv.original_filename}</div>
        <div className={s.cvSub}>
          {score !== null
            ? `Quality score: ${score}/100${score < 70 ? ' · Rebuild suggested' : ''}`
            : 'Parsing…'}
        </div>
      </div>
    </div>
  )
}
