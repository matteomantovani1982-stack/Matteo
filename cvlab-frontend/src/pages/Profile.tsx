import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { profileApi, type ExperienceEntry } from '../api/profile'
import { jobsApi } from '../api/jobs'
import { cvsApi } from '../api/cvs'
import { Button } from '../components/ui/Button'
import s from './Profile.module.css'

const INTAKE_QS = [
  { key: 'context',      icon: '🧩', label: 'Situation when you joined',   hint: 'Company stage, team size, what was missing. What you inherited.' },
  { key: 'contribution', icon: '🎯', label: 'What YOU specifically owned', hint: 'Not the team. Your decisions, initiatives, workstreams.' },
  { key: 'outcomes',     icon: '📈', label: 'Impact & results',            hint: 'What changed because of you? Numbers always win.' },
  { key: 'methods',      icon: '⚙️', label: 'How you did it',             hint: 'Frameworks, tools, approaches behind the outcomes.' },
  { key: 'hidden',       icon: '💡', label: "What the CV misses",         hint: 'Hardest challenge, near-failure, proudest moment.' },
]
const PREF = {
  workType:    ['Full-time employed','Freelance / Consulting','Board / Advisory','Part-time','Open to any'],
  level:       ['C-Suite (CEO/COO/CFO/CTO)','VP / SVP / EVP','Director','Senior Manager','Manager'],
  roles:       ['CEO','COO','CFO','CTO','CMO','CPO','Chief of Staff','VP Commercial','VP Sales','VP Operations','VP Product','Strategy Director','GM / MD','Head of Growth','Head of Finance'],
  regions:     ['UAE','KSA','Qatar','Kuwait','Bahrain','Oman','Jordan','Egypt','Morocco','UK','Europe','USA','Canada','Global / Remote'],
  companyType: ['Startup (0-50)','Scale-up (50-500)','Corporate / MNC','Family Business','PE-backed','Consulting','Government'],
  stage:       ['Pre-seed / Seed','Series A','Series B','Series C+','Pre-IPO','Public','Established private'],
  sectors:     ['Tech / SaaS','Fintech / Payments','E-commerce','Real Estate','F&B / Hospitality','Healthcare','Logistics','Energy','Media','Education','Professional Services','Manufacturing'],
  relocation:  ['No — staying put','Yes — selected markets','Yes — anywhere'],
  notice:      ['Available now','2 weeks','1 month','2 months','3+ months'],
}

function Chips({ opts, sel, onChange, single }: { opts:string[]; sel:string[]; onChange:(v:string[])=>void; single?:boolean }) {
  const toggle = (v:string) => { if(single) return onChange(sel[0]===v?[]:[v]); onChange(sel.includes(v)?sel.filter(x=>x!==v):[...sel,v]) }
  return <div className={s.chips}>{opts.map(o=><button key={o} className={[s.chip,sel.includes(o)?s.chipOn:''].join(' ')} onClick={()=>toggle(o)}>{o}</button>)}</div>
}
function F({ label, hint, children }: { label:string; hint?:string; children:React.ReactNode }) {
  return <div className={s.field}><label className={s.fieldLabel}>{label}</label>{hint&&<div className={s.fieldHint}>{hint}</div>}{children}</div>
}
function SecTitle({ icon, title, sub }: { icon:string; title:string; sub?:string }) {
  return <div className={s.secTitle}><span className={s.secIcon}>{icon}</span><div><div className={s.secName}>{title}</div>{sub&&<div className={s.secSub}>{sub}</div>}</div></div>
}

function ExpCard({ exp, profileId, onDeleted }: { exp:ExperienceEntry; profileId:string; onDeleted:()=>void }) {
  const qc = useQueryClient()
  const [open, setOpen] = useState(false)
  const [form, setForm] = useState<Partial<ExperienceEntry>>({...exp})
  const [saved, setSaved] = useState(false)
  const updateMut = useMutation({ mutationFn: ()=>profileApi.updateExperience(profileId, exp.id, form),
    onSuccess: ()=>{ qc.invalidateQueries({queryKey:['profile']}); setSaved(true); setTimeout(()=>setSaved(false),2000) } })
  const deleteMut = useMutation({ mutationFn: ()=>profileApi.deleteExperience(profileId, exp.id),
    onSuccess: ()=>{ qc.invalidateQueries({queryKey:['profile']}); onDeleted() } })
  const set = (k:string,v:string) => setForm(f=>({...f,[k]:v}))
  const filled = INTAKE_QS.filter(q=>(form as any)[q.key]?.trim()).length
  return (
    <div className={[s.expCard,open?s.expCardOpen:''].join(' ')}>
      <div className={s.expHeader} onClick={()=>setOpen(o=>!o)}>
        <div className={s.expHeaderLeft}>
          <div className={s.expRole}>{form.role_title||<span className={s.placeholder}>Untitled role</span>}</div>
          <div className={s.expCo}>{form.company_name&&<span>{form.company_name}</span>}{form.start_date&&<span className={s.expDate}> · {form.start_date}→{form.end_date||'Present'}</span>}{form.location&&<span className={s.expLoc}> · {form.location}</span>}</div>
        </div>
        <div className={s.expHeaderRight}>
          <div className={s.expProg}><div className={s.expProgBar}><div className={s.expProgFill} style={{width:`${filled/5*100}%`}}/></div><span>{filled}/5</span></div>
          <span className={s.chevron}>{open?'▲':'▼'}</span>
        </div>
      </div>
      {open && (
        <div className={s.expBody}>
          <div className={s.expBasic}>
            {[['role_title','Job title','e.g. Chief Operating Officer'],['company_name','Company','e.g. Careem'],['start_date','Start','e.g. 2021-03'],['end_date','End','e.g. Present'],['location','Location','e.g. Dubai, UAE']].map(([k,l,p])=>(
              <div key={k} className={s.field}><label className={s.fieldLabel}>{l}</label><input className={s.input} value={(form as any)[k]||''} onChange={e=>set(k,e.target.value)} placeholder={p}/></div>
            ))}
          </div>
          <div className={s.deepHeader}><div className={s.deepTitle}>Deep dive — Claude uses this to position you</div><div className={s.deepSub}>The more specific, the better your generated CVs. These stay private.</div></div>
          {INTAKE_QS.map(q=>(
            <div key={q.key} className={s.iq}>
              <div className={s.iqTop}><span className={s.iqIcon}>{q.icon}</span><div><div className={s.iqLabel}>{q.label}</div><div className={s.iqHint}>{q.hint}</div></div>{(form as any)[q.key]?.trim()&&<span className={s.iqDone}>✓</span>}</div>
              <textarea className={s.textarea} rows={3} value={(form as any)[q.key]||''} onChange={e=>set(q.key,e.target.value)} placeholder="Write as much as you want…"/>
            </div>
          ))}
          <div className={s.iq}>
            <div className={s.iqTop}><span className={s.iqIcon}>📝</span><div><div className={s.iqLabel}>Anything else</div><div className={s.iqHint}>Salary, equity, board relationship, culture, anything that adds colour.</div></div></div>
            <textarea className={s.textarea} rows={2} value={form.freeform||''} onChange={e=>set('freeform',e.target.value)} placeholder="Free text…"/>
          </div>
          <div className={s.expFoot}>
            <button className={s.deleteBtn} onClick={()=>{ if(confirm('Delete this role?')) deleteMut.mutate() }}>🗑 Delete</button>
            <Button variant="primary" loading={updateMut.isPending} onClick={()=>updateMut.mutate()}>{saved?'✓ Saved':'Save changes'}</Button>
          </div>
        </div>
      )}
    </div>
  )
}


export default function Profile() {
  const qc = useQueryClient()
  const { data: profile, isLoading } = useQuery({ queryKey:['profile'], queryFn: profileApi.get })
  const { data: cvs } = useQuery({ queryKey:['cvs'], queryFn: jobsApi.listCVs })

  const [tab, setTab] = useState<'about'|'experience'|'preferences'>('about')
  const [importLoading, setImportLoading] = useState(false)
  const [importError, setImportError] = useState('')
  const [importPreview, setImportPreview] = useState<any>(null)
  const [selCvId, setSelCvId] = useState('')
  const [lkdInput, setLkdInput] = useState('')
  const [lkdPaste, setLkdPaste] = useState('')
  const [showLkdPaste, setShowLkdPaste] = useState(false)
  const [cvUploading, setCvUploading] = useState(false)
  const [cvUploadErr, setCvUploadErr] = useState('')

  const [fullName, setFullName] = useState('')
  const [headline, setHeadline] = useState('')
  const [location, setLocation] = useState('')
  const [linkedinUrl, setLinkedinUrl] = useState('')
  const [email, setEmail] = useState('')
  const [phone, setPhone] = useState('')
  const [nationality, setNationality] = useState('')
  const [languages, setLanguages] = useState('')
  const [summary, setSummary] = useState('')
  const [skills, setSkills] = useState('')

  const [prefWorkType, setPrefWorkType] = useState<string[]>([])
  const [prefLevel, setPrefLevel] = useState<string[]>([])
  const [prefRoles, setPrefRoles] = useState<string[]>([])
  const [prefRegions, setPrefRegions] = useState<string[]>([])
  const [prefCompanyType, setPrefCompanyType] = useState<string[]>([])
  const [prefStage, setPrefStage] = useState<string[]>([])
  const [prefSectors, setPrefSectors] = useState<string[]>([])
  const [prefSalMin, setPrefSalMin] = useState('')
  const [prefSalMax, setPrefSalMax] = useState('')
  const [prefCurrency, setPrefCurrency] = useState('AED')
  const [prefReloc, setPrefReloc] = useState<string[]>([])
  const [prefNotice, setPrefNotice] = useState<string[]>([])
  const [prefOpenTo, setPrefOpenTo] = useState('')
  const [saved, setSaved] = useState(false)

  const getCtx = () => { try { return JSON.parse(profile?.global_context||'{}') } catch { return {} } }

  useEffect(() => {
    if (!profile) return
    const ctx = getCtx()
    setFullName(ctx.full_name||''); setHeadline(profile.headline||'')
    setLocation((profile as any).location||''); setLinkedinUrl(ctx.linkedin_url||'')
    setEmail(ctx.email||''); setPhone(ctx.phone||''); setNationality(ctx.nationality||'')
    setLanguages((ctx.languages||[]).join(', ')); setSummary(ctx.summary||'')
    setSkills((ctx.skills||[]).join(', '))
    if (!lkdInput) setLkdInput(ctx.linkedin_url||'')
    const p = ctx.__preferences||{}
    setPrefWorkType(p.workType||[]); setPrefLevel(p.level||[]); setPrefRoles(p.roles||[])
    setPrefRegions(p.regions||[]); setPrefCompanyType(p.companyType||[]); setPrefStage(p.stage||[])
    setPrefSectors(p.sectors||[]); setPrefSalMin(p.salaryMin||''); setPrefSalMax(p.salaryMax||'')
    setPrefCurrency(p.currency||'AED'); setPrefReloc(p.relocation||[]); setPrefNotice(p.notice||[])
    setPrefOpenTo(p.openTo||'')
    const hasExp = (profile.experiences||[]).length > 0
  }, [profile])

  const ensure = async (): Promise<string> => {
    if (profile?.id) return profile.id
    const c = await profileApi.create({ headline: '' }); qc.invalidateQueries({queryKey:['profile']}); return c.id
  }

  const buildCtx = () => {
    const ctx = getCtx()
    Object.assign(ctx, { full_name:fullName, email, phone, nationality, linkedin_url:linkedinUrl,
      languages: languages.split(',').map((s:string)=>s.trim()).filter(Boolean),
      summary, skills: skills.split(',').map((s:string)=>s.trim()).filter(Boolean),
      __preferences: { workType:prefWorkType, level:prefLevel, roles:prefRoles, regions:prefRegions,
        companyType:prefCompanyType, stage:prefStage, sectors:prefSectors,
        salaryMin:prefSalMin, salaryMax:prefSalMax, currency:prefCurrency,
        relocation:prefReloc, notice:prefNotice, openTo:prefOpenTo } })
    return JSON.stringify(ctx)
  }

  const saveMut = useMutation({
    mutationFn: async () => { const pid = await ensure(); return profileApi.update(pid, { headline, location, global_context: buildCtx() } as any) },
    onSuccess: () => { qc.invalidateQueries({queryKey:['profile']}); setSaved(true); setTimeout(()=>setSaved(false),2500) },
  })

  const addExpMut = useMutation({
    mutationFn: async () => { const pid = await ensure(); return profileApi.addExperience(pid, { role_title:'', company_name:'', display_order:(profile?.experiences?.length||0) }) },
    onSuccess: () => { qc.invalidateQueries({queryKey:['profile']}); setTab('experience') },
  })

  const handleCvUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    setCvUploading(true); setCvUploadErr(''); setImportError('')
    try {
      // Step 1: upload file
      const cv = await cvsApi.upload(file)
      await qc.invalidateQueries({ queryKey: ['cvs'] })
      setSelCvId(cv.id)
      setCvUploading(false)
      setImportLoading(true)
      try {
        const pid = await ensure()
        const r = await profileApi.importFromCV(pid, cv.id)
        if (!r || typeof r !== 'object') { setImportError('Server returned empty response'); return }
        // Apply immediately
        await profileApi.applyImport(pid, r, true)
        if (r.full_name)         setFullName(r.full_name)
        if (r.headline)          setHeadline(r.headline)
        if (r.location)          setLocation(r.location)
        if (r.email)             setEmail(r.email)
        if (r.phone)             setPhone(r.phone)
        if (r.nationality)       setNationality(r.nationality)
        if (r.linkedin_url)      { setLinkedinUrl(r.linkedin_url); setLkdInput(r.linkedin_url) }
        if (r.skills?.length)    setSkills(r.skills.join(', '))
        if (r.languages?.length) setLanguages(r.languages.join(', '))
        if (r.summary)           setSummary(r.summary)
        await qc.invalidateQueries({queryKey:['profile']})
        setImportPreview({...r, profileId: pid})
        setTab('experience')
      } catch(ie: any) {
        console.error('[import] error=', ie)
        setImportError(ie?.response?.data?.detail || ie?.message || 'Extraction failed')
      } finally {
        setImportLoading(false)
      }
    } catch(e: any) {
      setCvUploading(false)
      setCvUploadErr(e?.response?.data?.detail || 'Upload failed — check file size and format')
    }
  }

  const doImportCV = async () => {
    if (!selCvId) return
    setImportLoading(true); setImportError(''); setImportPreview(null)
    try {
      const pid = await ensure()
      const r = await profileApi.importFromCV(pid, selCvId)
      // Apply immediately — no extra click needed
      try {
        await profileApi.applyImport(pid, r, true)
      } catch(applyErr: any) {
        setImportError(applyErr?.response?.data?.detail || applyErr?.message || 'Failed to apply import')
      }
      if (r.full_name)         setFullName(r.full_name)
      if (r.headline)          setHeadline(r.headline)
      if (r.location)          setLocation(r.location)
      if (r.email)             setEmail(r.email)
      if (r.phone)             setPhone(r.phone)
      if (r.nationality)       setNationality(r.nationality)
      if (r.linkedin_url)      { setLinkedinUrl(r.linkedin_url); setLkdInput(r.linkedin_url) }
      if (r.skills?.length)    setSkills(r.skills.join(', '))
      if (r.languages?.length) setLanguages(r.languages.join(', '))
      if (r.summary)           setSummary(r.summary)
      await qc.invalidateQueries({queryKey:['profile']})
      setImportPreview({...r, profileId: pid})
      setTab('experience')
    } catch(e:any) {
      console.error('[doImportCV] error=', e)
      const detail = e?.response?.data?.detail || e?.message || 'Extraction failed'
      setImportError(detail)
    } finally { setImportLoading(false) }
  }
  const doImportLinkedIn = async () => {
    const hasUrl = lkdInput.includes('linkedin.com')
    const hasPaste = lkdPaste.trim().length > 50
    if (!hasUrl && !hasPaste) { setImportError('Enter a LinkedIn URL or paste your profile text'); return }
    setImportLoading(true); setImportError(''); setImportPreview(null)
    try { const pid = await ensure(); const r = await profileApi.importFromLinkedIn(pid, lkdInput, lkdPaste); setImportPreview({...r, profileId:pid}) }
    catch(e:any) { setImportError(e?.response?.data?.detail||'Import failed') } finally { setImportLoading(false) }
  }
  const doApply = async (overwrite:boolean) => {
    if (!importPreview) return; setImportLoading(true)
    try {
      await profileApi.applyImport(importPreview.profileId, importPreview, overwrite)
      // Populate ALL fields from import immediately — don't wait for DB refetch
      if (importPreview.full_name)   setFullName(importPreview.full_name)
      if (importPreview.headline)    setHeadline(importPreview.headline)
      if (importPreview.location)    setLocation(importPreview.location)
      if (importPreview.email)       setEmail(importPreview.email)
      if (importPreview.phone)       setPhone(importPreview.phone)
      if (importPreview.nationality) setNationality(importPreview.nationality)
      if (importPreview.linkedin_url) { setLinkedinUrl(importPreview.linkedin_url); setLkdInput(importPreview.linkedin_url) }
      if (importPreview.skills?.length)    setSkills(importPreview.skills.join(', '))
      if (importPreview.languages?.length) setLanguages(importPreview.languages.join(', '))
      if (importPreview.summary) setSummary(importPreview.summary)
      // Then refresh from DB to get experiences
      await qc.invalidateQueries({queryKey:['profile']})
      setImportPreview(null)
      setLkdPaste('')
      setShowLkdPaste(false)
      setTab('experience')
    } catch(e:any) { setImportError(e?.response?.data?.detail||'Apply failed') } finally { setImportLoading(false) }
  }

  if (isLoading) return <div className={s.loading}>Loading…</div>

  const experiences = profile?.experiences||[]
  const parsedCvs = (cvs||[]).filter((c:any)=>c.status==='parsed')
  const prefComplete = prefRegions.length>0 && prefRoles.length>0
  const pct = [fullName,headline,location,summary,skills].filter(Boolean).length*20

  return (
    <div className={s.page}>

      <header className={s.topbar}>
        <div className={s.topbarLeft}>
          <h1 className={s.pageTitle}>My Profile</h1>
          <div className={s.pctWrap}><div className={s.pctBar}><div className={s.pctFill} style={{width:`${pct}%`}}/></div><span className={s.pctTxt}>{pct}%</span></div>
        </div>
        <div className={s.tabBar}>
          <button className={[s.tab,tab==='about'?s.tabActive:''].join(' ')} onClick={()=>setTab('about')}>👤 About</button>
          <button className={[s.tab,tab==='experience'?s.tabActive:''].join(' ')} onClick={()=>setTab('experience')}>💼 Experience{experiences.length>0&&<span className={s.tabBadge}>{experiences.length}</span>}</button>
          <button className={[s.tab,tab==='preferences'?s.tabActive:''].join(' ')} onClick={()=>setTab('preferences')}>🎯 Job Preferences{prefComplete&&<span className={s.tabCheck}>✓</span>}</button>
        </div>
        <div className={s.topbarRight}><Button variant="primary" loading={saveMut.isPending} onClick={()=>saveMut.mutate()}>{saved?'✓ Saved':'Save profile'}</Button></div>
      </header>

      {/* ABOUT */}
      {tab==='about' && (
        <div className={s.content}>
          <div className={s.importStrip}>
            <div className={s.importStripTitle}>⚡ Auto-fill your profile</div>

            {/* CV row */}
            <div className={s.importBlock}>
              <div className={s.importBlockLabel}>📄 From CV</div>
              <div className={s.importStripRow}>
                {parsedCvs.length > 0 && (
                  <>
                    <select className={s.importSelect} value={selCvId} onChange={e=>setSelCvId(e.target.value)}>
                      <option value="">Select existing CV…</option>
                      {parsedCvs.map((c:any)=><option key={c.id} value={c.id}>{c.original_filename}</option>)}
                    </select>
                    <Button variant="ghost" loading={importLoading} onClick={doImportCV} disabled={!selCvId}>Extract →</Button>
                    <div className={s.importOr}>or</div>
                  </>
                )}
                <label className={[s.uploadBtn, cvUploading?s.uploadBtnBusy:''].join(' ')}>
                  {cvUploading ? '⏳ Uploading & parsing…' : '⬆ Upload new CV (.pdf / .docx)'}
                  <input type="file" accept=".pdf,.docx" style={{display:'none'}} onChange={handleCvUpload} disabled={cvUploading}/>
                </label>
              </div>
              {cvUploadErr && <div className={s.importErr}>{cvUploadErr}</div>}
              {cvUploading && <div className={s.uploadProgress}>Parsing your CV — this takes ~20 seconds…</div>}
            </div>

            {/* LinkedIn row — URL + paste in same area, no toggle */}
            <div className={s.importBlock}>
              <div className={s.importBlockLabel}>💼 From LinkedIn — paste URL, profile text, or both</div>
              <div className={s.importStripRow}>
                <input
                  className={s.importInput}
                  value={lkdInput}
                  onChange={e=>setLkdInput(e.target.value)}
                  placeholder="https://linkedin.com/in/yourname  (optional if you paste text below)"
                />
                <Button variant="ghost" loading={importLoading} onClick={doImportLinkedIn}>Extract →</Button>
              </div>
              <textarea
                className={s.importPaste}
                value={lkdPaste}
                onChange={e=>setLkdPaste(e.target.value)}
                placeholder="Optional but recommended: go to your LinkedIn profile → Ctrl+A → Ctrl+C → paste here. Gives Claude much richer data than URL scraping alone."
                rows={3}
              />
            </div>

            {importError && <div className={s.importErr}>{importError}</div>}
          </div>

          {importPreview && (
            <div className={s.importPreview}>
              <div className={s.ipHeader}>
                <div className={s.ipHeaderLeft}>
                  <div className={s.ipCheckmark}>✓</div>
                  <div>
                    <div className={s.ipTitle}>Extraction complete</div>
                    <div className={s.ipStats}>{importPreview.experiences?.length||0} roles · {importPreview.skills?.length||0} skills · {importPreview.education?.length||0} education</div>
                  </div>
                </div>
                <button className={s.ipCancel} onClick={()=>setImportPreview(null)}>✕</button>
              </div>

              {/* Identity */}
              {(importPreview.full_name||importPreview.headline||importPreview.location) && (
                <div className={s.ipSection}>
                  {importPreview.full_name && <div className={s.ipName}>{importPreview.full_name}</div>}
                  {importPreview.headline && <div className={s.ipHeadline}>{importPreview.headline}</div>}
                  <div className={s.ipMetaRow}>
                    {importPreview.location && <span>📍 {importPreview.location}</span>}
                    {importPreview.email && <span>✉ {importPreview.email}</span>}
                    {importPreview.phone && <span>📞 {importPreview.phone}</span>}
                    {importPreview.nationality && <span>🌍 {importPreview.nationality}</span>}
                  </div>
                </div>
              )}

              {/* Summary */}
              {importPreview.summary && (
                <div className={s.ipSection}>
                  <div className={s.ipSectionLabel}>Summary</div>
                  <div className={s.ipSummary}>{importPreview.summary}</div>
                </div>
              )}

              {/* Experiences */}
              {importPreview.experiences?.length > 0 && (
                <div className={s.ipSection}>
                  <div className={s.ipSectionLabel}>Experience ({importPreview.experiences.length} roles)</div>
                  {importPreview.experiences.map((e:any,i:number) => (
                    <div key={i} className={s.ipExpCard}>
                      <div className={s.ipExpTop}>
                        <div className={s.ipExpRole}>{e.role_title}</div>
                        <div className={s.ipExpDates}>{e.start_date}{e.start_date&&'→'}{e.end_date||'Present'}</div>
                      </div>
                      <div className={s.ipExpCo}>{e.company_name}{e.location&&` · ${e.location}`}</div>
                      {e.outcomes && <div className={s.ipExpOutcome}>📈 {e.outcomes.slice(0,200)}{e.outcomes.length>200?'…':''}</div>}
                      {e.contribution && <div className={s.ipExpContrib}>🎯 {e.contribution.slice(0,150)}{e.contribution.length>150?'…':''}</div>}
                    </div>
                  ))}
                </div>
              )}

              {/* Skills */}
              {importPreview.skills?.length > 0 && (
                <div className={s.ipSection}>
                  <div className={s.ipSectionLabel}>Skills ({importPreview.skills.length})</div>
                  <div className={s.ipSkillsWrap}>{importPreview.skills.map((sk:string)=><span key={sk} className={s.chip}>{sk}</span>)}</div>
                </div>
              )}

              {/* Education */}
              {importPreview.education?.length > 0 && (
                <div className={s.ipSection}>
                  <div className={s.ipSectionLabel}>Education</div>
                  {importPreview.education.map((ed:any,i:number) => (
                    <div key={i} className={s.ipEd}>{ed.degree&&`${ed.degree} · `}{ed.institution}{ed.year&&` · ${ed.year}`}</div>
                  ))}
                </div>
              )}

              <div className={s.ipActions}>
                <div className={s.ipActionHint}>Apply this to your profile — you can edit any field afterwards</div>
                <div className={s.ipActionBtns}>
                  <button className={s.ipReplace} onClick={()=>doApply(true)}>Replace all existing data</button>
                  <Button variant="primary" loading={importLoading} onClick={()=>doApply(false)}>Add to profile →</Button>
                </div>
              </div>
            </div>
          )}

          <div className={s.card}>
            <SecTitle icon="👤" title="Personal information" sub="Used in your CV header and profile matching"/>
            <div className={s.grid3}>
              <F label="Full name"><input className={s.input} value={fullName} onChange={e=>setFullName(e.target.value)} placeholder="e.g. Mohammed Al Rashidi"/></F>
              <F label="Email"><input className={s.input} type="email" value={email} onChange={e=>setEmail(e.target.value)} placeholder="you@email.com"/></F>
              <F label="Phone"><input className={s.input} value={phone} onChange={e=>setPhone(e.target.value)} placeholder="+971 50 123 4567"/></F>
              <F label="Current location"><input className={s.input} value={location} onChange={e=>setLocation(e.target.value)} placeholder="e.g. Dubai, UAE"/></F>
              <F label="Nationality"><input className={s.input} value={nationality} onChange={e=>setNationality(e.target.value)} placeholder="e.g. British, Emirati"/></F>
              <F label="LinkedIn URL"><input className={s.input} value={linkedinUrl} onChange={e=>setLinkedinUrl(e.target.value)} placeholder="https://linkedin.com/in/yourname"/></F>
            </div>
            <F label="Languages" hint="Comma-separated e.g. English (native), Arabic (fluent)"><input className={s.input} value={languages} onChange={e=>setLanguages(e.target.value)} placeholder="English, Arabic, French…"/></F>
          </div>

          <div className={s.card}>
            <SecTitle icon="✍️" title="Professional headline" sub="One line that defines you — top of your CV and profile"/>
            <input className={s.input} value={headline} onChange={e=>setHeadline(e.target.value)} placeholder="e.g. COO | MENA Operations | Series A–C | P&L $50M+"/>
          </div>

          <div className={s.card}>
            <SecTitle icon="📝" title="Professional summary" sub="2–4 sentences. Claude uses this for cover letters and packs."/>
            <textarea className={s.textarea} rows={4} value={summary} onChange={e=>setSummary(e.target.value)} placeholder="Write your summary, or let CV import fill it…"/>
          </div>

          <div className={s.card}>
            <SecTitle icon="🛠" title="Core skills" sub="Comma-separated. Used for keyword matching in tailored CVs."/>
            <textarea className={s.textarea} rows={3} value={skills} onChange={e=>setSkills(e.target.value)} placeholder="P&L management, OKR planning, M&A integration, GTM strategy, MENA market entry, Fundraising…"/>
          </div>
        </div>
      )}

      {/* EXPERIENCE */}
      {tab==='experience' && (
        <div className={s.content}>
          <div className={s.expTopBar}>
            <div><div className={s.expTopTitle}>{experiences.length} roles</div><div className={s.expTopSub}>Expand each role to add deep-dive answers. More detail = better CVs.</div></div>
            <div className={s.expTopRight}>
              <button className={s.smallBtn} onClick={()=>setTab('about')}>← Re-import</button>
              <Button variant="primary" loading={addExpMut.isPending} onClick={()=>addExpMut.mutate()}>+ Add role</Button>
            </div>
          </div>
          {experiences.length===0
            ? <div className={s.emptyExp}>
                <div className={s.emptyExpIcon}>💼</div>
                <div className={s.emptyExpTitle}>No experience yet</div>
                <div className={s.emptyExpSub}>Import from the About tab or add manually.</div>
                <div className={s.emptyExpBtns}><button className={s.smallBtn} onClick={()=>setTab('about')}>← Import from CV / LinkedIn</button><Button variant="primary" loading={addExpMut.isPending} onClick={()=>addExpMut.mutate()}>+ Add role</Button></div>
              </div>
            : experiences.map(exp=><ExpCard key={exp.id} exp={exp} profileId={profile!.id} onDeleted={()=>qc.invalidateQueries({queryKey:['profile']})}/>)
          }
        </div>
      )}

      {/* PREFERENCES */}
      {tab==='preferences' && (
        <div className={s.content}>
          <div className={s.prefHeader}><div className={s.prefHeaderTitle}>What are you looking for?</div><div className={s.prefHeaderSub}>The Signal Engine uses these to detect opportunities and score fit. Be specific.</div></div>
          <div className={s.card}><SecTitle icon="⚡" title="Work type & level"/><F label="Work type"><Chips opts={PREF.workType} sel={prefWorkType} onChange={setPrefWorkType}/></F><F label="Seniority level" hint="Select all that apply"><Chips opts={PREF.level} sel={prefLevel} onChange={setPrefLevel}/></F></div>
          <div className={s.card}><SecTitle icon="🎯" title="Target roles" sub="Which titles are you targeting?"/><Chips opts={PREF.roles} sel={prefRoles} onChange={setPrefRoles}/></div>
          <div className={s.card}><SecTitle icon="🌍" title="Geography"/><F label="Target regions"><Chips opts={PREF.regions} sel={prefRegions} onChange={setPrefRegions}/></F><F label="Open to relocation?"><Chips opts={PREF.relocation} sel={prefReloc} onChange={setPrefReloc} single/></F></div>
          <div className={s.card}><SecTitle icon="🏢" title="Company type & stage"/><F label="Company type"><Chips opts={PREF.companyType} sel={prefCompanyType} onChange={setPrefCompanyType}/></F><F label="Company stage"><Chips opts={PREF.stage} sel={prefStage} onChange={setPrefStage}/></F><F label="Target sectors"><Chips opts={PREF.sectors} sel={prefSectors} onChange={setPrefSectors}/></F></div>
          <div className={s.card}>
            <SecTitle icon="💰" title="Compensation" sub="Private — used to filter opportunities"/>
            <div className={s.salRow}>
              <F label="Minimum salary"><input className={s.input} value={prefSalMin} onChange={e=>setPrefSalMin(e.target.value)} placeholder="e.g. 600,000"/></F>
              <F label="Maximum / target"><input className={s.input} value={prefSalMax} onChange={e=>setPrefSalMax(e.target.value)} placeholder="e.g. 1,200,000"/></F>
              <F label="Currency"><select className={s.select} value={prefCurrency} onChange={e=>setPrefCurrency(e.target.value)}>{['AED','USD','GBP','EUR','SAR','QAR'].map(c=><option key={c}>{c}</option>)}</select></F>
            </div>
            <F label="Open to equity / bonus?"><input className={s.input} value={prefOpenTo} onChange={e=>setPrefOpenTo(e.target.value)} placeholder="e.g. Yes — equity + bonus preferred, base flexible"/></F>
          </div>
          <div className={s.card}><SecTitle icon="📅" title="Notice period / availability"/><Chips opts={PREF.notice} sel={prefNotice} onChange={setPrefNotice} single/></div>
          <div className={s.saveRow}><Button variant="primary" loading={saveMut.isPending} onClick={()=>saveMut.mutate()}>{saved?'✓ Preferences saved':'Save all preferences'}</Button></div>
        </div>
      )}
    </div>
  )
}
