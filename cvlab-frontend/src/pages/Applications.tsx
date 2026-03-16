import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { jobsApi } from '../api/jobs'
import { ScoreRing } from '../components/ui/ScoreRing'
import { Button } from '../components/ui/Button'
import type { JobRunStatus } from '../types'
import s from './Applications.module.css'

const TERMINAL: JobRunStatus[] = ['queued', 'completed', 'failed']

const STAGES = [
  { id: 'watching',     label: 'Watching',     emoji: '👀', color: '#6b7280' },
  { id: 'applied',      label: 'Applied',      emoji: '📤', color: '#2563eb' },
  { id: 'interviewing', label: 'Interviewing', emoji: '🎯', color: '#7c3aed' },
  { id: 'offer',        label: 'Offer',        emoji: '🎉', color: '#059669' },
  { id: 'rejected',     label: 'Rejected',     emoji: '✕',  color: '#dc2626' },
]

function fmtDate(iso: string) {
  return new Date(iso).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
}

function extractTitle(run: { jd_text?: string | null; jd_url?: string | null; output_s3_key?: string | null; role_title?: string | null; company_name?: string | null }) {
  // Best: use role_title from reports (populated after generation)
  if (run.role_title) {
    const title = run.company_name ? `${run.company_name} · ${run.role_title}` : run.role_title
    return title.length > 60 ? title.slice(0, 57) + '…' : title
  }
  // Fallback: first line of JD text
  if (run.jd_text) { const l = run.jd_text.split('\n')[0].trim(); return l.length > 55 ? l.slice(0,52)+'…' : l || 'Untitled' }
  // Fallback: URL hostname
  if (run.jd_url) { try { return new URL(run.jd_url).hostname.replace('www.','') } catch { return run.jd_url ?? 'Untitled' } }
  return 'Untitled'
}

function extractCompany(run: { jd_text?: string | null; jd_url?: string | null; company_name?: string | null }) {
  if (run.company_name) return run.company_name
  if (run.jd_url) { try { return new URL(run.jd_url).hostname.replace('www.','').split('.')[0] } catch {} }
  if (run.jd_text) { const l = run.jd_text.split('\n').filter(Boolean); return l[1]?.trim().slice(0,40) || '' }
  return ''
}

interface Run {
  id: string; status: JobRunStatus; jd_text?: string|null; jd_url?: string|null
  keyword_match_score?: number|null; pipeline_stage?: string|null
  pipeline_notes?: string|null; applied_at?: string|null; created_at: string
  output_s3_key?: string|null; role_title?: string|null; company_name?: string|null
  apply_url?: string|null
}

function KanbanCard({ run, onStageChange, onNotesChange }: { run: Run; onStageChange:(id:string,stage:string)=>void; onNotesChange:(id:string,notes:string)=>void }) {
  const navigate = useNavigate()
  const [showNotes, setShowNotes] = useState(false)
  const [notes, setNotes] = useState(run.pipeline_notes || '')
  const [dragging, setDragging] = useState(false)
  const isCompleted = run.status === 'completed'
  const isProcessing = !TERMINAL.includes(run.status)

  const handleCardClick = () => { if (isCompleted) navigate(`/applications/${run.id}`) }

  return (
    <div
      className={[s.card, dragging ? s.cardDragging : ''].join(' ')}
      draggable
      style={{cursor: isCompleted ? 'pointer' : 'default'}}
      onClick={handleCardClick}
      onDragStart={e => { e.dataTransfer.setData('runId', run.id); setDragging(true) }}
      onDragEnd={() => setDragging(false)}
    >
      {isProcessing && <div className={s.cardProcessing}><span className={s.processingDot}/>Generating pack…</div>}
      <div className={s.cardTitle}>{extractTitle(run)}</div>
      <div className={s.cardMeta}>
        <span className={s.cardDate}>{fmtDate(run.created_at)}</span>
        {run.keyword_match_score != null && <ScoreRing score={run.keyword_match_score} size="sm" />}
      </div>
      {showNotes ? (
        <div className={s.notesArea} onClick={e => e.stopPropagation()}>
          <textarea className={s.notesInput} value={notes} onChange={e=>setNotes(e.target.value)} placeholder="Add notes, follow-up dates, contacts…" rows={3} autoFocus />
          <div className={s.notesBtns}>
            <button className={s.notesSave} onClick={()=>{ onNotesChange(run.id,notes); setShowNotes(false) }}>Save</button>
            <button className={s.notesCancel} onClick={()=>setShowNotes(false)}>Cancel</button>
          </div>
        </div>
      ) : (
        run.pipeline_notes && <div className={s.notesBadge} onClick={e=>{e.stopPropagation();setShowNotes(true)}}>📝 {run.pipeline_notes.slice(0,60)}{run.pipeline_notes.length>60?'…':''}</div>
      )}
      <div className={s.cardActions} onClick={e => e.stopPropagation()}>
        <button className={s.cardBtn} onClick={()=>setShowNotes(v=>!v)} title="Notes">📝</button>
        {isCompleted && <>
          <button className={s.cardBtn} onClick={()=>navigate(`/applications/${run.id}`)} title="View pack">→</button>
          <button className={s.cardBtn} onClick={async()=>{ const{url}=await jobsApi.downloadUrl(run.id); window.open(url,'_blank') }} title="Download CV">↓</button>
          {(run.apply_url || run.jd_url) && (
            <button className={s.cardBtn} onClick={()=>window.open(run.apply_url||run.jd_url||'','_blank')} title="Apply now">↗</button>
          )}
        </>}
        <div className={s.stagePicker}>
          {STAGES.filter(st=>st.id!==(run.pipeline_stage||'watching')).map(st=>(
            <button key={st.id} className={s.stagePickerBtn} onClick={()=>onStageChange(run.id,st.id)} title={`→ ${st.label}`} style={{color:st.color}}>{st.emoji}</button>
          ))}
        </div>
      </div>
    </div>
  )
}

function KanbanColumn({ stage, runs, onStageChange, onNotesChange, onDrop }: {
  stage: typeof STAGES[0]; runs: Run[]
  onStageChange:(id:string,stage:string)=>void; onNotesChange:(id:string,notes:string)=>void; onDrop:(runId:string,toStage:string)=>void
}) {
  const [dragOver, setDragOver] = useState(false)
  return (
    <div
      className={[s.column, dragOver?s.columnDragOver:''].join(' ')}
      onDragOver={e=>{e.preventDefault();setDragOver(true)}}
      onDragLeave={()=>setDragOver(false)}
      onDrop={e=>{ e.preventDefault(); setDragOver(false); const id=e.dataTransfer.getData('runId'); if(id) onDrop(id,stage.id) }}
    >
      <div className={s.colHeader}>
        <div className={s.colTitle}><span>{stage.emoji}</span>{stage.label}</div>
        <span className={s.colCount} style={{background:stage.color+'18',color:stage.color}}>{runs.length}</span>
      </div>
      <div className={s.colCards}>
        {runs.length===0 && <div className={s.colEmpty}>Drag cards here</div>}
        {runs.map(run=><KanbanCard key={run.id} run={run} onStageChange={onStageChange} onNotesChange={onNotesChange}/>)}
      </div>
    </div>
  )
}

export default function Applications() {
  const navigate = useNavigate()
  const qc = useQueryClient()
  const [view, setView] = useState<'kanban'|'list'>('kanban')

  const { data: runs, isLoading } = useQuery({
    queryKey: ['runs'], queryFn: jobsApi.list,
    refetchInterval: q => { const d=q.state.data; if(!Array.isArray(d)) return 5000; return d.some(r=>!TERMINAL.includes(r.status))?3000:false },
  })

  const stageMut = useMutation({
    mutationFn: ({id,stage,notes}:{id:string;stage:string;notes?:string}) => jobsApi.updateStage(id,stage,notes),
    onMutate: async({id,stage}) => {
      await qc.cancelQueries({queryKey:['runs']})
      const prev = qc.getQueryData(['runs'])
      qc.setQueryData(['runs'], (old:Run[]|undefined) => old?.map(r=>r.id===id?{...r,pipeline_stage:stage}:r))
      return {prev}
    },
    onError: (_e,_v,ctx) => qc.setQueryData(['runs'],ctx?.prev),
    onSettled: () => qc.invalidateQueries({queryKey:['runs']}),
  })

  const notesMut = useMutation({
    mutationFn: ({id,notes}:{id:string;notes:string}) => {
      const stage = runs?.find(r=>r.id===id)?.pipeline_stage || 'watching'
      return jobsApi.updateStage(id,stage,notes)
    },
    onMutate: async({id,notes}) => {
      await qc.cancelQueries({queryKey:['runs']})
      const prev = qc.getQueryData(['runs'])
      qc.setQueryData(['runs'],(old:Run[]|undefined)=>old?.map(r=>r.id===id?{...r,pipeline_notes:notes}:r))
      return {prev}
    },
    onError:(_e,_v,ctx)=>qc.setQueryData(['runs'],ctx?.prev),
    onSettled:()=>qc.invalidateQueries({queryKey:['runs']}),
  })

  const handleStageChange = (id:string, stage:string) => stageMut.mutate({id,stage})
  const handleNotesChange = (id:string, notes:string) => notesMut.mutate({id,notes})

  const grouped = STAGES.reduce((acc,st)=>{
    acc[st.id] = (runs||[]).filter(r=>(r.pipeline_stage||'watching')===st.id)
    return acc
  },{} as Record<string,Run[]>)

  const total = runs?.length||0
  const active = (runs||[]).filter(r=>['applied','interviewing','offer'].includes(r.pipeline_stage||'')).length
  const interviewing = grouped['interviewing']?.length||0
  const offers = grouped['offer']?.length||0

  return (
    <div className={s.page}>
      <header className={s.topbar}>
        <h1 className={s.pageTitle}>Applications</h1>
        <div className={s.topbarRight}>
          <div className={s.viewToggle}>
            <button className={[s.viewBtn,view==='kanban'?s.viewBtnActive:''].join(' ')} onClick={()=>setView('kanban')}>⬜ Kanban</button>
            <button className={[s.viewBtn,view==='list'?s.viewBtnActive:''].join(' ')} onClick={()=>setView('list')}>☰ List</button>
          </div>
          <Button variant="primary" onClick={()=>navigate('/applications/new')}>+ New</Button>
        </div>
      </header>

      <div className={s.statsStrip}>
        <div className={s.stat}><span className={s.statNum}>{total}</span><span className={s.statLbl}>Total</span></div>
        <div className={s.statDiv}/>
        <div className={s.stat}><span className={s.statNum} style={{color:'#2563eb'}}>{active}</span><span className={s.statLbl}>Active</span></div>
        <div className={s.statDiv}/>
        <div className={s.stat}><span className={s.statNum} style={{color:'#7c3aed'}}>{interviewing}</span><span className={s.statLbl}>Interviewing</span></div>
        <div className={s.statDiv}/>
        <div className={s.stat}><span className={s.statNum} style={{color:'#059669'}}>{offers}</span><span className={s.statLbl}>Offers</span></div>
      </div>

      {isLoading && <div className={s.loading}>Loading your applications…</div>}

      {!isLoading && (!runs||runs.length===0) && (
        <div className={s.empty}>
          <div className={s.emptyIcon}>📋</div>
          <div className={s.emptyTitle}>No applications yet</div>
          <div className={s.emptySub}>Scout a job from the Dashboard or paste a JD to get started</div>
          <Button variant="primary" onClick={()=>navigate('/applications/new')}>+ New Application</Button>
        </div>
      )}

      {!isLoading && runs && runs.length>0 && view==='kanban' && (
        <div className={s.board}>
          {STAGES.map(stage=>(
            <KanbanColumn key={stage.id} stage={stage} runs={grouped[stage.id]||[]}
              onStageChange={handleStageChange} onNotesChange={handleNotesChange} onDrop={handleStageChange}/>
          ))}
        </div>
      )}

      {!isLoading && runs && runs.length>0 && view==='list' && (
        <div className={s.listWrap}>
          <div className={s.listHead}>
            <div className={s.lth}>Role</div>
            <div className={s.lth}>Stage</div>
            <div className={s.lth}>Score</div>
            <div className={s.lth}>Applied</div>
            <div className={s.lth}>Actions</div>
          </div>
          {runs.map(run=>{
            const stage=STAGES.find(st=>st.id===(run.pipeline_stage||'watching'))||STAGES[0]
            return (
              <div key={run.id} className={s.listRow} style={{cursor:run.status==='completed'?'pointer':'default'}} onClick={()=>run.status==='completed'&&navigate(`/applications/${run.id}`)}>
                <div><div className={s.listTitle}>{extractTitle(run)}</div><div className={s.listCompany}>{extractCompany(run)}</div></div>
                <div><span className={s.stagePill} style={{background:stage.color+'18',color:stage.color}}>{stage.emoji} {stage.label}</span></div>
                <div><ScoreRing score={run.keyword_match_score} size="sm"/></div>
                <div className={s.listDate}>{run.applied_at?fmtDate(run.applied_at):'—'}</div>
                <div className={s.listActions} onClick={e=>e.stopPropagation()}>
                  <select className={s.stageSelect} value={run.pipeline_stage||'watching'} onChange={e=>handleStageChange(run.id,e.target.value)}>
                    {STAGES.map(st=><option key={st.id} value={st.id}>{st.emoji} {st.label}</option>)}
                  </select>
                  {run.status==='completed'&&<button className={s.listBtn} onClick={()=>navigate(`/applications/${run.id}`)}>→</button>}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
