import type { JobRunStatus } from '../../types'
import s from './StatusPill.module.css'

const LABEL: Record<JobRunStatus, string> = {
  queued:        'Queued',
  retrieving:    'Researching…',
  llm_processing:'Processing…',
  rendering:     'Rendering…',
  completed:     'Complete',
  failed:        'Failed',
}

const VARIANT: Record<JobRunStatus, string> = {
  queued:         s.queued,
  retrieving:     s.running,
  llm_processing: s.running,
  rendering:      s.running,
  completed:      s.done,
  failed:         s.failed,
}

export function StatusPill({ status }: { status: JobRunStatus }) {
  return (
    <span className={[s.pill, VARIANT[status]].join(' ')}>
      <span className={s.dot} />
      {LABEL[status]}
    </span>
  )
}
