import s from './ScoreRing.module.css'

export function ScoreRing({ score, size = 'md' }: { score?: number | null; size?: 'sm' | 'md' }) {
  if (score == null) return <span className={s.na}>—</span>
  const cls = score >= 80 ? s.hi : score >= 65 ? s.mid : s.lo
  return <div className={[s.ring, cls, size === 'sm' ? s.sm : ''].join(' ')}>{score}</div>
}
