import { useState } from 'react'
import { useAuthStore } from '../store/auth'
import { api } from '../api/client'
import { Button } from '../components/ui/Button'
import s from './Settings.module.css'

export default function Settings() {
  const { user, logout } = useAuthStore()
  const [loggingOut, setLoggingOut] = useState(false)

  // Name editing
  const [editingName, setEditingName] = useState(false)
  const [name, setName] = useState(user?.full_name ?? '')
  const [nameSaving, setNameSaving] = useState(false)
  const [nameMsg, setNameMsg] = useState('')

  // Password
  const [editingPw, setEditingPw] = useState(false)
  const [currentPw, setCurrentPw] = useState('')
  const [newPw, setNewPw] = useState('')
  const [confirmPw, setConfirmPw] = useState('')
  const [pwSaving, setPwSaving] = useState(false)
  const [pwMsg, setPwMsg] = useState('')
  const [pwError, setPwError] = useState('')

  const handleLogout = async () => {
    setLoggingOut(true)
    try { await api.post('/api/v1/auth/logout') } catch { /* ignore */ }
    logout()
    window.location.href = '/login'
  }

  const saveName = async () => {
    if (!name.trim()) return
    setNameSaving(true)
    setNameMsg('')
    try {
      await api.patch('/api/v1/auth/me', { full_name: name.trim() })
      setNameMsg('Name updated')
      setEditingName(false)
    } catch {
      setNameMsg('Could not update name')
    } finally {
      setNameSaving(false)
    }
  }

  const savePassword = async () => {
    setPwError('')
    if (newPw.length < 8) { setPwError('Password must be at least 8 characters'); return }
    if (newPw !== confirmPw) { setPwError('Passwords do not match'); return }
    setPwSaving(true)
    try {
      await api.post('/api/v1/auth/change-password', { current_password: currentPw, new_password: newPw })
      setPwMsg('Password updated')
      setEditingPw(false)
      setCurrentPw(''); setNewPw(''); setConfirmPw('')
    } catch (e: any) {
      setPwError(e?.response?.data?.error ?? 'Could not update password')
    } finally {
      setPwSaving(false)
    }
  }

  return (
    <div className={s.page}>
      <div className={s.header}>
        <h1 className={s.title}>Settings</h1>
      </div>

      {/* Account info */}
      <div className={s.section}>
        <div className={s.sectionTitle}>Account</div>

        <div className={s.row}>
          <div className={s.rowLabel}>Email</div>
          <div className={s.rowValue}>{user?.email ?? '—'}</div>
        </div>

        <div className={s.row}>
          <div className={s.rowLabel}>Full name</div>
          {editingName ? (
            <div className={s.editRow}>
              <input
                className={s.input}
                value={name}
                onChange={e => setName(e.target.value)}
                placeholder="Your name"
                autoFocus
              />
              <Button variant="primary" loading={nameSaving} onClick={saveName}>Save</Button>
              <Button variant="ghost" onClick={() => { setEditingName(false); setName(user?.full_name ?? '') }}>Cancel</Button>
            </div>
          ) : (
            <div className={s.rowWithAction}>
              <div className={s.rowValue}>{user?.full_name ?? '—'}</div>
              <button className={s.editBtn} onClick={() => setEditingName(true)}>Edit</button>
            </div>
          )}
        </div>
        {nameMsg && <div className={s.successMsg}>{nameMsg}</div>}

        <div className={s.row}>
          <div className={s.rowLabel}>Account status</div>
          <div className={s.rowValue}>
            <span className={user?.is_verified ? s.verified : s.unverified}>
              {user?.is_verified ? '✓ Verified' : 'Unverified — check your email'}
            </span>
          </div>
        </div>
      </div>

      {/* Password */}
      <div className={s.section}>
        <div className={s.sectionTitle}>Security</div>
        <div className={s.row}>
          <div>
            <div className={s.rowLabel}>Password</div>
            <div className={s.rowHint}>Change your login password</div>
          </div>
          {!editingPw && (
            <Button variant="ghost" onClick={() => setEditingPw(true)}>Change password</Button>
          )}
        </div>

        {editingPw && (
          <div className={s.passwordForm}>
            <input className={s.input} type="password" placeholder="Current password" value={currentPw} onChange={e => setCurrentPw(e.target.value)} />
            <input className={s.input} type="password" placeholder="New password (min 8 chars)" value={newPw} onChange={e => setNewPw(e.target.value)} />
            <input className={s.input} type="password" placeholder="Confirm new password" value={confirmPw} onChange={e => setConfirmPw(e.target.value)} />
            {pwError && <div className={s.errorMsg}>{pwError}</div>}
            {pwMsg && <div className={s.successMsg}>{pwMsg}</div>}
            <div className={s.pwButtons}>
              <Button variant="primary" loading={pwSaving} onClick={savePassword}>Update password</Button>
              <Button variant="ghost" onClick={() => { setEditingPw(false); setPwError(''); setPwMsg('') }}>Cancel</Button>
            </div>
          </div>
        )}
      </div>

      {/* Sign out */}
      <div className={s.section}>
        <div className={s.sectionTitle}>Session</div>
        <div className={s.row}>
          <div>
            <div className={s.rowLabel}>Sign out</div>
            <div className={s.rowHint}>You'll need to sign back in to continue.</div>
          </div>
          <Button variant="ghost" loading={loggingOut} onClick={handleLogout}>Sign out</Button>
        </div>
      </div>
    </div>
  )
}
