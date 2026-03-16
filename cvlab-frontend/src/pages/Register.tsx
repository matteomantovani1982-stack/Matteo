import { useState, FormEvent } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { authApi } from '../api/auth'
import { useAuthStore } from '../store/auth'
import { Button } from '../components/ui/Button'
import s from './Auth.module.css'

export default function Register() {
  const navigate = useNavigate()
  const { setToken, setUser } = useAuthStore()
  const [fullName, setFullName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault()
    if (password.length < 8) {
      setError('Password must be at least 8 characters.')
      return
    }
    setLoading(true)
    setError('')
    try {
      // Register returns user + tokens in one call — no need to login separately
      const res = await authApi.register(email, password, fullName)
      setToken(res.access_token)
      setUser(res.user)
      navigate('/')
    } catch (err: any) {
      const detail = err?.response?.data?.detail
      if (detail?.includes('already') || detail?.includes('duplicate') || err?.response?.status === 409) {
        setError('An account with this email already exists. Try signing in.')
      } else if (err?.response?.status === 422) {
        setError('Please check your details and try again.')
      } else {
        setError('Could not create account. Please try again.')
      }
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className={s.page}>
      <div className={s.card}>
        <div className={s.logo}>CV<span>Lab</span></div>
        <h1 className={s.title}>Create account</h1>
        <form onSubmit={handleSubmit} className={s.form}>
          <div className={s.field}>
            <label className={s.label}>Full name</label>
            <input
              className={s.input}
              type="text"
              autoComplete="name"
              value={fullName}
              onChange={(e) => setFullName(e.target.value)}
              required
            />
          </div>
          <div className={s.field}>
            <label className={s.label}>Email</label>
            <input
              className={s.input}
              type="email"
              autoComplete="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
          </div>
          <div className={s.field}>
            <label className={s.label}>Password</label>
            <input
              className={s.input}
              type="password"
              autoComplete="new-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={8}
              placeholder="Minimum 8 characters"
            />
          </div>
          {error && <div className={s.error}>{error}</div>}
          <Button
            variant="primary"
            loading={loading}
            style={{ width: '100%', justifyContent: 'center' }}
          >
            Create account
          </Button>
        </form>
        <div className={s.footer}>
          Already have an account?{' '}
          <Link to="/login" className={s.link}>Sign in →</Link>
        </div>
      </div>
    </div>
  )
}
