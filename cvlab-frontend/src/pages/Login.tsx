import { useState, FormEvent } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { authApi } from '../api/auth'
import { useAuthStore } from '../store/auth'
import { Button } from '../components/ui/Button'
import s from './Auth.module.css'

export default function Login() {
  const navigate = useNavigate()
  const { setToken, setUser } = useAuthStore()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      const token = await authApi.login(email, password)
      setToken(token.access_token)
      const user = await authApi.me()
      setUser(user)
      navigate('/')
    } catch (err: any) {
      const status = err?.response?.status
      if (status === 401 || status === 403) {
        setError('Incorrect email or password.')
      } else if (status === 422) {
        setError('Please enter a valid email address.')
      } else {
        setError('Could not sign in. Please try again.')
      }
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className={s.page}>
      <div className={s.card}>
        <div className={s.logo}>CV<span>Lab</span></div>
        <h1 className={s.title}>Sign in</h1>
        <form onSubmit={handleSubmit} className={s.form}>
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
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>
          {error && <div className={s.error}>{error}</div>}
          <Button
            variant="primary"
            loading={loading}
            style={{ width: '100%', justifyContent: 'center' }}
          >
            Sign in
          </Button>
        </form>
        <div className={s.footer}>
          Don't have an account?{' '}
          <Link to="/register" className={s.link}>Create one →</Link>
        </div>
      </div>
    </div>
  )
}
