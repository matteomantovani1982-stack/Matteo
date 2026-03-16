import { useEffect } from 'react'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Profile from './pages/Profile'
import Billing from './pages/Billing'
import Settings from './pages/Settings'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { authApi } from './api/auth'
import { useAuthStore } from './store/auth'
import { AppShell } from './components/layout/AppShell'
import Dashboard from './pages/Dashboard'
import Home from './pages/Home'
import Applications from './pages/Applications'
import NewApplication from './pages/NewApplication'
import IntelPack from './pages/IntelPack'
import Login from './pages/Login'
import Register from './pages/Register'
import './styles/globals.css'

const qc = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, staleTime: 30_000 },
  },
})

function RequireAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token)
  if (!token) return <Navigate to="/login" replace />
  return <>{children}</>
}

function AuthInit() {
  const { token, setUser, logout } = useAuthStore()
  useEffect(() => {
    if (token) {
      authApi.me().then(setUser).catch(() => {
        logout()
      })
    }
  }, [token, setUser, logout])
  return null
}

export default function App() {
  return (
    <QueryClientProvider client={qc}>
      <BrowserRouter>
        <AuthInit />
        <Routes>
          {/* Public */}
          <Route path="/login"    element={<Login />} />
          <Route path="/register" element={<Register />} />

          {/* Protected */}
          <Route element={<RequireAuth><AppShell /></RequireAuth>}>
            <Route index element={<Home />} />
            <Route path="dashboard" element={<Dashboard />} />
            <Route path="applications" element={<Applications />} />
            <Route path="applications/new" element={<NewApplication />} />
            <Route path="applications/:runId" element={<IntelPack />} />
            {/* Profile / billing stubs */}
            <Route path="profile"  element={<Profile />} />
            <Route path="billing"  element={<Billing />} />
            <Route path="settings" element={<Settings />} />
          </Route>

          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  )
}

function PlaceholderPage({ title }: { title: string }) {
  return (
    <div style={{ padding: '40px 24px', color: 'var(--text-2)', fontSize: 14 }}>
      <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--text)', marginBottom: 8 }}>{title}</div>
      <div>Coming soon — Sprint L/M.</div>
    </div>
  )
}
