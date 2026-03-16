import { NavLink, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { profileApi } from '../../api/profile'
import { useAuthStore } from '../../store/auth'
import s from './Sidebar.module.css'

const NAV = [
  {
    group: 'Workspace',
    items: [
      { to: '/',             label: 'Home',         icon: <HomeIcon /> },
      { to: '/dashboard',    label: 'Job Scout',    icon: <GridIcon /> },
      { to: '/applications', label: 'Applications', icon: <FileIcon /> },
      { to: '/profile',      label: 'My Profile',   icon: <UserIcon /> },
    ],
  },
  {
    group: 'Account',
    items: [
      { to: '/billing',  label: 'Billing',  icon: <CardIcon /> },
      { to: '/settings', label: 'Settings', icon: <SettingsIcon /> },
    ],
  },
]

export function Sidebar() {
  const { user, logout } = useAuthStore()
  const navigate = useNavigate()

  const { data: usage } = useQuery({
    queryKey: ['usage'],
    queryFn: profileApi.usage,
    staleTime: 60_000,
  })

  const pctUsed = usage
    ? usage.monthly_run_limit
      ? (usage.runs_used_this_month / usage.monthly_run_limit) * 100
      : 0
    : 0

  const initials = user?.full_name
    ? user.full_name.split(' ').map((n) => n[0]).join('').slice(0, 2).toUpperCase()
    : user?.email?.slice(0, 2).toUpperCase() ?? '??'

  return (
    <aside className={s.sidebar}>
      {/* Logo */}
      <div className={s.logo}>
        <div className={s.logoMark}>
          <FileIcon stroke="white" />
        </div>
        <span className={s.logoName}>Stealth<span>Role</span></span>
      </div>

      {/* Nav */}
      <nav className={s.nav}>
        {NAV.map((group) => (
          <div key={group.group} className={s.navGroup}>
            <div className={s.groupLabel}>{group.group}</div>
            {group.items.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.to === '/'}
                className={({ isActive }) =>
                  [s.navItem, isActive ? s.active : ''].join(' ')
                }
              >
                <span className={s.navIcon}>{item.icon}</span>
                {item.label}
              </NavLink>
            ))}
          </div>
        ))}
      </nav>

      {/* Bottom */}
      <div className={s.bottom}>
        {usage && (
          <div className={s.planCard}>
            <div className={s.planTop}>
              <span className={s.planName}>{usage.plan} plan</span>
              <span className={s.planCount}>
                {usage.runs_used_this_month} / {usage.monthly_run_limit ?? '∞'} packs
              </span>
            </div>
            <div className={s.planTrack}>
              <div className={s.planFill} style={{ width: `${Math.min(pctUsed, 100)}%` }} />
            </div>
            {usage.runs_remaining !== null && (
              <div className={s.planSub}>{usage.runs_remaining} remaining this month</div>
            )}
          </div>
        )}

        <button
          className={s.userRow}
          onClick={() => { logout(); navigate('/login') }}
          title="Sign out"
        >
          <div className={s.avatar}>{initials}</div>
          <div className={s.userInfo}>
            <div className={s.userName}>{user?.full_name ?? user?.email}</div>
            <div className={s.userEmail}>{user?.email}</div>
          </div>
          <span className={s.caret}>⌄</span>
        </button>
      </div>
    </aside>
  )
}

// ── Inline icons (no external dep) ───────────────────────────────────────────

function HomeIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" />
      <polyline points="9 22 9 12 15 12 15 22" />
    </svg>
  )
}
function GridIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="7" height="7" /><rect x="14" y="3" width="7" height="7" />
      <rect x="14" y="14" width="7" height="7" /><rect x="3" y="14" width="7" height="7" />
    </svg>
  )
}
function FileIcon({ stroke = 'currentColor' }: { stroke?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke={stroke} strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <polyline points="14 2 14 8 20 8" />
      <line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" />
    </svg>
  )
}
function UserIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round">
      <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" /><circle cx="12" cy="7" r="4" />
    </svg>
  )
}
function CardIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round">
      <rect x="1" y="4" width="22" height="16" rx="2" /><line x1="1" y1="10" x2="23" y2="10" />
    </svg>
  )
}
function SettingsIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="3" />
      <path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14" />
    </svg>
  )
}
