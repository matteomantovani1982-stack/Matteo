# CVLab Frontend

React + TypeScript + Vite single-page application.

## Stack

- **React 18** + **TypeScript**
- **React Router v6** — client-side routing
- **TanStack Query v5** — server state, polling, caching
- **Zustand** — auth token + user state
- **Axios** — API client with JWT interceptors
- **CSS Modules** — scoped styles, no utility framework
- **Plus Jakarta Sans** + **Lora** — typography
- **Vite** — build tool, dev server, HMR

## Project structure

```
src/
  api/          # API client layer (auth, cvs, jobs, profile)
  components/
    layout/     # AppShell, Sidebar
    ui/         # Button, StatusPill, ScoreRing
  pages/        # Dashboard, NewApplication, IntelPack, Login, Register
  store/        # Zustand auth store
  styles/       # globals.css (design tokens + resets)
  types/        # All TypeScript types matching FastAPI schemas
  App.tsx       # Router + auth guard
  main.tsx      # Entry point
```

## Development

```bash
# Install dependencies
npm install

# Copy env and configure
cp .env.example .env.local

# Start dev server (proxies /api to localhost:8000)
npm run dev
```

The Vite dev server runs on **http://localhost:3000** and proxies
all `/api` requests to the FastAPI backend on `http://localhost:8000`.

Start the backend first:
```bash
cd ../careeros
make dev
```

## Production build

```bash
npm run build
# Output in dist/ — serve as static files
```

For Docker, copy `dist/` into an nginx container and configure:
- Serve `index.html` for all routes (SPA routing)
- Proxy `/api` to the FastAPI backend

## Pages

| Route | Page | Description |
|-------|------|-------------|
| `/` | Dashboard | Stats + recent applications table |
| `/applications` | Applications | Full applications list |
| `/applications/new` | NewApplication | Three-step run creation |
| `/applications/:runId` | IntelPack | Intelligence pack reading view |
| `/login` | Login | Email/password auth |
| `/register` | Register | Account creation |

## Design decisions

- **CSS Modules** over Tailwind — full control, no purge concerns, co-located with components
- **No UI library** — every component matches the design concept exactly
- **Polling via TanStack Query** — `refetchInterval` auto-stops when run completes
- **Optimistic nav** — clicking a completed run row navigates immediately
- **Copy buttons** on outreach messages — the single most-used action in the contacts tab
