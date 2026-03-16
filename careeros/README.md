# CareerOS

**Application Intelligence Engine** — turns a CV + job description into a tailored DOCX, a positioning strategy, and an intelligence pack.

---

## What it does

1. User builds a **Candidate Profile** — 5 structured questions per role (context, contribution, outcomes, methods, hidden context)
2. User pastes a **Job Description**
3. CareerOS generates:
   - **Tailored DOCX** — same layout as original CV, content rewritten using full profile knowledge
   - **Positioning Strategy** — 3 strongest angles, gaps + mitigations, narrative thread, red flags + responses
   - **Intelligence Pack** — company intel, salary benchmarks, networking targets, interview themes

---

## Quick start (local development)

### Prerequisites
- Docker + Docker Compose
- An Anthropic API key (get one at [console.anthropic.com](https://console.anthropic.com))

### 1. Clone and configure

```bash
git clone https://github.com/your-org/careeros.git
cd careeros
cp .env.example .env
```

Open `.env` and fill in:
- `SECRET_KEY` — generate with `python3 -c "import secrets; print(secrets.token_hex(32))"`
- `ANTHROPIC_API_KEY` — your Claude API key
- `STRIPE_SECRET_KEY` + `STRIPE_WEBHOOK_SECRET` — from Stripe dashboard (use test keys for dev)
- Everything else has working defaults for local development

### 2. Start the stack

```bash
make up
```

This will:
- Build all Docker images
- Start PostgreSQL, Redis, MinIO
- Run database migrations
- Start the API server (hot reload)
- Start 3 Celery worker processes (default, llm, rendering)
- Start Flower (task monitor)

### 3. Verify it's running

| Service | URL |
|---------|-----|
| API docs | http://localhost:8000/docs |
| Health check | http://localhost:8000/health |
| Flower (task monitor) | http://localhost:5555 (admin / careeros) |
| MinIO console | http://localhost:9001 (minioadmin / minioadmin) |

---

## Common commands

```bash
make logs              # Tail all logs
make logs s=api        # Tail API logs only
make logs s=worker_llm # Tail LLM worker logs
make shell             # Bash inside API container
make psql              # PostgreSQL session
make test              # Run test suite
make migrate           # Run pending migrations
make makemigration m="add index"  # Auto-generate migration
make down              # Stop everything
make clean             # Wipe all data (destructive)
```

---

## Architecture

```
Browser / Mobile
      │
      ▼
  FastAPI API  (:8000)
      │
      ├── Auth routes      /api/v1/auth/...
      ├── Profile routes   /api/v1/profiles/...
      ├── Job routes       /api/v1/jobs/...
      ├── Billing routes   /api/v1/billing/...
      └── Upload routes    /api/v1/cvs/...
           │
           ▼
      Celery Workers (3 queues)
           │
           ├── default   — CV parsing
           ├── llm       — Claude API calls (EditPlan + Positioning + ReportPack)
           └── rendering — DOCX render + S3 upload
                │
                ├── PostgreSQL  (data)
                ├── Redis       (broker + cache)
                ├── MinIO/S3    (CV files + output DOCXs)
                └── Anthropic   (Claude API)
```

### Services

| Container | Role |
|-----------|------|
| `careeros_api` | FastAPI server, hot-reload in dev |
| `careeros_worker_default` | CV parsing, general tasks |
| `careeros_worker_llm` | LLM calls — Claude API (concurrency=2) |
| `careeros_worker_rendering` | DOCX rendering + S3 upload |
| `careeros_beat` | Periodic task scheduler |
| `careeros_flower` | Celery task monitor UI |
| `careeros_postgres` | PostgreSQL 16 |
| `careeros_redis` | Redis 7 (broker + result backend) |
| `careeros_minio` | S3-compatible local storage |
| `careeros_migrator` | Alembic migration runner (exits after running) |

---

## Database migrations

Migrations live in `app/db/migrations/versions/`. Each sprint added one:

| Migration | Tables |
|-----------|--------|
| `001_initial_schema` | cvs, job_runs, job_steps |
| `002_users_profiles` | users, candidate_profiles, experience_entries |
| `003_billing` | subscriptions, usage_records |

```bash
# Apply all migrations
make migrate

# Roll back one migration
docker compose -f docker/docker-compose.yml run --rm migrator alembic downgrade -1

# Check current revision
docker compose -f docker/docker-compose.yml run --rm migrator alembic current
```

---

## API overview

### Auth
```
POST /api/v1/auth/register    Create account + auto-login
POST /api/v1/auth/login       Get access + refresh tokens
POST /api/v1/auth/refresh     Rotate refresh token
POST /api/v1/auth/logout      Revoke session
GET  /api/v1/auth/me          Current user info
```

### Candidate Profile
```
GET  /api/v1/profile/questions              Intake question definitions
POST /api/v1/profiles                       Create profile
GET  /api/v1/profiles/active                Get active profile
POST /api/v1/profiles/{id}/activate         Activate draft profile
POST /api/v1/profiles/{id}/experiences      Add experience entry
PATCH /api/v1/profiles/{id}/experiences/{eid}  Fill in intake answers
```

### Job Runs
```
POST /api/v1/jobs              Create run (CV + JD → Intelligence Pack)
GET  /api/v1/jobs/{id}         Poll status + step detail
GET  /api/v1/jobs/{id}/download  Download tailored DOCX
```

### Billing
```
GET  /api/v1/billing/plans     Available plans
GET  /api/v1/billing/status    Current plan + usage
POST /api/v1/billing/checkout  Create Stripe checkout session
POST /api/v1/billing/portal    Create Stripe billing portal session
POST /api/v1/billing/webhook   Stripe webhook receiver
```

---

## Plans

| Plan | Packs/month | Price | Positioning | Company Intel |
|------|-------------|-------|-------------|---------------|
| Free | 3 | $0 | ✗ | ✗ |
| Starter | 10 | $19/mo | ✓ | ✓ |
| Pro | 30 | $49/mo | ✓ | ✓ |
| Unlimited | ∞ | $99/mo | ✓ | ✓ |

---

## Production deployment

```bash
# Build and deploy production stack
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml up -d

# Check health
curl https://your-domain.com/health
```

For production:
- Replace MinIO with AWS S3 (remove `S3_ENDPOINT_URL` from env)
- Use a managed PostgreSQL (e.g. Supabase, Neon, RDS)
- Use a managed Redis (e.g. Upstash, ElastiCache)
- Set `APP_ENV=production` and a strong `SECRET_KEY`
- Configure Stripe live keys
- Set up Stripe webhook forwarding to your production URL

---

## Environment variables

See `.env.example` for the full list with documentation.

Required for the app to start:
- `SECRET_KEY`
- `ANTHROPIC_API_KEY`
- `S3_ACCESS_KEY_ID` + `S3_SECRET_ACCESS_KEY`

Required for payments:
- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`

Optional (degrades gracefully if missing):
- `SERPER_API_KEY` — enables company intel in the Intelligence Pack
- `STRIPE_PUBLISHABLE_KEY` — needed by frontend only
