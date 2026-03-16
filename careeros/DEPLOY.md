# CVLab — Deployment Runbook

Complete guide for deploying CVLab to a production server.

---

## Prerequisites

- Ubuntu 22.04 VPS (minimum 4 vCPU, 8GB RAM — for LLM worker headroom)
- Domain pointing at the server IP (`cvlab.co` A record)
- Docker + Docker Compose installed
- AWS S3 bucket created
- Sentry project created (get DSN from project settings)
- Stripe account with live keys + webhook configured

---

## 1. Server setup (first time only)

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER && newgrp docker

# Install Docker Compose plugin
sudo apt-get install -y docker-compose-plugin

# Clone the repo
git clone https://github.com/yourorg/cvlab.git /opt/cvlab
cd /opt/cvlab
```

---

## 2. Configure environment

```bash
cp .env.production.example .env.production
vim .env.production   # fill in ALL values
```

Required keys:
- `SECRET_KEY` — generate with `openssl rand -hex 32`
- `POSTGRES_PASSWORD` — strong random password
- `ANTHROPIC_API_KEY` — your Claude API key
- `SERPER_API_KEY` — for web intelligence retrieval
- `STRIPE_*` — live Stripe keys
- `S3_*` — AWS S3 credentials
- `SMTP_*` — transactional email (Resend recommended)
- `SENTRY_DSN` — from your Sentry project

---

## 3. Build the frontend

```bash
cd cvlab-frontend
npm install
npm run build   # outputs to dist/
cd ..
```

Then copy `dist/` into the nginx volume or mount it (already configured in docker-compose.prod.yml).

---

## 4. SSL certificate (first time only)

Before nginx can start with SSL, get the cert:

```bash
# Start nginx on HTTP only first (comment out SSL lines in cvlab.conf temporarily)
docker compose -f docker/docker-compose.prod.yml --env-file .env.production up -d nginx

# Get the cert
docker compose -f docker/docker-compose.prod.yml --env-file .env.production run --rm certbot \
  certbot certonly --webroot \
  -w /var/www/certbot \
  -d cvlab.co -d www.cvlab.co \
  --email admin@cvlab.co \
  --agree-tos --no-eff-email

# Re-enable SSL lines in cvlab.conf, then restart nginx
docker compose -f docker/docker-compose.prod.yml --env-file .env.production restart nginx
```

---

## 5. First deploy

```bash
cd /opt/cvlab

# Run migrations
docker compose -f docker/docker-compose.prod.yml --env-file .env.production run --rm migrator

# Start all services
docker compose -f docker/docker-compose.prod.yml --env-file .env.production up -d

# Check everything is up
docker compose -f docker/docker-compose.prod.yml --env-file .env.production ps
```

---

## 6. Verify deployment

```bash
# Liveness
curl https://cvlab.co/health

# Readiness (DB + Redis)
curl https://cvlab.co/health/ready

# Deep check (all services)
curl https://cvlab.co/health/deep | python3 -m json.tool

# Check Sentry is receiving events (trigger a test error)
curl https://cvlab.co/api/v1/sentry-test   # returns 500 intentionally
# → should appear in Sentry dashboard within 30 seconds
```

---

## 7. Subsequent deploys (zero-downtime)

```bash
cd /opt/cvlab

# Pull latest code
git pull origin main

# Rebuild and restart only changed services (one at a time for zero-downtime)
docker compose -f docker/docker-compose.prod.yml --env-file .env.production \
  up -d --no-deps --build api

docker compose -f docker/docker-compose.prod.yml --env-file .env.production \
  up -d --no-deps --build worker_default worker_llm worker_rendering

# Run any new migrations
docker compose -f docker/docker-compose.prod.yml --env-file .env.production run --rm migrator
```

---

## 8. Stripe webhook setup

In Stripe Dashboard → Webhooks → Add endpoint:
- URL: `https://cvlab.co/api/v1/billing/webhook`
- Events: `customer.subscription.created`, `customer.subscription.updated`,
  `customer.subscription.deleted`, `invoice.payment_succeeded`, `invoice.payment_failed`
- Copy the webhook signing secret → `STRIPE_WEBHOOK_SECRET` in `.env.production`

---

## 9. Monitoring

**Sentry** — errors + performance:
- API errors appear automatically
- Celery task failures tagged with `run_id` and `user_id`
- LLM call breadcrumbs visible in each event's timeline
- Set up alerts: error rate spike, p95 latency > 30s

**Health endpoint monitoring** (e.g. UptimeRobot or Better Uptime):
- Monitor: `https://cvlab.co/health/ready`
- Alert on non-200

**Log aggregation**:
```bash
# Follow all service logs
docker compose -f docker/docker-compose.prod.yml --env-file .env.production logs -f

# LLM worker only
docker compose -f docker/docker-compose.prod.yml --env-file .env.production logs -f worker_llm
```

---

## 10. Backup

PostgreSQL daily backup (add to cron):
```bash
# /etc/cron.daily/cvlab-backup
#!/bin/bash
docker exec cvlab_postgres_1 pg_dump -U cvlab cvlab \
  | gzip > /backups/cvlab-$(date +%Y%m%d).sql.gz

# Keep last 30 days
find /backups -name "cvlab-*.sql.gz" -mtime +30 -delete
```

S3 backups: CVs and rendered DOCXs are already in S3 — enable versioning on the bucket.

---

## Scaling

**More API throughput**: increase `replicas` on the `api` service (nginx load balances automatically).

**More LLM throughput**: increase `worker_llm` concurrency (watch Anthropic rate limits).

**Managed DB**: replace postgres service with RDS/Supabase URL in `DATABASE_URL` — remove the postgres service from compose.

**Managed Redis**: replace with ElastiCache/Upstash URL — remove the redis service from compose.

---

## Environment sizes

| Size | vCPU | RAM | Supports |
|------|------|-----|----------|
| Small | 2 | 4GB | ~20 concurrent runs/day |
| Medium | 4 | 8GB | ~100 concurrent runs/day |
| Large | 8 | 16GB | ~500 concurrent runs/day |

LLM concurrency is the bottleneck — each run holds ~100K tokens in memory during processing.
