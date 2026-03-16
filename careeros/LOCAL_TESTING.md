# CVLab — Local Testing Guide

## What you need

| Thing | Status |
|-------|--------|
| Docker Desktop | ✅ Running |
| Anthropic API key | ✅ Have it |
| Serper API key | → Get from serper.dev (2 min) |
| Everything else | ✅ Auto-configured for local |

---

## Step 1 — Create your .env file

In the `careeros/` folder, create a file called `.env`:

```bash
cp .env.example .env
```

Then open `.env` and fill in **only these two lines** — everything else is pre-filled for local:

```
ANTHROPIC_API_KEY=sk-ant-api03-YOUR-KEY-HERE
SERPER_API_KEY=YOUR-SERPER-KEY-HERE
```

Also set a secret key (just paste any long random string):
```
SECRET_KEY=local-dev-secret-key-change-this-in-production-32chars
```

Stripe can be left as the placeholder `sk_test_...` — billing endpoints won't work but everything else will.

---

## Step 2 — Start the backend

```bash
cd careeros
make up
```

First run takes ~3 minutes (downloads Postgres, Redis, builds the Python image).
Subsequent runs take ~20 seconds.

You'll see:
```
✓ CVLab is starting up.
  API:      http://localhost:8000
  Docs:     http://localhost:8000/docs
  MinIO:    http://localhost:9001
  Flower:   http://localhost:5555
```

**Check it's working:**
```bash
curl http://localhost:8000/health
# → {"status":"ok","app":"CVLab",...}

curl http://localhost:8000/health/ready
# → {"status":"ok","checks":{"database":{"status":"ok"},"redis":{"status":"ok"}}}
```

---

## Step 3 — Start the frontend

In a new terminal:

```bash
cd cvlab-frontend
npm install        # first time only, ~1 minute
npm run dev
```

Open **http://localhost:3000** in your browser.

---

## Step 4 — Test the full flow

1. **Register** — create an account at http://localhost:3000/register
2. **Upload a CV** — go to Dashboard → Upload CV (use any DOCX)
3. **New Application** — paste any job description URL or text
4. **Watch it run** — status updates every 3 seconds automatically
5. **Read the pack** — open the completed run, check all tabs including Interview Prep
6. **Download the CV** — the tailored DOCX

---

## What to watch

**Backend logs** (in the careeros folder):
```bash
make logs           # all services
make logs s=api     # API only
make logs s=worker_llm   # LLM task (most interesting)
```

**The LLM task log** shows you exactly what's happening — retrieval, Claude call, rendering. Any errors appear here.

**Flower** (Celery task monitor): http://localhost:5555
- Shows every task: queued → started → completed/failed
- Click a task to see its arguments, result, and runtime

**API docs**: http://localhost:8000/docs
- All endpoints documented and testable directly in the browser

---

## Common issues

**`make up` fails with "fill in your API keys"**
→ You haven't edited `.env` yet. Open it and add your keys.

**CV upload fails**
→ Check `make logs s=worker_default` — the parser runs as a Celery task

**Run stays in "Processing" forever**
→ Check `make logs s=worker_llm` — Claude API error or timeout
→ Make sure `ANTHROPIC_API_KEY` in `.env` is correct

**"CORS error" in browser**
→ Make sure the API is running on port 8000 (`curl http://localhost:8000/health`)

**Port already in use**
→ Something else is on 8000 or 3000. Change `ports` in docker-compose.yml or stop the conflicting service.

---

## Stopping

```bash
make down    # stops containers, keeps data
make clean   # stops + deletes all data (fresh start)
```
