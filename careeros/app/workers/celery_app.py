"""
app/workers/celery_app.py

Celery application instance and queue configuration.

Three queues with different priorities:
  - default:   lightweight tasks (status updates, cleanup)
  - llm:       Claude API calls — rate-limited, expensive, long-running
  - rendering: DOCX rendering — CPU-bound, medium duration

Workers can be specialised per queue:
  celery -A app.workers.celery_app.celery worker -Q llm --concurrency=2
  celery -A app.workers.celery_app.celery worker -Q rendering --concurrency=4
  celery -A app.workers.celery_app.celery worker -Q default --concurrency=8

Import `celery` from this module everywhere — never instantiate a second app.
"""

from celery import Celery
from celery.signals import worker_process_init
from kombu import Exchange, Queue

from app.config import settings


@worker_process_init.connect
def init_worker(**kwargs):
    """Initialise Sentry in each worker process on startup."""
    from app.monitoring.sentry import init_sentry
    init_sentry()

# ── Application instance ───────────────────────────────────────────────────
celery = Celery(
    "careeros",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    # Autodiscover tasks in all registered task modules
    include=[
        "app.workers.tasks.parse_cv",
        "app.workers.tasks.run_llm",
        "app.workers.tasks.render_docx",
    ],
)

# ── Queue definitions ──────────────────────────────────────────────────────
default_exchange = Exchange("default", type="direct")
llm_exchange = Exchange("llm", type="direct")
rendering_exchange = Exchange("rendering", type="direct")

celery.conf.task_queues = (
    Queue("default", default_exchange, routing_key="default"),
    Queue("llm", llm_exchange, routing_key="llm"),
    Queue("rendering", rendering_exchange, routing_key="rendering"),
)

celery.conf.task_default_queue = "default"
celery.conf.task_default_exchange = "default"
celery.conf.task_default_routing_key = "default"

# ── Task routing — map tasks to queues ────────────────────────────────────
celery.conf.task_routes = {
    "app.workers.tasks.parse_cv.parse_cv_task": {
        "queue": "default",
        "routing_key": "default",
    },
    "app.workers.tasks.run_llm.run_llm_task": {
        "queue": "llm",
        "routing_key": "llm",
    },
    "app.workers.tasks.render_docx.render_docx_task": {
        "queue": "rendering",
        "routing_key": "rendering",
    },
}

# ── Serialization ──────────────────────────────────────────────────────────
celery.conf.task_serializer = "json"
celery.conf.result_serializer = "json"
celery.conf.accept_content = ["json"]
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ── Reliability ────────────────────────────────────────────────────────────
# Acknowledge task AFTER completion, not before
# Prevents message loss if worker crashes mid-task
celery.conf.task_acks_late = True
celery.conf.task_reject_on_worker_lost = True

# Retry connection to broker on startup (handles Docker race conditions)
celery.conf.broker_connection_retry_on_startup = True

# ── Result backend ─────────────────────────────────────────────────────────
# Results expire after 24 hours — we persist status in DB, not Celery
celery.conf.result_expires = 86400

# ── Rate limits per queue ──────────────────────────────────────────────────
# LLM tasks: respect Anthropic API rate limits
# Adjust based on your API tier
celery.conf.task_annotations = {
    "app.workers.tasks.run_llm.run_llm_task": {
        "rate_limit": "10/m",   # 10 LLM calls per minute max
    },
}

# ── Soft/hard time limits ──────────────────────────────────────────────────
celery.conf.task_soft_time_limit = 600   # 5 min: raises SoftTimeLimitExceeded
celery.conf.task_time_limit = 720        # 6 min: hard kill
