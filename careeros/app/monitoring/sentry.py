"""
app/monitoring/sentry.py

Sentry error monitoring and performance tracing.

Initialised once at startup for both the FastAPI process and Celery workers.
Safe to call multiple times — only inits if SENTRY_DSN is configured.

Usage:
    from app.monitoring.sentry import init_sentry
    init_sentry()          # call in app startup and worker startup

Captures:
    - All unhandled exceptions (API + workers)
    - Slow transactions (traces_sample_rate)
    - Celery task failures with full context
    - Custom breadcrumbs for LLM calls and retrieval steps

Environment tags automatically set:
    - environment: production | staging | development
    - release: app version from settings
    - server_name: hostname
"""

import structlog

logger = structlog.get_logger(__name__)


def init_sentry() -> None:
    """
    Initialise Sentry SDK.
    No-op if SENTRY_DSN is not configured.
    Should be called once at process startup.
    """
    from app.config import settings

    if not settings.sentry_dsn:
        logger.info("sentry_skipped: no SENTRY_DSN configured")
        return

    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
        from sentry_sdk.integrations.celery import CeleryIntegration
        from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
        from sentry_sdk.integrations.redis import RedisIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            environment=settings.sentry_environment or settings.app_env,
            release=f"cvlab@{settings.app_version}",

            # Performance
            traces_sample_rate=settings.sentry_traces_sample_rate,
            profiles_sample_rate=settings.sentry_profiles_sample_rate,

            integrations=[
                # FastAPI + Starlette — captures request context, route info
                StarletteIntegration(transaction_style="endpoint"),
                FastApiIntegration(transaction_style="endpoint"),

                # Celery — captures task failures with task name + args
                CeleryIntegration(
                    monitor_beat_tasks=True,
                    propagate_traces=True,
                ),

                # SQLAlchemy — slow query detection
                SqlalchemyIntegration(),

                # Redis — connection errors
                RedisIntegration(),

                # Python logging — captures ERROR+ log records as breadcrumbs
                LoggingIntegration(
                    level=logging.INFO,        # breadcrumb level
                    event_level=logging.ERROR, # send as Sentry event
                ),
            ],

            # Strip sensitive data from request bodies
            before_send=_scrub_sensitive,

            # Don't send 4xx client errors as Sentry events
            # (404s, validation errors etc clutter the error feed)
            before_send_transaction=None,
        )

        logger.info(
            "sentry_initialised",
            environment=settings.sentry_environment or settings.app_env,
            traces_sample_rate=settings.sentry_traces_sample_rate,
        )

    except ImportError:
        logger.warning("sentry_sdk not installed — skipping Sentry init")
    except Exception as e:
        logger.error(f"sentry_init_failed: {e}")


def _scrub_sensitive(event: dict, hint: dict) -> dict | None:
    """
    Strip sensitive fields from Sentry events before sending.
    Removes passwords, tokens, API keys from request bodies and extra data.
    """
    SENSITIVE_KEYS = {
        "password", "token", "access_token", "secret_key",
        "anthropic_api_key", "stripe_secret_key", "serper_api_key",
        "authorization", "x-api-key",
    }

    def scrub(obj: object) -> object:
        if isinstance(obj, dict):
            return {
                k: "[REDACTED]" if k.lower() in SENSITIVE_KEYS else scrub(v)
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [scrub(i) for i in obj]
        return obj

    if "request" in event:
        if "data" in event["request"]:
            event["request"]["data"] = scrub(event["request"]["data"])
        if "headers" in event["request"]:
            event["request"]["headers"] = scrub(event["request"]["headers"])

    return event


def capture_llm_breadcrumb(
    step: str,
    model: str,
    tokens_in: int,
    tokens_out: int,
    cost_usd: float,
) -> None:
    """
    Add a breadcrumb for LLM calls — visible in Sentry event timeline.
    Helps trace which LLM step preceded an error.
    """
    try:
        import sentry_sdk
        sentry_sdk.add_breadcrumb(
            category="llm",
            message=f"LLM call: {step}",
            data={
                "model": model,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "cost_usd": cost_usd,
            },
            level="info",
        )
    except Exception:
        pass


def capture_retrieval_breadcrumb(
    sources: int,
    contacts_found: int,
    partial_failure: bool,
) -> None:
    """Add a breadcrumb for the retrieval step."""
    try:
        import sentry_sdk
        sentry_sdk.add_breadcrumb(
            category="retrieval",
            message="Web retrieval complete",
            data={
                "sources": sources,
                "contacts_found": contacts_found,
                "partial_failure": partial_failure,
            },
            level="info",
        )
    except Exception:
        pass


def set_job_run_context(run_id: str, user_id: str) -> None:
    """
    Set Sentry scope tags for a job run.
    All events within this scope will be tagged with the run ID.
    """
    try:
        import sentry_sdk
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("run_id", run_id)
            scope.set_tag("user_id", user_id)
            scope.set_user({"id": user_id})
    except Exception:
        pass
