"""
app/main.py

FastAPI application factory and entry point.
All app-level setup lives here: routers, middleware, lifespan events.
Business logic NEVER goes here.
"""

import structlog
from fastapi import FastAPI
from app.monitoring.sentry import init_sentry
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.api.middleware.error_handler import register_error_handlers
from app.api.routes import health
from app.config import settings

logger = structlog.get_logger(__name__)


def create_app() -> FastAPI:
    """
    Application factory.
    Returns a fully configured FastAPI instance.
    """
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description=(
            "CareerOS API — generates Application Intelligence Packs "
            "by parsing CVs, analysing job descriptions, and calling Claude."
        ),
        docs_url="/docs" if not settings.is_production else None,
        redoc_url="/redoc" if not settings.is_production else None,
        openapi_url="/openapi.json" if not settings.is_production else None,
    )

    # ── Middleware ─────────────────────────────────────────────────────────
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    # CORS — in dev allow all, in prod use ALLOWED_ORIGINS env var
    _origins = (
        ["*"] if settings.is_development
        else [o.strip() for o in settings.allowed_origins.split(",") if o.strip()]
             or [settings.frontend_url]
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Error handlers ─────────────────────────────────────────────────────
    register_error_handlers(app)

    # ── Routers ────────────────────────────────────────────────────────────
    # All routes are versioned under /api/v1
    app.include_router(health.router)               # /health, /health/ready

    # Auth routes — /api/v1/auth
    from app.api.routes import auth
    app.include_router(auth.router)

    # CV upload routes — /api/v1/cvs
    from app.api.routes import uploads
    app.include_router(uploads.router)

    # Job run routes — /api/v1/jobs
    from app.api.routes import jobs
    app.include_router(jobs.router)

    # CV Builder routes — /api/v1/cv-builder
    from app.api.routes import cv_builder
    app.include_router(cv_builder.router)

    # Billing routes — /api/v1/billing
    from app.api.routes import billing
    app.include_router(billing.router)

    # Candidate profile routes — /api/v1/profiles
    from app.api.routes import profiles
    from app.api.routes import profile_import
    app.include_router(profiles.router)
    app.include_router(profile_import.router)

    from app.api.routes import scout
    app.include_router(scout.router)

    from app.api.routes import email as email_routes
    app.include_router(
        email_routes.router,
        prefix="/api/v1",
    )

    # ── Startup / shutdown events ──────────────────────────────────────────
    @app.on_event("startup")
    async def on_startup() -> None:
        init_sentry()
        logger.info(
            "cvlab_starting",
            env=settings.app_env,
            version=settings.app_version,
        )

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        logger.info("careeros_shutdown")

    return app


# Uvicorn entry point
app = create_app()
