"""
app/api/routes/health.py

Health check endpoints for liveness, readiness, and deep system status.
"""

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.config import settings
from app.db.session import AsyncSessionLocal

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/health", tags=["Health"])


@router.get("", summary="Liveness probe")
async def health_check() -> dict:
    return {
        "status": "ok",
        "app": settings.app_name,
        "version": settings.app_version,
        "env": settings.app_env,
        "timestamp": datetime.now(UTC).isoformat(),
    }


@router.get("/ready", summary="Readiness probe (DB + Redis)")
async def readiness_check():
    checks: dict = {}

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = {"status": "ok"}
    except Exception as e:
        checks["database"] = {"status": "error", "error": str(e)}
        logger.error("health_db_failed", error=str(e))

    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.redis_url, socket_connect_timeout=2)
        await r.ping()
        await r.aclose()
        checks["redis"] = {"status": "ok"}
    except Exception as e:
        checks["redis"] = {"status": "error", "error": str(e)}

    overall = "ok" if all(c["status"] == "ok" for c in checks.values()) else "degraded"
    return JSONResponse(
        status_code=200 if overall == "ok" else 503,
        content={"status": overall, "checks": checks, "timestamp": datetime.now(UTC).isoformat()},
    )


@router.get("/deep", summary="Deep system check")
async def deep_check() -> dict:
    checks: dict = {}

    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'")
            )
            checks["database"] = {"status": "ok", "tables": result.scalar()}
    except Exception as e:
        checks["database"] = {"status": "error", "error": str(e)}

    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.redis_url, socket_connect_timeout=2)
        info = await r.info("server")
        await r.aclose()
        checks["redis"] = {"status": "ok", "version": info.get("redis_version")}
    except Exception as e:
        checks["redis"] = {"status": "error", "error": str(e)}

    try:
        import redis as sync_redis
        r = sync_redis.from_url(settings.redis_url, socket_connect_timeout=2)
        checks["celery_queues"] = {
            "status": "ok",
            "lengths": {q: r.llen(q) for q in ["default", "llm", "rendering"]},
        }
        r.close()
    except Exception as e:
        checks["celery_queues"] = {"status": "error", "error": str(e)}

    checks["sentry"] = {
        "status": "configured" if settings.sentry_dsn else "not_configured",
        "environment": settings.sentry_environment or settings.app_env,
    }

    critical_ok = all(
        checks.get(k, {}).get("status") == "ok" for k in ("database", "redis")
    )
    return {
        "status": "ok" if critical_ok else "degraded",
        "version": settings.app_version,
        "env": settings.app_env,
        "checks": checks,
        "timestamp": datetime.now(UTC).isoformat(),
    }
