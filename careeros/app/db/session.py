"""
app/db/session.py

Async SQLAlchemy engine and session factory.
Never import this directly in routes — use dependencies.py instead.
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings

# ── Engine ─────────────────────────────────────────────────────────────────
engine = create_async_engine(
    settings.database_url,
    pool_size=settings.database_pool_size,
    max_overflow=settings.database_max_overflow,
    # Echo SQL only in development — never in production
    echo=settings.is_development,
    # Recycle connections after 30 minutes to avoid stale connections
    pool_recycle=1800,
    pool_pre_ping=True,
)

# ── Session factory ────────────────────────────────────────────────────────
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,  # Prevent lazy-load errors after commit
    autoflush=False,
    autocommit=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency-injectable async DB session.
    Automatically rolls back on exception and always closes.

    Usage in routes:
        async def my_route(db: AsyncSession = Depends(get_db_session)):
            ...
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


from contextlib import asynccontextmanager

@asynccontextmanager
async def get_async_db_for_sync():
    """
    Async DB session for use inside sync Celery tasks via asyncio.run().
    Yields a session that auto-commits and closes.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
