"""
app/workers/db_utils.py

Synchronous database session helper for Celery workers.

Celery tasks run in a synchronous context (no event loop).
We use SQLAlchemy's sync engine here — separate from the async engine
used by FastAPI routes.

Pattern for every Celery task:
    with get_sync_db() as db:
        cv = db.get(CV, cv_id)
        cv.status = CVStatus.PARSING
        db.commit()

Never use the async session (AsyncSession) inside Celery tasks.
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings


def _build_sync_url(async_url: str) -> str:
    """
    Convert an asyncpg DATABASE_URL to a psycopg2-compatible sync URL.

    asyncpg:  postgresql+asyncpg://user:pass@host/db
    psycopg2: postgresql+psycopg2://user:pass@host/db
    """
    return async_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")


# Sync engine — created once at module load, reused by all workers
_sync_engine = create_engine(
    _build_sync_url(settings.database_url),
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
    pool_pre_ping=True,
)

_SyncSessionLocal = sessionmaker(
    bind=_sync_engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)


@contextmanager
def get_sync_db() -> Generator[Session, None, None]:
    """
    Context manager providing a synchronous DB session for Celery tasks.

    Usage:
        with get_sync_db() as db:
            cv = db.get(CV, cv_id)
            ...
            db.commit()

    Automatically rolls back on exception and always closes.
    """
    session = _SyncSessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
