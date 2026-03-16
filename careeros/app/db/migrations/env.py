"""
app/db/migrations/env.py

Alembic migration environment — configured for async SQLAlchemy.
Reads the DATABASE_URL from app settings so migrations always
target the same database as the application.
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.config import settings

# ── Alembic config object ──────────────────────────────────────────────────
config = context.config

# Override the sqlalchemy.url with the value from our settings
# This ensures migrations always use the same DB as the app
config.set_main_option("sqlalchemy.url", settings.database_url)

# Set up logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ── Import all models so Alembic can detect them ──────────────────────────
# Add new model imports here as they are created
from app.models.base import Base  # noqa: E402
from app.models.cv import CV  # noqa: E402, F401
from app.models.job_run import JobRun  # noqa: E402, F401
from app.models.job_step import JobStep  # noqa: E402, F401
from app.models.user import User  # noqa: E402, F401
from app.models.candidate_profile import CandidateProfile, ExperienceEntry  # noqa: E402, F401
from app.models.subscription import Subscription, UsageRecord  # noqa: E402, F401
from app.models.cv_template import CVTemplate  # noqa: E402, F401

target_metadata = Base.metadata


# ── Offline mode (generate SQL without DB connection) ─────────────────────
def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


# ── Online mode (run against live DB) ─────────────────────────────────────
def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


# ── Entry point ────────────────────────────────────────────────────────────
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
