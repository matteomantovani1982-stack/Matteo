"""
app/models/__init__.py

Re-exports all ORM models.
Alembic's env.py imports Base from here to detect all tables.
Add new models to this file as they are created.
"""

from app.models.base import Base, TimestampMixin, UUIDMixin
from app.models.cv import CV, CVStatus
from app.models.job_run import JobRun, JobRunStatus
from app.models.job_step import JobStep, StepName, StepStatus

__all__ = [
    "Base",
    "TimestampMixin",
    "UUIDMixin",
    "CV",
    "CVStatus",
    "JobRun",
    "JobRunStatus",
    "JobStep",
    "StepName",
    "StepStatus",
]

from app.models.user import User
