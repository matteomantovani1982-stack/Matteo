"""
app/config.py

Central configuration loaded from environment variables.
All settings are validated at startup via pydantic-settings.
Import `settings` anywhere in the app — never read os.environ directly.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn, RedisDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Demo mode — returns fake LLM responses without calling Claude API
    demo_mode: bool = Field(default=False)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Application ────────────────────────────────────────
    app_env: Literal["development", "staging", "production"] = "development"
    app_name: str = "CareerOS"
    app_version: str = "0.1.0"
    debug: bool = False

    # ── Security ───────────────────────────────────────────
    secret_key: str = Field(default="migration-only-secret-key-not-used-xx")
    api_key_header: str = "X-API-Key"

    # ── Database ───────────────────────────────────────────
    database_url: str = Field(
        default="postgresql+asyncpg://careeros:careeros@localhost:5432/careeros"
    )
    database_pool_size: int = 10
    database_max_overflow: int = 20

    # ── Redis ──────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── Celery ─────────────────────────────────────────────
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"

    # ── S3 Storage ─────────────────────────────────────────
    s3_endpoint_url: str | None = None   # None = use AWS default endpoint
    s3_access_key_id: str = Field(default="")
    s3_secret_access_key: str = Field(default="")
    s3_bucket_name: str = "careeros"
    s3_region: str = "us-east-1"

    # ── Claude API ─────────────────────────────────────────
    anthropic_api_key: str = Field(default="")
    claude_model: str = "claude-sonnet-4-6"
    claude_max_tokens: int = 16000

    # ── Retrieval ──────────────────────────────────────────
    serper_api_key: str | None = None
    adzuna_app_id: str | None = None
    adzuna_app_key: str | None = None
    jsearch_api_key: str | None = None   # RapidAPI key for JSearch

    # ── Email / SMTP ───────────────────────────────────────
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None
    smtp_from: str = "noreply@cvlab.co"
    smtp_from_name: str = "CVLab"
    smtp_use_tls: bool = True
    smtp_use_ssl: bool = False
    app_base_url: str = "http://localhost:3000"

    # ── Stripe ────────────────────────────────────────────
    stripe_secret_key: str = "sk_test_placeholder"
    stripe_publishable_key: str = "pk_test_placeholder"
    stripe_webhook_secret: str = "whsec_placeholder"

    # ── Frontend / CORS ────────────────────────────────────
    frontend_url: str = "http://localhost:3000"
    allowed_origins: str = ""  # comma-separated list, overrides frontend_url in prod

    # ── Sentry ────────────────────────────────────────────
    sentry_dsn: str | None = None
    sentry_traces_sample_rate: float = 0.1   # 10% of transactions
    sentry_profiles_sample_rate: float = 0.1
    sentry_environment: str | None = None    # defaults to app_env

    # ── Computed helpers ───────────────────────────────────
    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def is_development(self) -> bool:
        return self.app_env == "development"

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        if not v.startswith("postgresql+asyncpg://"):
            raise ValueError(
                "DATABASE_URL must use asyncpg driver: "
                "postgresql+asyncpg://..."
            )
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns a cached singleton Settings instance.
    Use this everywhere — do NOT instantiate Settings() directly.
    """
    return Settings()


# Module-level singleton for convenience imports
settings = get_settings()
