"""Centralised configuration.

All runtime configuration is read from environment variables (12-factor).
Defaults are safe for local development only. See ``.env.example``.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", case_sensitive=False
    )

    # --- Core ---
    env: str = Field(default="local", alias="PRAVAHA_ENV")
    log_level: str = Field(default="INFO", alias="PRAVAHA_LOG_LEVEL")
    log_json: bool = Field(default=True, alias="PRAVAHA_LOG_JSON")

    # --- API ---
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")
    api_cors_origins: str = Field(default="http://localhost:3000", alias="API_CORS_ORIGINS")
    jwt_secret: str = Field(default="dev-only-insecure-secret-change-me", alias="API_JWT_SECRET")
    jwt_algorithm: str = Field(default="HS256", alias="API_JWT_ALGORITHM")
    access_token_ttl_minutes: int = Field(default=720, alias="API_ACCESS_TOKEN_TTL_MINUTES")
    bootstrap_admin_email: str = Field(default="admin@pravaha.local", alias="BOOTSTRAP_ADMIN_EMAIL")
    bootstrap_admin_password: str = Field(default="admin12345", alias="BOOTSTRAP_ADMIN_PASSWORD")

    # --- PostgreSQL ---
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="pravaha", alias="POSTGRES_DB")
    postgres_user: str = Field(default="pravaha", alias="POSTGRES_USER")
    postgres_password: str = Field(default="pravaha", alias="POSTGRES_PASSWORD")
    database_url_override: str | None = Field(default=None, alias="DATABASE_URL")

    # --- Redis ---
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_db: int = Field(default=0, alias="REDIS_DB")
    redis_url_override: str | None = Field(default=None, alias="REDIS_URL")

    # --- Kafka ---
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_client_id: str = Field(default="pravaha", alias="KAFKA_CLIENT_ID")
    kafka_acks: str = Field(default="all", alias="KAFKA_ACKS")
    kafka_producer_linger_ms: int = Field(default=5, alias="KAFKA_PRODUCER_LINGER_MS")
    kafka_producer_max_retries: int = Field(default=5, alias="KAFKA_PRODUCER_MAX_RETRIES")
    kafka_topic_prefix: str = Field(default="", alias="KAFKA_TOPIC_PREFIX")
    kafka_default_partitions: int = Field(default=6, alias="KAFKA_DEFAULT_PARTITIONS")
    kafka_default_replication: int = Field(default=1, alias="KAFKA_DEFAULT_REPLICATION")

    # --- Stream processing ---
    watermark_allowed_lateness_seconds: int = Field(
        default=30, alias="WATERMARK_ALLOWED_LATENESS_SECONDS"
    )
    watermark_idle_advance_seconds: int = Field(default=10, alias="WATERMARK_IDLE_ADVANCE_SECONDS")
    consumer_max_poll_records: int = Field(default=200, alias="CONSUMER_MAX_POLL_RECORDS")
    consumer_max_concurrency: int = Field(default=8, alias="CONSUMER_MAX_CONCURRENCY")
    consumer_retry_max_attempts: int = Field(default=5, alias="CONSUMER_RETRY_MAX_ATTEMPTS")
    consumer_retry_base_ms: int = Field(default=200, alias="CONSUMER_RETRY_BASE_MS")
    consumer_retry_max_ms: int = Field(default=15000, alias="CONSUMER_RETRY_MAX_MS")

    # --- Ingestion ---
    ingest_max_body_bytes: int = Field(default=1_048_576, alias="INGEST_MAX_BODY_BYTES")
    ingest_max_batch_size: int = Field(default=500, alias="INGEST_MAX_BATCH_SIZE")
    ingest_rate_limit_default_per_min: int = Field(
        default=6000, alias="INGEST_RATE_LIMIT_DEFAULT_PER_MIN"
    )
    ingest_idempotency_ttl_seconds: int = Field(default=86_400, alias="INGEST_IDEMPOTENCY_TTL_SECONDS")

    # --- Retention ---
    event_retention_days: int = Field(default=7, alias="EVENT_RETENTION_DAYS")
    event_cleanup_interval_seconds: int = Field(default=3600, alias="EVENT_CLEANUP_INTERVAL_SECONDS")

    # --- AI ---
    ai_provider_order: str = Field(default="mock", alias="AI_PROVIDER_ORDER")
    ai_openai_api_key: str = Field(default="", alias="AI_OPENAI_API_KEY")
    ai_openai_base_url: str = Field(default="https://api.openai.com/v1", alias="AI_OPENAI_BASE_URL")
    ai_openai_model: str = Field(default="gpt-4o-mini", alias="AI_OPENAI_MODEL")
    ai_groq_api_key: str = Field(default="", alias="AI_GROQ_API_KEY")
    ai_groq_base_url: str = Field(
        default="https://api.groq.com/openai/v1", alias="AI_GROQ_BASE_URL"
    )
    ai_groq_model: str = Field(default="llama-3.3-70b-versatile", alias="AI_GROQ_MODEL")
    ai_request_timeout_seconds: int = Field(default=45, alias="AI_REQUEST_TIMEOUT_SECONDS")
    ai_max_tool_calls: int = Field(default=12, alias="AI_MAX_TOOL_CALLS")
    ai_investigation_rate_limit_per_hour: int = Field(
        default=60, alias="AI_INVESTIGATION_RATE_LIMIT_PER_HOUR"
    )

    @field_validator("log_level")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()

    @property
    def database_url(self) -> str:
        if self.database_url_override:
            return self.database_url_override
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def sync_database_url(self) -> str:
        """Sync URL used by Alembic migrations."""
        return self.database_url.replace("+asyncpg", "+psycopg")

    @property
    def redis_url(self) -> str:
        if self.redis_url_override:
            return self.redis_url_override
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.api_cors_origins.split(",") if o.strip()]

    @property
    def ai_providers(self) -> list[str]:
        return [p.strip().lower() for p in self.ai_provider_order.split(",") if p.strip()]

    def topic(self, name: str) -> str:
        """Apply the configured topic prefix (used to isolate test runs)."""
        return f"{self.kafka_topic_prefix}{name}" if self.kafka_topic_prefix else name


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
