"""Shared Pydantic request/response models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageMeta(BaseModel):
    total: int
    limit: int
    offset: int
    returned: int


class Paginated(BaseModel, Generic[T]):
    items: list[T]
    meta: PageMeta


class OkResponse(BaseModel):
    ok: bool = True
    detail: str = ""


# --- auth ---
class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_minutes: int
    role: str
    email: str


class UserOut(BaseModel):
    id: str
    email: str
    full_name: str
    role: str
    is_active: bool
    created_at: datetime


class CreateUserRequest(BaseModel):
    email: str
    password: str = Field(min_length=8, max_length=128)
    full_name: str = ""
    role: str = "VIEWER"


# --- producers ---
class CreateProducerRequest(BaseModel):
    name: str = Field(min_length=2, max_length=128, pattern=r"^[a-zA-Z0-9._-]+$")
    description: str = ""
    allowed_event_types: list[str] = Field(default_factory=list)
    rate_limit_per_min: int = Field(6000, ge=1, le=1_000_000)
    default_region: str | None = None


class UpdateProducerRequest(BaseModel):
    description: str | None = None
    allowed_event_types: list[str] | None = None
    rate_limit_per_min: int | None = Field(None, ge=1, le=1_000_000)
    default_region: str | None = None


class ProducerOut(BaseModel):
    id: str
    name: str
    description: str
    status: str
    api_key_prefix: str
    allowed_event_types: list[str]
    rate_limit_per_min: int
    default_region: str | None
    created_at: datetime
    updated_at: datetime


class ProducerCreatedOut(ProducerOut):
    api_key: str = Field(description="Shown once. Store it now; it is not recoverable.")


# --- schemas ---
class CreateSchemaRequest(BaseModel):
    event_type: str = Field(pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")
    description: str = ""
    json_schema: dict[str, Any]
    compatibility: str = "BACKWARD"


class RegisterVersionRequest(BaseModel):
    json_schema: dict[str, Any]
    notes: str = ""
    activate: bool = True
    force: bool = False


class SchemaVersionOut(BaseModel):
    id: str
    version: int
    is_active: bool
    json_schema: dict[str, Any]
    notes: str
    created_by: str | None
    created_at: datetime


class SchemaOut(BaseModel):
    id: str
    event_type: str
    description: str
    compatibility: str
    active_version: int | None
    versions: list[SchemaVersionOut]
    created_at: datetime


# --- events ---
class IngestResponse(BaseModel):
    outcome: str
    event_id: str
    event_type: str
    valid: bool
    quality_flags: list[str]
    schema_version: int | None
    kafka: dict[str, Any] | None
    errors: list[str]


class BatchIngestResponse(BaseModel):
    accepted: int
    rejected: int
    duplicate: int
    results: list[IngestResponse]


class EventOut(BaseModel):
    event_id: str
    event_type: str
    event_version: str
    producer: str
    producer_id: str | None
    event_time: datetime
    ingestion_time: datetime
    partition_key: str
    kafka_partition: int | None
    kafka_offset: int | None
    kafka_topic: str | None
    correlation_id: str
    trace_id: str
    region: str | None
    lateness_at_ingest: str
    is_replay: bool
    payload: dict[str, Any]
    metadata: dict[str, Any]
    amount: float | None


# --- alert rules ---
class CreateAlertRuleRequest(BaseModel):
    name: str
    description: str = ""
    metric: str
    group_key: str = "_all"
    operator: str = Field(pattern="^(gt|gte|lt|lte|drop_pct|rise_pct)$")
    threshold: float
    duration_seconds: int = Field(60, ge=0, le=86400)
    severity: str = Field("MEDIUM", pattern="^(LOW|MEDIUM|HIGH|CRITICAL)$")
    cooldown_seconds: int = Field(300, ge=0, le=86400)
    enabled: bool = True


class UpdateAlertRuleRequest(BaseModel):
    description: str | None = None
    threshold: float | None = None
    duration_seconds: int | None = None
    severity: str | None = None
    cooldown_seconds: int | None = None
    enabled: bool | None = None


# --- replay ---
class CreateReplayRequest(BaseModel):
    time_from: datetime
    time_to: datetime
    filter_event_type: str | None = None
    filter_producer_id: str | None = None
    target_topic: str = "events.replay"


# --- pipelines ---
class PipelineGraph(BaseModel):
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]


class CreatePipelineRequest(BaseModel):
    name: str
    description: str = ""
    graph: PipelineGraph


class UpdatePipelineDraftRequest(BaseModel):
    graph: PipelineGraph


# --- ai ---
class StartInvestigationRequest(BaseModel):
    anomaly_id: str
    provider: str | None = None


class StreamSummaryRequest(BaseModel):
    time_from: datetime
    time_to: datetime
