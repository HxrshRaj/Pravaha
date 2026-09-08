"""The standardized Pravaha event envelope.

Every event that enters the platform is wrapped in this envelope. The envelope
separates three distinct notions of time (see docs/decisions/0005-event-time.md):

* ``event_time``      - when the thing actually happened, per the producer.
* ``ingestion_time``  - when Pravaha's ingestion API accepted the event.
* ``processing_time`` - when a stream processor handled it (assigned late, not
  part of the wire envelope).

Producers submit :class:`EventEnvelopeIn` (a permissive input shape). Ingestion
normalises and enriches it into :class:`EventEnvelope`, which is what gets
published to Kafka and persisted.
"""

from __future__ import annotations

import enum
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

EVENT_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")
SCHEMA_VERSION_RE = re.compile(r"^\d+\.\d+$")
MAX_FUTURE_SKEW = timedelta(minutes=5)


def new_event_id() -> str:
    return str(uuid.uuid4())


def utcnow() -> datetime:
    return datetime.now(UTC)


class TimeSemantics(str, enum.Enum):
    EVENT_TIME = "event_time"
    INGESTION_TIME = "ingestion_time"
    PROCESSING_TIME = "processing_time"


class LatenessClass(str, enum.Enum):
    ON_TIME = "on_time"
    ACCEPTED_LATE = "accepted_late"
    TOO_LATE = "too_late"


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        # epoch seconds or millis
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        dt = datetime.fromtimestamp(ts, tz=UTC)
    elif isinstance(value, str):
        s = value.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
    else:
        raise ValueError(f"unparseable timestamp: {value!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class EventEnvelopeIn(BaseModel):
    """Permissive shape accepted from producers on the ingestion API."""

    model_config = {"extra": "forbid"}

    event_id: str | None = None
    event_type: str
    event_version: str = "1.0"
    producer: str | None = None
    event_time: Any | None = None
    timestamp: Any | None = None  # producer alias for event_time
    partition_key: str | None = None
    correlation_id: str | None = None
    trace_id: str | None = None
    region: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_type")
    @classmethod
    def _valid_type(cls, v: str) -> str:
        v = v.strip().lower()
        if not EVENT_TYPE_RE.match(v):
            raise ValueError(
                "event_type must be dot-namespaced lower_snake, e.g. 'payment.failed'"
            )
        return v

    @field_validator("event_version")
    @classmethod
    def _valid_version(cls, v: str) -> str:
        if not SCHEMA_VERSION_RE.match(v):
            raise ValueError("event_version must look like '<major>.<minor>', e.g. '1.0'")
        return v


class EventEnvelope(BaseModel):
    """Canonical, fully-enriched envelope. This is the Kafka wire format."""

    model_config = {"extra": "forbid"}

    event_id: str
    event_type: str
    event_version: str
    producer: str
    producer_id: str | None = None
    schema_id: str | None = None
    schema_version_id: str | None = None

    event_time: datetime
    ingestion_time: datetime
    partition_key: str
    correlation_id: str
    trace_id: str
    region: str | None = None

    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    # populated by ingestion after watermark-independent checks
    lateness_at_ingest: LatenessClass = LatenessClass.ON_TIME
    is_replay: bool = False
    replay_job_id: str | None = None

    @field_validator("event_time", "ingestion_time")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v.astimezone(UTC)

    @model_validator(mode="after")
    def _sanity(self) -> EventEnvelope:
        if self.event_time - self.ingestion_time > MAX_FUTURE_SKEW:
            raise ValueError("event_time is too far in the future relative to ingestion_time")
        return self

    @classmethod
    def from_input(
        cls,
        raw: EventEnvelopeIn,
        *,
        producer_name: str,
        producer_id: str | None,
        ingestion_time: datetime | None = None,
        default_region: str | None = None,
    ) -> EventEnvelope:
        now = ingestion_time or utcnow()
        raw_time = raw.event_time if raw.event_time is not None else raw.timestamp
        event_time = _parse_dt(raw_time) if raw_time is not None else now
        eid = raw.event_id or new_event_id()
        corr = raw.correlation_id or eid
        trace = raw.trace_id or corr
        pkey = raw.partition_key or corr
        return cls(
            event_id=eid,
            event_type=raw.event_type,
            event_version=raw.event_version,
            producer=raw.producer or producer_name,
            producer_id=producer_id,
            event_time=event_time,
            ingestion_time=now,
            partition_key=str(pkey),
            correlation_id=str(corr),
            trace_id=str(trace),
            region=raw.region or default_region,
            payload=raw.payload,
            metadata=raw.metadata,
        )

    def to_wire(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> EventEnvelope:
        return cls.model_validate(data)
