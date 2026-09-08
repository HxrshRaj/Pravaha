"""Event ingestion pipeline.

For each event:

1. envelope parse (permissive -> canonical), with per-event DQ assessment
2. producer authorization for the event type
3. idempotency check (Redis ``SET NX`` on ``event_id`` scoped to the producer)
4. schema validation against the active registered version (if any)
5. enrichment (ids, ingestion_time, region default, replay flags)
6. publish to ``events.raw`` always; publish to ``events.validated`` only if valid
7. invalid events -> ``events.dlq`` + ``dead_letter_events`` row, never the
   validated stream

Rate limiting and body-size limits are enforced in the API layer before this is
called (they need the request object); this module assumes the caller already
did that and passes ``producer``.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.config import settings
from pravaha.dataquality.engine import QualityFlag, assess_envelope
from pravaha.events.envelope import EventEnvelope, EventEnvelopeIn
from pravaha.kafka.producer import EventProducer
from pravaha.kafka.topics import RAW, VALIDATED
from pravaha.logging import get_logger
from pravaha.models import Producer
from pravaha.observability.metrics import (
    EVENTS_INGESTED_TOTAL,
    EVENTS_REJECTED_TOTAL,
)
from pravaha.producers.service import producer_allows_event_type
from pravaha.redis_client import get_redis
from pravaha.registry.service import validate_event_payload

log = get_logger(__name__)


class IngestionOutcome(str, enum.Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


class IngestionError(Exception):
    pass


class IngestionRejected(IngestionError):
    def __init__(self, code: str, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


@dataclass
class IngestionResult:
    outcome: IngestionOutcome
    event_id: str
    event_type: str
    valid: bool
    quality_flags: list[str] = field(default_factory=list)
    schema_version: int | None = None
    kafka: dict | None = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome.value,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "valid": self.valid,
            "quality_flags": self.quality_flags,
            "schema_version": self.schema_version,
            "kafka": self.kafka,
            "errors": self.errors,
        }


async def _known_event_types(session: AsyncSession) -> set[str]:
    from pravaha.models import EventSchema

    rows = await session.scalars(select(EventSchema.event_type))
    return set(rows)


async def _dedup_reserve(producer_id: str, event_id: str) -> bool:
    """Return True if this is the first time we've seen (producer, event_id)."""
    try:
        r = get_redis()
        key = f"pravaha:idem:ingest:{producer_id}:{event_id}"
        ok = await r.set(key, "1", nx=True, ex=settings.ingest_idempotency_ttl_seconds)
        return bool(ok)
    except Exception as exc:  # noqa: BLE001 - Redis down => fail open, dedup best-effort
        log.warning("ingestion.dedup_unavailable", error=str(exc))
        return True


async def ingest_event(
    session: AsyncSession,
    producer: Producer,
    raw_in: dict,
    *,
    producer_obj: EventProducer,
    is_replay: bool = False,
    replay_job_id: str | None = None,
    known_types: set[str] | None = None,
) -> IngestionResult:
    now = datetime.now(UTC)
    known_types = known_types if known_types is not None else await _known_event_types(session)

    finding = assess_envelope(raw_in, known_event_types=known_types, now=now)

    # 1. envelope parse
    try:
        envelope_in = EventEnvelopeIn.model_validate(raw_in)
    except Exception as exc:  # noqa: BLE001
        finding.add(QualityFlag.MALFORMED_PAYLOAD, error=str(exc)[:300])
        EVENTS_REJECTED_TOTAL.labels(reason="malformed_envelope").inc()
        eid = raw_in.get("event_id") or "unknown"
        await _dlq(session, producer_obj, raw_in, eid, "malformed_envelope", str(exc))
        raise IngestionRejected(
            "MALFORMED_ENVELOPE", "event envelope could not be parsed", {"error": str(exc)}
        ) from exc

    event_type = envelope_in.event_type

    # 2. producer authorization
    if not producer_allows_event_type(producer, event_type):
        EVENTS_REJECTED_TOTAL.labels(reason="event_type_not_allowed").inc()
        raise IngestionRejected(
            "EVENT_TYPE_NOT_ALLOWED",
            f"producer '{producer.name}' is not allowed to emit '{event_type}'",
            {"allowed": producer.allowed_event_types},
        )

    canonical = EventEnvelope.from_input(
        envelope_in,
        producer_name=producer.name,
        producer_id=producer.id,
        ingestion_time=now,
        default_region=producer.default_region,
    )
    canonical.is_replay = is_replay
    canonical.replay_job_id = replay_job_id

    # 3. idempotency
    first_seen = await _dedup_reserve(producer.id, canonical.event_id)
    if not first_seen:
        finding.add(QualityFlag.DUPLICATE, event_id=canonical.event_id)
        EVENTS_REJECTED_TOTAL.labels(reason="duplicate").inc()
        return IngestionResult(
            outcome=IngestionOutcome.DUPLICATE,
            event_id=canonical.event_id,
            event_type=event_type,
            valid=False,
            quality_flags=[f.value for f in finding.flags],
        )

    # 4. schema validation
    schema_errors: list[str] = []
    schema_version_no: int | None = None
    version, schema_errors = await validate_event_payload(session, event_type, canonical.payload)
    if version is not None:
        schema_version_no = version.version
        canonical.schema_id = version.schema_id
        canonical.schema_version_id = version.id
        if schema_errors:
            finding.add(QualityFlag.INVALID_SCHEMA, errors=schema_errors[:10])

    valid = finding.is_valid and not schema_errors

    # 5/6. publish. Quality findings ride inside metadata (envelope forbids
    # unknown top-level keys, and consumers rebuild the envelope from the wire).
    canonical.metadata = {
        **canonical.metadata,
        "_quality": {"flags": [f.value for f in finding.flags], "detail": finding.detail},
    }
    wire = canonical.to_wire()
    raw_meta = await producer_obj.publish(RAW.name, wire, key=canonical.partition_key)

    result = IngestionResult(
        outcome=IngestionOutcome.ACCEPTED if valid else IngestionOutcome.REJECTED,
        event_id=canonical.event_id,
        event_type=event_type,
        valid=valid,
        quality_flags=[f.value for f in finding.flags],
        schema_version=schema_version_no,
        errors=schema_errors,
    )

    if valid:
        validated_meta = await producer_obj.publish(
            VALIDATED.name, wire, key=canonical.partition_key
        )
        result.kafka = validated_meta
        EVENTS_INGESTED_TOTAL.labels(producer=producer.name, event_type=event_type).inc()
        log.info(
            "ingestion.accepted",
            event_id=canonical.event_id,
            event_type=event_type,
            partition=validated_meta["partition"],
            offset=validated_meta["offset"],
        )
    else:
        result.kafka = raw_meta
        reason = "invalid_schema" if schema_errors else "data_quality"
        EVENTS_REJECTED_TOTAL.labels(reason=reason).inc()
        await _dlq(
            session,
            producer_obj,
            wire,
            canonical.event_id,
            reason,
            "; ".join(schema_errors) or f"quality flags: {result.quality_flags}",
            envelope=wire,
        )
        log.info(
            "ingestion.rejected",
            event_id=canonical.event_id,
            event_type=event_type,
            reason=reason,
            errors=schema_errors[:5],
        )

    return result


async def _dlq(
    session: AsyncSession,
    producer_obj: EventProducer,
    raw_payload: dict,
    event_id: str,
    reason: str,
    detail: str,
    envelope: dict | None = None,
) -> None:
    from sqlalchemy.dialects.postgresql import insert

    from pravaha.kafka.topics import DLQ
    from pravaha.models import DeadLetterEvent

    record = {
        "event_id": event_id,
        "processor": "ingestion",
        "source_topic": settings.topic(RAW.name),
        "failure_reason": reason,
        "error_detail": detail[:8000],
        "error_class": "permanent",
        "attempt_count": 1,
        "raw_payload": raw_payload,
        "envelope": envelope,
        "status": "PENDING",
    }
    try:
        await producer_obj.publish(
            DLQ.name, {**record, "dlq_recorded_at": datetime.now(UTC).isoformat()}, key=event_id
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("ingestion.dlq_publish_failed", error=str(exc))
    try:
        async with session.begin_nested():
            await session.execute(insert(DeadLetterEvent).values(**record))
    except Exception as exc:  # noqa: BLE001
        log.error("ingestion.dlq_persist_failed", error=str(exc))
