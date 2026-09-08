"""Producer management + API-key authentication."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.auth.security import hash_api_key, new_api_key
from pravaha.logging import get_logger
from pravaha.models import Producer
from pravaha.models.producers import ProducerStatus

log = get_logger(__name__)


class ProducerError(Exception):
    pass


@dataclass
class CreatedProducer:
    producer: Producer
    api_key: str  # plaintext, returned exactly once


async def create_producer(
    session: AsyncSession,
    *,
    name: str,
    description: str = "",
    allowed_event_types: list[str] | None = None,
    rate_limit_per_min: int = 6000,
    default_region: str | None = None,
) -> CreatedProducer:
    exists = await session.scalar(select(Producer.id).where(Producer.name == name))
    if exists:
        raise ProducerError(f"producer '{name}' already exists")
    key = new_api_key()
    producer = Producer(
        name=name,
        description=description,
        status=ProducerStatus.ACTIVE,
        api_key_hash=hash_api_key(key),
        api_key_prefix=key[:12],
        allowed_event_types=allowed_event_types or [],
        rate_limit_per_min=rate_limit_per_min,
        default_region=default_region,
    )
    session.add(producer)
    await session.flush()
    log.info("producer.created", producer=name, id=producer.id)
    return CreatedProducer(producer=producer, api_key=key)


async def rotate_api_key(session: AsyncSession, producer_id: str) -> str:
    producer = await session.get(Producer, producer_id)
    if producer is None:
        raise ProducerError("producer not found")
    key = new_api_key()
    producer.api_key_hash = hash_api_key(key)
    producer.api_key_prefix = key[:12]
    await session.flush()
    log.info("producer.key_rotated", producer=producer.name)
    return key


async def set_producer_status(
    session: AsyncSession, producer_id: str, status: ProducerStatus
) -> Producer:
    producer = await session.get(Producer, producer_id)
    if producer is None:
        raise ProducerError("producer not found")
    producer.status = status
    await session.flush()
    log.info("producer.status_changed", producer=producer.name, status=status.value)
    return producer


async def update_producer(
    session: AsyncSession,
    producer_id: str,
    *,
    description: str | None = None,
    allowed_event_types: list[str] | None = None,
    rate_limit_per_min: int | None = None,
    default_region: str | None = None,
) -> Producer:
    producer = await session.get(Producer, producer_id)
    if producer is None:
        raise ProducerError("producer not found")
    if description is not None:
        producer.description = description
    if allowed_event_types is not None:
        producer.allowed_event_types = allowed_event_types
    if rate_limit_per_min is not None:
        producer.rate_limit_per_min = rate_limit_per_min
    if default_region is not None:
        producer.default_region = default_region
    await session.flush()
    return producer


async def authenticate_producer(session: AsyncSession, api_key: str) -> Producer:
    """Look a producer up by API key. Raises :class:`ProducerError` on failure."""
    if not api_key:
        raise ProducerError("missing API key")
    key_hash = hash_api_key(api_key)
    producer = await session.scalar(
        select(Producer).where(Producer.api_key_hash == key_hash)
    )
    if producer is None:
        raise ProducerError("invalid API key")
    if producer.status != ProducerStatus.ACTIVE:
        raise ProducerError("producer is disabled")
    return producer


def producer_allows_event_type(producer: Producer, event_type: str) -> bool:
    return not producer.allowed_event_types or event_type in producer.allowed_event_types


async def producer_health(session: AsyncSession, producer_id: str) -> dict:
    """Best-effort health snapshot from the event store + DQ records."""
    from datetime import UTC, datetime, timedelta

    from pravaha.models import DataQualityRecord, Event

    producer = await session.get(Producer, producer_id)
    if producer is None:
        raise ProducerError("producer not found")
    since = datetime.now(UTC) - timedelta(minutes=15)

    total = await session.scalar(
        select(func.count(Event.id)).where(
            Event.producer_id == producer_id, Event.ingestion_time >= since
        )
    )
    last_seen = await session.scalar(
        select(func.max(Event.ingestion_time)).where(Event.producer_id == producer_id)
    )
    dq = await session.scalar(
        select(func.avg(DataQualityRecord.overall_score)).where(
            DataQualityRecord.producer_id == producer_id,
            DataQualityRecord.bucket_start >= since,
        )
    )
    return {
        "producer_id": producer_id,
        "name": producer.name,
        "status": producer.status.value,
        "events_last_15m": int(total or 0),
        "events_per_min_last_15m": round((total or 0) / 15.0, 2),
        "last_seen": last_seen.isoformat() if last_seen else None,
        "data_quality_score_15m": round(float(dq), 4) if dq is not None else None,
        "rate_limit_per_min": producer.rate_limit_per_min,
    }
