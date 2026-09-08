"""Persistence processor.

Consumer group ``pravaha.persistence``. Consumes ``events.validated`` and writes
each event into the ``events`` table (the searchable event store). Idempotent on
``events.event_id`` (unique). This is deliberately a *separate* consumer group
from analytics so a slow DB write cannot back-pressure metric computation and
vice-versa.
"""

from __future__ import annotations

from pravaha.events.envelope import EventEnvelope
from pravaha.kafka.consumer import ProcessingContext, StreamConsumer
from pravaha.logging import get_logger

log = get_logger(__name__)


def _extract_amount(event: EventEnvelope) -> float | None:
    for path in ("amount", "total", "value", "price"):
        v = event.payload.get(path)
        if isinstance(v, (int, float)):
            return float(v)
    return None


class PersistenceProcessor(StreamConsumer):
    group_id = "pravaha.persistence"
    processor = "persistence"
    topics = ["events.validated"]

    async def handle(self, event: EventEnvelope, ctx: ProcessingContext) -> None:
        from sqlalchemy.dialects.postgresql import insert

        from pravaha.db import session_scope
        from pravaha.models import Event

        row = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "event_version": event.event_version,
            "producer": event.producer,
            "producer_id": event.producer_id,
            "schema_version_id": event.schema_version_id,
            "event_time": event.event_time,
            "ingestion_time": event.ingestion_time,
            "partition_key": event.partition_key,
            "kafka_partition": ctx.partition,
            "kafka_offset": ctx.offset,
            "kafka_topic": ctx.topic,
            "correlation_id": event.correlation_id,
            "trace_id": event.trace_id,
            "region": event.region,
            "lateness_at_ingest": event.lateness_at_ingest,
            "is_replay": event.is_replay,
            "replay_job_id": event.replay_job_id,
            "payload": event.payload,
            "event_metadata": event.metadata,  # ORM attr key; DB column is "metadata"
            "amount": _extract_amount(event),
        }
        stmt = insert(Event).values(**row).on_conflict_do_nothing(index_elements=["event_id"])
        async with session_scope() as s:
            await s.execute(stmt)
