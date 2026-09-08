"""Event replay engine.

Reads historical events from the ``events`` table for a time range + optional
filters and republishes them to a target topic (default ``events.replay``).

Safety / correctness:
* Replayed events keep their original ``event_id`` but are re-wrapped with
  ``is_replay=true`` and the ``replay_job_id``. Business processors decide
  whether to apply side effects for replayed events; the analytics processor,
  for example, still aggregates them (so metrics recover) but the persistence
  processor's ``ON CONFLICT DO NOTHING`` means no duplicate rows.
* Idempotency records are per (event_id, processor); a replayed event that was
  already processed is skipped by every consumer -- replay is safe to run twice.
* Progress + counts are written back to the ``replay_jobs`` row so the UI can
  show a progress bar, and the job is fully auditable.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.events.envelope import EventEnvelope
from pravaha.kafka.producer import EventProducer
from pravaha.kafka.topics import REPLAY
from pravaha.logging import get_logger
from pravaha.models import Event, ReplayJob

log = get_logger(__name__)


def _base_query(job: ReplayJob):
    q = select(Event).where(
        Event.event_time >= job.time_from, Event.event_time <= job.time_to
    )
    if job.filter_event_type:
        q = q.where(Event.event_type == job.filter_event_type)
    if job.filter_producer_id:
        q = q.where(Event.producer_id == job.filter_producer_id)
    return q


async def estimate_replay(session: AsyncSession, job: ReplayJob) -> int:
    q = _base_query(job).with_only_columns(func.count(Event.id)).order_by(None)
    return int(await session.scalar(q) or 0)


async def run_replay_job(
    session_factory,  # async_sessionmaker
    producer: EventProducer,
    job_id: str,
    *,
    batch_size: int = 500,
) -> None:
    from pravaha.db import session_scope

    async with session_scope() as s:
        job = await s.get(ReplayJob, job_id)
        if job is None:
            raise ValueError(f"replay job {job_id} not found")
        job.status = "RUNNING"
        job.started_at = datetime.now(UTC)
        total = await estimate_replay(s, job)
        job.estimated_count = total
        time_from, time_to = job.time_from, job.time_to
        ftype, fpid = job.filter_event_type, job.filter_producer_id
        target = job.target_topic or REPLAY.name

    await producer.start()
    replayed = 0
    failed = 0
    last_id: str | None = None

    try:
        while True:
            async with session_scope() as s:
                q = select(Event).where(
                    Event.event_time >= time_from, Event.event_time <= time_to
                )
                if ftype:
                    q = q.where(Event.event_type == ftype)
                if fpid:
                    q = q.where(Event.producer_id == fpid)
                if last_id is not None:
                    q = q.where(Event.id > last_id)
                q = q.order_by(Event.id.asc()).limit(batch_size)
                rows = list(await s.scalars(q))

            if not rows:
                break

            for ev in rows:
                last_id = ev.id
                try:
                    envelope = EventEnvelope(
                        event_id=ev.event_id,
                        event_type=ev.event_type,
                        event_version=ev.event_version,
                        producer=ev.producer,
                        producer_id=ev.producer_id,
                        schema_version_id=ev.schema_version_id,
                        event_time=ev.event_time,
                        ingestion_time=datetime.now(UTC),
                        partition_key=ev.partition_key,
                        correlation_id=ev.correlation_id,
                        trace_id=ev.trace_id,
                        region=ev.region,
                        payload=ev.payload,
                        metadata={**(ev.event_metadata or {}), "_replayed_from": ev.ingestion_time.isoformat()},
                        is_replay=True,
                        replay_job_id=job_id,
                    )
                    await producer.publish(target, envelope.to_wire(), key=envelope.partition_key)
                    replayed += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    log.warning("replay.event_failed", event_id=ev.event_id, error=str(exc))

            async with session_scope() as s:
                job = await s.get(ReplayJob, job_id)
                if job is None or job.status == "CANCELLED":
                    log.info("replay.cancelled", job_id=job_id)
                    return
                job.replayed_count = replayed
                job.failed_count = failed

        async with session_scope() as s:
            job = await s.get(ReplayJob, job_id)
            job.status = "COMPLETED"
            job.finished_at = datetime.now(UTC)
            job.replayed_count = replayed
            job.failed_count = failed
            job.stats = {
                "target_topic": target,
                "duration_seconds": (job.finished_at - job.started_at).total_seconds()
                if job.started_at
                else None,
            }
        log.info("replay.completed", job_id=job_id, replayed=replayed, failed=failed)

    except Exception as exc:  # noqa: BLE001
        log.error("replay.failed", job_id=job_id, error=str(exc))
        async with session_scope() as s:
            job = await s.get(ReplayJob, job_id)
            if job is not None:
                job.status = "FAILED"
                job.error = str(exc)[:4000]
                job.finished_at = datetime.now(UTC)
                job.replayed_count = replayed
                job.failed_count = failed
        raise
