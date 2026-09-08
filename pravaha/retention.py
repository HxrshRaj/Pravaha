"""Event-store retention / cleanup worker.

For the portfolio implementation PostgreSQL is the searchable event store. This
worker enforces a configurable retention window (``EVENT_RETENTION_DAYS``) by
deleting expired rows from ``events`` and old rows from the high-churn
observability tables (``consumer_lag``, ``aggregations`` older than 30d,
``event_processing_records`` older than retention).

The :class:`ArchiveSink` abstraction marks where an object-storage / data-lake
archival step would hook in (write Parquet to S3/GCS before delete). The local
default is :class:`NullArchiveSink` (no archive) and this is called out in
docs/decisions/0009-event-storage.md.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import delete, select

from pravaha.config import settings
from pravaha.db import session_scope
from pravaha.logging import get_logger
from pravaha.models import Aggregation, ConsumerLag, Event, EventProcessingRecord

log = get_logger(__name__)


class ArchiveSink(Protocol):
    async def archive(self, rows: list[dict]) -> None: ...


class NullArchiveSink:
    async def archive(self, rows: list[dict]) -> None:  # noqa: D401
        return None


class RetentionWorker:
    def __init__(self, archive: ArchiveSink | None = None) -> None:
        self._archive = archive or NullArchiveSink()
        self._stop = asyncio.Event()

    async def run(self) -> None:
        log.info(
            "retention.started",
            retention_days=settings.event_retention_days,
            interval_seconds=settings.event_cleanup_interval_seconds,
        )
        while not self._stop.is_set():
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001
                log.error("retention.cycle_failed", error=str(exc))
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(), timeout=settings.event_cleanup_interval_seconds
                )

    async def run_once(self) -> dict[str, int]:
        now = datetime.now(UTC)
        event_cutoff = now - timedelta(days=settings.event_retention_days)
        agg_cutoff = now - timedelta(days=30)
        lag_cutoff = now - timedelta(days=2)

        deleted: dict[str, int] = {}
        async with session_scope() as s:
            # archive-then-delete for events (batched)
            batch = list(
                await s.scalars(
                    select(Event).where(Event.event_time < event_cutoff).limit(5000)
                )
            )
            if batch:
                await self._archive.archive(
                    [{"event_id": e.event_id, "event_type": e.event_type} for e in batch]
                )
                ids = [e.id for e in batch]
                res = await s.execute(delete(Event).where(Event.id.in_(ids)))
                deleted["events"] = res.rowcount or 0

            res = await s.execute(
                delete(EventProcessingRecord).where(
                    EventProcessingRecord.created_at < event_cutoff
                )
            )
            deleted["event_processing_records"] = res.rowcount or 0

            res = await s.execute(delete(ConsumerLag).where(ConsumerLag.sampled_at < lag_cutoff))
            deleted["consumer_lag"] = res.rowcount or 0

            res = await s.execute(
                delete(Aggregation).where(Aggregation.bucket_start < agg_cutoff)
            )
            deleted["aggregations"] = res.rowcount or 0

        log.info("retention.cycle_done", **deleted)
        return deleted

    def stop(self) -> None:
        self._stop.set()


async def main() -> None:
    from pravaha.logging import configure_logging

    configure_logging()
    await RetentionWorker().run()


if __name__ == "__main__":
    asyncio.run(main())
