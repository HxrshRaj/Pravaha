"""Data-quality processor.

Consumer group ``pravaha.dataquality``. Consumes ``events.validated`` **and**
``events.raw`` so it sees both accepted and rejected events, and rolls per
producer / per minute quality counters into ``data_quality_records`` with the
five sub-scores + overall Data Quality Score.

The per-event flags come from the ``_quality`` block that ingestion attaches to
the wire payload, so this processor does not re-run schema validation.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from pravaha.dataquality.engine import score_from_counts
from pravaha.db import session_scope
from pravaha.events.envelope import EventEnvelope
from pravaha.kafka.consumer import ProcessingContext, StreamConsumer
from pravaha.logging import get_logger

log = get_logger(__name__)

FLUSH_INTERVAL = 10
_COUNT_KEYS = (
    "total", "valid", "invalid_schema", "missing_fields", "invalid_timestamp",
    "future_timestamp", "duplicate_events", "unknown_event_type", "malformed_payload",
    "late_events",
)


def _bucket(dt: datetime) -> datetime:
    return datetime.fromtimestamp((dt.timestamp() // 60) * 60, tz=UTC)


class DataQualityProcessor(StreamConsumer):
    group_id = "pravaha.dataquality"
    processor = "dataquality"
    topics = ["events.validated", "events.raw"]

    def __init__(self) -> None:
        super().__init__()
        # (producer_id, producer_name, bucket) -> counts
        self._acc: dict[tuple[str, str, datetime], dict[str, int]] = defaultdict(
            lambda: dict.fromkeys(_COUNT_KEYS, 0)
        )
        self._flusher: asyncio.Task | None = None

    async def start(self) -> None:
        await super().start()
        self._flusher = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        if self._flusher:
            self._flusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flusher
        with contextlib.suppress(Exception):
            await self._flush(force=True)
        await super().stop()

    async def handle(self, event: EventEnvelope, ctx: ProcessingContext) -> None:
        # only count each event once: use events.validated for valid, events.raw
        # only for events that never reached validated (rejected). We approximate
        # by keying on topic: raw contributes just the rejection flags.
        pid = event.producer_id or "unknown"
        pname = event.producer or "unknown"
        b = _bucket(event.event_time)
        counts = self._acc[(pid, pname, b)]

        quality = event.metadata.get("_quality") if isinstance(event.metadata, dict) else None
        flags = set((quality or {}).get("flags", []))

        is_validated_topic = ctx.topic.endswith("events.validated")
        if is_validated_topic:
            counts["total"] += 1
            counts["valid"] += 1
            if "late_events" in flags:
                counts["late_events"] += 1
        else:
            # events.raw: only tally the ones that carry blocking flags (rejected)
            blocking = flags & {
                "invalid_schema", "missing_fields", "invalid_timestamp",
                "malformed_payload", "duplicate_events",
            }
            if blocking or "future_timestamp" in flags or "unknown_event_type" in flags:
                counts["total"] += 1
                for f in flags:
                    if f in counts:
                        counts[f] += 1

    async def _flush_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(FLUSH_INTERVAL)
                await self._flush()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("dataquality.flush_error", error=str(exc))

    async def _flush(self, force: bool = False) -> None:
        from sqlalchemy.dialects.postgresql import insert

        from pravaha.models import DataQualityRecord

        if not self._acc:
            return
        now = datetime.now(UTC)
        cutoff = _bucket(now) - timedelta(seconds=90)
        to_write = [
            (k, v) for k, v in self._acc.items() if force or k[2] <= cutoff
        ]
        if not to_write:
            return

        async with session_scope() as s:
            for (pid, pname, bucket), counts in to_write:
                scores = score_from_counts(counts)
                values = dict(
                    producer_id=None if pid == "unknown" else pid,
                    producer_name=pname,
                    bucket_start=bucket,
                    total_events=counts["total"],
                    valid_events=counts["valid"],
                    invalid_schema=counts["invalid_schema"],
                    missing_fields=counts["missing_fields"],
                    invalid_timestamp=counts["invalid_timestamp"],
                    future_timestamp=counts["future_timestamp"],
                    duplicate_events=counts["duplicate_events"],
                    unknown_event_type=counts["unknown_event_type"],
                    malformed_payload=counts["malformed_payload"],
                    late_events=counts["late_events"],
                    validity_score=scores["validity_score"],
                    completeness_score=scores["completeness_score"],
                    uniqueness_score=scores["uniqueness_score"],
                    timeliness_score=scores["timeliness_score"],
                    schema_compliance_score=scores["schema_compliance_score"],
                    overall_score=scores["overall_score"],
                    breakdown={"counts": counts, "scores": scores},
                )
                stmt = (
                    insert(DataQualityRecord)
                    .values(**values)
                    .on_conflict_do_update(
                        constraint="uq_dq_producer_bucket",
                        set_={k: values[k] for k in values if k not in ("producer_id", "bucket_start")},
                    )
                )
                await s.execute(stmt)

        for k, _ in to_write:
            self._acc.pop(k, None)
        log.info("dataquality.flushed", records=len(to_write))
