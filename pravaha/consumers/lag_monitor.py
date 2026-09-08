"""Consumer lag monitor.

Periodically samples, for every Pravaha consumer group, the committed offset vs
the log-end offset per partition, writing rows into ``consumer_lag`` and
updating the ``pravaha_consumer_lag`` gauge. Also refreshes ``consumer_groups``
and evaluates lag-based alert rules (metric ``consumer_lag_total`` /
``consumer_lag:<group>``).
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import OffsetAndMetadata  # noqa: F401  (documentation)

from pravaha.config import settings
from pravaha.db import session_scope
from pravaha.kafka.topics import TOPICS
from pravaha.logging import get_logger
from pravaha.observability.metrics import CONSUMER_LAG

log = get_logger(__name__)

SAMPLE_INTERVAL_SECONDS = 15

GROUPS = [
    ("pravaha.analytics", ["events.validated"]),
    ("pravaha.persistence", ["events.validated"]),
    ("pravaha.dataquality", ["events.validated", "events.raw"]),
    ("pravaha.anomaly_ai", ["events.anomalies"]),
    ("pravaha.audit", ["events.audit"]),
]


class LagMonitor:
    def __init__(self) -> None:
        self._stop = asyncio.Event()
        self._admin: AIOKafkaAdminClient | None = None

    async def start(self) -> None:
        self._admin = AIOKafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-lagmon",
        )
        await self._admin.start()
        await self._register_groups()
        log.info("lag_monitor.started")

    async def stop(self) -> None:
        self._stop.set()
        if self._admin is not None:
            await self._admin.close()

    async def run(self) -> None:
        await self.start()
        try:
            while not self._stop.is_set():
                try:
                    await self._sample_once()
                except Exception as exc:  # noqa: BLE001
                    log.error("lag_monitor.sample_failed", error=str(exc))
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=SAMPLE_INTERVAL_SECONDS)
        finally:
            await self.stop()

    async def _register_groups(self) -> None:
        from sqlalchemy.dialects.postgresql import insert

        from pravaha.models import ConsumerGroup

        async with session_scope() as s:
            for name, topics in GROUPS:
                await s.execute(
                    insert(ConsumerGroup)
                    .values(name=name, topics=",".join(topics), description="")
                    .on_conflict_do_update(index_elements=["name"], set_={"topics": ",".join(topics)})
                )

    async def _sample_once(self) -> None:
        now = datetime.now(UTC)
        rows: list[dict] = []
        for group, topics in GROUPS:
            prefixed_group = settings.topic(group)
            prefixed_topics = [settings.topic(t) for t in topics]
            probe = AIOKafkaConsumer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=prefixed_group,
                enable_auto_commit=False,
                client_id=f"{settings.kafka_client_id}-lagprobe",
            )
            await probe.start()
            try:
                # Ensure cluster metadata is loaded before asking for partitions.
                await probe._client.force_metadata_update()
                parts: set[TopicPartition] = set()
                for t in prefixed_topics:
                    pset = probe.partitions_for_topic(t)
                    if not pset:
                        spec = TOPICS.get(t[len(settings.kafka_topic_prefix):]) if settings.kafka_topic_prefix else TOPICS.get(t)
                        pset = set(range(spec.partitions)) if spec else set()
                    for p in pset:
                        parts.add(TopicPartition(t, p))
                if not parts:
                    continue
                # Manually assign so committed() resolves against this group's
                # stored offsets (without assignment aiokafka returns None ->
                # we'd report the whole partition as lag).
                probe.assign(list(parts))
                end_offsets = await probe.end_offsets(list(parts))
                for tp in parts:
                    committed = await probe.committed(tp)
                    cur = committed if committed is not None else 0
                    leo = end_offsets.get(tp, 0)
                    lag = max(int(leo) - int(cur), 0)
                    CONSUMER_LAG.labels(
                        group=group, topic=tp.topic, partition=str(tp.partition)
                    ).set(lag)
                    rows.append(
                        {
                            "group_name": group,
                            "topic": tp.topic,
                            "partition": tp.partition,
                            "current_offset": int(cur),
                            "log_end_offset": int(leo),
                            "lag": lag,
                            "sampled_at": now,
                        }
                    )
            finally:
                await probe.stop()

        if rows:
            await self._persist(rows)
        total = sum(r["lag"] for r in rows)
        log.info("lag_monitor.sampled", partitions=len(rows), total_lag=total)

    async def _persist(self, rows: list[dict]) -> None:
        from sqlalchemy.dialects.postgresql import insert

        from pravaha.models import ConsumerLag

        async with session_scope() as s:
            for r in rows:
                await s.execute(
                    insert(ConsumerLag)
                    .values(**r)
                    .on_conflict_do_nothing(constraint="uq_consumer_lag_sample")
                )


async def main() -> None:
    from pravaha.logging import configure_logging

    configure_logging()
    await LagMonitor().run()


if __name__ == "__main__":
    asyncio.run(main())
