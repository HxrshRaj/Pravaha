"""Async Kafka producer wrapper.

Wraps :class:`aiokafka.AIOKafkaProducer` with:
* ``acks=all`` + bounded idempotent retries (configurable)
* deterministic partitioning on the event ``partition_key`` (murmur2, matching
  the Java client) so ordering is preserved per business key
* orjson serialization
* a small publish-latency histogram hook for observability
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import orjson
from aiokafka import AIOKafkaProducer

from pravaha.config import settings
from pravaha.logging import get_logger
from pravaha.observability.metrics import KAFKA_PUBLISH_LATENCY, KAFKA_PUBLISH_TOTAL

log = get_logger(__name__)


def _murmur2(data: bytes) -> int:
    """Kafka's murmur2 (matches org.apache.kafka DefaultPartitioner)."""
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    h = seed ^ length
    length4 = length // 4
    for i in range(length4):
        i4 = i * 4
        k = (
            (data[i4 + 0] & 0xFF)
            + ((data[i4 + 1] & 0xFF) << 8)
            + ((data[i4 + 2] & 0xFF) << 16)
            + ((data[i4 + 3] & 0xFF) << 24)
        )
        k = (k * m) & 0xFFFFFFFF
        k ^= (k % 0x100000000) >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    extra = length & 3
    if extra >= 3:
        h ^= (data[(length & ~3) + 2] & 0xFF) << 16
    if extra >= 2:
        h ^= (data[(length & ~3) + 1] & 0xFF) << 8
    if extra >= 1:
        h ^= data[length & ~3] & 0xFF
        h = (h * m) & 0xFFFFFFFF
    h ^= (h % 0x100000000) >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= (h % 0x100000000) >> 15
    return h


def partition_for_key(key: str, num_partitions: int) -> int:
    """Deterministic partition for a business key. Used for previews / tests."""
    return (_murmur2(key.encode()) & 0x7FFFFFFF) % num_partitions


class EventProducer:
    def __init__(self, client_id: str | None = None) -> None:
        self._client_id = client_id or settings.kafka_client_id
        self._producer: AIOKafkaProducer | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._producer is not None:
                return
            self._producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id=self._client_id,
                acks=settings.kafka_acks,
                enable_idempotence=settings.kafka_acks == "all",
                linger_ms=settings.kafka_producer_linger_ms,
                request_timeout_ms=15000,
                retry_backoff_ms=200,
                value_serializer=lambda v: orjson.dumps(v),
                key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
                max_batch_size=64 * 1024,
            )
            await self._producer.start()
            log.info("kafka.producer_started", client_id=self._client_id)

    async def stop(self) -> None:
        async with self._lock:
            if self._producer is not None:
                await self._producer.stop()
                self._producer = None
                log.info("kafka.producer_stopped", client_id=self._client_id)

    async def publish(
        self,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> dict[str, Any]:
        if self._producer is None:
            await self.start()
        assert self._producer is not None
        full_topic = settings.topic(topic)
        started = time.perf_counter()
        try:
            md = await self._producer.send_and_wait(
                full_topic, value=value, key=key, headers=headers
            )
            elapsed = time.perf_counter() - started
            KAFKA_PUBLISH_LATENCY.labels(topic=topic).observe(elapsed)
            KAFKA_PUBLISH_TOTAL.labels(topic=topic, result="ok").inc()
            return {
                "topic": full_topic,
                "partition": md.partition,
                "offset": md.offset,
                "timestamp": md.timestamp,
            }
        except Exception as exc:
            KAFKA_PUBLISH_TOTAL.labels(topic=topic, result="error").inc()
            log.error("kafka.publish_failed", topic=full_topic, error=str(exc))
            raise

    async def healthy(self) -> bool:
        try:
            if self._producer is None:
                await self.start()
            assert self._producer is not None
            await self._producer.client.fetch_all_metadata()
            return True
        except Exception:  # noqa: BLE001
            return False


_default_producer: EventProducer | None = None


def get_producer() -> EventProducer:
    global _default_producer
    if _default_producer is None:
        _default_producer = EventProducer()
    return _default_producer
