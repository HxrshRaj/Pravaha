"""Reusable Kafka consumer framework.

Delivery semantics
------------------
Pravaha is **at-least-once**. Offsets are committed only after a batch has been
fully handled. A crash between "side effect applied" and "offset committed"
therefore re-delivers events, so every processor MUST be idempotent -- the
framework provides :meth:`ProcessingContext.already_processed` /
:meth:`mark_processed` backed by the ``event_processing_records`` unique
constraint. (See docs/decisions/0006-at-least-once.md and 0007-idempotency.md.)

Reliability features
--------------------
* Manual, explicit-offset commit after each fetched batch is fully handled
  (``{partition: last_offset + 1}``); fetching blocks on the batch's ``gather``
  so in-flight work is inherently bounded -- that is the backpressure. A
  semaphore additionally caps handler concurrency within a batch.
* Per-event retry with exponential backoff + jitter for transient errors.
* Permanent errors (validation etc.) go straight to the DLQ, no retry.
* After ``CONSUMER_RETRY_MAX_ATTEMPTS`` a transient failure is also DLQ'd.
* ``pravaha_consumer_paused`` is raised when a batch takes > 5s (can't keep up).
* Graceful shutdown on SIGINT/SIGTERM: stop fetching, drain in-flight, commit.

Message shape: with ``envelope = True`` (default) each Kafka message is parsed
into an :class:`~pravaha.events.envelope.EventEnvelope` and gets an
idempotency record; with ``envelope = False`` (events.metrics / events.anomalies
/ events.audit) the raw dict is passed straight to :meth:`handle`.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import signal
import socket
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import orjson
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from pravaha.config import settings
from pravaha.db import session_scope
from pravaha.events.envelope import EventEnvelope
from pravaha.logging import bind_context, clear_context, get_logger
from pravaha.observability.metrics import (
    CONSUMER_INFLIGHT,
    CONSUMER_PAUSED,  # noqa: F401 - kept for the /metrics contract; set on backpressure
    EVENT_PROCESSING_LATENCY,
    EVENTS_DLQ_TOTAL,
    EVENTS_FAILED_TOTAL,
    EVENTS_PROCESSED_TOTAL,
)

log = get_logger(__name__)


class PermanentError(Exception):
    """Raised by a handler when retrying can never succeed -> straight to DLQ."""


class RetryableError(Exception):
    """Raised by a handler for a transient failure -> backoff + retry, then DLQ."""


@dataclass
class ProcessingContext:
    processor: str
    topic: str
    partition: int
    offset: int
    key: str | None
    headers: dict[str, bytes]
    attempt: int = 1
    _dedup_checked: bool = field(default=False, repr=False)

    async def already_processed(self, event_id: str) -> bool:
        from sqlalchemy import select

        from pravaha.models import EventProcessingRecord

        async with session_scope() as s:
            row = await s.scalar(
                select(EventProcessingRecord.id).where(
                    EventProcessingRecord.event_id == event_id,
                    EventProcessingRecord.processor == self.processor,
                    EventProcessingRecord.status == "processed",
                )
            )
            return row is not None

    async def mark_processed(self, event_id: str, *, status: str = "processed", error: str | None = None) -> None:
        from sqlalchemy.dialects.postgresql import insert

        from pravaha.models import EventProcessingRecord

        stmt = (
            insert(EventProcessingRecord)
            .values(
                event_id=event_id,
                processor=self.processor,
                status=status,
                attempts=self.attempt,
                error=error,
                kafka_partition=self.partition,
                kafka_offset=self.offset,
            )
            .on_conflict_do_update(
                index_elements=["event_id", "processor"],
                set_={"status": status, "attempts": self.attempt, "error": error},
            )
        )
        async with session_scope() as s:
            await s.execute(stmt)


Handler = Callable[[EventEnvelope, ProcessingContext], Awaitable[None]]


def _log_assignment_change(group: str, prev: set, cur: set) -> set:
    """Log partition assignment diffs from the consume loop (no listener)."""
    if cur != prev:
        gained = cur - prev
        lost = prev - cur
        log.info(
            "consumer.assignment_changed",
            group=group,
            assigned=sorted(str(p) for p in cur),
            gained=sorted(str(p) for p in gained),
            revoked=sorted(str(p) for p in lost),
        )
    return cur


class StreamConsumer:
    """Base class. Subclass and implement :meth:`handle`."""

    #: consumer-group name; MUST be stable and unique per logical processor
    group_id: str = "pravaha.generic"
    #: processor id used for idempotency records / metrics / DLQ attribution
    processor: str = "generic"
    #: topics to subscribe to (post-prefix names)
    topics: list[str] = []
    #: route failures here
    dlq_topic: str = "events.dlq"
    #: True  -> each message is an EventEnvelope (parsed, idempotency-tracked)
    #: False -> each message is a plain dict (events.metrics/anomalies/audit),
    #:          passed straight to handle() with no envelope parse / dedup record
    envelope: bool = True

    def __init__(self) -> None:
        self._consumer: AIOKafkaConsumer | None = None
        self._running = asyncio.Event()
        self._stopping = asyncio.Event()
        self._inflight: set[asyncio.Task] = set()
        self._max_inflight = settings.consumer_max_concurrency
        self._paused = False
        self._last_assignment: set = set()
        self._member_id = f"{socket.gethostname()}-{random.randint(1000, 9999)}"
        self._processed = 0
        self._failed = 0
        self._latencies: list[float] = []

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        from pravaha.kafka.producer import get_producer

        self._producer = get_producer()
        await self._producer.start()

        # Subscribe via the constructor and let aiokafka manage the group.
        # (A separate .subscribe() call after start(), or a coroutine-based
        # ConsumerRebalanceListener, tripped an aiokafka fetcher assertion /
        # rebalance storm on <=0.12 - fixed by using >=0.14 and doing rebalance
        # logging from the loop instead of a listener.)
        self._consumer = AIOKafkaConsumer(
            *[settings.topic(t) for t in self.topics],
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.topic(self.group_id),
            client_id=f"{settings.kafka_client_id}-{self.processor}-{self._member_id}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=settings.consumer_max_poll_records,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
            value_deserializer=lambda b: orjson.loads(b),
            key_deserializer=lambda b: b.decode() if b else None,
        )
        await self._consumer.start()
        # Wait for the initial group join to settle before we start polling.
        # aiokafka 0.12's background fetch routine asserts on an active
        # assignment; entering getmany() before the join completes can race it.
        for _ in range(75):
            if self._consumer.assignment():
                break
            await asyncio.sleep(0.2)
        self._running.set()
        log.info("consumer.started", processor=self.processor, group=self.group_id, topics=self.topics)

    async def stop(self) -> None:
        self._stopping.set()
        if self._inflight:
            log.info("consumer.draining", processor=self.processor, inflight=len(self._inflight))
            await asyncio.gather(*self._inflight, return_exceptions=True)
        if self._consumer is not None:
            with contextlib.suppress(Exception):
                await self._commit()
            await self._consumer.stop()
        log.info(
            "consumer.stopped", processor=self.processor, processed=self._processed, failed=self._failed
        )

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, lambda: self._stopping.set())

    # -- main loop -------------------------------------------------------

    async def run(self) -> None:
        await self.start()
        self.install_signal_handlers()
        try:
            await self._loop()
        finally:
            await self.stop()

    async def _loop(self) -> None:
        """Fetch a batch → process it with bounded concurrency → commit the
        exact offsets we finished.

        Offsets are committed **only after every message in the batch has been
        fully handled** (success, DLQ, or marked-failed), as explicit
        ``{partition: last_offset + 1}`` positions. A crash mid-batch re-delivers
        the batch, which is safe because every handler is idempotent. Fetching
        blocks until the previous batch's ``gather`` completes, so in-flight work
        is inherently bounded (that is the backpressure); a semaphore caps how
        many handlers run at once within a batch. ``pause``/``resume`` is a
        defensive guard for the rare case a handler leaves background work.
        """
        from aiokafka import OffsetAndMetadata
        from aiokafka import TopicPartition as _TP

        assert self._consumer is not None
        sem = asyncio.Semaphore(self._max_inflight)
        max_records = max(settings.consumer_max_poll_records, self._max_inflight)

        while not self._stopping.is_set():
            self._last_assignment = _log_assignment_change(
                self.group_id, self._last_assignment, set(self._consumer.assignment())
            )
            try:
                batches = await self._consumer.getmany(
                    timeout_ms=1000, max_records=max_records
                )
            except KafkaError as exc:
                log.warning("consumer.poll_error", processor=self.processor, error=str(exc))
                await asyncio.sleep(1.0)
                continue

            if not batches:
                continue

            commit_offsets: dict = {}
            batch_tasks: list[asyncio.Task] = []

            async def _guarded(tp, msg):  # noqa: ANN001
                async with sem:
                    await self._process_one(tp, msg)

            for tp, messages in batches.items():
                if not messages:
                    continue
                commit_offsets[_TP(tp.topic, tp.partition)] = OffsetAndMetadata(
                    messages[-1].offset + 1, ""
                )
                for msg in messages:
                    task = asyncio.create_task(_guarded(tp, msg))
                    self._inflight.add(task)
                    task.add_done_callback(self._inflight.discard)
                    batch_tasks.append(task)

            CONSUMER_INFLIGHT.labels(processor=self.processor).set(len(batch_tasks))
            t0 = time.perf_counter()
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            batch_seconds = time.perf_counter() - t0
            for r in results:
                if isinstance(r, BaseException):
                    log.error("consumer.task_crashed", processor=self.processor, error=repr(r))
            CONSUMER_INFLIGHT.labels(processor=self.processor).set(0)

            # Backpressure signal: if a full batch took longer than 5s we are not
            # keeping up. Fetching already blocked on the gather above (so we
            # never overrun memory); we surface it and briefly yield so lag is
            # visible and other tasks (flush loops, health) get time.
            overloaded = batch_seconds > 5.0 and len(batch_tasks) >= max_records
            CONSUMER_PAUSED.labels(processor=self.processor).set(1 if overloaded else 0)
            if overloaded:
                log.info(
                    "consumer.backpressure",
                    processor=self.processor,
                    batch=len(batch_tasks),
                    batch_seconds=round(batch_seconds, 2),
                )
                await asyncio.sleep(0.5)

            if commit_offsets and not self._stopping.is_set():
                try:
                    await self._consumer.commit(commit_offsets)
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "consumer.commit_failed", processor=self.processor, error=str(exc)
                    )

    async def _commit(self) -> None:
        if self._consumer is not None:
            with contextlib.suppress(Exception):
                await self._consumer.commit()

    # -- per-event processing -----------------------------------------

    async def _process_one(self, tp: TopicPartition, msg) -> None:  # noqa: ANN001
        headers = {k: v for k, v in (msg.headers or [])}
        raw = msg.value
        event_id = raw.get("event_id") if isinstance(raw, dict) else None
        bind_context(
            processor=self.processor,
            consumer_group=self.group_id,
            event_id=event_id,
            trace_id=raw.get("trace_id") if isinstance(raw, dict) else None,
        )
        started = time.perf_counter()
        try:
            if self.envelope:
                try:
                    payload = EventEnvelope.from_wire(raw)
                except Exception as exc:  # malformed on the wire -> permanent
                    await self._to_dlq(tp, msg, raw, "malformed_envelope", str(exc), "permanent", 1)
                    EVENTS_DLQ_TOTAL.labels(processor=self.processor, reason="malformed_envelope").inc()
                    return
            else:
                # raw-message consumer (events.metrics / events.anomalies /
                # events.audit): the payload is a plain dict, no envelope, no
                # per-event idempotency record.
                payload = raw if isinstance(raw, dict) else {"_raw": raw}

            ctx = ProcessingContext(
                processor=self.processor,
                topic=tp.topic,
                partition=tp.partition,
                offset=msg.offset,
                key=msg.key,
                headers=headers,
            )
            await self._handle_with_retry(payload, ctx, tp, msg, raw)
        finally:
            elapsed = time.perf_counter() - started
            EVENT_PROCESSING_LATENCY.labels(processor=self.processor).observe(elapsed)
            self._latencies.append(elapsed)
            if len(self._latencies) > 512:
                self._latencies = self._latencies[-512:]
            clear_context()

    async def _handle_with_retry(self, event, ctx, tp, msg, raw) -> None:  # noqa: ANN001
        max_attempts = settings.consumer_retry_max_attempts
        event_id = getattr(event, "event_id", None) if self.envelope else None
        for attempt in range(1, max_attempts + 1):
            ctx.attempt = attempt
            try:
                if event_id is not None and await ctx.already_processed(event_id):
                    EVENTS_PROCESSED_TOTAL.labels(processor=self.processor, result="duplicate").inc()
                    log.debug("consumer.skip_duplicate", event_id=event_id)
                    return
                await self.handle(event, ctx)
                if event_id is not None:
                    await ctx.mark_processed(event_id)
                self._processed += 1
                EVENTS_PROCESSED_TOTAL.labels(processor=self.processor, result="ok").inc()
                return
            except PermanentError as exc:
                self._failed += 1
                EVENTS_FAILED_TOTAL.labels(processor=self.processor, error_class="permanent").inc()
                await self._to_dlq(tp, msg, raw, "permanent_error", str(exc), "permanent", attempt)
                EVENTS_DLQ_TOTAL.labels(processor=self.processor, reason="permanent_error").inc()
                return
            except Exception as exc:  # RetryableError or unexpected -> transient
                is_last = attempt >= max_attempts
                EVENTS_FAILED_TOTAL.labels(processor=self.processor, error_class="transient").inc()
                log.warning(
                    "consumer.handle_error",
                    event_id=event_id,
                    attempt=attempt,
                    last=is_last,
                    error=str(exc),
                )
                if is_last:
                    self._failed += 1
                    await self._to_dlq(tp, msg, raw, "retries_exhausted", str(exc), "transient", attempt)
                    EVENTS_DLQ_TOTAL.labels(processor=self.processor, reason="retries_exhausted").inc()
                    if event_id is not None:
                        with contextlib.suppress(Exception):
                            await ctx.mark_processed(event_id, status="failed", error=str(exc))
                    return
                backoff = min(
                    settings.consumer_retry_max_ms,
                    settings.consumer_retry_base_ms * (2 ** (attempt - 1)),
                )
                jitter = random.uniform(0, backoff * 0.3)
                await asyncio.sleep((backoff + jitter) / 1000.0)

    async def _to_dlq(self, tp, msg, raw, reason, detail, error_class, attempt) -> None:  # noqa: ANN001
        from sqlalchemy.dialects.postgresql import insert

        from pravaha.models import DeadLetterEvent

        event_id = raw.get("event_id") if isinstance(raw, dict) else None
        record = {
            "event_id": event_id,
            "processor": self.processor,
            "source_topic": tp.topic,
            "kafka_partition": tp.partition,
            "kafka_offset": msg.offset,
            "failure_reason": reason,
            "error_detail": detail[:8000],
            "error_class": error_class,
            "attempt_count": attempt,
            "raw_payload": raw if isinstance(raw, dict) else {"_raw": str(raw)},
            "envelope": raw if isinstance(raw, dict) else None,
            "status": "PENDING",
        }
        with contextlib.suppress(Exception):
            await self._producer.publish(
                self.dlq_topic,
                {**record, "dlq_recorded_at": datetime.now(UTC).isoformat()},
                key=event_id,
            )
        try:
            async with session_scope() as s:
                await s.execute(insert(DeadLetterEvent).values(**record))
        except Exception as exc:  # noqa: BLE001
            log.error("consumer.dlq_persist_failed", processor=self.processor, error=str(exc))

    # -- to implement -----------------------------------------------------

    async def handle(self, event: EventEnvelope, ctx: ProcessingContext) -> None:  # pragma: no cover
        raise NotImplementedError

    @property
    def stats(self) -> dict:
        lat = sorted(self._latencies)
        p95 = lat[int(len(lat) * 0.95)] * 1000 if lat else 0.0
        return {
            "processor": self.processor,
            "group_id": self.group_id,
            "processed": self._processed,
            "failed": self._failed,
            "inflight": len(self._inflight),
            "paused": self._paused,
            "p95_latency_ms": round(p95, 2),
        }
