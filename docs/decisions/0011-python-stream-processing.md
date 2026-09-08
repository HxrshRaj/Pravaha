# ADR 0011 — Python + asyncio for stream processing (no Flink/Spark)

**Status:** accepted

## Context
The brief asks for a *deep* demonstration of stream-processing concepts, implemented in
Python, without pulling in a JVM stream processor.

## Decision
Implement the processing layer in Python with `asyncio` + `aiokafka`:
- `pravaha.kafka.consumer.StreamConsumer` — the reusable framework: manual commit, per-event
  retry with backoff/jitter, DLQ routing, bounded-concurrency backpressure with
  pause/resume, graceful drain-on-shutdown, idempotency helpers, lag/latency stats.
- `pravaha.processing` — pure primitives (`Filter`, `Transform`, incremental `Aggregator`s)
  + windowers + watermark + Redis/PG state stores.
- Each concern is its own consumer group / process.

## Consequences
- We can *show* and *explain* every mechanism (offsets, watermarks, backpressure, windows,
  state, idempotency) because we wrote it, rather than delegating to a framework.
- Throughput per process is lower than a JVM engine; horizontal scaling is by adding
  consumer instances (partitions permitting). This is an explicit tradeoff, not a hidden
  one — load numbers are measured, see `docs/operations/load-testing.md`.
- No cluster/JobManager to operate; the whole platform is `docker compose up`.
