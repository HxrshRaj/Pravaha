# ADR 0001 — Apache Kafka as the event backbone

**Status:** accepted

## Context
Pravāha needs a durable, partitioned, replayable log with independent consumer groups and
explicit offset semantics so we can demonstrate real distributed-streaming concepts
(partitions, consumer groups, lag, delivery semantics, replay).

## Decision
Use **Apache Kafka** (KRaft mode, single broker locally via Docker Compose). Topics, their
partition counts and purpose are declared in `pravaha/kafka/topics.py`.

## Consequences
- We get partition-level ordering, consumer-group offset management, retention, and a
  natural replay substrate for free.
- Ordering guarantees are *per partition only*; we route by `partition_key` and document
  this everywhere rather than pretending global ordering exists.
- Local setup is single-broker, RF=1 — **not** highly available. Production would use ≥3
  brokers and RF≥3; nothing in the code assumes RF=1 except the compose file.
- We explicitly **do not** substitute an in-memory queue when Kafka is down (see
  `docs/decisions/0008-dlq-strategy.md` and the fallback rules): the fix is to repair the
  broker/config, and ingestion surfaces the failure.
