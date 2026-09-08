# Architecture Decision Records

| # | Decision |
| --- | --- |
| [0001](0001-why-kafka.md) | Apache Kafka as the event backbone |
| [0002](0002-consolidated-python-package.md) | One Python package, many worker entrypoints |
| [0003](0003-why-redis.md) | Redis for ephemeral state, dedup, rate limiting, coordination |
| [0004](0004-why-postgresql.md) | PostgreSQL as the primary store |
| [0005](0005-event-time.md) | Event-time processing, watermarks and lateness |
| [0006](0006-at-least-once.md) | At-least-once delivery |
| [0007](0007-idempotency.md) | Idempotency strategy |
| [0008](0008-dlq-strategy.md) | Dead-letter queue & retry strategy |
| [0009](0009-event-storage.md) | Event storage & replay design |
| [0010](0010-ai-provider-abstraction.md) | AI provider abstraction & grounding |
| [0011](0011-python-stream-processing.md) | Python + asyncio for stream processing (no Flink/Spark) |
