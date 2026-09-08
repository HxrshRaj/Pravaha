# ADR 0009 — Event storage & replay design

**Status:** accepted

## Decision
For the portfolio scope, **PostgreSQL is the searchable event store** (`events` table,
indexed on `event_id`, `(event_type,event_time)`, `(producer_id,event_time)`,
`correlation_id`, `ingestion_time`, `region`). Replay reads from this table.

Retention is explicit, not "keep forever":
- `EVENT_RETENTION_DAYS` (default 7) — the `retention` worker archive-then-deletes expired
  `events`, and trims `event_processing_records`, `consumer_lag`, and old `aggregations`.
- `pravaha.retention.ArchiveSink` is the seam where an **object-storage / data-lake** tier
  would attach (write Parquet to S3/GCS before delete). The local default is
  `NullArchiveSink`.

## Replay
`replay_jobs` row + dedicated `replay-runner` worker (not the API process, so a large replay
never blocks requests). Replayed events keep their `event_id`, are re-wrapped with
`is_replay=true` + `replay_job_id`, and go to the **isolated** `events.replay` topic.
Consumers decide per-processor whether replayed events cause side effects; combined with
idempotency this makes replay safe to run repeatedly.

## Consequences / evolution
- Postgres is fine for demo volumes; at production scale the event store would move to
  object storage + a query engine, with Postgres keeping only recent/hot data and metadata.
  The envelope, topics and replay API do not change in that evolution.
