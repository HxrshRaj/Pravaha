# Performance notes

## Database
Indexes that matter (see `pravaha/models/`):
- `events`: unique `event_id`; composite `(event_type,event_time)`, `(producer_id,event_time)`;
  `correlation_id`; `ingestion_time`; `region`.
- `aggregations`: unique `(metric,bucket_start,group_key)`; composite `(metric,bucket_start)`.
- `anomalies`: `(metric,detected_at)`, `detected_at`; unique `dedup_key`.
- `consumer_lag`: unique `(group_name,topic,partition,sampled_at)`; `(group_name,sampled_at)`.
- `audit_logs`: `created_at`, `(actor,created_at)`, `(resource_type,resource_id)`.
- `event_processing_records`: unique `(event_id,processor)`, `(processor,created_at)`.

Hot queries are all time-bounded and hit a composite index:
- analytics timeseries: `WHERE metric=? AND group_key=? AND bucket_start BETWEEN ? AND ?
  ORDER BY bucket_start` → `ix_aggregations_metric_bucket`.
- anomaly `query_metrics`/series: same shape.
- lag overview: `MAX(sampled_at) per group` subquery + join on the newest sample.

Check a plan: `EXPLAIN (ANALYZE, BUFFERS) SELECT ...` inside `docker compose exec postgres
psql -U pravaha`.

## Avoiding the usual traps
- **No N+1**: list endpoints use one query + one count; `consumers/groups` does a bounded
  per-group query (few groups). Correlated-events tool batches by `correlation_id` in memory
  from a single `IN`-scoped query.
- **Bounded memory**: consumer in-flight set is capped (`CONSUMER_MAX_CONCURRENCY`), poll
  size capped (`CONSUMER_MAX_POLL_RECORDS`); latency sample buffers are ring-trimmed;
  Redis window state is TTL'd; the SSE layer drops oldest frames past a bounded queue.
- **Frontend**: live stream is virtualised to 300 rows; every list is paginated; charts use
  `isAnimationActive={false}` and time-bounded queries; polling intervals are 4–20s.
- **Async loop hygiene**: all I/O is `await`ed; CPU-ish work (Isolation Forest fit) runs in
  the anomaly worker, off the API path; AI calls are `BackgroundTasks` / a dedicated worker,
  never inline in a request.

## Latency budget (targets, verify with `/metrics`)
| Stage | Metric | Target (local) |
| --- | --- | --- |
| Ingest handler | `pravaha_ingest_latency_seconds` | p95 < 50 ms |
| Kafka publish | `pravaha_kafka_publish_latency_seconds` | p95 < 25 ms |
| Per-event processing | `pravaha_event_processing_latency_seconds` | p95 < 25 ms |
| Consumer lag (steady) | `pravaha_consumer_lag` | ~0, recovers after a burst |
