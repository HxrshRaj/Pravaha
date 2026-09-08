# ADR 0007 — Idempotency strategy

**Status:** accepted

## Decision
The idempotency boundary is **`(event_id, processor)`**, enforced by a unique constraint on
`event_processing_records`. `ProcessingContext.already_processed()` is checked *before*
side effects; `mark_processed()` (upsert) runs *after*. A duplicate delivery finds the row
and is skipped (`pravaha_events_processed_total{result="duplicate"}`).

Layered defences:

| Layer | Mechanism | On failure |
| --- | --- | --- |
| Ingestion dedup | Redis `SET NX EX` on `(producer_id, event_id)` | fails open (best-effort) |
| Processing dedup | Postgres unique `(event_id, processor)` | authoritative |
| Persistence write | `INSERT ... ON CONFLICT (event_id) DO NOTHING` | no dup rows |
| Aggregation write | `INSERT ... ON CONFLICT (metric,bucket_start,group_key) DO UPDATE` | converges |
| DLQ retry | republish to `events.raw`; consumers still dedupe | no dup effects |

## Consequences
- Safe to replay, safe to reprocess after a crash, safe under duplicate Kafka delivery.
- The processing-record table grows with event volume; `pravaha.retention` trims it on the
  same retention window as the event store.
- Two *different* processors both handle the same event by design (analytics + persistence +
  data-quality) — the boundary is per processor, not global.
