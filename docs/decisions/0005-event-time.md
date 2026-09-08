# ADR 0005 — Event-time processing, watermarks and lateness

**Status:** accepted

## Context
Ingestion-time-only analytics are wrong the moment producers buffer, retry or clock-skew.
The brief requires real event-time processing with watermarks and late-event handling.

## Decision
- Carry three times: `event_time` (producer), `ingestion_time` (platform), `processing_time`
  (assigned by a processor, not on the wire).
- Window assignment is by **`event_time`**, boundaries aligned to the Unix epoch so any
  instance computes the same window identity.
- Watermark `W = max(event_time_seen) − allowed_lateness` (`WATERMARK_ALLOWED_LATENESS_SECONDS`,
  default 30s), with a bounded wall-clock advance after
  `WATERMARK_IDLE_ADVANCE_SECONDS` of no data so an idle partition can't stall finalisation
  forever.
- A window is finalised when `W ≥ window_end` (analytics adds a grace period before it
  deletes Redis state). Events are classified `on_time` / `accepted_late` / `too_late`;
  late counts are tracked per window (`windows.late_event_count`) and globally
  (`pravaha_late_events_total`). `too_late` events are counted but not folded into a
  finalised window.

## Honest limitations
- The watermark is **per consumer process**. With multiple partitions in one process we take
  the effective min; across *separate* processes there is **no watermark coordination** —
  each analytics instance closes its own windows. A real Flink-style distributed watermark
  is out of scope and not claimed.
- `accepted_late` events update Redis state if the window key is still alive; once the
  durable row is flushed and state deleted, a later `accepted_late` event for that window is
  effectively `too_late`. This is a deliberate simplification.
