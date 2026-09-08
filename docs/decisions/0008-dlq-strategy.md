# ADR 0008 — Dead-letter queue & retry strategy

**Status:** accepted

## Decision
- **Transient** failures in a processor are retried in-process with exponential backoff +
  jitter up to `CONSUMER_RETRY_MAX_ATTEMPTS`, then dead-lettered.
- **Permanent** failures (`PermanentError` — schema/validation, unparseable envelope) are
  dead-lettered immediately, never retried.
- A dead-letter is written **both** to the `events.dlq` topic (for stream-level tooling) and
  the `dead_letter_events` table (for the UI/API): reason, processor, source topic,
  partition/offset, attempt count, error class, error detail, and the original envelope.
- Operators can **inspect / retry / discard** (discard needs `?confirm=true`). Retry
  republishes the stored envelope to `events.raw`; idempotency records mean any consumer
  that already processed it skips it, so retry cannot double-apply effects.

## Consequences
- No silent event loss: a failed event is always visible and actionable.
- The DLQ is bounded by the same retention job as events.
- Ingestion-time rejections (schema-invalid) also land in the DLQ so bad producer output is
  never lost and can be replayed after a producer fix.
