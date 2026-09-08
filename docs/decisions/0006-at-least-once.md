# ADR 0006 — At-least-once delivery

**Status:** accepted

## Decision
Pravāha is **at-least-once** end to end:

- The Kafka producer uses `acks=all` + idempotent producer (bounded retries) so a publish
  either lands once or errors to the caller.
- Consumers use **manual offset commit after a batch is fully handled**. A crash between
  "side effect applied" and "offset committed" re-delivers events.
- Therefore **every processor must be idempotent** — see
  `docs/decisions/0007-idempotency.md`.

## Why not exactly-once
Kafka transactions / EOS across a Python `aiokafka` consumer + Postgres + Redis + an
external AI call is not something we can honestly guarantee. Instead we get
*exactly-once **effects*** on the paths where application-level idempotency enforces it
(persistence `ON CONFLICT DO NOTHING`, aggregation upserts keyed by window identity,
processing-record unique constraint) and we document where duplicates are merely *counted*
(ingestion duplicate metric).

## Consequences
- Duplicate Kafka delivery is expected and safe.
- Replaying the same events twice is safe (idempotency records short-circuit already-done
  work).
- Consumers never commit offsets past work that failed to persist.
