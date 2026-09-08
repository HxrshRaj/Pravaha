# ADR 0003 — Redis for ephemeral state, dedup, rate limiting, coordination

**Status:** accepted

## Context
Windowed processing needs fast counters/sets while a window is open; ingestion needs
per-key idempotency reservations and distributed rate limiting; workers occasionally need a
lightweight flag/lock.

## Decision
Use **Redis 7** for: open-window accumulators (`pravaha.processing.state.RedisStateStore` —
hashes for count/sum, sets for distinct, zsets for top-N, all TTL'd), ingestion dedup
(`SET NX EX`), a sliding-window rate limiter (Lua, `rate_limit_check`), and the scenario
simulator's `inject_delay_ms` flag.

## Consequences
- Redis is treated as **best-effort**. Every call site catches failures:
  - rate limiter **fails open** (returns allowed);
  - ingestion dedup **fails open** (treats event as first-seen — the durable
    `(event_id, processor)` idempotency check still protects processing);
  - window state falls back to what Postgres has.
- We never claim Redis-backed exactly-once semantics survive a Redis outage — the
  at-least-once + idempotency contract is what actually holds.
- The **durable** source of truth for window results is Postgres (`windows`,
  `aggregations`); Redis is a cache in front of it.
