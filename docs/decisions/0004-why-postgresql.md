# ADR 0004 — PostgreSQL as the primary store

**Status:** accepted

## Context
We need a relational store for producers, schemas, pipelines, anomalies, alerts, consumer
lag samples, DLQ, replay jobs, AI investigation records and audit logs, plus a queryable
event store and a metric time-series.

## Decision
**PostgreSQL 16** via async SQLAlchemy 2 + Alembic. JSONB is used where the shape is
genuinely open (event `payload`/`metadata`, schema documents, AI tool results, evidence
bags); everything with a stable shape is modelled relationally with real FKs, unique
constraints and composite indexes.

## Consequences
- One engine covers OLTP config, the event store and the metric series — simple to operate
  locally, easy to reason about, easy to index.
- The event series and `aggregations` will grow; `pravaha.retention` enforces a configurable
  window and shows where an archival/data-lake tier would attach
  (`docs/decisions/0009-event-storage.md`).
- Migration `0001` bootstraps the schema from model metadata (28 tables) so it always
  matches the ORM; later migrations use `alembic revision --autogenerate`.
- Heavy analytical scans (top-N over a window) are bounded by time filters + indexes; query
  plans for the hot paths are reviewed in `docs/operations/performance.md`.
