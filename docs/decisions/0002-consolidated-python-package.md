# ADR 0002 — One Python package, many worker entrypoints

**Status:** accepted

## Context
The brief lists `apps/api`, `apps/ingestion`, `apps/stream_processor`,
`apps/anomaly_detector`, `apps/ai_service`. Implemented as five separate Python projects
they would duplicate config, models, Kafka/DB plumbing and the event envelope, and drift.

## Decision
Ship **one installable package `pravaha`** containing every capability, with thin worker
entrypoints under `pravaha/workers` and `pravaha/consumers`. `docker-compose.yml` runs the
**same image** for `api`, `analytics`, `persistence`, `dataquality`, `anomaly`, `ai-worker`,
`audit`, `lag-monitor`, `replay-runner`, `retention` — differing only by `command:`.

This is the standard pattern for systems like Airflow (webserver/scheduler/worker) and
Celery. It keeps the shared domain (`pravaha.events`, `pravaha.models`, `pravaha.kafka`,
`pravaha.processing`) authoritative and testable in one place.

## Consequences
- Each logical service is still a **separate process with its own consumer group and
  offsets**, deployed and scaled independently — the isolation the brief wants.
- Single dependency set / single image to build and scan in CI.
- A change to a shared module rebuilds everything; acceptable at this scale.
