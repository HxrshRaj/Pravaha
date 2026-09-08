# Pravāha architecture

## System architecture

```mermaid
flowchart LR
    subgraph Producers
      SVC[Simulated e-commerce services / generator]
    end
    SVC -->|"HTTP POST /api/v1/events(/batch)"| API

    subgraph API tier
      API[FastAPI: ingestion + query + admin + SSE]
    end

    API -->|"schema-valid"| KV[(events.validated)]
    API -->|"all"| KR[(events.raw)]
    API -->|"invalid"| KDLQ[(events.dlq)]

    subgraph "Stream processing (independent consumer groups)"
      AN[analytics]
      PE[persistence]
      DQ[dataquality]
      AD[anomaly + alerts]
      AIW[ai]
      AU[audit]
    end

    KV --> AN & PE & DQ
    AN -->|"window samples"| KM[(events.metrics)]
    KM --> AD
    AD -->|"anomaly"| KA[(events.anomalies)]
    KA --> AIW
    API -->|"audit events"| KAUD[(events.audit)]
    KAUD --> AU

    AN & PE & DQ & AD & AIW & AU --> PG[(PostgreSQL)]
    AN --- RE[(Redis)]
    API --- RE

    PG --> API
    API -->|"SSE /live/*"| WEB[Next.js dashboard]

    LM[lag-monitor] --> PG
    RR[replay-runner] -->|"re-publish"| KREP[(events.replay)]
    RET[retention] --> PG
```

## Streaming pipeline (analytics worker internals)

```mermaid
flowchart TD
    S["Kafka source: events.validated"] --> WMK["observe event_time → advance watermark"]
    WMK --> CLS{"classify vs target window"}
    CLS -->|too_late| LT["count late_events, drop"]
    CLS -->|on_time / accepted_late| CLA["classify_event → metric contributions"]
    CLA --> ST["Redis accumulators: count/sum · distinct sets · top-N zsets<br/>keyed by (metric, bucket, group)"]
    FL["flush loop (5s)"] --> FIN{"watermark ≥ bucket_end + grace?"}
    FIN -->|yes| COMPOSE["compose metrics: derive rates, avg_order_value, events_per_sec"]
    COMPOSE --> UPS["upsert aggregations + windows (Postgres)"]
    UPS --> EMIT["emit sample → events.metrics"]
```

## Kafka topic topology

```mermaid
flowchart LR
    RAW[events.raw<br/>6p · 7d] 
    VAL[events.validated<br/>6p · 7d]
    MET[events.metrics<br/>3p]
    ANO[events.anomalies<br/>3p · 30d]
    DLQ[events.dlq<br/>3p · 30d]
    REP[events.replay<br/>6p]
    AUD[events.audit<br/>3p · 90d]

    ING((ingestion)) --> RAW & VAL
    ING --> DLQ
    VAL --> G1([cg: pravaha.analytics])
    VAL --> G2([cg: pravaha.persistence])
    VAL --> G3([cg: pravaha.dataquality])
    G1 --> MET --> G4([cg: pravaha.anomaly])
    G4 --> ANO --> G5([cg: pravaha.anomaly_ai])
    AUD --> G6([cg: pravaha.audit])
    RR((replay-runner)) --> REP
```

Ordering: **per-partition only.** `partition_key = correlation_id | order_id | user_id |
producer key`, so one business transaction stays on one partition and is processed in order.
There is no global ordering.

## Consumer group architecture

| Group | Topic(s) | Job | Idempotency key |
| --- | --- | --- | --- |
| `pravaha.analytics` | `events.validated` | event-time windowed aggregation → `aggregations`, `windows`, `events.metrics` | `(event_id, "analytics")` |
| `pravaha.persistence` | `events.validated` | write `events` (searchable store) | `INSERT ON CONFLICT (event_id)` |
| `pravaha.dataquality` | `events.validated`, `events.raw` | per-producer DQ scoring → `data_quality_records` | `(event_id, "dataquality")` |
| `pravaha.anomaly` | `events.metrics` | detectors + alert rules → `anomalies`, `alerts`, `events.anomalies` | `dedup_key` on anomaly |
| `pravaha.anomaly_ai` | `events.anomalies` | create + run AI investigation | investigation per anomaly |
| `pravaha.audit` | `events.audit` | append `audit_logs` | append-only |

## AI investigation flow

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> RUNNING: run_investigation()
    RUNNING --> RUNNING: chat() → tool_call → run read-only tool → store evidence
    RUNNING --> WAITING_FOR_TOOL: (phase marker)
    WAITING_FOR_TOOL --> RUNNING
    RUNNING --> COMPLETED: valid JSON conclusion, refs grounded
    RUNNING --> FAILED: provider error / bad output / budget exhausted
    RUNNING --> CANCELLED: operator
    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

Evidence pipeline: `ANOMALY_DETECTED → INVESTIGATION_CREATED → EVIDENCE_COLLECTION
(query_metrics, get_correlated_events, get_consumer_lag, get_data_quality, get_recent_alerts,
get_anomaly_history) → HYPOTHESIS_GENERATION (H1–H5 scored) → HYPOTHESIS_TESTING →
CONCLUSION → RECOMMENDATIONS`. Final `evidence_refs` are intersected with ids tools actually
returned.

## Failure / recovery flow

```mermaid
flowchart TD
    C["consumer crash"] --> R["restart"] --> RES["resume from last committed offset"] --> RD["re-deliver uncommitted batch"] --> ID{"already_processed?"}
    ID -->|yes| SKIP["skip (duplicate)"]
    ID -->|no| PROC["process → mark_processed → commit"]

    HE["handler error"] --> TY{"PermanentError?"}
    TY -->|yes| DLQ1["DLQ immediately"]
    TY -->|no| RETRY["backoff+jitter retry ≤ N"] --> OK{"ok?"}
    OK -->|yes| DONE["commit"]
    OK -->|no, N reached| DLQ2["DLQ + mark failed"]

    OVER["consumer overload"] --> BP["in-flight full → pause partitions"] --> DRAIN["drain"] --> RESUME["resume"]
    LAGUP["lag rises"] --> LM["lag-monitor samples → consumer_lag → alerts"]
```

## Replay architecture

```mermaid
flowchart LR
    UI["Replay UI / API"] -->|"estimate → confirm"| JOB["replay_jobs row (CREATED)"]
    RUN["replay-runner (polls CREATED)"] --> JOB
    RUN -->|"page events by id"| PG[(events)]
    RUN -->|"re-wrap: is_replay=true, replay_job_id"| REP[(events.replay)]
    RUN -->|"progress: replayed/failed counts"| JOB
    REP --> CONS["consumers (idempotent) → metrics recover, no dup rows"]
```
