# pravaha-batch (Scala / Spark)

The batch layer described in the root [README](../README.md#24-batch-layer-spark--scala). Two
jobs, both reading Pravaha's real `events` table and both run via `sbt runMain` (embedded
`local[*]` `SparkSession`, forked JVM - no Spark install, no cluster):

```bash
alembic upgrade head          # from the repo root: creates the two batch_* tables
cd spark

# Job 1: hourly (event_type, region) rollup -> batch_event_rollups
sbt "runMain pravaha.batch.EventRollupJob --hours 24"
sbt "runMain pravaha.batch.EventRollupJob --from 2026-09-17T00:00:00Z --to 2026-09-17T06:00:00Z"

# Job 2: cross-event-type correlation summary -> batch_event_correlations
sbt "runMain pravaha.batch.EventCorrelationJob"                    # all history, 1-minute buckets
sbt "runMain pravaha.batch.EventCorrelationJob --bucket-minutes 5"
sbt "runMain pravaha.batch.EventCorrelationJob --from 2026-09-08T00:00:00Z --to 2026-09-18T00:00:00Z"

sbt test    # both jobs' aggregation logic, unit-tested with hand-computed expected values
```

Needs: JDK 17+ and `sbt` on PATH. Connects to the same Postgres Pravaha's API/workers use
(`POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`,
same names as the root `.env`).
