# pravaha-batch (Scala / Spark)

The batch layer described in the root [README](../README.md#24-batch-layer-spark--scala). Quick reference:

```bash
cd spark
sbt "run --hours 24"                                            # last 24h, now-anchored
sbt "run --from 2026-09-17T00:00:00Z --to 2026-09-17T06:00:00Z" # explicit window
sbt test                                                          # aggregation-logic unit tests
```

Needs: JDK 17+ and `sbt` on PATH. No Spark install, no cluster - `sbt run` launches an
embedded `local[*]` `SparkSession` in a forked JVM. Reads `events`, writes
`batch_event_rollups`, both in the same Postgres Pravaha's API/workers use
(`POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` / `POSTGRES_USER` /
`POSTGRES_PASSWORD`, same names as the root `.env`). Run
`alembic upgrade head` from the repo root first so `batch_event_rollups` exists.
