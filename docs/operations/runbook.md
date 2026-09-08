# Runbook

## Start / stop
```bash
docker compose up -d --build          # everything
docker compose run --rm migrate       # migrations (idempotent)
docker compose --profile demo up seed # demo traffic
docker compose logs -f api analytics anomaly ai-worker
docker compose down                   # keep volumes
docker compose down -v                # wipe pgdata + kafkadata (clean start)
```

## Health
- `GET http://localhost:8000/api/v1/health` — liveness
- `GET http://localhost:8000/api/v1/ready` — deps (Postgres + Kafka required; Redis
  degradable)
- `GET http://localhost:8000/api/v1/system/health` — full check incl. processor liveness
- Each worker: `http://<worker>:9100/healthz | /readyz | /metrics`

## Demo scenarios
```bash
python -m pravaha.scripts.scenarios list
python -m pravaha.scripts.scenarios run payment_failure_spike --bootstrap
python -m pravaha.scripts.scenarios run consumer_lag --bootstrap
python -m pravaha.scripts.scenarios run data_quality_degradation --bootstrap
```
Then check `/anomalies`, `/ai`, `/consumers/lag`, `/data-quality`, `/dlq` in the dashboard.

## Common issues
| Symptom | Cause | Fix |
| --- | --- | --- |
| API `readyz` 503, `kafka:false` | broker still starting | wait for `kafka` healthcheck; `docker compose logs kafka` |
| No aggregations appearing | analytics worker not flushing | it needs event-time to advance past `bucket_end + grace`; keep the generator running ≥ 90 s |
| `/live/*` shows "reconnecting" | token missing/expired | re-login; SSE uses `?access_token=` |
| DLQ filling with `invalid_schema` | producer payload vs active schema | inspect entry, fix producer or register a compatible schema version, then retry/replay |
| Consumer lag climbing and not recovering | a stuck handler or injected delay | check `pravaha_consumer_paused`; clear `pravaha:scenario:inject_delay_ms` in Redis; scale the worker |
| AI investigation `FAILED: provider ...` | no/broken AI provider | expected without keys; set `AI_PROVIDER_ORDER=mock` or add `AI_OPENAI_API_KEY` |

## Clean-start verification
See the checklist in the project README §"Clean-start verification" and
`tests/e2e/pravaha.spec.ts`.
