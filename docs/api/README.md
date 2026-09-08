# API reference

Base: `http://localhost:8000` · versioned prefix `/api/v1` · OpenAPI at `/docs` and
`/openapi.json`.

## Auth
- `POST /api/v1/auth/login` → `{access_token, role, email}` (no auth)
- `GET /api/v1/auth/me`
- `GET /api/v1/auth/users` · `POST /api/v1/auth/users` (ADMIN)

Send `Authorization: Bearer <token>` for user endpoints; `X-API-Key: pvh_…` for ingestion.

## Error model
Every non-2xx response:
```json
{ "error": { "code": "SCHEMA_VALIDATION_FAILED", "message": "...", "details": {}, "request_id": "..." } }
```
Codes: `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `CONFLICT`, `VALIDATION_FAILED`,
`RATE_LIMITED`, `PAYLOAD_TOO_LARGE`, `BATCH_TOO_LARGE`, `PIPELINE_INVALID`,
`SCHEMA_INCOMPATIBLE`, `INTERNAL_ERROR`.

## Endpoint groups
| Prefix | Purpose | Notable endpoints |
| --- | --- | --- |
| `/events` | ingest + query | `POST /events`, `POST /events/batch`, `GET /events`, `GET /events/{id}`, `GET /events/{id}/correlated` |
| `/producers` | producer mgmt (ADMIN write) | `POST /producers` (returns key once), `POST /{id}/disable\|enable\|rotate-key`, `GET /{id}/health` |
| `/schemas` | registry (ADMIN write) | `POST /schemas`, `POST /{type}/versions`, `POST /{type}/versions/{v}/activate`, `POST /{type}/validate`, `POST /{type}/compatibility` |
| `/analytics` | windowed metrics | `GET /timeseries`, `GET /summary`, `GET /top`, `GET /throughput`, `GET /metrics` |
| `/anomalies` | anomalies + evidence | `GET /anomalies`, `GET /{id}`, `GET /{id}/evidence`, `POST /{id}/status` |
| `/alerts` | rules + fired alerts (ENGINEER write) | `GET/POST /rules`, `PATCH/DELETE /rules/{id}`, `GET /alerts` |
| `/consumers` | groups + lag | `GET /groups`, `GET /lag`, `GET /lag/overview` |
| `/dlq` | dead letters (ENGINEER actions) | `GET /dlq`, `GET /stats`, `POST /{id}/retry`, `POST /{id}/discard?confirm=true` |
| `/replay` | replay jobs (ENGINEER) | `POST /estimate`, `POST /replay`, `GET /replay`, `POST /{id}/cancel` |
| `/data-quality` | DQ scores | `GET /overview`, `GET /timeseries` |
| `/pipelines` | pipeline builder (ENGINEER) | `POST /validate`, `POST /pipelines`, `PUT /{id}/draft`, `POST /{id}/versions/{v}/publish`, `GET /{id}/metrics` |
| `/ai` | investigations + cost | `GET /status`, `GET/POST /investigations`, `GET /investigations/{id}`, `POST /{id}/cancel`, `GET /usage` |
| `/system` | health + overview | `GET /health`, `GET /overview`, `GET /topics` |
| `/audit` | audit log (ENGINEER) | `GET /audit` |
| `/live` | SSE | `/live/events`, `/live/metrics`, `/live/anomalies`, `/live/alerts`, `/live/system-health` |
| `/health`, `/ready`, `/metrics` | unprefixed ops | liveness / readiness / Prometheus |

## Ingest example
```bash
curl -sX POST http://localhost:8000/api/v1/events \
  -H "X-API-Key: pvh_..." -H "content-type: application/json" \
  -d '{"event_type":"payment.failed","payload":{"order_id":"o1","user_id":"u1","reason":"card_declined","amount":42.0}}'
```
Response `202`: `{outcome, event_id, valid, quality_flags, schema_version, kafka:{partition,offset}, errors}`.
