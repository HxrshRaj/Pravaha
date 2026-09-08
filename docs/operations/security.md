# Security notes

## AuthN / AuthZ
- **Users**: email + bcrypt(12) password → JWT (HS256, `API_JWT_SECRET`, TTL
  `API_ACCESS_TOKEN_TTL_MINUTES`). SSE endpoints also accept `?access_token=` because
  `EventSource` cannot set headers.
- **Producers**: `X-API-Key: pvh_…`. Stored as `HMAC-SHA256(jwt_secret, key)` — a DB leak
  alone does not allow offline brute force of short keys. Plaintext is returned **once** at
  creation / rotation.
- **RBAC**: `VIEWER < ANALYST < ENGINEER < ADMIN`, enforced by `require_role(...)`
  dependencies server-side. Examples: replay/DLQ/pipelines/alert-rules → ENGINEER;
  producers/schemas/users → ADMIN; start AI investigation / inspect events → ANALYST.

## Input / transport
- Body size capped at `INGEST_MAX_BODY_BYTES` (middleware, pre-handler); batch size capped
  at `INGEST_MAX_BATCH_SIZE`.
- Redis sliding-window rate limits on ingestion (per producer) and expensive endpoints.
- Secure headers (`X-Content-Type-Options`, `X-Frame-Options: DENY`, `Referrer-Policy`,
  `COOP`); CORS restricted to `API_CORS_ORIGINS`.
- All DB access is parameterised via SQLAlchemy; no string-built SQL.
- Consistent error envelope: `{"error":{"code","message","details","request_id"}}`.

## AI safety
- Tools are **allow-listed** (`pravaha.ai.tools.ALLOWED_TOOLS`) and **read-only** (SELECT
  only). No shell, no writes, no Kafka admin, no arbitrary DB access.
- Every tool call is persisted (`ai_tool_calls`); every provider call's usage/cost is
  persisted (`ai_usage`).
- Model output is schema-checked; `evidence_refs` are intersected with ids tools returned;
  hallucinated refs are dropped + flagged and confidence is downgraded.
- Prompts are versioned and stored per investigation.

## Secrets
- Only via environment. `.env` is git-ignored; `.env.example` holds safe local defaults.
- CI runs `pip-audit` and `npm audit` (see `.github/workflows/security.yml`).
- Bootstrap admin credentials are dev-only defaults — override `BOOTSTRAP_ADMIN_*` and
  `API_JWT_SECRET` for anything shared.

## Known gaps (portfolio scope)
- No refresh tokens / token revocation list; no MFA.
- Single JWT secret (no key rotation).
- Local Kafka/PG/Redis are unauthenticated on the compose network.
