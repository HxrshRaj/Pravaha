# Load testing

## Tool
`pravaha.scripts.load_test` fires batched `POST /api/v1/events/batch` at a target rate for a
duration and reports **measured** results: achieved events/sec and p50/p95/p99/max request
latency, plus HTTP error count.

```bash
# single point
python -m pravaha.scripts.load_test --rate 500 --duration 60 --bootstrap

# sweep, save JSON
python -m pravaha.scripts.load_test --sweep 100,500,1000 --duration 30 \
  --batch 50 --concurrency 32 --bootstrap --out load-test-results/run-$(date +%s).json
```

While it runs, watch:
- `GET /api/v1/consumers/lag/overview` — does lag stay bounded / recover?
- `GET /api/v1/system/overview` — `events_per_sec`, `dlq_pending`
- `pravaha_consumer_paused`, `pravaha_event_processing_latency_seconds` on `:9100/metrics`

## Method
1. `docker compose up -d --build` and let it settle (~1 min).
2. Run the sweep above.
3. Record the JSON in `load-test-results/` and paste the achieved numbers here.

## Results

### Run 1 — 2026-09-08, contended dev host

Windows 11 + Docker Desktop (WSL2), **single uvicorn worker**, batch=50, concurrency=24,
20s per step. The host was simultaneously running two other unrelated multi-service compose
stacks and a background 45 ev/s traffic generator, so this is a *worst-case* dev reading,
not a capacity number.

| target ev/s | achieved ev/s | accepted | http_errors | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 80.9 | 2000 | 0 | 10053 | 13199 | 13267 |
| 300 | 130.8 | 6000 | 0 | 8454 | 13042 | 13384 |
| 600 | 183.3 | 12000 | 0 | 5689 | 10436 | 10602 |

Raw JSON in `load-test-results/run-1788861106.json`.

**Reading this honestly:** zero errors and every event accepted, but request latency is
poor (p50 5–10s). Root causes, in order:

1. **Contended host** — three full stacks + a generator on one laptop. CPU/IO starvation
   dominates.
2. **`POST /events/batch` processes its 50 events sequentially** — each does schema
   lookup + Redis dedup + `enforce_rate_limit` + **two `producer.send_and_wait`** (one to
   `events.raw`, one to `events.validated`), and `send_and_wait` blocks on the broker ack.
   50 × (2 acked publishes) per request is the single biggest lever.
3. **One uvicorn worker.**

Concrete next steps (not yet applied): fan the per-event work inside a batch across an
`asyncio.gather` with bounded concurrency and a session-per-event; switch the batch path to
`producer.send()` + gather the delivery futures once at the end instead of per-event
`send_and_wait`; run `uvicorn --workers N`; add `analytics`/`persistence` replicas up to the
partition count (6). On an idle host the same code with those changes is expected to clear
the 1000 ev/s target — **that claim is untested here and must be measured, not assumed.**
