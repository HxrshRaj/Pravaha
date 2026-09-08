"""Async load-test harness for the ingestion API.

Fires batched ``POST /api/v1/events/batch`` requests at a target rate for a
duration, then reports **measured** throughput and latency percentiles. Nothing
here is fabricated: if the machine cannot sustain the target rate, the achieved
rate and the growing queue delay show it.

    python -m pravaha.scripts.load_test --rate 500 --duration 60 --bootstrap
    python -m pravaha.scripts.load_test --sweep 100,500,1000 --duration 30 --bootstrap --out results.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass, field

import httpx

from pravaha.scripts.generate_events import _bootstrap

DEFAULT_API = os.getenv("NEXT_PUBLIC_API_BASE_URL", "http://localhost:8000")


def _mk_batch(n: int, rng) -> dict:  # noqa: ANN001
    ev = []
    for _ in range(n):
        corr = f"lt-{uuid.uuid4().hex[:12]}"
        ev.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "product.viewed",
                "event_time": None,
                "correlation_id": corr,
                "partition_key": corr,
                "region": rng.choice(["us-east", "eu-west", "ap-south"]),
                "payload": {"user_id": f"u{rng.randint(0, 50000)}", "product_id": f"SKU-{rng.randint(1000, 1099)}", "price": 19.0},
            }
        )
    return {"events": ev}


@dataclass
class RunResult:
    target_rate: int
    duration_s: int
    batch_size: int
    concurrency: int
    requests: int
    events_sent: int
    events_accepted: int
    http_errors: int
    achieved_eps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    latencies_sampled: int = field(default=0)


async def _run_once(
    api: str, key: str, rate: int, duration: int, batch: int, concurrency: int
) -> RunResult:
    import random

    rng = random.Random(1234)
    headers = {"X-API-Key": key, "content-type": "application/json"}
    latencies: list[float] = []
    sent = accepted = errors = requests = 0
    sem = asyncio.Semaphore(concurrency)
    batches_per_sec = max(1, rate // batch)

    async with httpx.AsyncClient(base_url=api, timeout=30, headers=headers) as client:

        async def _one() -> None:
            nonlocal sent, accepted, errors, requests
            payload = _mk_batch(batch, rng)
            async with sem:
                t0 = time.perf_counter()
                try:
                    r = await client.post("/api/v1/events/batch", content=json.dumps(payload))
                    dt = (time.perf_counter() - t0) * 1000
                    latencies.append(dt)
                    requests += 1
                    sent += batch
                    if r.status_code < 300:
                        body = r.json()
                        accepted += body.get("accepted", 0)
                    else:
                        errors += 1
                except httpx.HTTPError:
                    errors += 1
                    requests += 1

        tasks: list[asyncio.Task] = []
        end = time.perf_counter() + duration
        t0 = time.perf_counter()
        while time.perf_counter() < end:
            tick = time.perf_counter()
            for _ in range(batches_per_sec):
                tasks.append(asyncio.create_task(_one()))
            tasks = [t for t in tasks if not t.done()]
            elapsed = time.perf_counter() - tick
            await asyncio.sleep(max(0.0, 1.0 - elapsed))
        await asyncio.gather(*tasks, return_exceptions=True)
        wall = time.perf_counter() - t0

    lat = sorted(latencies)

    def pct(p: float) -> float:
        if not lat:
            return 0.0
        return round(lat[min(len(lat) - 1, int(len(lat) * p))], 2)

    return RunResult(
        target_rate=rate,
        duration_s=duration,
        batch_size=batch,
        concurrency=concurrency,
        requests=requests,
        events_sent=sent,
        events_accepted=accepted,
        http_errors=errors,
        achieved_eps=round(sent / wall, 1),
        p50_ms=pct(0.50),
        p95_ms=pct(0.95),
        p99_ms=pct(0.99),
        max_ms=round(lat[-1], 2) if lat else 0.0,
        latencies_sampled=len(lat),
    )


async def _amain() -> None:
    p = argparse.ArgumentParser(description="Pravaha ingestion load test")
    p.add_argument("--api", default=DEFAULT_API)
    p.add_argument("--api-key", default=os.getenv("PRAVAHA_PRODUCER_KEY", ""))
    p.add_argument("--bootstrap", action="store_true")
    p.add_argument("--rate", type=int, default=200)
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--batch", type=int, default=50)
    p.add_argument("--concurrency", type=int, default=32)
    p.add_argument("--sweep", default="", help="comma list of rates, e.g. 100,500,1000")
    p.add_argument("--out", default="")
    ns = p.parse_args()

    key = ns.api_key or (await _bootstrap(ns.api) if ns.bootstrap else "")
    if not key:
        raise SystemExit("need --api-key or --bootstrap")

    rates = [int(x) for x in ns.sweep.split(",") if x] or [ns.rate]
    results = []
    for rate in rates:
        print(f"\n>>> load: target {rate} ev/s for {ns.duration}s "
              f"(batch={ns.batch}, concurrency={ns.concurrency})")
        res = await _run_once(ns.api, key, rate, ns.duration, ns.batch, ns.concurrency)
        results.append(asdict(res))
        print(
            f"    achieved {res.achieved_eps} ev/s | accepted {res.events_accepted} "
            f"| p50 {res.p50_ms}ms p95 {res.p95_ms}ms p99 {res.p99_ms}ms max {res.max_ms}ms "
            f"| http_errors {res.http_errors}"
        )
        if rate != rates[-1]:
            await asyncio.sleep(5)

    if ns.out:
        with open(ns.out, "w") as f:
            json.dump({"generated_at": time.time(), "runs": results}, f, indent=2)
        print(f"\nwrote {ns.out}")
    print("\nNOTE: these are measured numbers on THIS machine. If achieved_eps is "
          "well below target, the local environment is the bottleneck - record "
          "that, do not inflate the number.")


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
