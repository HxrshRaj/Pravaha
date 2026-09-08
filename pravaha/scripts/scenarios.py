"""Reproducible incident scenario simulator.

Each scenario shapes the demo event generator (and, for consumer-lag, injects an
artificial processing delay via a Redis flag the analytics processor honours) so
the platform exhibits a specific, observable failure mode end-to-end.

    python -m pravaha.scripts.scenarios list
    python -m pravaha.scripts.scenarios run payment_failure_spike --bootstrap

Scenarios:
  payment_failure_spike     payment failure rate jumps to ~55% for 90s
  traffic_surge             10x events/sec for 120s, error rates flat
  consumer_lag              inject 250ms/event processing delay for 120s
  data_quality_degradation  one producer emits ~35% malformed events for 120s
  late_event_storm          ~40% of events arrive 60-400s late for 90s
  multi_signal_anomaly      failure spike + surge + lag simultaneously
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from pravaha.logging import configure_logging, get_logger
from pravaha.redis_client import close_redis, get_redis
from pravaha.scripts.generate_events import Generator, Options, _bootstrap

log = get_logger("pravaha.scripts.scenarios")

LAG_FLAG_KEY = "pravaha:scenario:inject_delay_ms"


SCENARIOS: dict[str, dict] = {
    "payment_failure_spike": dict(
        duration=110, rate=60, scenario_for=90, payment_failure_rate=0.55,
        note="expect: payment_failure_rate anomaly (EWMA) + AI -> H1 payment degradation",
    ),
    "traffic_surge": dict(
        duration=150, rate=400, scenario_for=120, payment_failure_rate=0.06,
        note="expect: events_per_sec anomaly (z-score); AI -> H2 load, errors flat",
    ),
    "consumer_lag": dict(
        duration=150, rate=120, scenario_for=120, inject_delay_ms=250,
        note="expect: consumer lag climbs, backpressure engages; AI -> H3 bottleneck",
    ),
    "data_quality_degradation": dict(
        duration=150, rate=80, scenario_for=120, malformed_rate=0.35,
        note="expect: DQ score for demo producer drops; DLQ fills; AI -> H4",
    ),
    "late_event_storm": dict(
        duration=110, rate=90, scenario_for=90, late_rate=0.4, late_max_seconds=400,
        note="expect: accepted_late / too_late metrics rise; window late_event_count > 0",
    ),
    "multi_signal_anomaly": dict(
        duration=150, rate=350, scenario_for=120, payment_failure_rate=0.4,
        inject_delay_ms=150,
        note="expect: multiple concurrent anomalies; AI correlates signals",
    ),
}


async def _set_delay(ms: int, ttl: int) -> None:
    try:
        r = get_redis()
        if ms > 0:
            await r.set(LAG_FLAG_KEY, ms, ex=ttl)
            log.info("scenario.delay_injected", ms=ms, ttl=ttl)
        else:
            await r.delete(LAG_FLAG_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning("scenario.delay_flag_failed", error=str(exc))


async def _run(name: str, api: str, api_key: str | None, bootstrap: bool, seed: int) -> None:
    cfg = SCENARIOS[name]
    print(f"scenario: {name}\n  {cfg['note']}\n")
    key = api_key
    if not key and bootstrap:
        key = await _bootstrap(api)
    if not key:
        raise SystemExit("need --api-key or --bootstrap")

    delay = int(cfg.get("inject_delay_ms", 0))
    if delay:
        await _set_delay(delay, cfg["scenario_for"] + 15)

    opts = Options(
        api=api,
        rate=float(cfg["rate"]),
        duration=int(cfg["duration"]),
        seed=seed,
        batch=200,
        payment_failure_rate=float(cfg.get("payment_failure_rate", 0.07)),
        cancel_rate=float(cfg.get("cancel_rate", 0.06)),
        malformed_rate=float(cfg.get("malformed_rate", 0.0)),
        duplicate_rate=float(cfg.get("duplicate_rate", 0.0)),
        late_rate=float(cfg.get("late_rate", 0.03)),
        late_max_seconds=int(cfg.get("late_max_seconds", 180)),
        scenario_for=int(cfg["scenario_for"]),
    )
    gen = Generator(opts, key)
    stats = await gen.run()
    if delay:
        await _set_delay(0, 0)
    await close_redis()
    print("\n--- scenario result (real API counts) ---")
    print(f"  sent={stats.sent} accepted={stats.accepted} rejected={stats.rejected} "
          f"duplicate={stats.duplicate} errors={stats.errors}")
    print("  now check: /anomalies, /ai, /consumers/lag, /data-quality, /dlq")


def main() -> None:
    configure_logging()
    p = argparse.ArgumentParser(description="Pravaha incident scenario simulator")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    run = sub.add_parser("run")
    run.add_argument("scenario", choices=list(SCENARIOS))
    run.add_argument("--api", default="http://localhost:8000")
    run.add_argument("--api-key", default=None)
    run.add_argument("--bootstrap", action="store_true")
    run.add_argument("--seed", type=int, default=7)
    ns = p.parse_args()

    if ns.cmd == "list":
        for name, cfg in SCENARIOS.items():
            print(f"{name:26s} {cfg['note']}")
        return
    asyncio.run(_run(ns.scenario, ns.api, ns.api_key, ns.bootstrap, ns.seed))


if __name__ == "__main__":
    sys.exit(main())
