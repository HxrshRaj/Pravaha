"""Deterministic e-commerce event generator.

Produces correlated transaction chains (order -> payment -> confirm/cancel,
browse -> cart -> purchase) plus background traffic (logins, product views,
inventory, shipments) across several regions and a catalog of products.

It talks to the running ingestion API over HTTP:

    python -m pravaha.scripts.generate_events --rate 100 --duration 60 --seed 7

``--bootstrap`` logs in as the bootstrap admin, ensures a demo producer + a few
schemas exist, and prints/reuses the producer API key (cached in
``.pravaha_demo_key``). Anomaly knobs let you shape a scenario:

    --payment-failure-rate 0.5 --for 60           # payment failure spike
    --cancel-rate 0.4
    --malformed-rate 0.1                            # data-quality degradation
    --duplicate-rate 0.1
    --late-rate 0.2 --late-max-seconds 400         # late-event storm

Numbers printed at the end (sent / accepted / rejected / duplicates / errors)
are real counts from the API responses, not fabricated.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import httpx

DEFAULT_API = os.getenv("NEXT_PUBLIC_API_BASE_URL", "http://localhost:8000")
KEY_CACHE = ".pravaha_demo_key"

REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-southeast"]
REGION_WEIGHTS = [0.30, 0.20, 0.18, 0.12, 0.12, 0.08]
PRODUCTS = [
    ("SKU-1001", "Aurora Headphones", 129.0),
    ("SKU-1002", "Nimbus Keyboard", 89.0),
    ("SKU-1003", "Photon Mouse", 45.0),
    ("SKU-1004", "Cascade Monitor 27\"", 319.0),
    ("SKU-1005", "Drift Webcam", 69.0),
    ("SKU-1006", "Vortex SSD 2TB", 155.0),
    ("SKU-1007", "Halo Desk Lamp", 39.0),
    ("SKU-1008", "Pulse Smartwatch", 199.0),
    ("SKU-1009", "Ember Mug Warmer", 25.0),
    ("SKU-1010", "Strato Laptop Stand", 52.0),
]
FAILURE_REASONS = [
    "card_declined", "insufficient_funds", "expired_card", "processor_timeout",
    "fraud_suspected", "invalid_cvc", "gateway_error",
]

DEMO_SCHEMAS: dict[str, dict] = {
    "order.created": {
        "type": "object",
        "required": ["order_id", "user_id", "amount"],
        "properties": {
            "order_id": {"type": "string"},
            "user_id": {"type": "string"},
            "amount": {"type": "number", "minimum": 0},
            "items": {"type": "integer", "minimum": 1},
            "currency": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "payment.failed": {
        "type": "object",
        "required": ["order_id", "user_id", "reason"],
        "properties": {
            "order_id": {"type": "string"},
            "user_id": {"type": "string"},
            "amount": {"type": "number"},
            "reason": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "payment.completed": {
        "type": "object",
        "required": ["order_id", "user_id", "amount"],
        "properties": {
            "order_id": {"type": "string"},
            "user_id": {"type": "string"},
            "amount": {"type": "number", "minimum": 0},
        },
        "additionalProperties": True,
    },
}


@dataclass
class Stats:
    sent: int = 0
    accepted: int = 0
    rejected: int = 0
    duplicate: int = 0
    errors: int = 0
    by_type: dict[str, int] = field(default_factory=dict)

    def add_type(self, t: str) -> None:
        self.by_type[t] = self.by_type.get(t, 0) + 1


@dataclass
class Options:
    api: str
    rate: float
    duration: int
    seed: int
    batch: int
    payment_failure_rate: float
    cancel_rate: float
    malformed_rate: float
    duplicate_rate: float
    late_rate: float
    late_max_seconds: int
    scenario_for: int


class Generator:
    def __init__(self, opts: Options, api_key: str) -> None:
        self.o = opts
        self.key = api_key
        self.rng = random.Random(opts.seed)
        self.users = [f"user-{i:05d}" for i in range(2000)]
        self.stats = Stats()
        self._start = time.monotonic()

    # -- event construction --------------------------------------------

    def _region(self) -> str:
        return self.rng.choices(REGIONS, REGION_WEIGHTS)[0]

    def _now(self) -> datetime:
        return datetime.now(UTC)

    def _maybe_late(self, ts: datetime) -> datetime:
        if self.rng.random() < self._scaled(self.o.late_rate):
            return ts - timedelta(seconds=self.rng.randint(45, max(46, self.o.late_max_seconds)))
        return ts

    def _scaled(self, base: float) -> float:
        """Anomaly knobs only apply during the first ``scenario_for`` seconds
        (0 => whole run)."""
        if self.o.scenario_for <= 0:
            return base
        return base if (time.monotonic() - self._start) <= self.o.scenario_for else base * 0.02

    def _envelope(self, event_type: str, payload: dict, corr: str, region: str, ts: datetime) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_version": "1.0",
            "event_time": self._maybe_late(ts).isoformat(),
            "correlation_id": corr,
            "partition_key": corr,
            "region": region,
            "payload": payload,
        }

    def transaction(self) -> list[dict]:
        """One correlated order lifecycle."""
        corr = f"ord-{uuid.uuid4().hex[:12]}"
        region = self._region()
        user = self.rng.choice(self.users)
        sku, name, price = self.rng.choice(PRODUCTS)
        qty = self.rng.randint(1, 3)
        amount = round(price * qty, 2)
        t = self._now()
        evs = [
            self._envelope("order.created", {
                "order_id": corr, "user_id": user, "amount": amount, "items": qty,
                "product_id": sku, "currency": "USD",
            }, corr, region, t),
            self._envelope("inventory.reserved", {
                "order_id": corr, "product_id": sku, "quantity": qty,
            }, corr, region, t + timedelta(milliseconds=120)),
            self._envelope("payment.initiated", {
                "order_id": corr, "user_id": user, "amount": amount,
            }, corr, region, t + timedelta(milliseconds=300)),
        ]
        if self.rng.random() < self._scaled(self.o.payment_failure_rate):
            reason = self.rng.choice(FAILURE_REASONS)
            evs.append(self._envelope("payment.failed", {
                "order_id": corr, "user_id": user, "amount": amount, "reason": reason,
            }, corr, region, t + timedelta(milliseconds=900)))
            evs.append(self._envelope("inventory.released", {
                "order_id": corr, "product_id": sku, "quantity": qty,
            }, corr, region, t + timedelta(seconds=1)))
            evs.append(self._envelope("order.cancelled", {
                "order_id": corr, "user_id": user, "reason": "payment_failed",
            }, corr, region, t + timedelta(seconds=1, milliseconds=200)))
        else:
            evs.append(self._envelope("payment.completed", {
                "order_id": corr, "user_id": user, "amount": amount,
            }, corr, region, t + timedelta(milliseconds=950)))
            if self.rng.random() < self._scaled(self.o.cancel_rate):
                evs.append(self._envelope("order.cancelled", {
                    "order_id": corr, "user_id": user, "reason": "customer_request",
                }, corr, region, t + timedelta(seconds=3)))
            else:
                evs.append(self._envelope("order.confirmed", {
                    "order_id": corr, "user_id": user, "amount": amount,
                }, corr, region, t + timedelta(seconds=1, milliseconds=100)))
                evs.append(self._envelope("product.purchased", {
                    "user_id": user, "product_id": sku, "amount": amount, "quantity": qty,
                }, corr, region, t + timedelta(seconds=1, milliseconds=200)))
                evs.append(self._envelope("shipment.created", {
                    "order_id": corr, "carrier": self.rng.choice(["ups", "fedex", "dhl"]),
                }, corr, region, t + timedelta(seconds=4)))
                if self.rng.random() < 0.08:
                    evs.append(self._envelope("shipment.delayed", {
                        "order_id": corr, "delay_hours": self.rng.randint(6, 72),
                    }, corr, region, t + timedelta(seconds=6)))

        # duplicates
        if self.rng.random() < self._scaled(self.o.duplicate_rate) and evs:
            evs.append(dict(self.rng.choice(evs)))

        # malformed injection
        if self.rng.random() < self._scaled(self.o.malformed_rate):
            evs.append(self._malformed(corr, region))
        return evs

    def browse(self) -> list[dict]:
        corr = f"ses-{uuid.uuid4().hex[:12]}"
        region = self._region()
        user = self.rng.choice(self.users)
        t = self._now()
        out = [self._envelope("user.login", {"user_id": user}, corr, region, t)]
        views = self.rng.randint(1, 6)
        for i in range(views):
            sku, _, price = self.rng.choice(PRODUCTS)
            out.append(self._envelope("product.viewed", {
                "user_id": user, "product_id": sku, "price": price,
            }, corr, region, t + timedelta(seconds=2 * i)))
            if self.rng.random() < 0.35:
                out.append(self._envelope("product.added_to_cart", {
                    "user_id": user, "product_id": sku, "price": price,
                }, corr, region, t + timedelta(seconds=2 * i + 1)))
        if self.rng.random() < 0.15:
            out.append(self._envelope("user.logout", {"user_id": user}, corr, region,
                                      t + timedelta(seconds=2 * views + 5)))
        return out

    def _malformed(self, corr: str, region: str) -> dict:
        kind = self.rng.choice(["bad_ts", "missing_payload_field", "wrong_type", "future_ts"])
        base = self._envelope("payment.failed", {"order_id": corr, "user_id": "u"}, corr, region, self._now())
        if kind == "bad_ts":
            base["event_time"] = "not-a-timestamp"
        elif kind == "missing_payload_field":
            base["payload"] = {"order_id": corr}  # missing required 'reason','user_id'
        elif kind == "wrong_type":
            base["payload"] = {"order_id": corr, "user_id": 12345, "reason": ["x"]}
        elif kind == "future_ts":
            base["event_time"] = (self._now() + timedelta(hours=2)).isoformat()
        return base

    # -- sending -----------------------------------------------------

    async def run(self) -> Stats:
        headers = {"X-API-Key": self.key, "content-type": "application/json"}
        interval = 1.0
        async with httpx.AsyncClient(base_url=self.o.api, timeout=15, headers=headers) as client:
            end = time.monotonic() + self.o.duration
            while time.monotonic() < end:
                tick_start = time.monotonic()
                target = self.o.rate  # events this second (approx)
                batch: list[dict] = []
                while len(batch) < target:
                    batch.extend(
                        self.transaction() if self.rng.random() < 0.55 else self.browse()
                    )
                    if self.rng.random() < 0.1:
                        batch.extend(self._background())
                # chunk into API batches
                for i in range(0, len(batch), self.o.batch):
                    chunk = batch[i : i + self.o.batch]
                    await self._send(client, chunk)
                elapsed = time.monotonic() - tick_start
                await asyncio.sleep(max(0.0, interval - elapsed))
        return self.stats

    def _background(self) -> list[dict]:
        region = self._region()
        t = self._now()
        out = []
        if self.rng.random() < 0.5:
            sku, _, _ = self.rng.choice(PRODUCTS)
            out.append(self._envelope("inventory.low_stock", {
                "product_id": sku, "remaining": self.rng.randint(0, 5),
            }, f"inv-{uuid.uuid4().hex[:8]}", region, t))
        return out

    async def _send(self, client: httpx.AsyncClient, events: list[dict]) -> None:
        for e in events:
            self.stats.add_type(e["event_type"])
        try:
            r = await client.post("/api/v1/events/batch", content=json.dumps({"events": events}))
            self.stats.sent += len(events)
            if r.status_code >= 500:
                self.stats.errors += len(events)
                return
            body = r.json()
            if "accepted" in body:
                self.stats.accepted += body.get("accepted", 0)
                self.stats.rejected += body.get("rejected", 0)
                self.stats.duplicate += body.get("duplicate", 0)
            else:
                self.stats.errors += len(events)
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            self.stats.errors += len(events)
            print(f"  send error: {exc}")


# --------------------------------------------------------------------------- bootstrap


async def _bootstrap(api: str) -> str:
    if os.path.exists(KEY_CACHE):
        key = open(KEY_CACHE).read().strip()
        if key:
            return key
    admin_email = os.getenv("BOOTSTRAP_ADMIN_EMAIL", "admin@pravaha.local")
    admin_pw = os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "admin12345")
    async with httpx.AsyncClient(base_url=api, timeout=15) as c:
        r = await c.post("/api/v1/auth/login", json={"email": admin_email, "password": admin_pw})
        r.raise_for_status()
        token = r.json()["access_token"]
        h = {"authorization": f"Bearer {token}"}
        for et, js in DEMO_SCHEMAS.items():
            resp = await c.post(
                "/api/v1/schemas",
                headers=h,
                json={"event_type": et, "json_schema": js, "description": "demo"},
            )
            if resp.status_code not in (201, 409):
                print(f"  schema {et}: {resp.status_code} {resp.text[:160]}")
        r = await c.post(
            "/api/v1/producers",
            headers=h,
            json={
                "name": "demo-ecommerce",
                "description": "demo traffic generator",
                "allowed_event_types": [],
                "rate_limit_per_min": 1_000_000,
            },
        )
        if r.status_code == 201:
            key = r.json()["api_key"]
        elif r.status_code == 409:
            # rotate to get a usable key
            producers = (await c.get("/api/v1/producers", headers=h)).json()
            pid = next(p["id"] for p in producers if p["name"] == "demo-ecommerce")
            key = (await c.post(f"/api/v1/producers/{pid}/rotate-key", headers=h)).json()["api_key"]
        else:
            r.raise_for_status()
    with open(KEY_CACHE, "w") as f:
        f.write(key)
    print(f"  bootstrapped demo producer, key cached in {KEY_CACHE}")
    return key


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pravaha demo event generator")
    p.add_argument("--api", default=DEFAULT_API)
    p.add_argument("--rate", type=float, default=50, help="approx events/sec")
    p.add_argument("--duration", type=int, default=60, help="seconds to run")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--batch", type=int, default=200)
    p.add_argument("--api-key", default=os.getenv("PRAVAHA_PRODUCER_KEY", ""))
    p.add_argument("--bootstrap", action="store_true", help="auto-create demo producer/schemas")
    p.add_argument("--payment-failure-rate", type=float, default=0.08)
    p.add_argument("--cancel-rate", type=float, default=0.06)
    p.add_argument("--malformed-rate", type=float, default=0.0)
    p.add_argument("--duplicate-rate", type=float, default=0.0)
    p.add_argument("--late-rate", type=float, default=0.03)
    p.add_argument("--late-max-seconds", type=int, default=180)
    p.add_argument("--for", dest="scenario_for", type=int, default=0,
                   help="apply anomaly knobs only for the first N seconds")
    return p.parse_args(argv)


async def _amain(argv: list[str] | None = None) -> None:
    ns = _parse_args(argv)
    key = ns.api_key
    if not key and ns.bootstrap:
        key = await _bootstrap(ns.api)
    if not key:
        raise SystemExit("no producer API key: pass --api-key or --bootstrap")

    opts = Options(
        api=ns.api,
        rate=ns.rate,
        duration=ns.duration,
        seed=ns.seed,
        batch=ns.batch,
        payment_failure_rate=ns.payment_failure_rate,
        cancel_rate=ns.cancel_rate,
        malformed_rate=ns.malformed_rate,
        duplicate_rate=ns.duplicate_rate,
        late_rate=ns.late_rate,
        late_max_seconds=ns.late_max_seconds,
        scenario_for=ns.scenario_for,
    )
    print(
        f"generating ~{opts.rate} ev/s for {opts.duration}s "
        f"(seed={opts.seed}) -> {opts.api}"
    )
    gen = Generator(opts, key)
    t0 = time.monotonic()
    stats = await gen.run()
    dt = time.monotonic() - t0
    print("\n--- generation summary (from API responses) ---")
    print(f"  wall_seconds:  {dt:.1f}")
    print(f"  sent:          {stats.sent}  ({stats.sent / dt:.0f}/s)")
    print(f"  accepted:      {stats.accepted}")
    print(f"  rejected:      {stats.rejected}")
    print(f"  duplicate:     {stats.duplicate}")
    print(f"  errors:        {stats.errors}")
    print("  by_type:")
    for t, n in sorted(stats.by_type.items(), key=lambda kv: -kv[1]):
        print(f"    {t:26s} {n}")


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
