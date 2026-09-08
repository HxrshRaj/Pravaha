"""Analytics stream processor.

Consumer group ``pravaha.analytics``. Consumes ``events.validated`` and
maintains per-minute, event-time windowed accumulators in Redis. A background
flush task finalises windows once the watermark passes their end + allowed
lateness, writing rows into ``aggregations`` (time-series) and ``windows``
(window instances), and emitting metric samples to ``events.metrics``.

Event-time handling
-------------------
* bucket = floor(event_time to 60s)
* watermark = max(event_time) - allowed_lateness (see :class:`Watermark`)
* an event whose bucket_end <= watermark - allowed_lateness is *too late*:
  counted in ``late_events`` metric + the window's ``late_event_count`` but not
  folded into an already-flushed window's ``aggregations`` value (we still update
  Redis if the key is alive, so a not-yet-flushed but past window absorbs it).
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

from pravaha.analytics.metrics_catalog import METRIC_DEFS, classify_event
from pravaha.config import settings
from pravaha.db import session_scope
from pravaha.events.envelope import EventEnvelope, LatenessClass
from pravaha.kafka.consumer import ProcessingContext, StreamConsumer
from pravaha.kafka.producer import get_producer
from pravaha.kafka.topics import METRICS
from pravaha.logging import get_logger
from pravaha.observability.metrics import (
    LATE_EVENTS_TOTAL,
    WINDOWS_CLOSED_TOTAL,
)
from pravaha.processing.state import RedisStateStore, state_key
from pravaha.processing.watermark import Watermark

log = get_logger(__name__)

BUCKET_SECONDS = 60
FLUSH_INTERVAL_SECONDS = 5


def _bucket_start(dt: datetime) -> datetime:
    ts = dt.timestamp()
    return datetime.fromtimestamp((ts // BUCKET_SECONDS) * BUCKET_SECONDS, tz=UTC)


class AnalyticsProcessor(StreamConsumer):
    group_id = "pravaha.analytics"
    processor = "analytics"
    topics = ["events.validated"]

    def __init__(self) -> None:
        super().__init__()
        self._wm = Watermark()
        self._state = RedisStateStore(ttl_seconds=3600)
        self._open_buckets: set[datetime] = set()
        self._flusher: asyncio.Task | None = None

    async def start(self) -> None:
        await super().start()
        self._flusher = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        if self._flusher:
            self._flusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flusher
        with contextlib.suppress(Exception):
            await self._flush(force=True)
        await super().stop()

    # -- per-event ------------------------------------------------------

    async def handle(self, event: EventEnvelope, ctx: ProcessingContext) -> None:
        await self._maybe_inject_delay()
        self._wm.observe(event.event_time)
        bucket = _bucket_start(event.event_time)
        bucket_end = bucket + timedelta(seconds=BUCKET_SECONDS)

        klass = self._wm.classify(event.event_time, bucket_end)
        if klass == LatenessClass.TOO_LATE:
            LATE_EVENTS_TOTAL.labels(metric="events_total", klass="too_late").inc()
            await self._bump_late(bucket)
            log.debug("analytics.too_late", event_id=event.event_id, bucket=bucket.isoformat())
            return
        if klass == LatenessClass.ACCEPTED_LATE:
            LATE_EVENTS_TOTAL.labels(metric="events_total", klass="accepted_late").inc()
            await self._bump_late(bucket)

        self._open_buckets.add(bucket)
        contributions = classify_event(event)
        # group keys: "_all" plus per-region for a few metrics
        region = event.region or "unknown"

        for defn, cval in contributions:
            base_name = defn.name
            group_keys = ["_all"]
            if "region" in defn.group_by:
                group_keys.append(f"region:{region}")

            for gk in group_keys:
                key = state_key(base_name, f"{int(bucket.timestamp())}:{gk}")
                if defn.kind == "count":
                    await self._state.incr_fields(key, {"count": 1.0})
                elif defn.kind == "sum":
                    await self._state.incr_fields(
                        key, {"sum": cval["value"], "count": 1.0}
                    )
                elif defn.kind == "distinct":
                    await self._state.add_distinct(key, cval["member"])
                elif defn.kind == "topn":
                    await self._state.topn_incr(key, cval["key"], cval.get("weight", 1.0))

    async def _bump_late(self, bucket: datetime) -> None:
        key = state_key("_late", f"{int(bucket.timestamp())}:_all")
        await self._state.incr_fields(key, {"count": 1.0})

    async def _maybe_inject_delay(self) -> None:
        """Chaos hook: the scenario simulator can set a Redis flag to slow this
        processor and make consumer lag + backpressure observable. The flag is
        re-read at most every 2s so this adds no per-event Redis round-trip when
        no scenario is active."""
        import time as _t

        now = _t.monotonic()
        if now - getattr(self, "_delay_checked_at", 0.0) > 2.0:
            self._delay_checked_at = now
            try:
                from pravaha.redis_client import get_redis

                raw = await get_redis().get("pravaha:scenario:inject_delay_ms")
                self._delay_ms = min(int(raw), 2000) if raw else 0
            except Exception:  # noqa: BLE001
                self._delay_ms = 0
        if getattr(self, "_delay_ms", 0):
            await asyncio.sleep(self._delay_ms / 1000.0)

    # -- flush --------------------------------------------------------

    async def _flush_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
                await self._flush()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("analytics.flush_error", error=str(exc))

    async def _flush(self, force: bool = False) -> None:
        wm = self._wm.value
        if wm is None and not force:
            return
        grace = timedelta(seconds=settings.watermark_allowed_lateness_seconds)
        ready = []
        for bucket in sorted(self._open_buckets):
            bucket_end = bucket + timedelta(seconds=BUCKET_SECONDS)
            if force or (wm is not None and wm >= bucket_end + grace):
                ready.append(bucket)
        for bucket in ready:
            await self._finalize_bucket(bucket)
            self._open_buckets.discard(bucket)

    async def _finalize_bucket(self, bucket: datetime) -> None:
        bucket_end = bucket + timedelta(seconds=BUCKET_SECONDS)
        base_names = sorted({d.name for d in METRIC_DEFS})
        raw: dict[str, dict] = {}

        for name in base_names:
            defn = next(d for d in METRIC_DEFS if d.name == name)
            key = state_key(name, f"{int(bucket.timestamp())}:_all")
            if defn.kind in ("count", "sum"):
                st = await self._state.get(key) or {}
                raw[name] = {
                    "value": float(st.get("sum", st.get("count", 0.0))),
                    "count": int(st.get("count", 0)),
                }
            elif defn.kind == "distinct":
                n = await self._state.distinct_count(key)
                raw[name] = {"value": float(n), "count": n}
            elif defn.kind == "topn":
                top = await self._state.topn(key, 15)
                raw[name] = {
                    "value": float(len(top)),
                    "count": int(sum(s for _, s in top)),
                    "top": [{"key": k, "score": s} for k, s in top],
                }

        late_state = await self._state.get(state_key("_late", f"{int(bucket.timestamp())}:_all")) or {}
        late_count = int(late_state.get("count", 0))

        samples = self._compose_metrics(raw)
        await self._persist(bucket, bucket_end, samples, raw, late_count)

        with contextlib.suppress(Exception):
            await get_producer().publish(
                METRICS.name,
                {
                    "window_start": bucket.isoformat(),
                    "window_end": bucket_end.isoformat(),
                    "bucket_seconds": BUCKET_SECONDS,
                    "metrics": samples,
                    "watermark": self._wm.snapshot(),
                    "emitted_at": datetime.now(UTC).isoformat(),
                },
                key=bucket.isoformat(),
            )
        for wt in ("tumbling",):
            WINDOWS_CLOSED_TOTAL.labels(metric="_bucket", window_type=wt).inc()
        log.info(
            "analytics.window_closed",
            bucket=bucket.isoformat(),
            events=samples.get("events_total", {}).get("value", 0),
            late=late_count,
        )

    def _compose_metrics(self, raw: dict[str, dict]) -> dict[str, dict]:
        out: dict[str, dict] = {}

        def val(name: str) -> float:
            return float(raw.get(name, {}).get("value", 0.0))

        for name, entry in raw.items():
            if name.endswith((".num", ".den")):
                continue
            out[name] = {"value": entry["value"], "count": entry["count"]}
            if "top" in entry:
                out[name]["top"] = entry["top"]

        # derived
        out["events_per_sec"] = {"value": round(val("events_total") / 60.0, 4), "count": int(val("events_total"))}
        rates = {
            "payment_failure_rate": ("payment_failure_rate.num", "payment_failure_rate.den"),
            "payment_success_rate": ("payment_success_rate.num", "payment_success_rate.den"),
            "order_cancellation_rate": ("order_cancellation_rate.num", "order_cancellation_rate.den"),
            "conversion_rate": ("conversion_rate.num", "conversion_rate.den"),
        }
        for rname, (num, den) in rates.items():
            d = val(den)
            out[rname] = {
                "value": round(val(num) / d, 6) if d > 0 else 0.0,
                "count": int(d),
                "numerator": int(val(num)),
            }
        aov_den = val("avg_order_value.den")
        out["avg_order_value"] = {
            "value": round(val("avg_order_value.num") / aov_den, 4) if aov_den > 0 else 0.0,
            "count": int(aov_den),
        }
        return out

    async def _persist(
        self,
        bucket: datetime,
        bucket_end: datetime,
        samples: dict[str, dict],
        raw: dict[str, dict],
        late_count: int,
    ) -> None:
        from sqlalchemy.dialects.postgresql import insert

        from pravaha.models import Aggregation, WindowRecord

        async with session_scope() as s:
            for metric, sample in samples.items():
                extra = {k: v for k, v in sample.items() if k not in ("value", "count")}
                stmt = (
                    insert(Aggregation)
                    .values(
                        metric=metric,
                        bucket_start=bucket,
                        bucket_seconds=BUCKET_SECONDS,
                        group_key="_all",
                        value=float(sample.get("value", 0.0)),
                        count=int(sample.get("count", 0)),
                        extra=extra,
                        source_time_semantics="event_time",
                    )
                    .on_conflict_do_update(
                        constraint="uq_aggregation_point",
                        set_={
                            "value": float(sample.get("value", 0.0)),
                            "count": int(sample.get("count", 0)),
                            "extra": extra,
                        },
                    )
                )
                await s.execute(stmt)

            events_total = int(samples.get("events_total", {}).get("count", 0))
            win = (
                insert(WindowRecord)
                .values(
                    metric="events_total",
                    window_type="tumbling",
                    window_size_seconds=BUCKET_SECONDS,
                    window_slide_seconds=None,
                    window_start=bucket,
                    window_end=bucket_end,
                    group_key="_all",
                    state={},
                    result=samples.get("events_total", {}),
                    event_count=events_total,
                    late_event_count=late_count,
                    is_closed=True,
                    closed_at=datetime.now(UTC),
                )
                .on_conflict_do_update(
                    constraint="uq_window_identity",
                    set_={
                        "result": samples.get("events_total", {}),
                        "event_count": events_total,
                        "late_event_count": late_count,
                        "is_closed": True,
                        "closed_at": datetime.now(UTC),
                    },
                )
            )
            await s.execute(win)
