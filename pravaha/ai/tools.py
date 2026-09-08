"""Read-only evidence tools for the AI investigator.

Every tool:
* is explicitly registered here (allowlist -- the model can call nothing else),
* only reads (SELECT) from Postgres / Redis,
* returns plain JSON-serialisable dicts whose items carry stable evidence ids,
* is logged as an :class:`AIToolCall` row by the investigator.

The anomaly under investigation is passed in :class:`ToolContext` so tools can
scope queries to the right metric / time window without trusting model input for
that scoping.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import desc, func, select

from pravaha.ai.provider import ToolSpec
from pravaha.db import session_scope
from pravaha.logging import get_logger
from pravaha.models import (
    Aggregation,
    Alert,
    AnomalyRecord,
    ConsumerLag,
    DataQualityRecord,
    Event,
)

log = get_logger(__name__)


@dataclass
class ToolContext:
    anomaly: AnomalyRecord
    investigation_id: str

    @property
    def window_start(self) -> datetime:
        return self.anomaly.window_start

    @property
    def window_end(self) -> datetime:
        return self.anomaly.window_end


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


# --------------------------------------------------------------------------- tools


async def get_anomaly_context(ctx: ToolContext, **_: Any) -> dict[str, Any]:
    a = ctx.anomaly
    async with session_scope() as s:
        prior = await s.scalars(
            select(AnomalyRecord)
            .where(
                AnomalyRecord.metric == a.metric,
                AnomalyRecord.id != a.id,
                AnomalyRecord.detected_at >= a.detected_at - timedelta(days=7),
            )
            .order_by(desc(AnomalyRecord.detected_at))
            .limit(10)
        )
        prior_list = [
            {
                "ref": f"A:{p.id}",
                "detected_at": _iso(p.detected_at),
                "severity": p.severity,
                "observed_value": p.observed_value,
                "expected_value": p.expected_value,
                "algorithm": p.algorithm,
            }
            for p in prior
        ]
    refs = [f"A:{a.id}", f"M:{a.metric}@{int(a.window_start.timestamp())}"]
    return {
        "anomaly": {
            "ref": f"A:{a.id}",
            "metric": a.metric,
            "group_key": a.group_key,
            "detected_at": _iso(a.detected_at),
            "window_start": _iso(a.window_start),
            "window_end": _iso(a.window_end),
            "observed_value": a.observed_value,
            "expected_value": a.expected_value,
            "deviation": a.deviation,
            "severity": a.severity,
            "algorithm": a.algorithm,
            "confidence": a.confidence,
            "baseline_kind": a.baseline_kind,
            "detector_evidence": a.evidence,
        },
        "prior_anomalies_same_metric_7d": prior_list,
        "evidence_refs": refs,
    }


async def query_metrics(ctx: ToolContext, lookback_minutes: int = 30, **_: Any) -> dict[str, Any]:
    end = ctx.window_end
    start = ctx.window_start - timedelta(minutes=lookback_minutes)
    metrics_of_interest = sorted(
        {
            ctx.anomaly.metric,
            "events_per_sec",
            "events_total",
            "payment_failure_rate",
            "payment_success_rate",
            "orders_created",
            "orders_cancelled",
            "order_cancellation_rate",
            "revenue",
            "active_users",
            "conversion_rate",
        }
    )
    series: dict[str, list[dict[str, Any]]] = {}
    latest: dict[str, float] = {}
    baseline: dict[str, float] = {}
    async with session_scope() as s:
        for m in metrics_of_interest:
            rows = await s.execute(
                select(Aggregation.bucket_start, Aggregation.value, Aggregation.count)
                .where(
                    Aggregation.metric == m,
                    Aggregation.group_key == "_all",
                    Aggregation.bucket_start >= start,
                    Aggregation.bucket_start <= end,
                )
                .order_by(Aggregation.bucket_start.asc())
            )
            pts = [
                {"ref": f"M:{m}@{int(bs.timestamp())}", "t": _iso(bs), "value": float(v), "count": int(c)}
                for bs, v, c in rows.all()
            ]
            if not pts:
                continue
            series[m] = pts[-lookback_minutes:]
            latest[m] = pts[-1]["value"]
            pre = [p["value"] for p in pts[:-3]] or [pts[-1]["value"]]
            baseline[m] = round(sum(pre) / len(pre), 6)
    return {
        "window": {"start": _iso(start), "end": _iso(end)},
        "latest": latest,
        "baseline": baseline,
        "series": series,
        "evidence_refs": [pts[-1]["ref"] for pts in series.values()],
    }


async def get_correlated_events(ctx: ToolContext, limit: int = 25, **_: Any) -> dict[str, Any]:
    start = ctx.window_start - timedelta(minutes=5)
    end = ctx.window_end + timedelta(minutes=2)
    focus_types = _focus_event_types(ctx.anomaly.metric)
    async with session_scope() as s:
        q = (
            select(Event)
            .where(Event.event_time >= start, Event.event_time <= end)
            .order_by(desc(Event.event_time))
            .limit(limit * 3)
        )
        if focus_types:
            q = q.where(Event.event_type.in_(focus_types))
        rows = list(await s.scalars(q))

        by_corr: dict[str, list[Event]] = {}
        for e in rows:
            by_corr.setdefault(e.correlation_id, []).append(e)

        chains = []
        for corr, evs in list(by_corr.items())[:limit]:
            evs.sort(key=lambda x: x.event_time)
            chains.append(
                {
                    "correlation_id": corr,
                    "events": [
                        {
                            "ref": f"E:{e.event_id}",
                            "event_type": e.event_type,
                            "event_time": _iso(e.event_time),
                            "region": e.region,
                            "payload_keys": sorted(list(e.payload.keys()))[:8],
                            "reason": e.payload.get("reason"),
                            "amount": e.amount,
                        }
                        for e in evs
                    ],
                }
            )
        type_counts = dict(
            (t, c)
            for t, c in (
                await s.execute(
                    select(Event.event_type, func.count(Event.id))
                    .where(Event.event_time >= start, Event.event_time <= end)
                    .group_by(Event.event_type)
                    .order_by(desc(func.count(Event.id)))
                )
            ).all()
        )
    return {
        "window": {"start": _iso(start), "end": _iso(end)},
        "event_type_counts": type_counts,
        "chains": chains,
        "evidence_refs": [
            ev["ref"] for chain in chains for ev in chain["events"][:2]
        ][:15],
    }


async def get_event(ctx: ToolContext, event_id: str, **_: Any) -> dict[str, Any]:
    async with session_scope() as s:
        e = await s.scalar(select(Event).where(Event.event_id == event_id))
        if e is None:
            return {"error": "event not found", "event_id": event_id}
        return {
            "ref": f"E:{e.event_id}",
            "event_type": e.event_type,
            "producer": e.producer,
            "event_time": _iso(e.event_time),
            "ingestion_time": _iso(e.ingestion_time),
            "correlation_id": e.correlation_id,
            "trace_id": e.trace_id,
            "region": e.region,
            "partition": e.kafka_partition,
            "offset": e.kafka_offset,
            "payload": e.payload,
            "evidence_refs": [f"E:{e.event_id}"],
        }


async def get_anomaly_history(ctx: ToolContext, lookback_days: int = 7, **_: Any) -> dict[str, Any]:
    since = datetime.now(UTC) - timedelta(days=lookback_days)
    async with session_scope() as s:
        rows = await s.scalars(
            select(AnomalyRecord)
            .where(AnomalyRecord.detected_at >= since)
            .order_by(desc(AnomalyRecord.detected_at))
            .limit(50)
        )
        items = [
            {
                "ref": f"A:{r.id}",
                "metric": r.metric,
                "detected_at": _iso(r.detected_at),
                "severity": r.severity,
                "observed_value": r.observed_value,
                "expected_value": r.expected_value,
                "algorithm": r.algorithm,
                "status": r.status,
            }
            for r in rows
        ]
    return {"count": len(items), "anomalies": items, "evidence_refs": [i["ref"] for i in items[:10]]}


async def get_consumer_lag(ctx: ToolContext, lookback_minutes: int = 20, **_: Any) -> dict[str, Any]:
    start = ctx.window_start - timedelta(minutes=lookback_minutes)
    end = ctx.window_end + timedelta(minutes=5)
    async with session_scope() as s:
        rows = await s.execute(
            select(
                ConsumerLag.group_name,
                ConsumerLag.sampled_at,
                func.sum(ConsumerLag.lag).label("lag"),
            )
            .where(ConsumerLag.sampled_at >= start, ConsumerLag.sampled_at <= end)
            .group_by(ConsumerLag.group_name, ConsumerLag.sampled_at)
            .order_by(ConsumerLag.sampled_at.asc())
        )
        series = [
            {
                "ref": f"L:{g}@{int(ts.timestamp())}",
                "group": g,
                "t": _iso(ts),
                "lag": int(lag or 0),
            }
            for g, ts, lag in rows.all()
        ]
        peak = max((r["lag"] for r in series), default=0)
        by_group: dict[str, int] = {}
        for r in series:
            by_group[r["group"]] = max(by_group.get(r["group"], 0), r["lag"])
    return {
        "window": {"start": _iso(start), "end": _iso(end)},
        "peak_lag": peak,
        "peak_lag_by_group": by_group,
        "series": series[-60:],
        "evidence_refs": [r["ref"] for r in series[-5:]],
    }


async def get_data_quality(ctx: ToolContext, lookback_minutes: int = 30, **_: Any) -> dict[str, Any]:
    start = ctx.window_start - timedelta(minutes=lookback_minutes)
    end = ctx.window_end + timedelta(minutes=5)
    async with session_scope() as s:
        rows = await s.execute(
            select(
                DataQualityRecord.producer_name,
                func.avg(DataQualityRecord.overall_score),
                func.min(DataQualityRecord.overall_score),
                func.sum(DataQualityRecord.total_events),
                func.sum(DataQualityRecord.invalid_schema),
                func.sum(DataQualityRecord.malformed_payload),
                func.sum(DataQualityRecord.duplicate_events),
            )
            .where(
                DataQualityRecord.bucket_start >= start,
                DataQualityRecord.bucket_start <= end,
            )
            .group_by(DataQualityRecord.producer_name)
        )
        producers = [
            {
                "ref": f"D:{name}",
                "producer": name,
                "avg_overall_score": round(float(avg or 1.0), 4),
                "min_overall_score": round(float(mn or 1.0), 4),
                "total_events": int(tot or 0),
                "invalid_schema": int(inv or 0),
                "malformed_payload": int(mal or 0),
                "duplicate_events": int(dup or 0),
            }
            for name, avg, mn, tot, inv, mal, dup in rows.all()
        ]
    worst = min(producers, key=lambda p: p["min_overall_score"], default=None)
    return {
        "window": {"start": _iso(start), "end": _iso(end)},
        "producers": producers,
        "worst_producer": worst,
        "evidence_refs": [p["ref"] for p in producers],
    }


async def get_producer_health(ctx: ToolContext, producer_id: str | None = None, **_: Any) -> dict[str, Any]:
    from pravaha.models import Producer
    from pravaha.producers.service import producer_health

    async with session_scope() as s:
        if producer_id:
            data = await producer_health(s, producer_id)
            return {**data, "evidence_refs": [f"P:{producer_id}"]}
        prods = list(await s.scalars(select(Producer).limit(25)))
        out = []
        for p in prods:
            try:
                out.append(await producer_health(s, p.id))
            except Exception:  # noqa: BLE001
                continue
    return {"producers": out, "evidence_refs": [f"P:{p['producer_id']}" for p in out]}


async def get_recent_alerts(ctx: ToolContext, lookback_minutes: int = 60, **_: Any) -> dict[str, Any]:
    start = ctx.window_start - timedelta(minutes=lookback_minutes)
    async with session_scope() as s:
        rows = await s.scalars(
            select(Alert).where(Alert.fired_at >= start).order_by(desc(Alert.fired_at)).limit(30)
        )
        items = [
            {
                "ref": f"ALERT:{r.id}",
                "rule_name": r.rule_name,
                "metric": r.metric,
                "severity": r.severity,
                "observed_value": r.observed_value,
                "threshold": r.threshold,
                "fired_at": _iso(r.fired_at),
                "status": r.status,
            }
            for r in rows
        ]
    return {"count": len(items), "alerts": items, "evidence_refs": [i["ref"] for i in items[:10]]}


def _focus_event_types(metric: str) -> list[str]:
    m = metric.lower()
    if "payment" in m:
        return ["payment.initiated", "payment.completed", "payment.failed", "order.created", "order.cancelled"]
    if "order" in m or "cancellation" in m:
        return ["order.created", "order.confirmed", "order.cancelled", "payment.failed"]
    if "inventory" in m:
        return ["inventory.reserved", "inventory.released", "order.created"]
    if "conversion" in m or "user" in m:
        return ["product.viewed", "product.added_to_cart", "product.purchased", "user.login"]
    return []


# --------------------------------------------------------------------------- registry

TOOL_FUNCS = {
    "get_anomaly_context": get_anomaly_context,
    "query_metrics": query_metrics,
    "get_correlated_events": get_correlated_events,
    "get_event": get_event,
    "get_anomaly_history": get_anomaly_history,
    "get_consumer_lag": get_consumer_lag,
    "get_data_quality": get_data_quality,
    "get_producer_health": get_producer_health,
    "get_recent_alerts": get_recent_alerts,
}

TOOL_SPECS: list[ToolSpec] = [
    ToolSpec(
        "get_anomaly_context",
        "Full context for the anomaly under investigation plus prior anomalies on the same metric and the base evidence reference ids. Call this first.",
        {"type": "object", "properties": {}, "additionalProperties": False},
    ),
    ToolSpec(
        "query_metrics",
        "Per-minute time series for the affected metric and key related metrics around the anomaly window, with latest values and a pre-anomaly baseline.",
        {
            "type": "object",
            "properties": {
                "lookback_minutes": {"type": "integer", "minimum": 5, "maximum": 180, "default": 30}
            },
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        "get_correlated_events",
        "Recent events around the anomaly window grouped into correlation chains (same correlation_id), plus event-type counts. Scoped to event types relevant to the metric.",
        {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 5, "maximum": 100, "default": 25}},
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        "get_event",
        "Fetch one event by event_id, including full payload.",
        {
            "type": "object",
            "properties": {"event_id": {"type": "string"}},
            "required": ["event_id"],
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        "get_anomaly_history",
        "Recent anomalies across all metrics for context on concurrent/related signals.",
        {
            "type": "object",
            "properties": {"lookback_days": {"type": "integer", "minimum": 1, "maximum": 30, "default": 7}},
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        "get_consumer_lag",
        "Consumer-group lag time series (summed over partitions) around the anomaly window, with peak lag per group.",
        {
            "type": "object",
            "properties": {
                "lookback_minutes": {"type": "integer", "minimum": 5, "maximum": 120, "default": 20}
            },
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        "get_data_quality",
        "Per-producer data-quality scores and error counts around the anomaly window.",
        {
            "type": "object",
            "properties": {
                "lookback_minutes": {"type": "integer", "minimum": 5, "maximum": 120, "default": 30}
            },
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        "get_producer_health",
        "Health snapshot for one producer (by producer_id) or all producers: recent event rate, last seen, DQ score.",
        {
            "type": "object",
            "properties": {"producer_id": {"type": "string"}},
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        "get_recent_alerts",
        "Alerts that fired around the anomaly window.",
        {
            "type": "object",
            "properties": {
                "lookback_minutes": {"type": "integer", "minimum": 5, "maximum": 240, "default": 60}
            },
            "additionalProperties": False,
        },
    ),
]

ALLOWED_TOOLS = frozenset(TOOL_FUNCS)


async def run_tool(name: str, ctx: ToolContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if name not in ALLOWED_TOOLS:
        raise ValueError(f"tool '{name}' is not in the allowlist")
    fn = TOOL_FUNCS[name]
    safe_args = {k: v for k, v in (arguments or {}).items() if isinstance(k, str)}
    log.info("ai.tool_call", tool=name, investigation_id=ctx.investigation_id, args=safe_args)
    return await fn(ctx, **safe_args)


def parse_tool_arguments(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}
