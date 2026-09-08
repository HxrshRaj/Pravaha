from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.analytics.metrics_catalog import metric_names
from pravaha.api.deps import CurrentUser, get_db, require_viewer
from pravaha.models import Aggregation

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _parse_range(time_from: datetime | None, time_to: datetime | None, default_minutes: int):
    end = time_to or datetime.now(UTC)
    start = time_from or (end - timedelta(minutes=default_minutes))
    return start, end


@router.get("/metrics")
async def list_metrics(_: CurrentUser = Depends(require_viewer)):
    return {"metrics": metric_names()}


@router.get("/timeseries")
async def timeseries(
    metric: str = Query(...),
    group_key: str = Query("_all"),
    time_from: datetime | None = Query(None),
    time_to: datetime | None = Query(None),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    start, end = _parse_range(time_from, time_to, 60)
    rows = await db.execute(
        select(Aggregation.bucket_start, Aggregation.value, Aggregation.count, Aggregation.extra)
        .where(
            Aggregation.metric == metric,
            Aggregation.group_key == group_key,
            Aggregation.bucket_start >= start,
            Aggregation.bucket_start <= end,
        )
        .order_by(Aggregation.bucket_start.asc())
    )
    points = [
        {"t": bs.isoformat(), "value": float(v), "count": int(c), "extra": ex or {}}
        for bs, v, c, ex in rows.all()
    ]
    return {
        "metric": metric,
        "group_key": group_key,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "points": points,
    }


@router.get("/summary")
async def summary(
    minutes: int = Query(15, ge=1, le=1440),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    end = datetime.now(UTC)
    start = end - timedelta(minutes=minutes)
    wanted = [
        "events_per_sec", "events_total", "orders_created", "orders_cancelled",
        "order_cancellation_rate", "revenue", "avg_order_value", "payment_failure_rate",
        "payment_success_rate", "active_users", "conversion_rate", "logins",
        "inventory_reserved", "inventory_released",
    ]
    out: dict[str, dict] = {}
    for m in wanted:
        rows = await db.execute(
            select(Aggregation.bucket_start, Aggregation.value, Aggregation.count)
            .where(
                Aggregation.metric == m,
                Aggregation.group_key == "_all",
                Aggregation.bucket_start >= start,
            )
            .order_by(Aggregation.bucket_start.asc())
        )
        pts = [(bs, float(v), int(c)) for bs, v, c in rows.all()]
        if not pts:
            out[m] = {"latest": 0.0, "avg": 0.0, "min": 0.0, "max": 0.0, "sum": 0.0, "points": 0}
            continue
        vals = [v for _, v, _ in pts]
        counts = [c for _, _, c in pts]
        out[m] = {
            "latest": round(vals[-1], 6),
            "avg": round(sum(vals) / len(vals), 6),
            "min": round(min(vals), 6),
            "max": round(max(vals), 6),
            "sum": round(sum(counts), 6),
            "points": len(pts),
        }
    return {"window_minutes": minutes, "metrics": out}


@router.get("/top")
async def top_n(
    metric: str = Query(..., description="a topn metric, e.g. top_products, events_by_region"),
    n: int = Query(10, ge=1, le=50),
    minutes: int = Query(15, ge=1, le=1440),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    start = datetime.now(UTC) - timedelta(minutes=minutes)
    rows = await db.execute(
        select(Aggregation.extra)
        .where(
            Aggregation.metric == metric,
            Aggregation.group_key == "_all",
            Aggregation.bucket_start >= start,
        )
        .order_by(Aggregation.bucket_start.asc())
    )
    agg: dict[str, float] = {}
    for (extra,) in rows.all():
        for entry in (extra or {}).get("top", []):
            agg[entry["key"]] = agg.get(entry["key"], 0.0) + float(entry["score"])
    ranked = sorted(agg.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return {
        "metric": metric,
        "window_minutes": minutes,
        "top": [{"key": k, "score": round(v, 4)} for k, v in ranked],
    }


@router.get("/throughput")
async def throughput(
    minutes: int = Query(60, ge=1, le=1440),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    start = datetime.now(UTC) - timedelta(minutes=minutes)
    rows = await db.execute(
        select(Aggregation.bucket_start, Aggregation.value, Aggregation.count)
        .where(
            Aggregation.metric == "events_total",
            Aggregation.group_key == "_all",
            Aggregation.bucket_start >= start,
        )
        .order_by(Aggregation.bucket_start.asc())
    )
    pts = [
        {"t": bs.isoformat(), "events_per_min": int(c), "events_per_sec": round(c / 60.0, 3)}
        for bs, v, c in rows.all()
    ]
    total = sum(p["events_per_min"] for p in pts)
    return {"window_minutes": minutes, "total_events": total, "points": pts}
