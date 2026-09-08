from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.deps import CurrentUser, get_db, require_viewer
from pravaha.kafka.producer import get_producer
from pravaha.kafka.topics import all_topic_specs
from pravaha.models import (
    Aggregation,
    AIInvestigation,
    AnomalyRecord,
    ConsumerLag,
    DeadLetterEvent,
    Event,
    Producer,
)
from pravaha.models.producers import ProducerStatus
from pravaha.redis_client import redis_healthy

router = APIRouter(prefix="/system", tags=["system"])


@router.get("/health")
async def system_health(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    return await system_health_data(db)


async def system_health_data(db: AsyncSession) -> dict:
    checks: dict[str, dict] = {}

    try:
        t0 = datetime.now(UTC)
        await db.execute(text("SELECT 1"))
        checks["postgres"] = {
            "up": True,
            "latency_ms": (datetime.now(UTC) - t0).total_seconds() * 1000,
        }
    except Exception as exc:  # noqa: BLE001
        checks["postgres"] = {"up": False, "error": str(exc)}

    checks["redis"] = {"up": await redis_healthy()}
    try:
        checks["kafka"] = {"up": await get_producer().healthy()}
    except Exception as exc:  # noqa: BLE001
        checks["kafka"] = {"up": False, "error": str(exc)}

    from pravaha.ai.provider import available_providers

    providers = available_providers()
    checks["ai"] = {"up": bool(providers), "providers": providers, "required": False}

    # processor liveness inferred from recent writes
    now = datetime.now(UTC)
    recent_agg = await db.scalar(
        select(func.max(Aggregation.created_at))
    )
    recent_lag = await db.scalar(select(func.max(ConsumerLag.sampled_at)))
    checks["analytics_processor"] = {
        "up": recent_agg is not None and (now - recent_agg) < timedelta(minutes=5),
        "last_write": recent_agg.isoformat() if recent_agg else None,
    }
    checks["lag_monitor"] = {
        "up": recent_lag is not None and (now - recent_lag) < timedelta(minutes=3),
        "last_sample": recent_lag.isoformat() if recent_lag else None,
    }

    overall = all(c.get("up") for k, c in checks.items() if k in ("postgres", "kafka"))
    return {"healthy": overall, "checks": checks, "checked_at": now.isoformat()}


@router.get("/overview")
async def overview(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    return await system_overview_data(db)


async def system_overview_data(db: AsyncSession) -> dict:
    now = datetime.now(UTC)
    since = now - timedelta(minutes=5)

    events_5m = int(
        await db.scalar(
            select(func.coalesce(func.sum(Aggregation.count), 0)).where(
                Aggregation.metric == "events_total",
                Aggregation.group_key == "_all",
                Aggregation.bucket_start >= since,
            )
        )
        or 0
    )
    latest_eps = float(
        await db.scalar(
            select(Aggregation.value)
            .where(Aggregation.metric == "events_per_sec", Aggregation.group_key == "_all")
            .order_by(Aggregation.bucket_start.desc())
            .limit(1)
        )
        or 0.0
    )
    active_producers = int(
        await db.scalar(
            select(func.count(Producer.id)).where(Producer.status == ProducerStatus.ACTIVE)
        )
        or 0
    )
    anomalies_24h = int(
        await db.scalar(
            select(func.count(AnomalyRecord.id)).where(
                AnomalyRecord.detected_at >= now - timedelta(hours=24)
            )
        )
        or 0
    )
    dlq_pending = int(
        await db.scalar(
            select(func.count(DeadLetterEvent.id)).where(DeadLetterEvent.status == "PENDING")
        )
        or 0
    )
    ai_running = int(
        await db.scalar(
            select(func.count(AIInvestigation.id)).where(
                AIInvestigation.status.in_(["RUNNING", "CREATED", "WAITING_FOR_TOOL"])
            )
        )
        or 0
    )

    # total lag from newest sample per group
    sub = (
        select(ConsumerLag.group_name, func.max(ConsumerLag.sampled_at).label("newest"))
        .group_by(ConsumerLag.group_name)
        .subquery()
    )
    total_lag = int(
        await db.scalar(
            select(func.coalesce(func.sum(ConsumerLag.lag), 0)).join(
                sub,
                (ConsumerLag.group_name == sub.c.group_name)
                & (ConsumerLag.sampled_at == sub.c.newest),
            )
        )
        or 0
    )
    total_events = int(await db.scalar(select(func.count(Event.id))) or 0)

    return {
        "events_per_sec": round(latest_eps, 3),
        "events_last_5m": events_5m,
        "events_stored_total": total_events,
        "active_producers": active_producers,
        "consumer_lag_total": total_lag,
        "anomalies_24h": anomalies_24h,
        "dlq_pending": dlq_pending,
        "ai_investigations_running": ai_running,
        "generated_at": now.isoformat(),
    }


@router.get("/topics")
async def topics(_: CurrentUser = Depends(require_viewer)):
    return {
        "topics": [
            {
                "name": t.full_name,
                "partitions": t.partitions,
                "purpose": t.purpose,
                "retention_ms": t.retention_ms,
            }
            for t in all_topic_specs()
        ]
    }
