from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select

from pravaha.api.deps import CurrentUser, require_viewer
from pravaha.db import session_scope
from pravaha.kafka.topics import ANOMALIES, VALIDATED
from pravaha.models import Alert
from pravaha.realtime.sse import sse_response, tail_topic

router = APIRouter(prefix="/live", tags=["realtime"])


@router.get("/events")
async def live_events(
    request: Request,
    _: CurrentUser = Depends(require_viewer),
    event_type: str | None = Query(None),
    producer: str | None = Query(None),
):
    async def gen() -> AsyncIterator[tuple[str, Any]]:
        async for msg in tail_topic(VALIDATED.name):
            v = msg["value"]
            if not isinstance(v, dict):
                continue
            if event_type and v.get("event_type") != event_type:
                continue
            if producer and v.get("producer") != producer:
                continue
            yield (
                "event",
                {
                    "event_id": v.get("event_id"),
                    "event_type": v.get("event_type"),
                    "producer": v.get("producer"),
                    "region": v.get("region"),
                    "event_time": v.get("event_time"),
                    "ingestion_time": v.get("ingestion_time"),
                    "correlation_id": v.get("correlation_id"),
                    "partition": msg["partition"],
                    "offset": msg["offset"],
                    "payload": v.get("payload", {}),
                },
            )

    return await sse_response(request, gen())


@router.get("/anomalies")
async def live_anomalies(request: Request, _: CurrentUser = Depends(require_viewer)):
    async def gen() -> AsyncIterator[tuple[str, Any]]:
        async for msg in tail_topic(ANOMALIES.name):
            if isinstance(msg["value"], dict):
                yield ("anomaly", msg["value"])

    return await sse_response(request, gen())


@router.get("/metrics")
async def live_metrics(
    request: Request,
    _: CurrentUser = Depends(require_viewer),
    interval: int = Query(2, ge=1, le=30),
):
    async def gen() -> AsyncIterator[tuple[str, Any]]:
        from pravaha.api.routers.system import system_overview_data

        while True:
            async with session_scope() as s:
                data = await system_overview_data(s)
            yield ("metrics", data)
            await asyncio.sleep(interval)

    return await sse_response(request, gen(), ping_seconds=max(interval + 3, 6))


@router.get("/alerts")
async def live_alerts(
    request: Request,
    _: CurrentUser = Depends(require_viewer),
    interval: int = Query(5, ge=2, le=60),
):
    seen: set[str] = set()

    async def gen() -> AsyncIterator[tuple[str, Any]]:
        nonlocal seen
        first = True
        while True:
            async with session_scope() as s:
                rows = list(
                    await s.scalars(
                        select(Alert)
                        .where(Alert.fired_at >= datetime.now(UTC) - timedelta(minutes=30))
                        .order_by(Alert.fired_at.desc())
                        .limit(50)
                    )
                )
            for a in rows:
                if a.id in seen:
                    continue
                seen.add(a.id)
                if first:
                    continue
                yield (
                    "alert",
                    {
                        "id": a.id,
                        "rule_name": a.rule_name,
                        "metric": a.metric,
                        "severity": a.severity,
                        "observed_value": a.observed_value,
                        "threshold": a.threshold,
                        "fired_at": a.fired_at.isoformat(),
                    },
                )
            first = False
            await asyncio.sleep(interval)

    return await sse_response(request, gen(), ping_seconds=max(interval + 3, 8))


@router.get("/system-health")
async def live_system_health(
    request: Request,
    _: CurrentUser = Depends(require_viewer),
    interval: int = Query(5, ge=2, le=60),
):
    async def gen() -> AsyncIterator[tuple[str, Any]]:
        from pravaha.api.routers.system import system_health_data

        while True:
            async with session_scope() as s:
                data = await system_health_data(s)
            yield ("system-health", data)
            await asyncio.sleep(interval)

    return await sse_response(request, gen(), ping_seconds=max(interval + 3, 8))
