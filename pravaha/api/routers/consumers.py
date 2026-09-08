from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.deps import CurrentUser, get_db, require_viewer
from pravaha.models import ConsumerGroup, ConsumerInstance, ConsumerLag

router = APIRouter(prefix="/consumers", tags=["consumers"])


@router.get("/groups")
async def groups(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    rows = list(await db.scalars(select(ConsumerGroup).order_by(ConsumerGroup.name.asc())))
    now = datetime.now(UTC)
    out = []
    for g in rows:
        latest = await db.execute(
            select(
                ConsumerLag.topic,
                ConsumerLag.partition,
                ConsumerLag.lag,
                ConsumerLag.current_offset,
                ConsumerLag.log_end_offset,
                ConsumerLag.sampled_at,
            )
            .where(ConsumerLag.group_name == g.name)
            .order_by(ConsumerLag.sampled_at.desc())
            .limit(64)
        )
        parts = latest.all()
        # keep only the most recent sample timestamp
        if parts:
            newest_ts = max(p.sampled_at for p in parts)
            parts = [p for p in parts if p.sampled_at == newest_ts]
        total_lag = sum(int(p.lag) for p in parts)
        instances = list(
            await db.scalars(
                select(ConsumerInstance).where(
                    ConsumerInstance.group_name == g.name,
                    ConsumerInstance.last_seen >= now - timedelta(minutes=2),
                )
            )
        )
        out.append(
            {
                "name": g.name,
                "topics": g.topics.split(",") if g.topics else [],
                "total_lag": total_lag,
                "partitions": [
                    {
                        "topic": p.topic,
                        "partition": p.partition,
                        "lag": int(p.lag),
                        "current_offset": int(p.current_offset),
                        "log_end_offset": int(p.log_end_offset),
                    }
                    for p in sorted(parts, key=lambda x: (x.topic, x.partition))
                ],
                "active_instances": len(instances),
                "instances": [
                    {
                        "member_id": i.member_id,
                        "host": i.host,
                        "state": i.state,
                        "events_processed": i.events_processed,
                        "events_failed": i.events_failed,
                        "p95_latency_ms": i.processing_latency_ms_p95,
                        "paused": i.paused,
                        "last_seen": i.last_seen.isoformat(),
                    }
                    for i in instances
                ],
            }
        )
    return out


@router.get("/lag")
async def lag_series(
    group_name: str = Query(...),
    minutes: int = Query(60, ge=1, le=1440),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    start = datetime.now(UTC) - timedelta(minutes=minutes)
    rows = await db.execute(
        select(ConsumerLag.sampled_at, func.sum(ConsumerLag.lag))
        .where(ConsumerLag.group_name == group_name, ConsumerLag.sampled_at >= start)
        .group_by(ConsumerLag.sampled_at)
        .order_by(ConsumerLag.sampled_at.asc())
    )
    return {
        "group_name": group_name,
        "window_minutes": minutes,
        "points": [{"t": ts.isoformat(), "lag": int(lag or 0)} for ts, lag in rows.all()],
    }


@router.get("/lag/overview")
async def lag_overview(
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    sub = (
        select(
            ConsumerLag.group_name,
            func.max(ConsumerLag.sampled_at).label("newest"),
        )
        .group_by(ConsumerLag.group_name)
        .subquery()
    )
    rows = await db.execute(
        select(ConsumerLag.group_name, func.sum(ConsumerLag.lag))
        .join(
            sub,
            (ConsumerLag.group_name == sub.c.group_name)
            & (ConsumerLag.sampled_at == sub.c.newest),
        )
        .group_by(ConsumerLag.group_name)
    )
    return {"groups": [{"group_name": g, "lag": int(lag or 0)} for g, lag in rows.all()]}
