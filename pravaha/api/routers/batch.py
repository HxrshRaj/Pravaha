from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.deps import CurrentUser, Page, get_db, pagination, require_viewer
from pravaha.api.schemas import PageMeta, Paginated
from pravaha.models import BatchEventRollup

router = APIRouter(prefix="/batch", tags=["batch"])


def _out(r: BatchEventRollup) -> dict:
    return {
        "id": r.id,
        "event_type": r.event_type,
        "region": r.region,
        "hour_bucket": r.hour_bucket.isoformat(),
        "event_count": r.event_count,
        "distinct_correlation_ids": r.distinct_correlation_ids,
        "distinct_users": r.distinct_users,
        "total_amount": r.total_amount,
        "avg_amount": r.avg_amount,
        "late_event_count": r.late_event_count,
        "replay_event_count": r.replay_event_count,
        "window_from": r.window_from.isoformat(),
        "window_to": r.window_to.isoformat(),
        "computed_at": r.computed_at.isoformat(),
    }


@router.get("/rollups", response_model=Paginated[dict])
async def list_rollups(
    page: Page = Depends(pagination),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
    event_type: str | None = Query(None),
    region: str | None = Query(None),
    hours: int = Query(72, ge=1, le=24 * 30, description="only rollups whose hour_bucket falls in the last N hours"),
):
    since = datetime.now(UTC) - timedelta(hours=hours)
    q = select(BatchEventRollup).where(BatchEventRollup.hour_bucket >= since)
    cq = select(func.count(BatchEventRollup.id)).where(BatchEventRollup.hour_bucket >= since)
    if event_type:
        q = q.where(BatchEventRollup.event_type == event_type)
        cq = cq.where(BatchEventRollup.event_type == event_type)
    if region:
        q = q.where(BatchEventRollup.region == region)
        cq = cq.where(BatchEventRollup.region == region)

    order_col = BatchEventRollup.hour_bucket if page.sort in (None, "hour_bucket") else getattr(
        BatchEventRollup, page.sort, BatchEventRollup.hour_bucket
    )
    q = q.order_by(order_col.desc() if page.order == "desc" else order_col.asc())
    q = q.order_by(BatchEventRollup.event_type.asc(), BatchEventRollup.region.asc())
    q = q.limit(page.limit).offset(page.offset)

    total = int(await db.scalar(cq) or 0)
    rows = list(await db.scalars(q))
    return Paginated[dict](
        items=[_out(r) for r in rows],
        meta=PageMeta(total=total, limit=page.limit, offset=page.offset, returned=len(rows)),
    )


@router.get("/rollups/summary")
async def rollups_summary(
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
    hours: int = Query(72, ge=1, le=24 * 30),
):
    """Header-strip numbers for the dashboard: what the last batch run(s)
    covering the requested window actually computed, straight from
    batch_event_rollups - nothing here is computed by the API itself."""
    since = datetime.now(UTC) - timedelta(hours=hours)
    base = select(BatchEventRollup).where(BatchEventRollup.hour_bucket >= since).subquery()

    row = (
        await db.execute(
            select(
                func.count(base.c.id),
                func.coalesce(func.sum(base.c.event_count), 0),
                func.coalesce(func.sum(base.c.total_amount), 0.0),
                func.coalesce(func.sum(base.c.late_event_count), 0),
                func.coalesce(func.sum(base.c.replay_event_count), 0),
                func.max(base.c.computed_at),
                func.min(base.c.hour_bucket),
                func.max(base.c.hour_bucket),
            )
        )
    ).one()
    (
        rollup_rows,
        total_events,
        total_amount,
        late_events,
        replay_events,
        last_computed_at,
        earliest_hour,
        latest_hour,
    ) = row

    top_event_types = (
        await db.execute(
            select(base.c.event_type, func.sum(base.c.event_count).label("n"))
            .group_by(base.c.event_type)
            .order_by(func.sum(base.c.event_count).desc())
            .limit(10)
        )
    ).all()

    return {
        "window_hours": hours,
        "rollup_rows": int(rollup_rows or 0),
        "total_events": int(total_events or 0),
        "total_amount": float(total_amount or 0.0),
        "late_events": int(late_events or 0),
        "replay_events": int(replay_events or 0),
        "last_computed_at": last_computed_at.isoformat() if last_computed_at else None,
        "earliest_hour_bucket": earliest_hour.isoformat() if earliest_hour else None,
        "latest_hour_bucket": latest_hour.isoformat() if latest_hour else None,
        "top_event_types": [{"event_type": t, "event_count": int(n)} for t, n in top_event_types],
    }
