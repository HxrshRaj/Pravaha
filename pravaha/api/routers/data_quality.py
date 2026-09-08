from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.deps import CurrentUser, get_db, require_viewer
from pravaha.models import DataQualityRecord

router = APIRouter(prefix="/data-quality", tags=["data-quality"])

_SCORE_COLS = (
    "validity_score", "completeness_score", "uniqueness_score",
    "timeliness_score", "schema_compliance_score", "overall_score",
)


@router.get("/overview")
async def overview(
    minutes: int = Query(60, ge=1, le=1440),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    start = datetime.now(UTC) - timedelta(minutes=minutes)
    rows = await db.execute(
        select(
            DataQualityRecord.producer_name,
            func.avg(DataQualityRecord.overall_score),
            func.min(DataQualityRecord.overall_score),
            func.sum(DataQualityRecord.total_events),
            func.sum(DataQualityRecord.invalid_schema),
            func.sum(DataQualityRecord.malformed_payload),
            func.sum(DataQualityRecord.missing_fields),
            func.sum(DataQualityRecord.duplicate_events),
            func.sum(DataQualityRecord.future_timestamp),
            func.sum(DataQualityRecord.late_events),
        )
        .where(DataQualityRecord.bucket_start >= start)
        .group_by(DataQualityRecord.producer_name)
    )
    producers = [
        {
            "producer": name,
            "avg_overall_score": round(float(avg or 1.0), 4),
            "min_overall_score": round(float(mn or 1.0), 4),
            "total_events": int(tot or 0),
            "invalid_schema": int(inv or 0),
            "malformed_payload": int(mal or 0),
            "missing_fields": int(miss or 0),
            "duplicate_events": int(dup or 0),
            "future_timestamp": int(fut or 0),
            "late_events": int(late or 0),
        }
        for name, avg, mn, tot, inv, mal, miss, dup, fut, late in rows.all()
    ]
    platform_avg = (
        round(sum(p["avg_overall_score"] for p in producers) / len(producers), 4)
        if producers
        else 1.0
    )
    return {
        "window_minutes": minutes,
        "platform_avg_score": platform_avg,
        "producers": sorted(producers, key=lambda p: p["min_overall_score"]),
    }


@router.get("/timeseries")
async def timeseries(
    producer_name: str | None = Query(None),
    minutes: int = Query(120, ge=1, le=1440),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    start = datetime.now(UTC) - timedelta(minutes=minutes)
    cols = [DataQualityRecord.bucket_start] + [
        func.avg(getattr(DataQualityRecord, c)).label(c) for c in _SCORE_COLS
    ]
    q = select(*cols).where(DataQualityRecord.bucket_start >= start)
    if producer_name:
        q = q.where(DataQualityRecord.producer_name == producer_name)
    q = q.group_by(DataQualityRecord.bucket_start).order_by(DataQualityRecord.bucket_start.asc())
    rows = await db.execute(q)
    points = []
    for row in rows.all():
        d = row._mapping
        points.append(
            {
                "t": d[DataQualityRecord.bucket_start].isoformat(),
                **{c: round(float(d[c] or 1.0), 4) for c in _SCORE_COLS},
            }
        )
    return {"producer_name": producer_name, "window_minutes": minutes, "points": points}
