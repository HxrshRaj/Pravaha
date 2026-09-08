from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.deps import (
    CurrentUser,
    Page,
    get_db,
    pagination,
    require_analyst,
    require_viewer,
)
from pravaha.api.errors import NotFoundError
from pravaha.api.schemas import PageMeta, Paginated
from pravaha.models import (
    Aggregation,
    AIInvestigation,
    AnomalyRecord,
    Event,
)

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


def _out(a: AnomalyRecord) -> dict:
    return {
        "id": a.id,
        "metric": a.metric,
        "group_key": a.group_key,
        "detected_at": a.detected_at.isoformat(),
        "window_start": a.window_start.isoformat(),
        "window_end": a.window_end.isoformat(),
        "observed_value": a.observed_value,
        "expected_value": a.expected_value,
        "deviation": a.deviation,
        "severity": a.severity,
        "algorithm": a.algorithm,
        "confidence": a.confidence,
        "baseline_kind": a.baseline_kind,
        "status": a.status,
        "evidence": a.evidence,
        "notes": a.notes,
    }


@router.get("", response_model=Paginated[dict])
async def list_anomalies(
    page: Page = Depends(pagination),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
    metric: str | None = Query(None),
    severity: str | None = Query(None),
    status: str | None = Query(None),
    since_minutes: int = Query(1440, ge=1, le=43200),
):
    start = datetime.now(UTC) - timedelta(minutes=since_minutes)
    q = select(AnomalyRecord).where(AnomalyRecord.detected_at >= start)
    cq = select(func.count(AnomalyRecord.id)).where(AnomalyRecord.detected_at >= start)
    for cond in (
        (AnomalyRecord.metric == metric) if metric else None,
        (AnomalyRecord.severity == severity) if severity else None,
        (AnomalyRecord.status == status) if status else None,
    ):
        if cond is not None:
            q = q.where(cond)
            cq = cq.where(cond)
    q = q.order_by(AnomalyRecord.detected_at.desc()).limit(page.limit).offset(page.offset)
    total = int(await db.scalar(cq) or 0)
    rows = list(await db.scalars(q))
    return Paginated[dict](
        items=[_out(a) for a in rows],
        meta=PageMeta(total=total, limit=page.limit, offset=page.offset, returned=len(rows)),
    )


@router.get("/{anomaly_id}")
async def get_one(
    anomaly_id: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    a = await db.get(AnomalyRecord, anomaly_id)
    if a is None:
        raise NotFoundError("anomaly not found")
    investigations = list(
        await db.scalars(
            select(AIInvestigation)
            .where(AIInvestigation.anomaly_id == anomaly_id)
            .order_by(AIInvestigation.created_at.desc())
        )
    )
    # metric series around the window
    series_rows = await db.execute(
        select(Aggregation.bucket_start, Aggregation.value)
        .where(
            Aggregation.metric == a.metric,
            Aggregation.group_key == a.group_key,
            Aggregation.bucket_start >= a.window_start - timedelta(minutes=45),
            Aggregation.bucket_start <= a.window_end + timedelta(minutes=15),
        )
        .order_by(Aggregation.bucket_start.asc())
    )
    return {
        **_out(a),
        "series": [{"t": bs.isoformat(), "value": float(v)} for bs, v in series_rows.all()],
        "investigations": [
            {
                "id": i.id,
                "status": i.status,
                "phase": i.phase,
                "root_cause": i.root_cause,
                "confidence": i.conclusion_confidence,
                "created_at": i.created_at.isoformat(),
            }
            for i in investigations
        ],
    }


@router.get("/{anomaly_id}/evidence")
async def evidence(
    anomaly_id: str, _: CurrentUser = Depends(require_analyst), db: AsyncSession = Depends(get_db)
):
    a = await db.get(AnomalyRecord, anomaly_id)
    if a is None:
        raise NotFoundError("anomaly not found")
    ev_rows = await db.scalars(
        select(Event)
        .where(
            Event.event_time >= a.window_start - timedelta(minutes=5),
            Event.event_time <= a.window_end + timedelta(minutes=2),
        )
        .order_by(Event.event_time.desc())
        .limit(100)
    )
    events = list(ev_rows)
    by_type: dict[str, int] = {}
    for e in events:
        by_type[e.event_type] = by_type.get(e.event_type, 0) + 1
    return {
        "anomaly_id": anomaly_id,
        "detector_evidence": a.evidence,
        "event_type_counts": by_type,
        "sample_events": [
            {
                "event_id": e.event_id,
                "event_type": e.event_type,
                "event_time": e.event_time.isoformat(),
                "correlation_id": e.correlation_id,
                "region": e.region,
                "reason": e.payload.get("reason"),
                "amount": e.amount,
            }
            for e in events[:40]
        ],
    }


@router.post("/{anomaly_id}/status")
async def set_status(
    anomaly_id: str,
    status: str = Query(..., pattern="^(OPEN|INVESTIGATING|RESOLVED|DISMISSED)$"),
    _: CurrentUser = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    a = await db.get(AnomalyRecord, anomaly_id)
    if a is None:
        raise NotFoundError("anomaly not found")
    a.status = status
    return {"ok": True, "id": anomaly_id, "status": status}
