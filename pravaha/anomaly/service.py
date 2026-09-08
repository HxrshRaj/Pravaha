"""Anomaly evaluation + persistence.

Pulls the recent time-series for a metric from the ``aggregations`` table, asks
:func:`choose_detector` for the right strategy, optionally blends in a *dynamic
baseline* (same-minute-of-hour historical average when enough history exists),
and persists a de-duplicated :class:`AnomalyRecord`, also emitting it to
``events.anomalies`` for the AI + realtime consumers.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.anomaly.detectors import AnomalyResult, choose_detector, severity_for
from pravaha.logging import get_logger
from pravaha.models import Aggregation, AnomalyRecord
from pravaha.observability.metrics import ANOMALIES_DETECTED_TOTAL

log = get_logger(__name__)

DEDUP_WINDOW = timedelta(minutes=10)


async def _series(
    session: AsyncSession, metric: str, group_key: str, points: int
) -> list[tuple[datetime, float]]:
    rows = await session.execute(
        select(Aggregation.bucket_start, Aggregation.value)
        .where(Aggregation.metric == metric, Aggregation.group_key == group_key)
        .order_by(Aggregation.bucket_start.desc())
        .limit(points)
    )
    data = [(bs, float(v)) for bs, v in rows.all()]
    return list(reversed(data))


async def _dynamic_baseline(
    session: AsyncSession, metric: str, group_key: str, target: datetime
) -> float | None:
    """Average of the same minute-of-hour over the last 7 days, if >= 5 samples."""
    minute = target.minute
    since = target - timedelta(days=7)
    rows = await session.execute(
        select(Aggregation.value, Aggregation.bucket_start).where(
            Aggregation.metric == metric,
            Aggregation.group_key == group_key,
            Aggregation.bucket_start >= since,
            Aggregation.bucket_start < target - timedelta(minutes=1),
        )
    )
    same_minute = [float(v) for v, bs in rows.all() if bs.minute == minute]
    if len(same_minute) < 5:
        return None
    return sum(same_minute) / len(same_minute)


async def evaluate_metric(
    session: AsyncSession,
    *,
    metric: str,
    group_key: str = "_all",
    at: datetime | None = None,
    history_points: int = 90,
) -> tuple[AnomalyResult, AnomalyRecord | None]:
    at = at or datetime.now(UTC)
    series = await _series(session, metric, group_key, history_points + 1)
    if not series:
        detector = choose_detector(metric)
        return detector.detect([]), None

    times = [t for t, _ in series]
    values = [v for _, v in series]
    current = values[-1]
    window_start = times[-1]
    window_end = window_start + timedelta(minutes=1)

    detector = choose_detector(metric)
    result = detector.detect(values[:-1], current=current)

    baseline_kind = result.baseline_kind
    dyn = await _dynamic_baseline(session, metric, group_key, window_start)
    if dyn is not None:
        # Blend: expected is the mean of rolling + historical; recompute deviation.
        blended_expected = (result.expected_value + dyn) / 2.0
        result.evidence["historical_same_minute_avg"] = round(dyn, 6)
        result.evidence["blended_expected"] = round(blended_expected, 6)
        result.expected_value = round(blended_expected, 6)
        result.deviation = round(current - blended_expected, 6)
        baseline_kind = "historical+rolling"

    if not result.is_anomaly:
        return result, None

    severity = severity_for(result.score)
    dedup_key = f"{metric}|{group_key}|{severity}|{int(window_start.timestamp() // 600)}"

    existing = await session.scalar(
        select(AnomalyRecord.id).where(AnomalyRecord.dedup_key == dedup_key)
    )
    if existing:
        log.debug("anomaly.deduped", metric=metric, dedup_key=dedup_key)
        return result, None

    record_values = dict(
        metric=metric,
        group_key=group_key,
        detected_at=at,
        window_start=window_start,
        window_end=window_end,
        observed_value=result.observed_value,
        expected_value=result.expected_value,
        deviation=result.deviation,
        severity=severity,
        algorithm=result.algorithm,
        confidence=result.confidence,
        baseline_kind=baseline_kind,
        evidence=result.evidence,
        status="OPEN",
        dedup_key=dedup_key,
    )
    stmt = (
        insert(AnomalyRecord)
        .values(**record_values)
        .on_conflict_do_nothing(index_elements=["dedup_key"])
        .returning(AnomalyRecord.id)
    )
    new_id = await session.scalar(stmt)
    if new_id is None:
        return result, None

    record = await session.get(AnomalyRecord, new_id)
    ANOMALIES_DETECTED_TOTAL.labels(
        metric=metric, algorithm=result.algorithm, severity=severity
    ).inc()
    log.info(
        "anomaly.detected",
        metric=metric,
        group_key=group_key,
        severity=severity,
        observed=result.observed_value,
        expected=result.expected_value,
        algorithm=result.algorithm,
        anomaly_id=new_id,
    )
    return result, record
