"""Threshold + trend alert rule evaluation.

A rule fires when its condition holds continuously for ``duration_seconds`` and
the rule is outside its ``cooldown_seconds`` window since it last fired
(anti-alert-storm). ``drop_pct`` / ``rise_pct`` compare the latest value to the
mean of the preceding ``duration_seconds`` of samples.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.logging import get_logger
from pravaha.models import Aggregation, Alert, AlertRule
from pravaha.observability.metrics import ALERTS_FIRED_TOTAL

log = get_logger(__name__)

_CMP = {
    "gt": lambda v, t: v > t,
    "gte": lambda v, t: v >= t,
    "lt": lambda v, t: v < t,
    "lte": lambda v, t: v <= t,
}


async def _recent(
    session: AsyncSession, metric: str, group_key: str, since: datetime
) -> list[tuple[datetime, float]]:
    rows = await session.execute(
        select(Aggregation.bucket_start, Aggregation.value)
        .where(
            Aggregation.metric == metric,
            Aggregation.group_key == group_key,
            Aggregation.bucket_start >= since,
        )
        .order_by(Aggregation.bucket_start.asc())
    )
    return [(bs, float(v)) for bs, v in rows.all()]


async def evaluate_single_rule(
    session: AsyncSession, rule: AlertRule, *, now: datetime | None = None
) -> Alert | None:
    now = now or datetime.now(UTC)
    if not rule.enabled:
        return None
    if rule.last_fired_at and (now - rule.last_fired_at) < timedelta(seconds=rule.cooldown_seconds):
        return None

    lookback = max(rule.duration_seconds, 60) + 120
    samples = await _recent(session, rule.metric, rule.group_key, now - timedelta(seconds=lookback))
    if not samples:
        return None

    latest_ts, latest_val = samples[-1]
    in_window = [v for ts, v in samples if ts >= now - timedelta(seconds=rule.duration_seconds)]
    if not in_window:
        in_window = [latest_val]

    fired = False
    observed = latest_val
    if rule.operator in _CMP:
        fired = all(_CMP[rule.operator](v, rule.threshold) for v in in_window)
        observed = latest_val
    elif rule.operator in ("drop_pct", "rise_pct"):
        prior = [v for ts, v in samples if ts < now - timedelta(seconds=rule.duration_seconds)]
        base = (sum(prior) / len(prior)) if prior else (in_window[0] if in_window else 0.0)
        if base > 0:
            pct = (latest_val - base) / base * 100.0
            observed = pct
            fired = pct <= -rule.threshold if rule.operator == "drop_pct" else pct >= rule.threshold

    if not fired:
        return None

    alert = Alert(
        rule_id=rule.id,
        rule_name=rule.name,
        metric=rule.metric,
        group_key=rule.group_key,
        severity=rule.severity,
        observed_value=round(observed, 6),
        threshold=rule.threshold,
        fired_at=now,
        status="FIRING",
        context={
            "operator": rule.operator,
            "duration_seconds": rule.duration_seconds,
            "window_samples": len(in_window),
            "latest_value": round(latest_val, 6),
        },
    )
    session.add(alert)
    rule.last_fired_at = now
    await session.flush()
    ALERTS_FIRED_TOTAL.labels(rule=rule.name, severity=rule.severity).inc()
    log.info(
        "alert.fired",
        rule=rule.name,
        metric=rule.metric,
        severity=rule.severity,
        observed=alert.observed_value,
        threshold=rule.threshold,
    )
    return alert


async def evaluate_rules(session: AsyncSession, *, now: datetime | None = None) -> list[Alert]:
    rules = list(await session.scalars(select(AlertRule).where(AlertRule.enabled.is_(True))))
    fired: list[Alert] = []
    for rule in rules:
        try:
            alert = await evaluate_single_rule(session, rule, now=now)
            if alert is not None:
                fired.append(alert)
        except Exception as exc:  # noqa: BLE001
            log.error("alert.rule_eval_failed", rule=rule.name, error=str(exc))
    return fired
