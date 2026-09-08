"""Anomaly detector + alert evaluator.

Consumer group ``pravaha.anomaly``. Consumes ``events.metrics`` (window samples
emitted by the analytics processor). For each closed window it:

* runs :func:`evaluate_metric` for the configured watch-list of metrics,
* publishes any newly-created :class:`AnomalyRecord` to ``events.anomalies``
  (which the AI intelligence processor consumes),
* evaluates all enabled alert rules and emits fired alerts to ``events.audit``.

Independent consumer group => independent offsets, so a slow AI stage never
back-pressures detection.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pravaha.alerting.engine import evaluate_rules
from pravaha.anomaly.service import evaluate_metric
from pravaha.db import session_scope
from pravaha.kafka.consumer import ProcessingContext, StreamConsumer
from pravaha.kafka.producer import get_producer
from pravaha.kafka.topics import ANOMALIES, AUDIT
from pravaha.logging import get_logger

log = get_logger(__name__)

WATCH_METRICS = [
    "events_per_sec",
    "events_total",
    "payment_failure_rate",
    "payment_success_rate",
    "payments_failed",
    "orders_created",
    "orders_cancelled",
    "order_cancellation_rate",
    "revenue",
    "avg_order_value",
    "active_users",
    "conversion_rate",
    "inventory_reserved",
]


class AnomalyDetectorProcessor(StreamConsumer):
    group_id = "pravaha.anomaly"
    processor = "anomaly"
    topics = ["events.metrics"]
    envelope = False  # events.metrics carries plain window-sample dicts

    async def handle(self, message: dict, ctx: ProcessingContext) -> None:
        raw = message if isinstance(message, dict) else {}
        metrics = raw.get("metrics") or {}
        window_start = raw.get("window_start")
        detected_at = datetime.now(UTC)
        producer = get_producer()

        watch = [m for m in WATCH_METRICS if m in metrics] or list(metrics.keys())
        async with session_scope() as s:
            for metric in watch:
                try:
                    result, record = await evaluate_metric(
                        s, metric=metric, group_key="_all", at=detected_at
                    )
                except Exception as exc:  # noqa: BLE001
                    log.error("anomaly.eval_failed", metric=metric, error=str(exc))
                    continue
                if record is None:
                    continue
                await s.flush()
                await producer.publish(
                    ANOMALIES.name,
                    {
                        "anomaly_id": record.id,
                        "metric": record.metric,
                        "group_key": record.group_key,
                        "severity": record.severity,
                        "observed_value": record.observed_value,
                        "expected_value": record.expected_value,
                        "deviation": record.deviation,
                        "algorithm": record.algorithm,
                        "confidence": record.confidence,
                        "window_start": record.window_start.isoformat(),
                        "window_end": record.window_end.isoformat(),
                        "detected_at": record.detected_at.isoformat(),
                        "evidence": record.evidence,
                    },
                    key=record.metric,
                )
                log.info(
                    "anomaly.published",
                    anomaly_id=record.id,
                    metric=record.metric,
                    severity=record.severity,
                )

            fired = await evaluate_rules(s, now=detected_at)
            for alert in fired:
                await s.flush()
                await producer.publish(
                    AUDIT.name,
                    {
                        "kind": "alert.fired",
                        "alert_id": alert.id,
                        "rule_name": alert.rule_name,
                        "metric": alert.metric,
                        "severity": alert.severity,
                        "observed_value": alert.observed_value,
                        "threshold": alert.threshold,
                        "fired_at": alert.fired_at.isoformat(),
                    },
                    key=alert.rule_name,
                )

        _ = window_start  # (kept for future window-scoped evaluation)


async def main() -> None:
    from pravaha.logging import configure_logging

    configure_logging()
    await AnomalyDetectorProcessor().run()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
