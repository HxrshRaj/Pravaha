"""AI intelligence processor.

Consumer group ``pravaha.anomaly_ai``. Consumes ``events.anomalies`` and, for
each anomaly of severity >= MEDIUM (configurable), creates and runs an AI
investigation. Rate-limited per hour (``AI_INVESTIGATION_RATE_LIMIT_PER_HOUR``)
via Redis so a burst of anomalies cannot run up provider cost. If the AI
provider is unavailable the investigation is marked FAILED and processing
continues -- the anomaly itself is already persisted by the detector.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pravaha.ai.investigator import create_investigation, run_investigation
from pravaha.config import settings
from pravaha.kafka.consumer import ProcessingContext, StreamConsumer
from pravaha.logging import get_logger
from pravaha.redis_client import get_redis

log = get_logger(__name__)

_SEVERITY_MIN = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
_TRIGGER_AT = _SEVERITY_MIN["MEDIUM"]


class AIIntelligenceProcessor(StreamConsumer):
    group_id = "pravaha.anomaly_ai"
    processor = "anomaly_ai"
    topics = ["events.anomalies"]
    envelope = False  # events.anomalies carries plain anomaly dicts

    async def handle(self, message: dict, ctx: ProcessingContext) -> None:
        payload = message if isinstance(message, dict) else {}
        anomaly_id = payload.get("anomaly_id") or payload.get("id")
        severity = str(payload.get("severity", "LOW")).upper()
        if not anomaly_id:
            log.warning("ai_worker.no_anomaly_id", payload_keys=list(payload.keys()))
            return
        if _SEVERITY_MIN.get(severity, 0) < _TRIGGER_AT:
            log.info("ai_worker.skip_low_severity", anomaly_id=anomaly_id, severity=severity)
            return
        if not await self._rate_ok():
            log.warning("ai_worker.rate_limited", anomaly_id=anomaly_id)
            return

        try:
            inv_id = await create_investigation(anomaly_id, trigger="anomaly")
        except Exception as exc:  # noqa: BLE001
            log.error("ai_worker.create_failed", anomaly_id=anomaly_id, error=str(exc))
            return

        result = await run_investigation(inv_id)
        log.info(
            "ai_worker.investigation_done",
            anomaly_id=anomaly_id,
            investigation_id=inv_id,
            status=result.get("status"),
            root_cause=result.get("root_cause"),
        )

    async def _rate_ok(self) -> bool:
        try:
            r = get_redis()
            bucket = datetime.now(UTC).strftime("%Y%m%d%H")
            key = f"pravaha:ai:invrate:{bucket}"
            n = await r.incr(key)
            if n == 1:
                await r.expire(key, 3700)
            return n <= settings.ai_investigation_rate_limit_per_hour
        except Exception:  # noqa: BLE001
            return True


async def main() -> None:
    from pravaha.logging import configure_logging

    configure_logging()
    await AIIntelligenceProcessor().run()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
