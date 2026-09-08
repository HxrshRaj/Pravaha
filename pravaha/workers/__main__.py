from __future__ import annotations

import argparse
import asyncio
import os
import sys

from pravaha.logging import configure_logging, get_logger

log = get_logger("pravaha.workers")


async def _run_consumer(consumer_cls) -> None:  # noqa: ANN001
    consumer = consumer_cls()
    await consumer.run()


async def _dispatch(name: str) -> None:
    if name == "analytics":
        from pravaha.analytics.processor import AnalyticsProcessor

        await _run_consumer(AnalyticsProcessor)
    elif name == "persistence":
        from pravaha.analytics.persistence import PersistenceProcessor

        await _run_consumer(PersistenceProcessor)
    elif name == "dataquality":
        from pravaha.dataquality.processor import DataQualityProcessor

        await _run_consumer(DataQualityProcessor)
    elif name == "anomaly":
        from pravaha.consumers.anomaly_worker import AnomalyDetectorProcessor

        await _run_consumer(AnomalyDetectorProcessor)
    elif name == "ai":
        from pravaha.consumers.ai_worker import AIIntelligenceProcessor

        await _run_consumer(AIIntelligenceProcessor)
    elif name == "audit":
        from pravaha.consumers.audit_worker import AuditProcessor

        await _run_consumer(AuditProcessor)
    elif name == "lag-monitor":
        from pravaha.consumers.lag_monitor import LagMonitor

        await LagMonitor().run()
    elif name == "replay-runner":
        from pravaha.replay.runner import ReplayRunner

        await ReplayRunner().run()
    elif name == "retention":
        from pravaha.retention import RetentionWorker

        await RetentionWorker().run()
    else:
        log.error("worker.unknown", name=name)
        sys.exit(2)


def main() -> None:
    parser = argparse.ArgumentParser(prog="pravaha.workers")
    parser.add_argument(
        "worker",
        choices=[
            "analytics",
            "persistence",
            "dataquality",
            "anomaly",
            "ai",
            "audit",
            "lag-monitor",
            "replay-runner",
            "retention",
        ],
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=int(os.getenv("WORKER_HEALTH_PORT", "0")) or None,
    )
    args = parser.parse_args()

    configure_logging()
    if args.health_port:
        from pravaha.workers.health import start_health_server

        start_health_server(args.health_port, args.worker)

    log.info("worker.starting", worker=args.worker)
    try:
        asyncio.run(_dispatch(args.worker))
    except KeyboardInterrupt:
        log.info("worker.interrupted", worker=args.worker)


if __name__ == "__main__":
    main()
