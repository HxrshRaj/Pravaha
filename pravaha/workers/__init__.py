"""Worker process launcher.

Usage:  python -m pravaha.workers <worker> [--health-port N]

Workers:
  analytics        events.validated -> windowed aggregations (pravaha.analytics)
  persistence      events.validated -> events table          (pravaha.persistence)
  dataquality      events.validated/raw -> DQ scores          (pravaha.dataquality)
  anomaly          events.metrics -> anomalies + alerts       (pravaha.anomaly)
  ai               events.anomalies -> AI investigations      (pravaha.anomaly_ai)
  audit            events.audit -> audit_logs                 (pravaha.audit)
  lag-monitor      samples consumer lag for all groups
  replay-runner    executes queued replay jobs
  retention        periodic event-store cleanup

All workers expose a tiny aiohttp-free health server on ``--health-port`` (or
$WORKER_HEALTH_PORT) with /healthz, /readyz and /metrics.
"""
