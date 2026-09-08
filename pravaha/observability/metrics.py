"""Prometheus metric definitions, shared process-wide.

Exposed at ``GET /metrics`` on the API and on each worker's health server.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

_LATENCY_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)

# --- Ingestion ---
EVENTS_INGESTED_TOTAL = Counter(
    "pravaha_events_ingested_total", "Events accepted by ingestion", ["producer", "event_type"]
)
EVENTS_REJECTED_TOTAL = Counter(
    "pravaha_events_rejected_total", "Events rejected by ingestion", ["reason"]
)
INGEST_LATENCY = Histogram(
    "pravaha_ingest_latency_seconds", "Ingestion request handling latency", buckets=_LATENCY_BUCKETS
)

# --- Kafka ---
KAFKA_PUBLISH_TOTAL = Counter(
    "pravaha_kafka_publish_total", "Kafka publish attempts", ["topic", "result"]
)
KAFKA_PUBLISH_LATENCY = Histogram(
    "pravaha_kafka_publish_latency_seconds", "Kafka send_and_wait latency", ["topic"],
    buckets=_LATENCY_BUCKETS,
)

# --- Processing ---
EVENTS_PROCESSED_TOTAL = Counter(
    "pravaha_events_processed_total", "Events processed", ["processor", "result"]
)
EVENTS_FAILED_TOTAL = Counter(
    "pravaha_events_failed_total", "Event processing failures", ["processor", "error_class"]
)
EVENTS_DLQ_TOTAL = Counter(
    "pravaha_events_dlq_total", "Events routed to the DLQ", ["processor", "reason"]
)
EVENT_PROCESSING_LATENCY = Histogram(
    "pravaha_event_processing_latency_seconds", "Per-event processing latency", ["processor"],
    buckets=_LATENCY_BUCKETS,
)
LATE_EVENTS_TOTAL = Counter(
    "pravaha_late_events_total", "Late events seen by windowed processors", ["metric", "klass"]
)
CONSUMER_LAG = Gauge(
    "pravaha_consumer_lag", "Consumer lag (records)", ["group", "topic", "partition"]
)
CONSUMER_INFLIGHT = Gauge(
    "pravaha_consumer_inflight", "In-flight events per consumer (backpressure)", ["processor"]
)
CONSUMER_PAUSED = Gauge(
    "pravaha_consumer_paused", "1 when a consumer has paused fetching for backpressure", ["processor"]
)

# --- Windows / pipelines ---
WINDOWS_CLOSED_TOTAL = Counter(
    "pravaha_windows_closed_total", "Windows finalised", ["metric", "window_type"]
)
PIPELINE_THROUGHPUT = Gauge(
    "pravaha_pipeline_throughput_eps", "Pipeline throughput (events/sec)", ["pipeline"]
)

# --- Anomaly / AI ---
ANOMALIES_DETECTED_TOTAL = Counter(
    "pravaha_anomalies_detected_total", "Anomalies detected", ["metric", "algorithm", "severity"]
)
ALERTS_FIRED_TOTAL = Counter("pravaha_alerts_fired_total", "Alerts fired", ["rule", "severity"])
AI_INVESTIGATIONS_TOTAL = Counter(
    "pravaha_ai_investigations_total", "AI investigations", ["result"]
)
AI_LATENCY = Histogram(
    "pravaha_ai_latency_seconds", "AI provider call latency", ["provider", "operation"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 45),
)
AI_TOKENS_TOTAL = Counter(
    "pravaha_ai_tokens_total", "AI tokens consumed", ["provider", "model", "kind"]
)

# --- Dependencies ---
DB_LATENCY = Histogram(
    "pravaha_db_operation_latency_seconds", "DB operation latency", ["operation"],
    buckets=_LATENCY_BUCKETS,
)
DEP_UP = Gauge("pravaha_dependency_up", "1 if a dependency healthed OK", ["dependency"])

# --- HTTP ---
HTTP_REQUESTS_TOTAL = Counter(
    "pravaha_http_requests_total", "HTTP requests", ["method", "path", "status"]
)
HTTP_LATENCY = Histogram(
    "pravaha_http_request_latency_seconds", "HTTP request latency", ["method", "path"],
    buckets=_LATENCY_BUCKETS,
)
