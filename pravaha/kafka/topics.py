"""Kafka topic topology for Pravaha.

Every topic here has a documented purpose. Partition counts are sized for local
development (single broker, replication factor 1). See
docs/architecture/kafka-topology.md for the rationale and ordering guarantees.

Ordering: Kafka only guarantees order **within a partition**. Pravaha uses the
event ``partition_key`` (correlation_id / order_id / user_id / producer key) so
that all events of one business transaction land on the same partition and are
therefore processed in order. There is **no global ordering** across partitions
and Pravaha never claims one.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pravaha.config import settings
from pravaha.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    purpose: str
    retention_ms: int = 7 * 24 * 3600 * 1000
    config: dict[str, str] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        return settings.topic(self.name)


_P = settings.kafka_default_partitions

RAW = TopicSpec(
    "events.raw",
    _P,
    "Every event accepted by ingestion before schema validation. Lets us replay "
    "and audit exactly what producers sent, including events that later fail "
    "validation downstream of the synchronous API check.",
)
VALIDATED = TopicSpec(
    "events.validated",
    _P,
    "Events that passed schema + envelope validation. This is the main topic all "
    "business processors (analytics, persistence, data-quality, anomaly feed) "
    "consume from, each as an independent consumer group.",
)
ANOMALIES = TopicSpec(
    "events.anomalies",
    3,
    "Anomaly records emitted by the anomaly detector. Consumed by the AI "
    "intelligence processor and the real-time API fan-out.",
    retention_ms=30 * 24 * 3600 * 1000,
)
DLQ = TopicSpec(
    "events.dlq",
    3,
    "Dead-letter topic. Events that exhausted their retry budget in a processor, "
    "plus failure metadata. Also mirrored into the dead_letter_events table.",
    retention_ms=30 * 24 * 3600 * 1000,
)
REPLAY = TopicSpec(
    "events.replay",
    _P,
    "Target for the replay engine. Replayed events carry is_replay=true and a "
    "replay_job_id so consumers can choose to skip side effects.",
)
AUDIT = TopicSpec(
    "events.audit",
    3,
    "Platform audit events (producer created, schema published, replay started, "
    "DLQ retried, ...). Consumed by the audit processor into audit_logs.",
    retention_ms=90 * 24 * 3600 * 1000,
)
METRICS = TopicSpec(
    "events.metrics",
    3,
    "Windowed metric samples emitted by the analytics processor, fanned out to "
    "the anomaly detector and the live dashboard.",
)

TOPICS: dict[str, TopicSpec] = {
    t.name: t for t in [RAW, VALIDATED, ANOMALIES, DLQ, REPLAY, AUDIT, METRICS]
}


def all_topic_specs() -> list[TopicSpec]:
    return list(TOPICS.values())


async def ensure_topics(bootstrap_servers: str | None = None) -> list[str]:
    """Create any missing topics. Idempotent; safe to call on every boot."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    servers = bootstrap_servers or settings.kafka_bootstrap_servers
    admin = AIOKafkaAdminClient(bootstrap_servers=servers, client_id=f"{settings.kafka_client_id}-admin")
    created: list[str] = []
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        new_topics = []
        for spec in all_topic_specs():
            if spec.full_name in existing:
                continue
            new_topics.append(
                NewTopic(
                    name=spec.full_name,
                    num_partitions=spec.partitions,
                    replication_factor=settings.kafka_default_replication,
                    topic_configs={
                        "retention.ms": str(spec.retention_ms),
                        "cleanup.policy": "delete",
                        **spec.config,
                    },
                )
            )
        if new_topics:
            try:
                await admin.create_topics(new_topics)
                created = [t.name for t in new_topics]
                log.info("kafka.topics_created", topics=created)
            except TopicAlreadyExistsError:
                pass
    finally:
        await admin.close()
    return created
