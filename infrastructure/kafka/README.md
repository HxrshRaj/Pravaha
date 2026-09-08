# Kafka (local)

Single-broker **KRaft** (no ZooKeeper), `apache/kafka:3.9.0`, defined in
`docker-compose.yml` service `kafka`.

- Internal listener: `kafka:9092` (used by every app container).
- External listener: `localhost:9094` (host tools).
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` — topics are created explicitly by the
  `kafka-init` one-shot service (`python -m pravaha.scripts.ensure_topics`) and, redundantly,
  by the API on startup.

Topic definitions (name, partitions, retention, purpose) live in
[`pravaha/kafka/topics.py`](../../pravaha/kafka/topics.py) — that module is the single source
of truth. See [`docs/architecture/README.md`](../../docs/architecture/README.md) for the
topology diagram and ordering guarantees.

Inspect from the host:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events.validated --from-beginning --max-messages 5
```
