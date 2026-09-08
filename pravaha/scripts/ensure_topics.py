"""Create all Pravaha Kafka topics (idempotent). Used by the compose kafka-init
service and available as ``python -m pravaha.scripts.ensure_topics``."""

from __future__ import annotations

import asyncio

from pravaha.kafka.topics import all_topic_specs, ensure_topics
from pravaha.logging import configure_logging, get_logger

log = get_logger("pravaha.scripts.ensure_topics")


async def _run() -> None:
    created = await ensure_topics()
    specs = {t.full_name: t.partitions for t in all_topic_specs()}
    log.info("ensure_topics.done", created=created, all_topics=specs)
    for name, parts in specs.items():
        print(f"  {name}  (partitions={parts})")


def main() -> None:
    configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
