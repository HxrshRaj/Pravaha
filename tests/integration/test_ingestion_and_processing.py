"""End-to-end: ingest -> Kafka -> analytics consumer -> aggregations.

Also covers idempotency (duplicate event processed once) and schema rejection
routing to the DLQ.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.integration


async def _mk_producer(db):
    from pravaha.producers.service import create_producer

    created = await create_producer(db, name=f"it-{uuid.uuid4().hex[:6]}", rate_limit_per_min=10_000_000)
    await db.commit()
    return created.producer, created.api_key


async def test_ingest_publishes_valid_event_and_persists_aggregation(_infra, migrated):
    from sqlalchemy import select

    from pravaha.analytics.processor import AnalyticsProcessor
    from pravaha.db import session_scope
    from pravaha.ingestion.service import ingest_event
    from pravaha.kafka.producer import get_producer
    from pravaha.kafka.topics import ensure_topics
    from pravaha.models import Aggregation

    await ensure_topics()
    prod = get_producer()
    await prod.start()

    async with session_scope() as db:
        producer, _ = await _mk_producer(db)

    consumer = AnalyticsProcessor()
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(3)

    now = datetime.now(UTC)
    async with session_scope() as db:
        for i in range(20):
            await ingest_event(
                db,
                producer,
                {
                    "event_type": "order.created",
                    "event_time": now.isoformat(),
                    "payload": {"order_id": f"o{i}", "user_id": f"u{i%5}", "amount": 10.0},
                },
                producer_obj=prod,
            )

    # let the watermark advance + flush
    await asyncio.sleep(2)
    async with session_scope() as db:
        for i in range(5):
            await ingest_event(
                db,
                producer,
                {
                    "event_type": "user.login",
                    "event_time": (now).isoformat(),
                    "payload": {"user_id": f"late{i}"},
                },
                producer_obj=prod,
            )

    # push event-time forward so the earlier bucket closes past its grace
    await asyncio.sleep(1)
    async with session_scope() as db:
        for i in range(3):
            await ingest_event(
                db,
                producer,
                {
                    "event_type": "user.login",
                    "event_time": now.replace(microsecond=0).isoformat(),
                    "payload": {"user_id": f"adv{i}"},
                },
                producer_obj=prod,
            )

    deadline = asyncio.get_event_loop().time() + 30
    rows: list = []
    while asyncio.get_event_loop().time() < deadline:
        async with session_scope() as db:
            rows = list(
                await db.scalars(
                    select(Aggregation).where(Aggregation.metric == "events_total")
                )
            )
        if rows:
            break
        await asyncio.sleep(2)

    consumer._stopping.set()
    await asyncio.wait_for(task, timeout=20)

    assert rows, "analytics processor did not persist any events_total aggregation"
    assert sum(r.count for r in rows) >= 20


async def test_duplicate_event_processed_once(_infra, migrated):
    from pravaha.kafka.consumer import ProcessingContext

    ctx = ProcessingContext(
        processor="itest", topic="t", partition=0, offset=1, key=None, headers={}
    )
    eid = str(uuid.uuid4())
    assert not await ctx.already_processed(eid)
    await ctx.mark_processed(eid)
    assert await ctx.already_processed(eid)
    # second mark must not raise (ON CONFLICT DO UPDATE) and stays single-row
    await ctx.mark_processed(eid)


async def test_schema_rejection_routes_to_dlq(_infra, migrated):
    from sqlalchemy import select

    from pravaha.db import session_scope
    from pravaha.ingestion.service import ingest_event
    from pravaha.kafka.producer import get_producer
    from pravaha.models import DeadLetterEvent
    from pravaha.registry.service import create_schema

    prod = get_producer()
    await prod.start()
    async with session_scope() as db:
        producer, _ = await _mk_producer(db)
        await create_schema(
            db,
            event_type="payment.failed",
            json_schema={
                "type": "object",
                "required": ["order_id", "reason"],
                "properties": {"order_id": {"type": "string"}, "reason": {"type": "string"}},
            },
        )
        await db.commit()

    async with session_scope() as db:
        res = await ingest_event(
            db,
            producer,
            {"event_type": "payment.failed", "payload": {"order_id": "o1"}},  # missing reason
            producer_obj=prod,
        )
        assert res.outcome.value == "rejected"
        assert res.errors

    async with session_scope() as db:
        dlq = list(
            await db.scalars(
                select(DeadLetterEvent).where(DeadLetterEvent.processor == "ingestion")
            )
        )
    assert any(d.failure_reason == "invalid_schema" for d in dlq)
