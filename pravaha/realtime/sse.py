"""Server-Sent Events helpers.

* :func:`sse_response` wraps an async generator of ``(event, data)`` tuples into
  a ``text/event-stream`` response with periodic heartbeats and clean client
  disconnect handling.
* :func:`tail_topic` yields freshly-arriving messages from a Kafka topic using a
  throwaway consumer group per connection (so every client sees the live tail,
  never a shared/committed offset), with a bounded internal queue that drops the
  oldest events under backpressure rather than growing unbounded.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import orjson
from aiokafka import AIOKafkaConsumer
from starlette.requests import Request
from starlette.responses import StreamingResponse

from pravaha.config import settings
from pravaha.logging import get_logger

log = get_logger(__name__)

HEARTBEAT_SECONDS = 15
MAX_QUEUE = 500


async def sse_response(
    request: Request,
    source: AsyncIterator[tuple[str, Any]],
    *,
    ping_seconds: int = HEARTBEAT_SECONDS,
) -> StreamingResponse:
    async def _stream() -> AsyncIterator[bytes]:
        yield b": connected\n\n"
        queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE)
        dropped = 0

        async def _pump() -> None:
            nonlocal dropped
            try:
                async for item in source:
                    if queue.full():
                        with contextlib.suppress(asyncio.QueueEmpty):
                            queue.get_nowait()
                            dropped += 1
                    await queue.put(item)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await queue.put(("error", {"message": str(exc)}))

        pump = asyncio.create_task(_pump())
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event, data = await asyncio.wait_for(queue.get(), timeout=ping_seconds)
                except TimeoutError:
                    yield f": ping {dropped}\n\n".encode()
                    continue
                payload = (
                    data if isinstance(data, str) else orjson.dumps(data).decode()
                )
                yield f"event: {event}\ndata: {payload}\n\n".encode()
        finally:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def tail_topic(
    topic: str, *, from_latest: bool = True
) -> AsyncIterator[dict]:
    consumer = AIOKafkaConsumer(
        settings.topic(topic),
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"{settings.kafka_client_id}-sse-{uuid.uuid4().hex[:8]}",
        enable_auto_commit=False,
        auto_offset_reset="latest" if from_latest else "earliest",
        value_deserializer=lambda b: _loads(b),
        max_poll_records=100,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            yield {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key.decode() if msg.key else None,
                "value": msg.value,
            }
    finally:
        with contextlib.suppress(Exception):
            await consumer.stop()


def _loads(b: bytes) -> Any:
    try:
        return orjson.loads(b)
    except (orjson.JSONDecodeError, TypeError):
        try:
            return json.loads(b.decode())
        except Exception:  # noqa: BLE001
            return {"_raw": b.decode(errors="replace")}
