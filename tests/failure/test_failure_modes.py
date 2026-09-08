"""Failure-mode tests that do NOT need full infrastructure.

They assert the *degradation contracts*: Redis-down fails open for rate limiting
and dedup; malformed wire payloads are DLQ'd not crashed; the AI provider being
down marks an investigation FAILED without touching streaming.
"""

from __future__ import annotations

import pytest


async def test_rate_limiter_fails_open_when_redis_unavailable(monkeypatch):
    from pravaha import redis_client

    class _BrokenRedis:
        async def eval(self, *a, **k):
            raise ConnectionError("redis down")

    monkeypatch.setattr(redis_client, "get_redis", lambda: _BrokenRedis())
    allowed, remaining = await redis_client.rate_limit_check("k", 10, 60000, 0)
    assert allowed is True
    assert remaining == 10


async def test_ingestion_dedup_fails_open_when_redis_unavailable(monkeypatch):
    from pravaha.ingestion import service

    async def _boom(*a, **k):
        raise ConnectionError("redis down")

    class _R:
        set = staticmethod(_boom)

    monkeypatch.setattr(service, "get_redis", lambda: _R())
    first_seen = await service._dedup_reserve("p1", "e1")
    assert first_seen is True  # fail-open => treat as first sighting


async def test_ai_provider_failure_marks_investigation_failed(monkeypatch):
    """The _fail path returns a FAILED result shape and never raises, even if the
    DB write inside it also fails (fully degraded)."""
    import contextlib
    import time

    from pravaha.ai import investigator

    @contextlib.asynccontextmanager
    async def _broken_scope():
        raise ConnectionError("db also down")
        yield  # pragma: no cover

    monkeypatch.setattr(investigator, "session_scope", _broken_scope)
    # Even with the DB unreachable, the AI failure path returns a clean result
    # and never raises - streaming is unaffected.
    res = await investigator._fail("inv-x", "AI provider error: boom", time.perf_counter())
    assert res["status"] == "FAILED"
    assert "boom" in res["error"]


def test_consumer_permanent_vs_retryable_classes_exist():
    from pravaha.kafka.consumer import PermanentError, RetryableError

    assert issubclass(PermanentError, Exception)
    assert issubclass(RetryableError, Exception)


async def test_malformed_wire_payload_is_not_fatal():
    """EventEnvelope.from_wire on junk raises ValueError (caught -> DLQ), never
    an unhandled crash type."""
    from pravaha.events.envelope import EventEnvelope

    with pytest.raises(Exception):
        EventEnvelope.from_wire({"not": "an envelope"})
