"""Shared async Redis client plus small helpers used across the platform.

Redis is used for: short-lived stream-processing state, real-time counters,
idempotency/dedup keys, distributed rate limiting and light coordination
(leader locks). Every call site must treat Redis as *best-effort*: if Redis is
unavailable the dependent feature degrades but the core Kafka pipeline keeps
running (see docs/decisions/0003-why-redis.md).
"""

from __future__ import annotations

import redis.asyncio as aioredis

from pravaha.config import settings
from pravaha.logging import get_logger

log = get_logger(__name__)

_client: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            health_check_interval=15,
            retry_on_timeout=True,
        )
    return _client


async def redis_healthy() -> bool:
    try:
        return bool(await get_redis().ping())
    except Exception as exc:  # noqa: BLE001
        log.warning("redis.health_check_failed", error=str(exc))
        return False


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


# --- Distributed sliding-window rate limiter -------------------------------------------------

_RATE_LIMIT_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, now .. '-' .. math.random())
    redis.call('PEXPIRE', key, window)
    return {1, limit - count - 1}
end
return {0, 0}
"""


async def rate_limit_check(
    key: str, limit: int, window_ms: int, now_ms: int
) -> tuple[bool, int]:
    """Return ``(allowed, remaining)``. Fails *open* if Redis is unavailable."""
    try:
        r = get_redis()
        allowed, remaining = await r.eval(
            _RATE_LIMIT_LUA, 1, key, now_ms, window_ms, limit
        )
        return bool(allowed), int(remaining)
    except Exception as exc:  # noqa: BLE001
        log.warning("ratelimit.redis_unavailable_fail_open", key=key, error=str(exc))
        return True, limit
