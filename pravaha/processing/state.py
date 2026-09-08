"""State stores for stateful stream processing.

Two backends:
* :class:`RedisStateStore` - fast, ephemeral counters/sets used while a window
  is open (HLL-ish distinct via sets, hashes for sums/counts). Keys carry a TTL
  so a crashed processor cannot leak state forever.
* :class:`PgStateStore` - durable window results in the ``windows`` table; this
  is the source of truth used for recovery and for late-arriving updates.

State keys are deterministic: ``pravaha:state:{metric}:{window_identity}`` so any
processor instance computes the same key for the same logical window.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from pravaha.db import session_scope
from pravaha.models import WindowRecord
from pravaha.redis_client import get_redis


class StateStore(Protocol):
    async def get(self, key: str) -> dict[str, Any] | None: ...
    async def put(self, key: str, value: dict[str, Any]) -> None: ...
    async def incr_fields(self, key: str, fields: dict[str, float]) -> dict[str, float]: ...
    async def add_distinct(self, key: str, member: str) -> int: ...
    async def delete(self, key: str) -> None: ...


def state_key(metric: str, window_identity: str) -> str:
    return f"pravaha:state:{metric}:{window_identity}"


class RedisStateStore:
    def __init__(self, ttl_seconds: int = 3600) -> None:
        self._ttl = ttl_seconds

    async def get(self, key: str) -> dict[str, Any] | None:
        r = get_redis()
        raw = await r.hgetall(key)
        if not raw:
            return None
        return {k: _coerce(v) for k, v in raw.items()}

    async def put(self, key: str, value: dict[str, Any]) -> None:
        r = get_redis()
        mapping = {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in value.items()}
        async with r.pipeline(transaction=True) as pipe:
            pipe.delete(key)
            if mapping:
                pipe.hset(key, mapping=mapping)
            pipe.expire(key, self._ttl)
            await pipe.execute()

    async def incr_fields(self, key: str, fields: dict[str, float]) -> dict[str, float]:
        r = get_redis()
        async with r.pipeline(transaction=True) as pipe:
            for f, delta in fields.items():
                pipe.hincrbyfloat(key, f, delta)
            pipe.expire(key, self._ttl)
            res = await pipe.execute()
        return {f: float(res[i]) for i, f in enumerate(fields)}

    async def add_distinct(self, key: str, member: str) -> int:
        r = get_redis()
        dkey = f"{key}:distinct"
        async with r.pipeline(transaction=True) as pipe:
            pipe.sadd(dkey, member)
            pipe.scard(dkey)
            pipe.expire(dkey, self._ttl)
            res = await pipe.execute()
        return int(res[1])

    async def distinct_count(self, key: str) -> int:
        return int(await get_redis().scard(f"{key}:distinct"))

    async def topn_incr(self, key: str, member: str, delta: float = 1.0) -> None:
        r = get_redis()
        zkey = f"{key}:topn"
        async with r.pipeline(transaction=True) as pipe:
            pipe.zincrby(zkey, delta, member)
            pipe.expire(zkey, self._ttl)
            await pipe.execute()

    async def topn(self, key: str, n: int = 10) -> list[tuple[str, float]]:
        r = get_redis()
        rows = await r.zrevrange(f"{key}:topn", 0, n - 1, withscores=True)
        return [(m, float(s)) for m, s in rows]

    async def delete(self, key: str) -> None:
        r = get_redis()
        await r.delete(key, f"{key}:distinct", f"{key}:topn")


def _coerce(v: str) -> Any:
    try:
        return json.loads(v)
    except (ValueError, TypeError):
        try:
            return float(v) if "." in v or "e" in v.lower() else int(v)
        except (ValueError, TypeError):
            return v


class PgStateStore:
    """Durable upsert of window state/result into the ``windows`` table."""

    async def upsert_window(
        self,
        *,
        metric: str,
        window_type: str,
        size_seconds: int,
        slide_seconds: int | None,
        window_start: datetime,
        window_end: datetime,
        group_key: str,
        state: dict[str, Any],
        result: dict[str, Any],
        event_count: int,
        late_event_count: int,
        is_closed: bool,
        closed_at: datetime | None,
    ) -> None:
        stmt = (
            insert(WindowRecord)
            .values(
                metric=metric,
                window_type=window_type,
                window_size_seconds=size_seconds,
                window_slide_seconds=slide_seconds,
                window_start=window_start,
                window_end=window_end,
                group_key=group_key,
                state=state,
                result=result,
                event_count=event_count,
                late_event_count=late_event_count,
                is_closed=is_closed,
                closed_at=closed_at,
            )
            .on_conflict_do_update(
                constraint="uq_window_identity",
                set_={
                    "state": state,
                    "result": result,
                    "event_count": event_count,
                    "late_event_count": late_event_count,
                    "is_closed": is_closed,
                    "closed_at": closed_at,
                },
            )
        )
        async with session_scope() as s:
            await s.execute(stmt)

    async def load_open_windows(self, metric: str) -> list[WindowRecord]:
        async with session_scope() as s:
            rows = await s.scalars(
                select(WindowRecord).where(
                    WindowRecord.metric == metric, WindowRecord.is_closed.is_(False)
                )
            )
            return list(rows)
