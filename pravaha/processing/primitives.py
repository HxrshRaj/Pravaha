"""Composable stream-processing primitives.

These are deliberately small and pure so pipelines can be assembled from config
(see :mod:`pravaha.pipelines`). A pipeline is:

    Source -> Filter -> Transform -> Group -> Window -> Aggregate -> Sink

``Filter`` / ``Transform`` operate on single events. Aggregators are
incremental: ``update(state, event)`` then ``finalize(state)``. State is a plain
dict so it round-trips through Redis/Postgres.
"""

from __future__ import annotations

import operator
from collections.abc import Callable
from typing import Any

from pravaha.events.envelope import EventEnvelope

_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": operator.eq,
    "ne": operator.ne,
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
    "in": lambda a, b: a in b,
    "not_in": lambda a, b: a not in b,
    "contains": lambda a, b: b in (a or ""),
    "exists": lambda a, _b: a is not None,
}


def _resolve(event: EventEnvelope, path: str) -> Any:
    """Dot-path lookup over the envelope + payload, e.g. 'payload.amount'."""
    cur: Any = event
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            cur = getattr(cur, part, None)
        if cur is None:
            return None
    return cur


class Filter:
    """Keep events where all clauses match. Clause: ``{field, op, value}``."""

    def __init__(self, clauses: list[dict[str, Any]] | None = None, match: str = "all") -> None:
        self.clauses = clauses or []
        self.match = match

    def __call__(self, event: EventEnvelope) -> bool:
        if not self.clauses:
            return True
        results = []
        for c in self.clauses:
            op = _OPS.get(c["op"])
            if op is None:
                raise ValueError(f"unknown filter op: {c['op']}")
            results.append(bool(op(_resolve(event, c["field"]), c.get("value"))))
        return all(results) if self.match == "all" else any(results)


class Transform:
    """Derive/rename fields into ``event.metadata`` without mutating payload.

    ``spec`` maps output metadata key -> source dot-path or literal ``{"const": x}``.
    """

    def __init__(self, spec: dict[str, Any] | None = None) -> None:
        self.spec = spec or {}

    def __call__(self, event: EventEnvelope) -> EventEnvelope:
        if not self.spec:
            return event
        derived = dict(event.metadata)
        for out_key, src in self.spec.items():
            if isinstance(src, dict) and "const" in src:
                derived[out_key] = src["const"]
            else:
                derived[out_key] = _resolve(event, str(src))
        return event.model_copy(update={"metadata": derived})


class Aggregator:
    name = "base"

    def init_state(self) -> dict[str, Any]:
        return {}

    def update(self, state: dict[str, Any], event: EventEnvelope) -> dict[str, Any]:
        raise NotImplementedError

    def merge(self, a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def finalize(self, state: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class CountAggregator(Aggregator):
    name = "count"

    def init_state(self) -> dict[str, Any]:
        return {"count": 0}

    def update(self, state, event):  # noqa: ANN001
        state["count"] = state.get("count", 0) + 1
        return state

    def merge(self, a, b):  # noqa: ANN001
        return {"count": a.get("count", 0) + b.get("count", 0)}

    def finalize(self, state):  # noqa: ANN001
        return {"value": float(state.get("count", 0)), "count": int(state.get("count", 0))}


class SumAggregator(Aggregator):
    name = "sum"

    def __init__(self, field: str = "payload.amount") -> None:
        self.field = field

    def init_state(self):
        return {"sum": 0.0, "count": 0}

    def update(self, state, event):  # noqa: ANN001
        v = _resolve(event, self.field)
        if isinstance(v, (int, float)):
            state["sum"] = state.get("sum", 0.0) + float(v)
            state["count"] = state.get("count", 0) + 1
        return state

    def merge(self, a, b):  # noqa: ANN001
        return {"sum": a.get("sum", 0.0) + b.get("sum", 0.0), "count": a.get("count", 0) + b.get("count", 0)}

    def finalize(self, state):  # noqa: ANN001
        return {"value": float(state.get("sum", 0.0)), "count": int(state.get("count", 0))}


class AvgAggregator(SumAggregator):
    name = "avg"

    def finalize(self, state):  # noqa: ANN001
        c = int(state.get("count", 0))
        return {"value": (float(state.get("sum", 0.0)) / c) if c else 0.0, "count": c}


class RateAggregator(Aggregator):
    """Fraction of events matching a predicate (e.g. payment failure rate)."""

    name = "rate"

    def __init__(self, field: str, op: str, value: Any) -> None:
        self.field, self.op, self.value = field, op, value

    def init_state(self):
        return {"matched": 0, "total": 0}

    def update(self, state, event):  # noqa: ANN001
        state["total"] = state.get("total", 0) + 1
        fn = _OPS[self.op]
        if fn(_resolve(event, self.field), self.value):
            state["matched"] = state.get("matched", 0) + 1
        return state

    def merge(self, a, b):  # noqa: ANN001
        return {
            "matched": a.get("matched", 0) + b.get("matched", 0),
            "total": a.get("total", 0) + b.get("total", 0),
        }

    def finalize(self, state):  # noqa: ANN001
        t = int(state.get("total", 0))
        m = int(state.get("matched", 0))
        return {"value": (m / t) if t else 0.0, "count": t, "matched": m}


class DistinctCountAggregator(Aggregator):
    name = "distinct_count"

    def __init__(self, field: str = "payload.user_id") -> None:
        self.field = field

    def init_state(self):
        return {"members": []}

    def update(self, state, event):  # noqa: ANN001
        v = _resolve(event, self.field)
        if v is not None:
            members = set(state.get("members", []))
            members.add(str(v))
            state["members"] = list(members)
        return state

    def merge(self, a, b):  # noqa: ANN001
        return {"members": list(set(a.get("members", [])) | set(b.get("members", [])))}

    def finalize(self, state):  # noqa: ANN001
        return {"value": float(len(state.get("members", []))), "count": len(state.get("members", []))}


class TopNAggregator(Aggregator):
    name = "topn"

    def __init__(self, field: str = "payload.product_id", n: int = 10, weight_field: str | None = None) -> None:
        self.field, self.n, self.weight_field = field, n, weight_field

    def init_state(self):
        return {"counts": {}}

    def update(self, state, event):  # noqa: ANN001
        key = _resolve(event, self.field)
        if key is None:
            return state
        w = 1.0
        if self.weight_field:
            wv = _resolve(event, self.weight_field)
            w = float(wv) if isinstance(wv, (int, float)) else 1.0
        counts = state.setdefault("counts", {})
        counts[str(key)] = counts.get(str(key), 0.0) + w
        return state

    def merge(self, a, b):  # noqa: ANN001
        out = dict(a.get("counts", {}))
        for k, v in b.get("counts", {}).items():
            out[k] = out.get(k, 0.0) + v
        return {"counts": out}

    def finalize(self, state):  # noqa: ANN001
        counts = state.get("counts", {})
        top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: self.n]
        return {
            "value": float(len(counts)),
            "count": int(sum(counts.values())),
            "top": [{"key": k, "score": v} for k, v in top],
        }


AGGREGATORS: dict[str, type[Aggregator]] = {
    a.name: a
    for a in [
        CountAggregator,
        SumAggregator,
        AvgAggregator,
        RateAggregator,
        DistinctCountAggregator,
        TopNAggregator,
    ]
}


def build_aggregator(kind: str, **cfg: Any) -> Aggregator:
    cls = AGGREGATORS.get(kind)
    if cls is None:
        raise ValueError(f"unknown aggregator: {kind}")
    return cls(**cfg)
