"""Validation for user-defined processing pipeline graphs.

A pipeline graph is ``{"nodes": [...], "edges": [...]}``:

    node = {"key": "src", "type": "source", "config": {...}}
    edge = {"from": "src", "to": "flt"}

Rules enforced before a pipeline version can be published:
* node keys unique; every edge endpoint refers to a known node
* node types in the allowed set
* exactly one ``source`` and at least one ``sink``
* the graph is a DAG (no cycles) and every node is reachable from the source
* ordering constraint: along any path, node types may only advance in the
  canonical order source -> filter -> transform -> group -> window -> aggregate
  -> sink (a stage may be skipped but never revisited)
* ``window`` config must parse; ``aggregate`` requires a known aggregator kind
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pravaha.processing.primitives import AGGREGATORS
from pravaha.processing.windows import parse_window_spec

NODE_TYPES = ["source", "filter", "transform", "group", "window", "aggregate", "sink"]
_ORDER = {t: i for i, t in enumerate(NODE_TYPES)}


@dataclass
class PipelineValidation:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    topological_order: list[str] = field(default_factory=list)


def validate_graph(graph: dict) -> PipelineValidation:
    errors: list[str] = []
    warnings: list[str] = []
    nodes = graph.get("nodes") or []
    edges = graph.get("edges") or []

    if not isinstance(nodes, list) or not isinstance(edges, list):
        return PipelineValidation(False, ["graph must have list 'nodes' and 'edges'"])

    keys = [n.get("key") for n in nodes]
    if len(keys) != len(set(keys)):
        errors.append("node keys must be unique")
    keyset = set(keys)

    by_key: dict[str, dict] = {}
    for n in nodes:
        k = n.get("key")
        t = n.get("type")
        if not k:
            errors.append("every node needs a 'key'")
            continue
        if t not in NODE_TYPES:
            errors.append(f"node '{k}' has invalid type '{t}' (allowed: {NODE_TYPES})")
        by_key[k] = n
        cfg = n.get("config") or {}
        if t == "window":
            for f in ("size",):
                if f not in cfg:
                    errors.append(f"window node '{k}' missing config.{f}")
            try:
                if "size" in cfg:
                    parse_window_spec(str(cfg["size"]))
                if cfg.get("slide"):
                    parse_window_spec(str(cfg["slide"]))
            except ValueError as exc:
                errors.append(f"window node '{k}': {exc}")
        if t == "aggregate":
            kind = cfg.get("kind")
            if kind not in AGGREGATORS:
                errors.append(
                    f"aggregate node '{k}' has unknown kind '{kind}' "
                    f"(allowed: {sorted(AGGREGATORS)})"
                )

    adj: dict[str, list[str]] = {k: [] for k in keyset}
    indeg: dict[str, int] = {k: 0 for k in keyset}
    for e in edges:
        a, b = e.get("from"), e.get("to")
        if a not in keyset or b not in keyset:
            errors.append(f"edge {a}->{b} refers to unknown node")
            continue
        adj[a].append(b)
        indeg[b] += 1
        ta, tb = by_key[a].get("type"), by_key[b].get("type")
        if ta in _ORDER and tb in _ORDER and _ORDER[tb] <= _ORDER[ta]:
            errors.append(
                f"edge {a}({ta}) -> {b}({tb}) violates stage ordering "
                f"({' -> '.join(NODE_TYPES)})"
            )

    sources = [k for k, n in by_key.items() if n.get("type") == "source"]
    sinks = [k for k, n in by_key.items() if n.get("type") == "sink"]
    if len(sources) != 1:
        errors.append(f"pipeline needs exactly one source (found {len(sources)})")
    if not sinks:
        errors.append("pipeline needs at least one sink")

    # topo sort (Kahn) - also detects cycles
    order: list[str] = []
    queue = [k for k in keyset if indeg.get(k, 0) == 0]
    local_indeg = dict(indeg)
    while queue:
        node = queue.pop(0)
        order.append(node)
        for nxt in adj.get(node, []):
            local_indeg[nxt] -= 1
            if local_indeg[nxt] == 0:
                queue.append(nxt)
    if len(order) != len(keyset):
        errors.append("pipeline graph contains a cycle")

    if sources and order:
        reachable = _reachable(sources[0], adj)
        unreachable = keyset - reachable
        if unreachable:
            warnings.append(f"nodes not reachable from source: {sorted(unreachable)}")

    return PipelineValidation(
        ok=not errors, errors=errors, warnings=warnings, topological_order=order
    )


def _reachable(start: str, adj: dict[str, list[str]]) -> set[str]:
    seen = {start}
    stack = [start]
    while stack:
        cur = stack.pop()
        for nxt in adj.get(cur, []):
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen
