"""Catalog mapping e-commerce events -> the real-time metrics they feed.

Each :class:`MetricDef` declares how one event contributes to one metric's
per-minute accumulator. The analytics processor walks this catalog for every
event; the flush step turns accumulators into final values (rates are
numerator/denominator pairs).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pravaha.events.envelope import EventEnvelope


@dataclass(frozen=True)
class MetricDef:
    name: str
    kind: str                       # count | sum | distinct | rate_num | rate_den | topn
    event_types: tuple[str, ...]
    value_field: str | None = None  # for sum
    distinct_field: str | None = None
    topn_field: str | None = None
    topn_weight_field: str | None = None
    group_by: tuple[str, ...] = ()   # extra group keys, e.g. ("region",)
    description: str = ""
    unit: str = ""


# denominators/numerators are combined by name suffix at flush time:
#   "<x>_rate" = "<x>_rate.num" / "<x>_rate.den"
METRIC_DEFS: list[MetricDef] = [
    MetricDef("events_total", "count", ("*",), description="All events", unit="events/min"),
    MetricDef(
        "events_by_type", "topn", ("*",), topn_field="event_type",
        description="Event volume by type", unit="events/min",
    ),
    MetricDef(
        "events_by_region", "topn", ("*",), topn_field="region",
        description="Event volume by region", unit="events/min",
    ),
    MetricDef("orders_created", "count", ("order.created",), unit="orders/min"),
    MetricDef("orders_confirmed", "count", ("order.confirmed",), unit="orders/min"),
    MetricDef("orders_cancelled", "count", ("order.cancelled",), unit="orders/min"),
    MetricDef(
        "order_cancellation_rate.num", "count", ("order.cancelled",), unit="ratio",
        description="Cancelled orders (numerator)",
    ),
    MetricDef(
        "order_cancellation_rate.den", "count", ("order.created",), unit="ratio",
        description="Created orders (denominator)",
    ),
    MetricDef(
        "revenue", "sum", ("payment.completed", "order.confirmed"), value_field="payload.amount",
        description="Revenue from completed payments / confirmed orders", unit="currency/min",
    ),
    MetricDef(
        "avg_order_value.num", "sum", ("payment.completed",), value_field="payload.amount",
    ),
    MetricDef("avg_order_value.den", "count", ("payment.completed",)),
    MetricDef("payment_attempts", "count", ("payment.initiated",), unit="attempts/min"),
    MetricDef("payments_completed", "count", ("payment.completed",), unit="payments/min"),
    MetricDef("payments_failed", "count", ("payment.failed",), unit="payments/min"),
    MetricDef("payment_failure_rate.num", "count", ("payment.failed",)),
    MetricDef(
        "payment_failure_rate.den", "count",
        ("payment.completed", "payment.failed"),
    ),
    MetricDef("payment_success_rate.num", "count", ("payment.completed",)),
    MetricDef(
        "payment_success_rate.den", "count", ("payment.completed", "payment.failed"),
    ),
    MetricDef(
        "top_payment_failure_reasons", "topn", ("payment.failed",), topn_field="payload.reason",
        description="Most common payment failure reasons",
    ),
    MetricDef(
        "active_users", "distinct", ("*",), distinct_field="payload.user_id",
        description="Distinct users seen", unit="users/min",
    ),
    MetricDef("logins", "count", ("user.login",), unit="logins/min"),
    MetricDef("logouts", "count", ("user.logout",), unit="logouts/min"),
    MetricDef("product_views", "count", ("product.viewed",), unit="views/min"),
    MetricDef("add_to_cart", "count", ("product.added_to_cart",), unit="events/min"),
    MetricDef("purchases", "count", ("product.purchased",), unit="purchases/min"),
    MetricDef("conversion_rate.num", "count", ("product.purchased",)),
    MetricDef("conversion_rate.den", "count", ("product.viewed",)),
    MetricDef(
        "top_products", "topn", ("product.purchased",), topn_field="payload.product_id",
        topn_weight_field="payload.amount", description="Top products by purchase value",
    ),
    MetricDef("inventory_reserved", "count", ("inventory.reserved",), unit="events/min"),
    MetricDef("inventory_released", "count", ("inventory.released",), unit="events/min"),
    MetricDef("shipments_created", "count", ("shipment.created",), unit="events/min"),
    MetricDef("shipments_delayed", "count", ("shipment.delayed",), unit="events/min"),
    MetricDef(
        "low_stock_events", "count", ("inventory.low_stock",), unit="events/min",
    ),
]

RATE_METRICS = sorted({d.name.rsplit(".", 1)[0] for d in METRIC_DEFS if d.name.endswith((".num", ".den"))})


def metric_names() -> list[str]:
    base = {d.name.rsplit(".", 1)[0] if d.name.endswith((".num", ".den")) else d.name for d in METRIC_DEFS}
    return sorted(base | {"events_per_sec", "avg_order_value"})


def _matches(defn: MetricDef, event_type: str) -> bool:
    return "*" in defn.event_types or event_type in defn.event_types


def classify_event(event: EventEnvelope) -> list[tuple[MetricDef, dict[str, Any]]]:
    """Return the metric defs an event contributes to, with resolved values."""
    out: list[tuple[MetricDef, dict[str, Any]]] = []
    for defn in METRIC_DEFS:
        if not _matches(defn, event.event_type):
            continue
        ctx: dict[str, Any] = {}
        if defn.kind == "sum" and defn.value_field:
            v = _resolve(event, defn.value_field)
            if not isinstance(v, (int, float)):
                continue
            ctx["value"] = float(v)
        elif defn.kind == "distinct" and defn.distinct_field:
            v = _resolve(event, defn.distinct_field)
            if v is None:
                continue
            ctx["member"] = str(v)
        elif defn.kind == "topn" and defn.topn_field:
            v = _resolve(event, defn.topn_field)
            if v is None:
                continue
            ctx["key"] = str(v)
            w = _resolve(event, defn.topn_weight_field) if defn.topn_weight_field else 1.0
            ctx["weight"] = float(w) if isinstance(w, (int, float)) else 1.0
        out.append((defn, ctx))
    return out


def _resolve(event: EventEnvelope, path: str) -> Any:
    cur: Any = event
    for part in path.split("."):
        cur = cur.get(part) if isinstance(cur, dict) else getattr(cur, part, None)
        if cur is None:
            return None
    return cur
