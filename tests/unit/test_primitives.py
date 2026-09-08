from pravaha.events.envelope import EventEnvelope, EventEnvelopeIn
from pravaha.processing.primitives import (
    AvgAggregator,
    CountAggregator,
    DistinctCountAggregator,
    Filter,
    RateAggregator,
    SumAggregator,
    TopNAggregator,
    Transform,
    build_aggregator,
)


def _ev(t: str, **payload) -> EventEnvelope:
    return EventEnvelope.from_input(
        EventEnvelopeIn(event_type=t, payload=payload), producer_name="p", producer_id="p1"
    )


def test_filter_all_and_any():
    e = _ev("payment.failed", amount=5, reason="card_declined")
    assert Filter([{"field": "event_type", "op": "eq", "value": "payment.failed"}])(e)
    assert not Filter([{"field": "payload.amount", "op": "gt", "value": 10}])(e)
    assert Filter(
        [
            {"field": "payload.amount", "op": "gt", "value": 10},
            {"field": "payload.reason", "op": "eq", "value": "card_declined"},
        ],
        match="any",
    )(e)


def test_transform_derives_metadata_without_touching_payload():
    e = _ev("order.created", amount=100, user_id="u1")
    out = Transform({"amt": "payload.amount", "kind": {"const": "order"}})(e)
    assert out.metadata["amt"] == 100
    assert out.metadata["kind"] == "order"
    assert out.payload == {"amount": 100, "user_id": "u1"}


def test_count_sum_avg():
    events = [_ev("x.y", amount=v) for v in (10, 20, 30)]
    c = CountAggregator()
    st = c.init_state()
    for e in events:
        st = c.update(st, e)
    assert c.finalize(st)["value"] == 3

    s = SumAggregator("payload.amount")
    st = s.init_state()
    for e in events:
        st = s.update(st, e)
    assert s.finalize(st)["value"] == 60

    a = AvgAggregator("payload.amount")
    st = a.init_state()
    for e in events:
        st = a.update(st, e)
    assert a.finalize(st)["value"] == 20


def test_rate_and_merge_associative():
    agg = RateAggregator("event_type", "eq", "payment.failed")
    a = agg.init_state()
    for _ in range(3):
        a = agg.update(a, _ev("payment.failed"))
    b = agg.init_state()
    b = agg.update(b, _ev("payment.completed"))
    merged = agg.merge(a, b)
    assert agg.finalize(merged) == {"value": 0.75, "count": 4, "matched": 3}


def test_distinct_and_topn():
    d = DistinctCountAggregator("payload.user_id")
    st = d.init_state()
    for u in ("a", "a", "b", "c"):
        st = d.update(st, _ev("x.y", user_id=u))
    assert d.finalize(st)["value"] == 3

    t = TopNAggregator("payload.product_id", n=2, weight_field="payload.amount")
    st = t.init_state()
    for pid, amt in [("p1", 10), ("p2", 5), ("p1", 20), ("p3", 1)]:
        st = t.update(st, _ev("product.purchased", product_id=pid, amount=amt))
    top = t.finalize(st)["top"]
    assert top[0]["key"] == "p1" and top[0]["score"] == 30


def test_build_aggregator_unknown():
    import pytest

    with pytest.raises(ValueError):
        build_aggregator("nope")
