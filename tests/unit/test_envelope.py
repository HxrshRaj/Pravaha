from datetime import UTC, datetime, timedelta

import pytest

from pravaha.events.envelope import (
    EventEnvelope,
    EventEnvelopeIn,
    LatenessClass,
)


def test_input_rejects_bad_event_type():
    with pytest.raises(ValueError):
        EventEnvelopeIn(event_type="NotNamespaced")
    with pytest.raises(ValueError):
        EventEnvelopeIn(event_type="a..b")


def test_input_normalises_event_type_case():
    e = EventEnvelopeIn(event_type="Payment.Failed")
    assert e.event_type == "payment.failed"


def test_from_input_fills_ids_and_times():
    raw = EventEnvelopeIn(event_type="order.created", payload={"amount": 10})
    env = EventEnvelope.from_input(raw, producer_name="svc", producer_id="p1")
    assert env.event_id
    # correlation/trace/partition default to event_id when absent
    assert env.correlation_id == env.event_id
    assert env.trace_id == env.correlation_id
    assert env.partition_key == env.correlation_id
    assert env.producer == "svc"
    assert env.producer_id == "p1"
    assert env.ingestion_time.tzinfo is not None


def test_from_input_accepts_epoch_millis_timestamp():
    ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
    raw = EventEnvelopeIn(event_type="user.login", timestamp=ts_ms)
    env = EventEnvelope.from_input(raw, producer_name="svc", producer_id=None)
    assert env.event_time == datetime(2026, 1, 1, tzinfo=UTC)


def test_future_event_time_rejected():
    raw = EventEnvelopeIn(
        event_type="user.login",
        event_time=(datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
    )
    with pytest.raises(ValueError):
        EventEnvelope.from_input(raw, producer_name="svc", producer_id=None)


def test_wire_roundtrip():
    raw = EventEnvelopeIn(event_type="payment.failed", payload={"reason": "x"})
    env = EventEnvelope.from_input(raw, producer_name="svc", producer_id="p1")
    again = EventEnvelope.from_wire(env.to_wire())
    assert again.event_id == env.event_id
    assert again.payload == {"reason": "x"}
    assert again.lateness_at_ingest == LatenessClass.ON_TIME
