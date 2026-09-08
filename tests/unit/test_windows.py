from datetime import UTC, datetime

import pytest

from pravaha.processing.windows import (
    SessionWindower,
    SlidingWindower,
    TumblingWindower,
    parse_window_spec,
)


@pytest.mark.parametrize(
    "spec,seconds", [("30s", 30), ("5m", 300), ("1h", 3600), ("1d", 86400), ("2 m", 120)]
)
def test_parse_window_spec(spec, seconds):
    assert parse_window_spec(spec) == seconds


def test_parse_window_spec_bad():
    with pytest.raises(ValueError):
        parse_window_spec("5x")


def test_tumbling_alignment_to_epoch():
    t = datetime(2026, 9, 8, 3, 17, 42, 500000, tzinfo=UTC)
    w = TumblingWindower("1m").assign(t)
    assert len(w) == 1
    assert w[0].start == datetime(2026, 9, 8, 3, 17, 0, tzinfo=UTC)
    assert w[0].end == datetime(2026, 9, 8, 3, 18, 0, tzinfo=UTC)
    # deterministic identity regardless of instance
    w2 = TumblingWindower(60).assign(t)
    assert w2[0].identity == w[0].identity


def test_sliding_membership_count():
    t = datetime(2026, 9, 8, 3, 17, 42, tzinfo=UTC)
    ws = SlidingWindower("5m", "1m").assign(t)
    assert len(ws) == 5
    for w in ws:
        assert w.start <= t < w.end
        assert (w.end - w.start).total_seconds() == 300


def test_sliding_rejects_slide_gt_size():
    with pytest.raises(ValueError):
        SlidingWindower("1m", "5m")


def test_session_merge():
    a = SessionWindower("30s").assign(datetime(2026, 9, 8, 3, 0, 0, tzinfo=UTC))[0]
    b = SessionWindower("30s").assign(datetime(2026, 9, 8, 3, 0, 20, tzinfo=UTC))[0]
    merged = SessionWindower.merge(a, b)
    assert merged is not None
    assert merged.start == a.start
    assert merged.end == b.end

    c = SessionWindower("30s").assign(datetime(2026, 9, 8, 3, 5, 0, tzinfo=UTC))[0]
    assert SessionWindower.merge(a, c) is None
