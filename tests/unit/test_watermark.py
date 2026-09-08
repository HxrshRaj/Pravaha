from datetime import UTC, datetime

from pravaha.events.envelope import LatenessClass
from pravaha.processing.watermark import Watermark


def test_watermark_lags_max_event_time_by_allowed_lateness():
    wm = Watermark(allowed_lateness_seconds=30, idle_advance_seconds=9999)
    wm.observe(datetime(2026, 9, 8, 3, 20, 0, tzinfo=UTC))
    assert wm.value == datetime(2026, 9, 8, 3, 19, 30, tzinfo=UTC)
    # out-of-order older event does not move it back
    wm.observe(datetime(2026, 9, 8, 3, 19, 0, tzinfo=UTC))
    assert wm.value == datetime(2026, 9, 8, 3, 19, 30, tzinfo=UTC)


def test_window_closed_when_watermark_passes_end():
    wm = Watermark(allowed_lateness_seconds=10, idle_advance_seconds=9999)
    win_end = datetime(2026, 9, 8, 3, 5, 0, tzinfo=UTC)
    wm.observe(datetime(2026, 9, 8, 3, 5, 5, tzinfo=UTC))  # wm = 3:04:55
    assert not wm.is_closed(win_end)
    wm.observe(datetime(2026, 9, 8, 3, 5, 15, tzinfo=UTC))  # wm = 3:05:05
    assert wm.is_closed(win_end)


def test_lateness_classification():
    wm = Watermark(allowed_lateness_seconds=30, idle_advance_seconds=9999)
    win_end = datetime(2026, 9, 8, 3, 10, 0, tzinfo=UTC)

    # event within the still-open window
    wm.observe(datetime(2026, 9, 8, 3, 9, 50, tzinfo=UTC))
    assert wm.classify(datetime(2026, 9, 8, 3, 9, 55, tzinfo=UTC), win_end) == LatenessClass.ON_TIME

    # advance watermark well past window end -> a very old event is too late
    wm.observe(datetime(2026, 9, 8, 3, 12, 0, tzinfo=UTC))  # wm = 3:11:30
    assert wm.classify(datetime(2026, 9, 8, 3, 9, 0, tzinfo=UTC), win_end) == LatenessClass.TOO_LATE
