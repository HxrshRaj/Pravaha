"""A practical, single-process watermark.

Definition (Pravaha): the watermark is ``max_event_time_seen - allowed_lateness``.
It is a heuristic estimate of "we have probably seen every event with
event_time <= W". A window can be finalised once ``watermark >= window.end``.

Honest limitations (documented in docs/decisions/0005-event-time.md):
* This is a **per-consumer** watermark. With multiple partitions consumed by
  one process we take the min across partition watermarks; across *separate*
  processes there is no coordination, so each maintains its own and windows are
  closed per-processor.
* A partition that goes idle would stall the watermark; we advance it by wall
  clock after ``WATERMARK_IDLE_ADVANCE_SECONDS`` of no data (bounded idleness).
* Events later than the watermark by more than ``allowed_lateness`` are
  classified ``too_late`` and counted, not folded into closed windows.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pravaha.config import settings
from pravaha.events.envelope import LatenessClass


class Watermark:
    def __init__(
        self,
        allowed_lateness_seconds: int | None = None,
        idle_advance_seconds: int | None = None,
    ) -> None:
        self.allowed_lateness = timedelta(
            seconds=allowed_lateness_seconds
            if allowed_lateness_seconds is not None
            else settings.watermark_allowed_lateness_seconds
        )
        self.idle_advance = timedelta(
            seconds=idle_advance_seconds
            if idle_advance_seconds is not None
            else settings.watermark_idle_advance_seconds
        )
        self._max_event_time: datetime | None = None
        self._last_update_wall: datetime = datetime.now(UTC)

    @property
    def value(self) -> datetime | None:
        if self._max_event_time is None:
            return None
        base = self._max_event_time - self.allowed_lateness
        idle = datetime.now(UTC) - self._last_update_wall
        if idle > self.idle_advance:
            # advance by the excess idle time, bounded
            base = base + (idle - self.idle_advance)
        return base

    def observe(self, event_time: datetime) -> datetime | None:
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=UTC)
        if self._max_event_time is None or event_time > self._max_event_time:
            self._max_event_time = event_time
        self._last_update_wall = datetime.now(UTC)
        return self.value

    def classify(self, event_time: datetime, window_end: datetime) -> LatenessClass:
        """Classify an event relative to the window it targets.

        * ON_TIME       - watermark has not reached window_end (window still open)
        * ACCEPTED_LATE - window_end <= watermark < window_end + allowed_lateness
                          (window closed but still inside the grace period)
        * TOO_LATE      - watermark >= window_end + allowed_lateness (past grace)
        """
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=UTC)
        wm = self.value
        if wm is None or wm < window_end:
            return LatenessClass.ON_TIME
        if wm < window_end + self.allowed_lateness:
            return LatenessClass.ACCEPTED_LATE
        return LatenessClass.TOO_LATE

    def is_closed(self, window_end: datetime) -> bool:
        wm = self.value
        return wm is not None and wm >= window_end

    def snapshot(self) -> dict:
        return {
            "watermark": self.value.isoformat() if self.value else None,
            "max_event_time": self._max_event_time.isoformat() if self._max_event_time else None,
            "allowed_lateness_seconds": int(self.allowed_lateness.total_seconds()),
        }
