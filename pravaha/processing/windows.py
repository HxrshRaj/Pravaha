"""Window assignment for event-time stream processing.

Supports **tumbling**, **sliding** and **session** windows. Windowers are pure
functions of an event's *event_time* -> the set of window intervals it belongs
to. Finalisation (closing a window) is driven by the :class:`Watermark`, not by
these classes.

All window boundaries are aligned to the Unix epoch so that independent
processors/restarts compute identical window identities for the same event.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_SPEC_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$", re.IGNORECASE)


def parse_window_spec(spec: str) -> int:
    """'5m' -> 300, '1h' -> 3600."""
    m = _SPEC_RE.match(spec)
    if not m:
        raise ValueError(f"bad window spec: {spec!r} (use e.g. '30s', '5m', '1h', '1d')")
    return int(m.group(1)) * _UNIT_SECONDS[m.group(2).lower()]


def _epoch(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.timestamp()


def _from_epoch(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=UTC)


@dataclass(frozen=True)
class WindowAssignment:
    window_type: str
    size_seconds: int
    slide_seconds: int | None
    start: datetime
    end: datetime
    group_key: str = "_all"

    @property
    def identity(self) -> str:
        slide = self.slide_seconds if self.slide_seconds is not None else self.size_seconds
        return (
            f"{self.window_type}:{self.size_seconds}:{slide}:"
            f"{int(self.start.timestamp())}:{self.group_key}"
        )


class Windower:
    window_type = "base"

    def assign(self, event_time: datetime, group_key: str = "_all") -> list[WindowAssignment]:
        raise NotImplementedError


class TumblingWindower(Windower):
    window_type = "tumbling"

    def __init__(self, size: str | int) -> None:
        self.size = parse_window_spec(size) if isinstance(size, str) else int(size)
        if self.size <= 0:
            raise ValueError("window size must be > 0")

    def assign(self, event_time: datetime, group_key: str = "_all") -> list[WindowAssignment]:
        ts = _epoch(event_time)
        start = (ts // self.size) * self.size
        return [
            WindowAssignment(
                self.window_type,
                self.size,
                None,
                _from_epoch(start),
                _from_epoch(start + self.size),
                group_key,
            )
        ]


class SlidingWindower(Windower):
    window_type = "sliding"

    def __init__(self, size: str | int, slide: str | int) -> None:
        self.size = parse_window_spec(size) if isinstance(size, str) else int(size)
        self.slide = parse_window_spec(slide) if isinstance(slide, str) else int(slide)
        if self.size <= 0 or self.slide <= 0:
            raise ValueError("size and slide must be > 0")
        if self.slide > self.size:
            raise ValueError("slide must be <= size")

    def assign(self, event_time: datetime, group_key: str = "_all") -> list[WindowAssignment]:
        ts = _epoch(event_time)
        # The most recent window start <= ts, then step back by slide while the
        # window still covers ts.
        last_start = (ts // self.slide) * self.slide
        out: list[WindowAssignment] = []
        start = last_start
        while start + self.size > ts and start >= last_start - self.size:
            if start <= ts < start + self.size:
                out.append(
                    WindowAssignment(
                        self.window_type,
                        self.size,
                        self.slide,
                        _from_epoch(start),
                        _from_epoch(start + self.size),
                        group_key,
                    )
                )
            start -= self.slide
        return sorted(out, key=lambda w: w.start)


class SessionWindower(Windower):
    """Session windows: caller must merge overlapping sessions in state.

    ``assign`` returns a provisional single-event window ``[t, t+gap)``; the
    session processor is responsible for merging assignments whose intervals
    touch (that logic lives in the analytics processor's state handling).
    """

    window_type = "session"

    def __init__(self, gap: str | int) -> None:
        self.gap = parse_window_spec(gap) if isinstance(gap, str) else int(gap)
        if self.gap <= 0:
            raise ValueError("gap must be > 0")

    def assign(self, event_time: datetime, group_key: str = "_all") -> list[WindowAssignment]:
        start = _epoch(event_time)
        return [
            WindowAssignment(
                self.window_type,
                self.gap,
                None,
                _from_epoch(start),
                _from_epoch(start + self.gap),
                group_key,
            )
        ]

    @staticmethod
    def merge(a: WindowAssignment, b: WindowAssignment) -> WindowAssignment | None:
        if a.group_key != b.group_key:
            return None
        if a.start <= b.end and b.start <= a.end:
            start = min(a.start, b.start)
            end = max(a.end, b.end)
            return WindowAssignment(
                "session", int((end - start).total_seconds()), None, start, end, a.group_key
            )
        return None


def build_windower(window_type: str, **cfg: object) -> Windower:
    wt = window_type.lower()
    if wt == "tumbling":
        return TumblingWindower(cfg["size"])  # type: ignore[arg-type]
    if wt == "sliding":
        return SlidingWindower(cfg["size"], cfg["slide"])  # type: ignore[arg-type]
    if wt == "session":
        return SessionWindower(cfg["gap"])  # type: ignore[arg-type]
    raise ValueError(f"unknown window type: {window_type}")


def window_grace_deadline(w: WindowAssignment, allowed_lateness_seconds: int) -> datetime:
    return w.end + timedelta(seconds=allowed_lateness_seconds)
