"""Data-quality engine.

Two responsibilities:

1. :func:`assess_envelope` - synchronous, per-event checks run at ingestion time
   (missing fields, bad/future timestamps, unknown type, malformed payload).
   Schema validity and duplication are supplied by the caller (registry + Redis
   dedup) because they need I/O.
2. :func:`score_from_counts` - turns per-minute aggregate counts into the five
   sub-scores + overall Data Quality Score used on dashboards.
"""

from __future__ import annotations

import enum
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

FUTURE_TOLERANCE = timedelta(minutes=5)
STALE_TOLERANCE = timedelta(days=3)


class QualityFlag(str, enum.Enum):
    MISSING_FIELDS = "missing_fields"
    INVALID_SCHEMA = "invalid_schema"
    INVALID_TIMESTAMP = "invalid_timestamp"
    FUTURE_TIMESTAMP = "future_timestamp"
    DUPLICATE = "duplicate_events"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    MALFORMED_PAYLOAD = "malformed_payload"
    LATE_EVENT = "late_events"


@dataclass
class QualityFinding:
    flags: set[QualityFlag] = field(default_factory=set)
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        blocking = {
            QualityFlag.MISSING_FIELDS,
            QualityFlag.INVALID_SCHEMA,
            QualityFlag.INVALID_TIMESTAMP,
            QualityFlag.MALFORMED_PAYLOAD,
        }
        return not (self.flags & blocking)

    def add(self, flag: QualityFlag, **detail: Any) -> None:
        self.flags.add(flag)
        if detail:
            self.detail[flag.value] = detail


def assess_envelope(
    raw: dict[str, Any],
    *,
    known_event_types: set[str] | None = None,
    now: datetime | None = None,
) -> QualityFinding:
    now = now or datetime.now(UTC)
    finding = QualityFinding()

    required = ["event_type"]
    missing = [f for f in required if not raw.get(f)]
    if missing:
        finding.add(QualityFlag.MISSING_FIELDS, fields=missing)

    payload = raw.get("payload", {})
    if payload is not None and not isinstance(payload, dict):
        finding.add(QualityFlag.MALFORMED_PAYLOAD, type=type(payload).__name__)

    metadata = raw.get("metadata", {})
    if metadata is not None and not isinstance(metadata, dict):
        finding.add(QualityFlag.MALFORMED_PAYLOAD, field="metadata")

    raw_ts = raw.get("event_time") or raw.get("timestamp")
    if raw_ts is not None:
        parsed = _try_parse_dt(raw_ts)
        if parsed is None:
            finding.add(QualityFlag.INVALID_TIMESTAMP, value=str(raw_ts)[:64])
        else:
            if parsed - now > FUTURE_TOLERANCE:
                finding.add(
                    QualityFlag.FUTURE_TIMESTAMP,
                    event_time=parsed.isoformat(),
                    skew_seconds=(parsed - now).total_seconds(),
                )
            if now - parsed > STALE_TOLERANCE:
                finding.add(
                    QualityFlag.LATE_EVENT,
                    event_time=parsed.isoformat(),
                    age_seconds=(now - parsed).total_seconds(),
                )

    et = raw.get("event_type")
    if known_event_types is not None and et and et not in known_event_types:
        finding.add(QualityFlag.UNKNOWN_EVENT_TYPE, event_type=et)

    return finding


def _try_parse_dt(value: Any) -> datetime | None:
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=UTC)
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        s = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def score_from_counts(c: dict[str, int]) -> dict[str, float]:
    """Map per-bucket counts -> sub-scores in [0, 1] and an overall score.

    ``c`` keys: total, valid, invalid_schema, missing_fields, invalid_timestamp,
    future_timestamp, duplicate_events, unknown_event_type, malformed_payload,
    late_events.
    """
    total = max(int(c.get("total", 0)), 1)

    def ratio(k: str) -> float:
        return min(int(c.get(k, 0)) / total, 1.0)

    validity = 1.0 - ratio("invalid_schema") - ratio("malformed_payload")
    completeness = 1.0 - ratio("missing_fields")
    uniqueness = 1.0 - ratio("duplicate_events")
    timeliness = 1.0 - 0.5 * ratio("late_events") - ratio("future_timestamp") - ratio(
        "invalid_timestamp"
    )
    schema_compliance = 1.0 - ratio("invalid_schema") - 0.5 * ratio("unknown_event_type")

    subs = {
        "validity_score": _clamp(validity),
        "completeness_score": _clamp(completeness),
        "uniqueness_score": _clamp(uniqueness),
        "timeliness_score": _clamp(timeliness),
        "schema_compliance_score": _clamp(schema_compliance),
    }
    # Weighted geometric mean punishes a single very-bad dimension.
    weights = {
        "validity_score": 0.30,
        "completeness_score": 0.20,
        "uniqueness_score": 0.15,
        "timeliness_score": 0.15,
        "schema_compliance_score": 0.20,
    }
    log_sum = sum(weights[k] * math.log(max(v, 1e-6)) for k, v in subs.items())
    subs["overall_score"] = _clamp(math.exp(log_sum))
    return {k: round(v, 4) for k, v in subs.items()}


def _clamp(v: float) -> float:
    return max(0.0, min(1.0, v))
