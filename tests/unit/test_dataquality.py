from datetime import UTC, datetime, timedelta

from pravaha.dataquality.engine import QualityFlag, assess_envelope, score_from_counts


def test_assess_flags_missing_and_malformed():
    f = assess_envelope({"payload": "notadict"})
    assert QualityFlag.MISSING_FIELDS in f.flags
    assert QualityFlag.MALFORMED_PAYLOAD in f.flags
    assert not f.is_valid


def test_assess_future_and_bad_timestamp():
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    f = assess_envelope({"event_type": "a.b", "event_time": future})
    assert QualityFlag.FUTURE_TIMESTAMP in f.flags
    assert f.is_valid  # future ts is a warning, not blocking

    f2 = assess_envelope({"event_type": "a.b", "event_time": "garbage"})
    assert QualityFlag.INVALID_TIMESTAMP in f2.flags
    assert not f2.is_valid


def test_assess_unknown_event_type():
    f = assess_envelope({"event_type": "weird.thing"}, known_event_types={"order.created"})
    assert QualityFlag.UNKNOWN_EVENT_TYPE in f.flags


def test_score_from_counts_bounds_and_direction():
    perfect = score_from_counts({"total": 100, "valid": 100})
    assert perfect["overall_score"] == 1.0

    bad = score_from_counts(
        {
            "total": 100,
            "valid": 40,
            "invalid_schema": 30,
            "missing_fields": 20,
            "duplicate_events": 25,
            "future_timestamp": 10,
            "late_events": 40,
        }
    )
    assert 0.0 <= bad["overall_score"] < perfect["overall_score"]
    for k in (
        "validity_score",
        "completeness_score",
        "uniqueness_score",
        "timeliness_score",
        "schema_compliance_score",
    ):
        assert 0.0 <= bad[k] <= 1.0
    assert bad["validity_score"] < 1.0
    assert bad["uniqueness_score"] < 1.0
