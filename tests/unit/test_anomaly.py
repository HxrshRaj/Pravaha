import numpy as np

from pravaha.anomaly.detectors import (
    EWMADetector,
    IsolationForestDetector,
    RollingZScoreDetector,
    choose_detector,
    severity_for,
)


def _wave(n=60, base=100.0, amp=3.0):
    return [base + np.sin(i / 5) * amp for i in range(n)]


def test_rolling_zscore_flags_spike_not_noise():
    hist = _wave()
    r = RollingZScoreDetector(threshold=3.0).detect(hist + [180.0])
    assert r.is_anomaly and r.algorithm == "rolling_zscore"
    assert abs(r.score) > 3
    r2 = RollingZScoreDetector(threshold=3.0).detect(hist + [101.0])
    assert not r2.is_anomaly


def test_rolling_zscore_insufficient_history():
    r = RollingZScoreDetector(min_history=12).detect([1.0, 2.0, 3.0])
    assert not r.is_anomaly
    assert r.baseline_kind == "insufficient_history"


def test_ewma_reacts_to_rate_regime_change():
    hist = [0.02 + np.random.RandomState(i).randn() * 0.002 for i in range(40)]
    r = EWMADetector(alpha=0.35, threshold=3.0).detect(hist + [0.35])
    assert r.is_anomaly
    r2 = EWMADetector(alpha=0.35, threshold=3.0).detect(hist + [0.021])
    assert not r2.is_anomaly


def test_isolation_forest_multivariate():
    rs = np.random.RandomState(0)
    hist = [[100 + rs.randn() * 2, 1000 + rs.randn() * 20] for _ in range(80)]
    r = IsolationForestDetector(min_history=40).detect_matrix(hist, [200.0, 400.0])
    assert r.algorithm == "isolation_forest"
    assert r.is_anomaly
    r2 = IsolationForestDetector(min_history=40).detect_matrix(hist, [100.0, 1000.0])
    assert not r2.is_anomaly


def test_choose_detector_routing():
    assert isinstance(choose_detector("payment_failure_rate"), EWMADetector)
    assert isinstance(choose_detector("events_per_sec"), RollingZScoreDetector)


def test_severity_thresholds():
    assert severity_for(2.0) == "LOW"
    assert severity_for(3.5) == "MEDIUM"
    assert severity_for(5.0) == "HIGH"
    assert severity_for(7.0) == "CRITICAL"
