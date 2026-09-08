"""Anomaly detection strategies.

Three methods, chosen per metric by :func:`choose_detector`:

* :class:`RollingZScoreDetector` - rolling mean/std, flag on |z| >= threshold.
  Good default for smooth rate/throughput metrics.
* :class:`EWMADetector` - exponentially weighted mean + variance; reacts faster
  to regime changes, used for failure-rate style metrics.
* :class:`IsolationForestDetector` - scikit-learn, multivariate. Used when the
  caller can supply a feature matrix (e.g. [orders, revenue, failure_rate]).

Every detector returns an :class:`AnomalyResult` with observed/expected/deviation
plus the evidence needed to persist and to ground the AI explanation. Detectors
are pure and stateless w.r.t. storage - history is passed in.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


@dataclass
class AnomalyResult:
    is_anomaly: bool
    observed_value: float
    expected_value: float
    deviation: float
    score: float                 # algorithm-native score (z, ewma-z, -iforest)
    confidence: float            # 0..1
    algorithm: str
    baseline_kind: str = "rolling"
    evidence: dict = field(default_factory=dict)


def severity_for(z_like: float) -> str:
    a = abs(z_like)
    if a >= 6:
        return "CRITICAL"
    if a >= 4:
        return "HIGH"
    if a >= 3:
        return "MEDIUM"
    return "LOW"


class Detector:
    name = "base"

    def detect(self, series: list[float], current: float | None = None) -> AnomalyResult:
        raise NotImplementedError


class RollingZScoreDetector(Detector):
    name = "rolling_zscore"

    def __init__(self, threshold: float = 3.0, min_history: int = 12, window: int = 60) -> None:
        self.threshold = threshold
        self.min_history = min_history
        self.window = window

    def detect(self, series: list[float], current: float | None = None) -> AnomalyResult:
        hist = list(series)
        if current is None:
            if not hist:
                return _insufficient(0.0, self.name)
            current = hist[-1]
            hist = hist[:-1]
        hist = hist[-self.window :]
        if len(hist) < self.min_history:
            return _insufficient(current, self.name)

        arr = np.asarray(hist, dtype=float)
        mean = float(arr.mean())
        std = float(arr.std(ddof=1)) if len(arr) > 1 else 0.0
        # Floor std relative to the signal magnitude so a (near-)constant
        # history still yields a meaningful z for a real departure, and works
        # for both large counts and small fractional rates.
        std_eff = max(std, abs(mean) * 0.01, 1e-9)
        deviation = current - mean
        z = deviation / std_eff
        is_anom = abs(z) >= self.threshold

        return AnomalyResult(
            is_anomaly=is_anom,
            observed_value=round(current, 6),
            expected_value=round(mean, 6),
            deviation=round(deviation, 6),
            score=round(z, 4),
            confidence=_conf_from_z(z),
            algorithm=self.name,
            baseline_kind="rolling",
            evidence={
                "history_points": len(hist),
                "mean": round(mean, 6),
                "std": round(std, 6),
                "threshold": self.threshold,
                "z_score": round(z, 4),
                "recent": [round(x, 4) for x in hist[-12:]],
            },
        )


class EWMADetector(Detector):
    name = "ewma"

    def __init__(self, alpha: float = 0.3, threshold: float = 3.0, min_history: int = 10) -> None:
        self.alpha = alpha
        self.threshold = threshold
        self.min_history = min_history

    def detect(self, series: list[float], current: float | None = None) -> AnomalyResult:
        hist = list(series)
        if current is None:
            if not hist:
                return _insufficient(0.0, self.name)
            current = hist[-1]
            hist = hist[:-1]
        if len(hist) < self.min_history:
            return _insufficient(current, self.name)

        ewma = hist[0]
        ewvar = 0.0
        for x in hist[1:]:
            diff = x - ewma
            ewma += self.alpha * diff
            ewvar = (1 - self.alpha) * (ewvar + self.alpha * diff * diff)
        std = float(np.sqrt(max(ewvar, 0.0)))
        std_eff = max(std, abs(ewma) * 0.01, 1e-9)
        deviation = current - ewma
        z = deviation / std_eff
        is_anom = abs(z) >= self.threshold

        return AnomalyResult(
            is_anomaly=is_anom,
            observed_value=round(current, 6),
            expected_value=round(ewma, 6),
            deviation=round(deviation, 6),
            score=round(z, 4),
            confidence=_conf_from_z(z),
            algorithm=self.name,
            baseline_kind="ewma",
            evidence={
                "alpha": self.alpha,
                "ewma": round(ewma, 6),
                "ewma_std": round(std, 6),
                "threshold": self.threshold,
                "z_score": round(z, 4),
                "history_points": len(hist),
            },
        )


class IsolationForestDetector(Detector):
    name = "isolation_forest"

    def __init__(self, contamination: float = 0.05, min_history: int = 40, random_state: int = 42) -> None:
        self.contamination = contamination
        self.min_history = min_history
        self.random_state = random_state

    def detect_matrix(self, history: list[list[float]], current: list[float]) -> AnomalyResult:
        from sklearn.ensemble import IsolationForest

        if len(history) < self.min_history:
            return _insufficient(current[0] if current else 0.0, self.name)

        x = np.asarray(history, dtype=float)
        model = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=128,
        )
        model.fit(x)
        cur = np.asarray(current, dtype=float).reshape(1, -1)
        pred = int(model.predict(cur)[0])          # -1 anomaly, 1 normal
        raw_score = float(model.score_samples(cur)[0])
        train_scores = model.score_samples(x)
        thresh = float(np.quantile(train_scores, self.contamination))
        is_anom = pred == -1
        mean_vec = x.mean(axis=0)
        deviation = float(np.linalg.norm(cur.ravel() - mean_vec))
        # pseudo-z from percentile position of the score
        pct = float((train_scores < raw_score).mean())
        z_like = (0.5 - pct) * 8.0

        return AnomalyResult(
            is_anomaly=is_anom,
            observed_value=round(float(current[0]), 6) if current else 0.0,
            expected_value=round(float(mean_vec[0]), 6),
            deviation=round(deviation, 6),
            score=round(raw_score, 4),
            confidence=_clamp(abs(pct - 0.5) * 2.0 if is_anom else 0.0),
            algorithm=self.name,
            baseline_kind="model",
            evidence={
                "features": current,
                "feature_means": [round(float(m), 4) for m in mean_vec],
                "iforest_score": round(raw_score, 5),
                "decision_threshold": round(thresh, 5),
                "score_percentile": round(pct, 4),
                "train_points": len(history),
                "z_like": round(z_like, 3),
            },
        )

    def detect(self, series: list[float], current: float | None = None) -> AnomalyResult:
        hist = [[v] for v in series]
        cur = [current if current is not None else (series[-1] if series else 0.0)]
        return self.detect_matrix(hist, cur)


_RATE_METRICS = ("failure_rate", "success_rate", "cancellation_rate", "conversion_rate", "error_rate")


def choose_detector(metric: str) -> Detector:
    m = metric.lower()
    if any(k in m for k in _RATE_METRICS):
        return EWMADetector(alpha=0.35, threshold=3.0)
    if m.endswith((".multi", ":multi")) or "vector" in m:
        return IsolationForestDetector()
    return RollingZScoreDetector(threshold=3.0)


def _insufficient(current: float, algo: str) -> AnomalyResult:
    return AnomalyResult(
        is_anomaly=False,
        observed_value=round(float(current), 6),
        expected_value=round(float(current), 6),
        deviation=0.0,
        score=0.0,
        confidence=0.0,
        algorithm=algo,
        baseline_kind="insufficient_history",
        evidence={"reason": "insufficient history for a reliable baseline"},
    )


def _conf_from_z(z: float) -> float:
    return _clamp((abs(z) - 2.0) / 6.0)


def _clamp(v: float) -> float:
    return max(0.0, min(1.0, float(v)))
