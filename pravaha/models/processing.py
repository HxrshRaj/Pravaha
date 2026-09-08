from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    Enum,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class WindowRecord(UUIDPk, Timestamps, Base):
    """A materialised window instance and its final (or in-progress) aggregate."""

    __tablename__ = "windows"
    __table_args__ = (
        UniqueConstraint(
            "metric", "window_type", "window_size_seconds", "window_start", "group_key",
            name="uq_window_identity",
        ),
        Index("ix_windows_metric_start", "metric", "window_start"),
    )

    metric: Mapped[str] = mapped_column(String(96), nullable=False)
    window_type: Mapped[str] = mapped_column(String(16), nullable=False)  # tumbling|sliding|session
    window_size_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    window_slide_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    group_key: Mapped[str] = mapped_column(String(160), nullable=False, default="_all")

    state: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    late_event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_closed: Mapped[bool] = mapped_column(nullable=False, default=False, index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Aggregation(UUIDPk, Timestamps, Base):
    """Point-in-time metric samples emitted by the analytics processor.

    This is the time-series that analytics APIs, charts and anomaly detection
    read from. One row = one metric value for one minute bucket + group.
    """

    __tablename__ = "aggregations"
    __table_args__ = (
        UniqueConstraint("metric", "bucket_start", "group_key", name="uq_aggregation_point"),
        Index("ix_aggregations_metric_bucket", "metric", "bucket_start"),
    )

    metric: Mapped[str] = mapped_column(String(96), nullable=False, index=True)
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bucket_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    group_key: Mapped[str] = mapped_column(String(160), nullable=False, default="_all")
    value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    extra: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    source_time_semantics: Mapped[str] = mapped_column(String(20), nullable=False, default="event_time")


class AnomalyRecord(UUIDPk, Timestamps, Base):
    __tablename__ = "anomalies"
    __table_args__ = (
        Index("ix_anomalies_detected_at", "detected_at"),
        Index("ix_anomalies_metric_detected", "metric", "detected_at"),
    )

    metric: Mapped[str] = mapped_column(String(96), nullable=False)
    group_key: Mapped[str] = mapped_column(String(160), nullable=False, default="_all")
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    observed_value: Mapped[float] = mapped_column(Float, nullable=False)
    expected_value: Mapped[float] = mapped_column(Float, nullable=False)
    deviation: Mapped[float] = mapped_column(Float, nullable=False)
    severity: Mapped[str] = mapped_column(
        Enum("LOW", "MEDIUM", "HIGH", "CRITICAL", name="anomaly_severity", native_enum=False),
        nullable=False,
    )
    algorithm: Mapped[str] = mapped_column(String(32), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    baseline_kind: Mapped[str] = mapped_column(String(24), nullable=False, default="rolling")
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(
        Enum("OPEN", "INVESTIGATING", "RESOLVED", "DISMISSED", name="anomaly_status", native_enum=False),
        nullable=False,
        default="OPEN",
    )
    dedup_key: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")
