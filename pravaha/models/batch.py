from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class BatchEventRollup(UUIDPk, Timestamps, Base):
    """Hourly event rollups computed by the Scala/Spark batch job.

    See spark/src/main/scala/pravaha/batch/RollupLogic.scala and README.md
    section "Batch layer (Spark / Scala)" for what this computes and why.
    This is the write side of the batch layer: the Spark job populates it via
    plain JDBC (delete-then-insert per recomputed window); this model is the
    read side + the source of truth for the table's schema (Alembic-managed).
    """

    __tablename__ = "batch_event_rollups"
    __table_args__ = (
        UniqueConstraint(
            "event_type", "region", "hour_bucket", name="uq_batch_rollup_bucket"
        ),
        Index("ix_batch_rollup_hour", "hour_bucket"),
        Index("ix_batch_rollup_type_hour", "event_type", "hour_bucket"),
    )

    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    region: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown")
    hour_bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    event_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    distinct_correlation_ids: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    distinct_users: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_amount: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_amount: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    late_event_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    replay_event_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    window_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BatchEventCorrelation(UUIDPk, Timestamps, Base):
    """Cross-event-type correlation summary, the second Scala/Spark batch job.

    See spark/src/main/scala/pravaha/batch/CorrelationLogic.scala and README.md
    section "Batch layer (Spark / Scala)" for what this computes and why: for
    every unordered pair of event types, the Pearson correlation + sample
    covariance of their per-bucket event counts across the requested window -
    a whole-of-history statistic the bounded, per-minute real-time analytics
    worker cannot maintain, and a batch job computes in one pass.

    Unlike ``BatchEventRollup`` (an append-only per-hour fact table), this is a
    point-in-time summary: each run replaces every row for its
    ``bucket_minutes`` setting rather than accumulating one row per window.
    ``pearson_r`` / ``covariance`` are nullable because a pair where either
    event type has zero variance over the window has an undefined correlation
    - stored as NULL, never coerced to a misleading 0.0.
    """

    __tablename__ = "batch_event_correlations"
    __table_args__ = (
        UniqueConstraint(
            "event_type_a", "event_type_b", "bucket_minutes",
            name="uq_batch_correlation_pair",
        ),
        Index("ix_batch_correlation_pearson", "pearson_r"),
    )

    event_type_a: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type_b: Mapped[str] = mapped_column(String(128), nullable=False)
    bucket_minutes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)

    n_buckets: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    mean_count_a: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    mean_count_b: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    pearson_r: Mapped[float | None] = mapped_column(Float, nullable=True)
    covariance: Mapped[float | None] = mapped_column(Float, nullable=True)

    window_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
