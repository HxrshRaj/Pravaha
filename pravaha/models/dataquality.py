from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class DataQualityRecord(UUIDPk, Timestamps, Base):
    """Per-producer data-quality score for a one-minute bucket."""

    __tablename__ = "data_quality_records"
    __table_args__ = (
        UniqueConstraint("producer_id", "bucket_start", name="uq_dq_producer_bucket"),
        Index("ix_dq_producer_bucket", "producer_id", "bucket_start"),
    )

    producer_id: Mapped[str | None] = mapped_column(
        ForeignKey("producers.id", ondelete="SET NULL"), index=True, nullable=True
    )
    producer_name: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    total_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    valid_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    invalid_schema: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    missing_fields: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    invalid_timestamp: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    future_timestamp: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unknown_event_type: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    malformed_payload: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    late_events: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    validity_score: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    completeness_score: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    uniqueness_score: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    timeliness_score: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    schema_compliance_score: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    overall_score: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)

    breakdown: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
