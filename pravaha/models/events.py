from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.events.envelope import LatenessClass
from pravaha.models.mixins import Timestamps, UUIDPk


class Event(UUIDPk, Timestamps, Base):
    """The searchable event store (see docs/decisions/0009-event-storage.md)."""

    __tablename__ = "events"
    __table_args__ = (
        Index("ix_events_type_event_time", "event_type", "event_time"),
        Index("ix_events_producer_event_time", "producer_id", "event_time"),
        Index("ix_events_correlation", "correlation_id"),
        Index("ix_events_ingestion_time", "ingestion_time"),
    )

    event_id: Mapped[str] = mapped_column(String(36), unique=True, index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    event_version: Mapped[str] = mapped_column(String(16), nullable=False, default="1.0")
    producer: Mapped[str] = mapped_column(String(128), nullable=False)
    producer_id: Mapped[str | None] = mapped_column(
        ForeignKey("producers.id", ondelete="SET NULL"), index=True, nullable=True
    )
    schema_version_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingestion_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    partition_key: Mapped[str] = mapped_column(String(256), nullable=False)
    kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kafka_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    kafka_topic: Mapped[str | None] = mapped_column(String(128), nullable=True)

    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    trace_id: Mapped[str] = mapped_column(String(128), nullable=False)
    region: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)

    lateness_at_ingest: Mapped[LatenessClass] = mapped_column(
        Enum(LatenessClass, native_enum=False, length=16),
        nullable=False,
        default=LatenessClass.ON_TIME,
    )
    is_replay: Mapped[bool] = mapped_column(nullable=False, default=False, index=True)
    replay_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)

    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    event_metadata: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)

    # revenue/amount extracted for fast analytics rollups where present
    amount: Mapped[float | None] = mapped_column(Float, nullable=True)


class EventProcessingRecord(UUIDPk, Timestamps, Base):
    """One row per (event, processor) - the idempotency boundary + audit trail."""

    __tablename__ = "event_processing_records"
    __table_args__ = (
        UniqueConstraint("event_id", "processor", name="uq_event_processor"),
        Index("ix_epr_processor_created", "processor", "created_at"),
    )

    event_id: Mapped[str] = mapped_column(String(36), index=True, nullable=False)
    processor: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="processed")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kafka_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
