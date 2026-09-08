from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class ConsumerGroup(UUIDPk, Timestamps, Base):
    __tablename__ = "consumer_groups"

    name: Mapped[str] = mapped_column(String(96), unique=True, index=True, nullable=False)
    description: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    topics: Mapped[str] = mapped_column(String(512), nullable=False, default="")


class ConsumerInstance(UUIDPk, Timestamps, Base):
    __tablename__ = "consumer_instances"
    __table_args__ = (Index("ix_consumer_instances_group_seen", "group_name", "last_seen"),)

    group_name: Mapped[str] = mapped_column(String(96), index=True, nullable=False)
    member_id: Mapped[str] = mapped_column(String(160), nullable=False)
    host: Mapped[str] = mapped_column(String(160), nullable=False, default="")
    assigned_partitions: Mapped[str] = mapped_column(String(256), nullable=False, default="")
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="running")
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    events_processed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    events_failed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    processing_latency_ms_p95: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    paused: Mapped[bool] = mapped_column(nullable=False, default=False)


class ConsumerLag(UUIDPk, Timestamps, Base):
    __tablename__ = "consumer_lag"
    __table_args__ = (
        UniqueConstraint(
            "group_name", "topic", "partition", "sampled_at", name="uq_consumer_lag_sample"
        ),
        Index("ix_consumer_lag_group_time", "group_name", "sampled_at"),
    )

    group_name: Mapped[str] = mapped_column(String(96), nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    current_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    log_end_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    lag: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    sampled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
