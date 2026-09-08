from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Enum, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class DeadLetterEvent(UUIDPk, Timestamps, Base):
    __tablename__ = "dead_letter_events"
    __table_args__ = (
        Index("ix_dlq_status_created", "status", "created_at"),
        Index("ix_dlq_processor", "processor"),
    )

    event_id: Mapped[str | None] = mapped_column(String(36), index=True, nullable=True)
    processor: Mapped[str] = mapped_column(String(64), nullable=False)
    source_topic: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kafka_offset: Mapped[int | None] = mapped_column(Integer, nullable=True)

    failure_reason: Mapped[str] = mapped_column(String(96), nullable=False)
    error_detail: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error_class: Mapped[str] = mapped_column(String(32), nullable=False, default="transient")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    envelope: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    status: Mapped[str] = mapped_column(
        Enum("PENDING", "RETRIED", "RESOLVED", "DISCARDED", name="dlq_status", native_enum=False),
        nullable=False,
        default="PENDING",
    )
    last_action_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_action_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
