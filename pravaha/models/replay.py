from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Enum, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class ReplayJob(UUIDPk, Timestamps, Base):
    __tablename__ = "replay_jobs"

    requested_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    time_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    time_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    filter_event_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    filter_producer_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    source: Mapped[str] = mapped_column(String(24), nullable=False, default="event_store")
    target_topic: Mapped[str] = mapped_column(String(128), nullable=False, default="events.replay")
    speed_multiplier: Mapped[float] = mapped_column(nullable=False, default=0.0)

    estimated_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    replayed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    status: Mapped[str] = mapped_column(
        Enum(
            "CREATED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED",
            name="replay_status", native_enum=False,
        ),
        nullable=False,
        default="CREATED",
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    stats: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
