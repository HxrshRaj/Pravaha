from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, Float, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class AlertRule(UUIDPk, Timestamps, Base):
    __tablename__ = "alert_rules"

    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    metric: Mapped[str] = mapped_column(String(96), nullable=False)
    group_key: Mapped[str] = mapped_column(String(160), nullable=False, default="_all")
    operator: Mapped[str] = mapped_column(
        Enum("gt", "gte", "lt", "lte", "drop_pct", "rise_pct", name="alert_operator", native_enum=False),
        nullable=False,
    )
    threshold: Mapped[float] = mapped_column(Float, nullable=False)
    duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    severity: Mapped[str] = mapped_column(
        Enum("LOW", "MEDIUM", "HIGH", "CRITICAL", name="alert_severity", native_enum=False),
        nullable=False,
        default="MEDIUM",
    )
    cooldown_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=300)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Alert(UUIDPk, Timestamps, Base):
    __tablename__ = "alerts"
    __table_args__ = (Index("ix_alerts_fired_at", "fired_at"),)

    rule_id: Mapped[str] = mapped_column(String(36), index=True, nullable=False)
    rule_name: Mapped[str] = mapped_column(String(128), nullable=False)
    metric: Mapped[str] = mapped_column(String(96), nullable=False)
    group_key: Mapped[str] = mapped_column(String(160), nullable=False, default="_all")
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    observed_value: Mapped[float] = mapped_column(Float, nullable=False)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)
    fired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(
        Enum("FIRING", "RESOLVED", name="alert_status", native_enum=False),
        nullable=False,
        default="FIRING",
    )
    context: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
