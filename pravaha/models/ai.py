from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class AIInvestigation(UUIDPk, Timestamps, Base):
    __tablename__ = "ai_investigations"
    __table_args__ = (Index("ix_ai_investigations_status_created", "status", "created_at"),)

    anomaly_id: Mapped[str | None] = mapped_column(String(36), index=True, nullable=True)
    trigger: Mapped[str] = mapped_column(String(32), nullable=False, default="anomaly")
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    status: Mapped[str] = mapped_column(
        Enum(
            "CREATED", "RUNNING", "WAITING_FOR_TOOL", "COMPLETED", "FAILED", "CANCELLED",
            name="ai_investigation_status", native_enum=False,
        ),
        nullable=False,
        default="CREATED",
    )
    phase: Mapped[str] = mapped_column(String(32), nullable=False, default="created")

    provider: Mapped[str | None] = mapped_column(String(32), nullable=True)
    model: Mapped[str | None] = mapped_column(String(64), nullable=True)
    prompt_version: Mapped[str] = mapped_column(String(16), nullable=False, default="v1")

    summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    root_cause: Mapped[str] = mapped_column(Text, nullable=False, default="")
    impact: Mapped[str] = mapped_column(Text, nullable=False, default="")
    recommendations: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    conclusion_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    hypotheses: Mapped[list[AIHypothesis]] = relationship(
        back_populates="investigation", cascade="all, delete-orphan"
    )
    evidence_items: Mapped[list[AIEvidence]] = relationship(
        back_populates="investigation", cascade="all, delete-orphan"
    )
    tool_calls: Mapped[list[AIToolCall]] = relationship(
        back_populates="investigation", cascade="all, delete-orphan"
    )


class AIEvidence(UUIDPk, Timestamps, Base):
    __tablename__ = "ai_evidence"

    investigation_id: Mapped[str] = mapped_column(
        ForeignKey("ai_investigations.id", ondelete="CASCADE"), index=True, nullable=False
    )
    ref: Mapped[str] = mapped_column(String(48), nullable=False)  # e.g. "E:evt-123", "M:mw-9"
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    source_tool: Mapped[str] = mapped_column(String(48), nullable=False, default="")
    summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    investigation: Mapped[AIInvestigation] = relationship(back_populates="evidence_items")


class AIHypothesis(UUIDPk, Timestamps, Base):
    __tablename__ = "ai_hypotheses"

    investigation_id: Mapped[str] = mapped_column(
        ForeignKey("ai_investigations.id", ondelete="CASCADE"), index=True, nullable=False
    )
    key: Mapped[str] = mapped_column(String(8), nullable=False)  # H1..H5
    statement: Mapped[str] = mapped_column(Text, nullable=False)
    explanation: Mapped[str] = mapped_column(Text, nullable=False, default="")
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    status: Mapped[str] = mapped_column(
        Enum(
            "PROPOSED", "SUPPORTED", "REFUTED", "INCONCLUSIVE", "SELECTED",
            name="ai_hypothesis_status", native_enum=False,
        ),
        nullable=False,
        default="PROPOSED",
    )
    supporting: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    contradicting: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    evidence_refs: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    investigation: Mapped[AIInvestigation] = relationship(back_populates="hypotheses")


class AIToolCall(UUIDPk, Timestamps, Base):
    __tablename__ = "ai_tool_calls"

    investigation_id: Mapped[str] = mapped_column(
        ForeignKey("ai_investigations.id", ondelete="CASCADE"), index=True, nullable=False
    )
    seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tool_name: Mapped[str] = mapped_column(String(48), nullable=False)
    arguments: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result_summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    result: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ok: Mapped[bool] = mapped_column(nullable=False, default=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    investigation: Mapped[AIInvestigation] = relationship(back_populates="tool_calls")


class AIUsage(UUIDPk, Timestamps, Base):
    __tablename__ = "ai_usage"
    __table_args__ = (Index("ix_ai_usage_created", "created_at"),)

    investigation_id: Mapped[str | None] = mapped_column(String(36), index=True, nullable=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    model: Mapped[str] = mapped_column(String(64), nullable=False)
    operation: Mapped[str] = mapped_column(String(48), nullable=False, default="chat")
    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    estimated_cost_usd: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
