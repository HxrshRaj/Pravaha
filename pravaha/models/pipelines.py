from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class Pipeline(UUIDPk, Timestamps, Base):
    __tablename__ = "pipelines"

    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    active_version_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    versions: Mapped[list[PipelineVersion]] = relationship(
        back_populates="pipeline", cascade="all, delete-orphan"
    )


class PipelineVersion(UUIDPk, Timestamps, Base):
    __tablename__ = "pipeline_versions"
    __table_args__ = (UniqueConstraint("pipeline_id", "version", name="uq_pipeline_version"),)

    pipeline_id: Mapped[str] = mapped_column(
        ForeignKey("pipelines.id", ondelete="CASCADE"), index=True, nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        Enum("DRAFT", "PUBLISHED", "DISABLED", name="pipeline_status", native_enum=False),
        nullable=False,
        default="DRAFT",
    )
    # Immutable once PUBLISHED. Full graph is also denormalised here for fast load.
    graph: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    published_by: Mapped[str | None] = mapped_column(String(255), nullable=True)

    pipeline: Mapped[Pipeline] = relationship(back_populates="versions")
    nodes: Mapped[list[PipelineNode]] = relationship(
        back_populates="pipeline_version", cascade="all, delete-orphan"
    )
    edges: Mapped[list[PipelineEdge]] = relationship(
        back_populates="pipeline_version", cascade="all, delete-orphan"
    )


class PipelineNode(UUIDPk, Timestamps, Base):
    __tablename__ = "pipeline_nodes"

    pipeline_version_id: Mapped[str] = mapped_column(
        ForeignKey("pipeline_versions.id", ondelete="CASCADE"), index=True, nullable=False
    )
    node_key: Mapped[str] = mapped_column(String(64), nullable=False)
    node_type: Mapped[str] = mapped_column(
        Enum(
            "source", "filter", "transform", "group", "window", "aggregate", "sink",
            name="pipeline_node_type", native_enum=False,
        ),
        nullable=False,
    )
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    pipeline_version: Mapped[PipelineVersion] = relationship(back_populates="nodes")


class PipelineEdge(UUIDPk, Timestamps, Base):
    __tablename__ = "pipeline_edges"

    pipeline_version_id: Mapped[str] = mapped_column(
        ForeignKey("pipeline_versions.id", ondelete="CASCADE"), index=True, nullable=False
    )
    from_node: Mapped[str] = mapped_column(String(64), nullable=False)
    to_node: Mapped[str] = mapped_column(String(64), nullable=False)

    pipeline_version: Mapped[PipelineVersion] = relationship(back_populates="edges")


class PipelineExecution(UUIDPk, Timestamps, Base):
    __tablename__ = "pipeline_executions"

    pipeline_id: Mapped[str] = mapped_column(String(36), index=True, nullable=False)
    pipeline_version_id: Mapped[str] = mapped_column(String(36), index=True, nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    events_processed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    events_failed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    events_dlq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    retries: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    throughput_eps: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    latency_ms_p95: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
