from __future__ import annotations

import enum

from sqlalchemy import (
    Boolean,
    Enum,
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


class CompatibilityMode(str, enum.Enum):
    BACKWARD = "BACKWARD"
    NONE = "NONE"


class EventSchema(UUIDPk, Timestamps, Base):
    """A named schema, keyed by the event_type it validates."""

    __tablename__ = "event_schemas"

    event_type: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    compatibility: Mapped[CompatibilityMode] = mapped_column(
        Enum(CompatibilityMode, native_enum=False, length=16),
        nullable=False,
        default=CompatibilityMode.BACKWARD,
    )
    active_version_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    versions: Mapped[list[SchemaVersion]] = relationship(
        back_populates="schema", cascade="all, delete-orphan", order_by="SchemaVersion.version"
    )


class SchemaVersion(UUIDPk, Timestamps, Base):
    __tablename__ = "schema_versions"
    __table_args__ = (UniqueConstraint("schema_id", "version", name="uq_schema_version"),)

    schema_id: Mapped[str] = mapped_column(
        ForeignKey("event_schemas.id", ondelete="CASCADE"), index=True, nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    # JSON Schema (draft 2020-12) document describing the event *payload*.
    json_schema: Mapped[dict] = mapped_column(JSONB, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=False, default="")

    schema: Mapped[EventSchema] = relationship(back_populates="versions")
