from __future__ import annotations

import enum

from sqlalchemy import ARRAY, Enum, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class ProducerStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"


class Producer(UUIDPk, Timestamps, Base):
    __tablename__ = "producers"

    name: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    status: Mapped[ProducerStatus] = mapped_column(
        Enum(ProducerStatus, native_enum=False, length=16),
        nullable=False,
        default=ProducerStatus.ACTIVE,
    )
    # sha256(api_key). Plaintext keys are shown once at creation and never stored.
    api_key_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    api_key_prefix: Mapped[str] = mapped_column(String(12), nullable=False, default="")
    # Empty list => any event type allowed.
    allowed_event_types: Mapped[list[str]] = mapped_column(
        ARRAY(String(128)), nullable=False, default=list
    )
    rate_limit_per_min: Mapped[int] = mapped_column(Integer, nullable=False, default=6000)
    default_region: Mapped[str | None] = mapped_column(String(32), nullable=True)
