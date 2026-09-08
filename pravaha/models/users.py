from __future__ import annotations

import enum

from sqlalchemy import Boolean, Enum, String
from sqlalchemy.orm import Mapped, mapped_column

from pravaha.db import Base
from pravaha.models.mixins import Timestamps, UUIDPk


class Role(str, enum.Enum):
    VIEWER = "VIEWER"
    ANALYST = "ANALYST"
    ENGINEER = "ENGINEER"
    ADMIN = "ADMIN"

    @property
    def rank(self) -> int:
        return {"VIEWER": 0, "ANALYST": 1, "ENGINEER": 2, "ADMIN": 3}[self.value]

    def satisfies(self, required: Role) -> bool:
        return self.rank >= required.rank


class User(UUIDPk, Timestamps, Base):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[Role] = mapped_column(
        Enum(Role, native_enum=False, length=16), nullable=False, default=Role.VIEWER
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
