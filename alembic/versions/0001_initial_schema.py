"""initial schema

Bootstraps the full Pravaha schema from the SQLAlchemy model metadata so the
first migration is guaranteed to match the ORM exactly (28 tables). Subsequent
migrations are produced with ``alembic revision --autogenerate`` and use the
normal ``op.*`` operations.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-09-08
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from pravaha.db import Base

# import side-effect: populate Base.metadata with every table
import pravaha.models  # noqa: F401

revision: str = "0001_initial_schema"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
