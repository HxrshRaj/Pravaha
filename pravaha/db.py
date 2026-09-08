"""Async SQLAlchemy engine / session management."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

from pravaha.config import settings


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


# In the test env every pytest-asyncio test gets its own event loop; a pooled
# asyncpg connection bound to a previous loop then blows up on Windows'
# ProactorEventLoop. NullPool opens/closes a connection per checkout, which is
# loop-safe. Production keeps a real pool.
if settings.env == "test":
    _engine = create_async_engine(settings.database_url, poolclass=NullPool, future=True)
else:
    _engine = create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        future=True,
    )

_session_factory = async_sessionmaker(
    _engine, expire_on_commit=False, class_=AsyncSession, autoflush=False
)


def get_engine():
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Transactional scope: commit on success, rollback on error."""
    session = _session_factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_db() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency. Caller controls commit via service layer."""
    session = _session_factory()
    try:
        yield session
    finally:
        await session.close()


async def dispose_engine() -> None:
    await _engine.dispose()
