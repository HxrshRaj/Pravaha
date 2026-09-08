"""FastAPI dependencies: DB session, auth (JWT users + producer API keys), RBAC,
Redis-backed rate limiting, pagination."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from dataclasses import dataclass

from fastapi import Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.errors import (
    ForbiddenError,
    RateLimitedError,
    UnauthorizedError,
)
from pravaha.auth.security import decode_access_token
from pravaha.db import get_session_factory
from pravaha.models import Producer, User
from pravaha.models.users import Role
from pravaha.redis_client import rate_limit_check


async def get_db() -> AsyncIterator[AsyncSession]:
    session = get_session_factory()()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


@dataclass
class CurrentUser:
    id: str
    email: str
    role: Role


def _bearer(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    # EventSource / SSE clients cannot set headers -> allow ?access_token=
    qtok = request.query_params.get("access_token")
    if qtok:
        return qtok.strip()
    return None


async def current_user(
    request: Request, db: AsyncSession = Depends(get_db)
) -> CurrentUser:
    token = _bearer(request)
    if not token:
        raise UnauthorizedError("missing bearer token")
    payload = decode_access_token(token)
    if not payload or payload.get("typ") != "access":
        raise UnauthorizedError("invalid or expired token")
    user = await db.get(User, payload.get("sub", ""))
    if user is None or not user.is_active:
        raise UnauthorizedError("user not found or inactive")
    return CurrentUser(id=user.id, email=user.email, role=user.role)


async def optional_user(
    request: Request, db: AsyncSession = Depends(get_db)
) -> CurrentUser | None:
    try:
        return await current_user(request, db)
    except UnauthorizedError:
        return None


def require_role(minimum: Role):
    async def _dep(user: CurrentUser = Depends(current_user)) -> CurrentUser:
        if not user.role.satisfies(minimum):
            raise ForbiddenError(
                f"role {user.role.value} is insufficient; {minimum.value} required",
                details={"required": minimum.value, "have": user.role.value},
            )
        return user

    return _dep


require_viewer = require_role(Role.VIEWER)
require_analyst = require_role(Role.ANALYST)
require_engineer = require_role(Role.ENGINEER)
require_admin = require_role(Role.ADMIN)


async def authed_producer(
    request: Request, db: AsyncSession = Depends(get_db)
) -> Producer:
    from pravaha.producers.service import ProducerError, authenticate_producer

    key = request.headers.get("x-api-key") or (
        _bearer(request) if (_bearer(request) or "").startswith("pvh_") else None
    )
    if not key:
        raise UnauthorizedError("missing producer API key (X-API-Key header)")
    try:
        return await authenticate_producer(db, key)
    except ProducerError as exc:
        raise UnauthorizedError(str(exc)) from exc


async def enforce_rate_limit(
    request: Request, key: str, limit_per_min: int
) -> None:
    now_ms = int(time.time() * 1000)
    allowed, remaining = await rate_limit_check(
        f"pravaha:rl:{key}", limit_per_min, 60_000, now_ms
    )
    request.state.rate_remaining = remaining
    if not allowed:
        raise RateLimitedError(
            "rate limit exceeded",
            details={"limit_per_min": limit_per_min},
        )


@dataclass
class Page:
    limit: int
    offset: int
    sort: str | None
    order: str


def pagination(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort: str | None = Query(None),
    order: str = Query("desc", pattern="^(asc|desc)$"),
) -> Page:
    return Page(limit=limit, offset=offset, sort=sort, order=order)
