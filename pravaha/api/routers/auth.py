from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.audit import audit
from pravaha.api.deps import CurrentUser, current_user, get_db, require_admin
from pravaha.api.errors import ConflictError, UnauthorizedError
from pravaha.api.schemas import (
    CreateUserRequest,
    LoginRequest,
    TokenResponse,
    UserOut,
)
from pravaha.auth.security import create_access_token, hash_password, verify_password
from pravaha.config import settings
from pravaha.models import User
from pravaha.models.users import Role

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, request: Request, db: AsyncSession = Depends(get_db)):
    user = await db.scalar(select(User).where(User.email == body.email.lower().strip()))
    if user is None or not user.is_active or not verify_password(body.password, user.password_hash):
        await audit(
            db,
            actor=body.email,
            actor_role="",
            action="auth.login",
            resource_type="user",
            result="failure",
            request_id=getattr(request.state, "request_id", None),
        )
        raise UnauthorizedError("invalid credentials")
    token = create_access_token(user.id, user.role.value)
    await audit(
        db,
        actor=user.email,
        actor_role=user.role.value,
        action="auth.login",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
    )
    return TokenResponse(
        access_token=token,
        expires_in_minutes=settings.access_token_ttl_minutes,
        role=user.role.value,
        email=user.email,
    )


@router.get("/me", response_model=UserOut)
async def me(user: CurrentUser = Depends(current_user), db: AsyncSession = Depends(get_db)):
    row = await db.get(User, user.id)
    return UserOut(
        id=row.id,
        email=row.email,
        full_name=row.full_name,
        role=row.role.value,
        is_active=row.is_active,
        created_at=row.created_at,
    )


@router.get("/users", response_model=list[UserOut])
async def list_users(_: CurrentUser = Depends(require_admin), db: AsyncSession = Depends(get_db)):
    rows = await db.scalars(select(User).order_by(User.created_at.desc()))
    return [
        UserOut(
            id=r.id,
            email=r.email,
            full_name=r.full_name,
            role=r.role.value,
            is_active=r.is_active,
            created_at=r.created_at,
        )
        for r in rows
    ]


@router.post("/users", response_model=UserOut, status_code=201)
async def create_user(
    body: CreateUserRequest,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    email = body.email.lower().strip()
    if await db.scalar(select(User.id).where(User.email == email)):
        raise ConflictError("a user with that email already exists")
    try:
        role = Role(body.role.upper())
    except ValueError as exc:
        raise ConflictError(f"invalid role '{body.role}'") from exc
    user = User(
        email=email,
        full_name=body.full_name,
        password_hash=hash_password(body.password),
        role=role,
        is_active=True,
    )
    db.add(user)
    await db.flush()
    await audit(
        db,
        actor=actor.email,
        actor_role=actor.role.value,
        action="user.create",
        resource_type="user",
        resource_id=user.id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"role": role.value},
    )
    return UserOut(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        role=user.role.value,
        is_active=user.is_active,
        created_at=user.created_at,
    )
