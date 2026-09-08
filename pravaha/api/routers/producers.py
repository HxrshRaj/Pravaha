from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.audit import audit
from pravaha.api.deps import CurrentUser, get_db, require_admin, require_viewer
from pravaha.api.errors import NotFoundError
from pravaha.api.schemas import (
    CreateProducerRequest,
    ProducerCreatedOut,
    ProducerOut,
    UpdateProducerRequest,
)
from pravaha.models import Producer
from pravaha.models.producers import ProducerStatus
from pravaha.producers.service import (
    ProducerError,
    create_producer,
    producer_health,
    rotate_api_key,
    set_producer_status,
    update_producer,
)

router = APIRouter(prefix="/producers", tags=["producers"])


def _out(p: Producer) -> ProducerOut:
    return ProducerOut(
        id=p.id,
        name=p.name,
        description=p.description,
        status=p.status.value,
        api_key_prefix=p.api_key_prefix,
        allowed_event_types=p.allowed_event_types,
        rate_limit_per_min=p.rate_limit_per_min,
        default_region=p.default_region,
        created_at=p.created_at,
        updated_at=p.updated_at,
    )


@router.get("", response_model=list[ProducerOut])
async def list_producers(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    rows = await db.scalars(select(Producer).order_by(Producer.created_at.desc()))
    return [_out(p) for p in rows]


@router.post("", response_model=ProducerCreatedOut, status_code=201)
async def create(
    body: CreateProducerRequest,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    try:
        created = await create_producer(
            db,
            name=body.name,
            description=body.description,
            allowed_event_types=body.allowed_event_types,
            rate_limit_per_min=body.rate_limit_per_min,
            default_region=body.default_region,
        )
    except ProducerError as exc:
        from pravaha.api.errors import ConflictError

        raise ConflictError(str(exc)) from exc
    await audit(
        db,
        actor=actor.email,
        actor_role=actor.role.value,
        action="producer.create",
        resource_type="producer",
        resource_id=created.producer.id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"name": body.name},
    )
    return ProducerCreatedOut(**_out(created.producer).model_dump(), api_key=created.api_key)


@router.get("/{producer_id}", response_model=ProducerOut)
async def get_one(
    producer_id: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    p = await db.get(Producer, producer_id)
    if p is None:
        raise NotFoundError("producer not found")
    return _out(p)


@router.patch("/{producer_id}", response_model=ProducerOut)
async def update(
    producer_id: str,
    body: UpdateProducerRequest,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    try:
        p = await update_producer(
            db,
            producer_id,
            description=body.description,
            allowed_event_types=body.allowed_event_types,
            rate_limit_per_min=body.rate_limit_per_min,
            default_region=body.default_region,
        )
    except ProducerError as exc:
        raise NotFoundError(str(exc)) from exc
    await audit(
        db,
        actor=actor.email,
        actor_role=actor.role.value,
        action="producer.update",
        resource_type="producer",
        resource_id=producer_id,
        request_id=getattr(request.state, "request_id", None),
        metadata=body.model_dump(exclude_none=True),
    )
    return _out(p)


@router.post("/{producer_id}/disable", response_model=ProducerOut)
async def disable(
    producer_id: str,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    try:
        p = await set_producer_status(db, producer_id, ProducerStatus.DISABLED)
    except ProducerError as exc:
        raise NotFoundError(str(exc)) from exc
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="producer.disable",
        resource_type="producer", resource_id=producer_id,
        request_id=getattr(request.state, "request_id", None),
    )
    return _out(p)


@router.post("/{producer_id}/enable", response_model=ProducerOut)
async def enable(
    producer_id: str,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    try:
        p = await set_producer_status(db, producer_id, ProducerStatus.ACTIVE)
    except ProducerError as exc:
        raise NotFoundError(str(exc)) from exc
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="producer.enable",
        resource_type="producer", resource_id=producer_id,
        request_id=getattr(request.state, "request_id", None),
    )
    return _out(p)


@router.post("/{producer_id}/rotate-key")
async def rotate_key(
    producer_id: str,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    try:
        key = await rotate_api_key(db, producer_id)
    except ProducerError as exc:
        raise NotFoundError(str(exc)) from exc
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="producer.rotate_key",
        resource_type="producer", resource_id=producer_id,
        request_id=getattr(request.state, "request_id", None),
    )
    return {"api_key": key, "detail": "Store this now; it will not be shown again."}


@router.get("/{producer_id}/health")
async def health(
    producer_id: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    try:
        return await producer_health(db, producer_id)
    except ProducerError as exc:
        raise NotFoundError(str(exc)) from exc
