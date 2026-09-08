from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.audit import audit
from pravaha.api.deps import CurrentUser, get_db, require_admin, require_viewer
from pravaha.api.errors import ConflictError, NotFoundError, ValidationFailedError
from pravaha.api.schemas import (
    CreateSchemaRequest,
    RegisterVersionRequest,
    SchemaOut,
    SchemaVersionOut,
)
from pravaha.models import EventSchema, SchemaVersion
from pravaha.models.schemas import CompatibilityMode
from pravaha.registry.service import (
    SchemaRegistryError,
    SchemaValidationError,
    check_backward_compatible,
    create_schema,
    register_version,
    set_version_active,
    validate_payload,
)

router = APIRouter(prefix="/schemas", tags=["schemas"])


async def _schema_out(db: AsyncSession, schema: EventSchema) -> SchemaOut:
    versions = list(
        await db.scalars(
            select(SchemaVersion)
            .where(SchemaVersion.schema_id == schema.id)
            .order_by(SchemaVersion.version.asc())
        )
    )
    active = next((v.version for v in versions if v.id == schema.active_version_id), None)
    return SchemaOut(
        id=schema.id,
        event_type=schema.event_type,
        description=schema.description,
        compatibility=schema.compatibility.value,
        active_version=active,
        versions=[
            SchemaVersionOut(
                id=v.id,
                version=v.version,
                is_active=v.is_active,
                json_schema=v.json_schema,
                notes=v.notes,
                created_by=v.created_by,
                created_at=v.created_at,
            )
            for v in versions
        ],
        created_at=schema.created_at,
    )


@router.get("", response_model=list[SchemaOut])
async def list_schemas(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    rows = list(await db.scalars(select(EventSchema).order_by(EventSchema.event_type.asc())))
    return [await _schema_out(db, s) for s in rows]


@router.post("", response_model=SchemaOut, status_code=201)
async def create(
    body: CreateSchemaRequest,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    try:
        schema, _ = await create_schema(
            db,
            event_type=body.event_type,
            json_schema=body.json_schema,
            description=body.description,
            compatibility=CompatibilityMode(body.compatibility.upper()),
            created_by=actor.email,
        )
    except SchemaRegistryError as exc:
        raise ConflictError(str(exc)) from exc
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="schema.create",
        resource_type="schema", resource_id=schema.id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"event_type": body.event_type},
    )
    return await _schema_out(db, schema)


@router.get("/{event_type}", response_model=SchemaOut)
async def get_one(
    event_type: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    schema = await db.scalar(select(EventSchema).where(EventSchema.event_type == event_type))
    if schema is None:
        raise NotFoundError(f"no schema for '{event_type}'")
    return await _schema_out(db, schema)


@router.post("/{event_type}/versions", response_model=SchemaOut, status_code=201)
async def add_version(
    event_type: str,
    body: RegisterVersionRequest,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    try:
        await register_version(
            db,
            event_type=event_type,
            json_schema=body.json_schema,
            notes=body.notes,
            created_by=actor.email,
            activate=body.activate,
            force=body.force,
        )
    except SchemaValidationError as exc:
        raise ValidationFailedError(
            str(exc), code="SCHEMA_INCOMPATIBLE", details={"problems": exc.errors}
        ) from exc
    except SchemaRegistryError as exc:
        raise NotFoundError(str(exc)) from exc
    schema = await db.scalar(select(EventSchema).where(EventSchema.event_type == event_type))
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="schema.register_version",
        resource_type="schema", resource_id=schema.id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"event_type": event_type, "activate": body.activate, "force": body.force},
    )
    return await _schema_out(db, schema)


@router.post("/{event_type}/versions/{version}/activate", response_model=SchemaOut)
async def activate(
    event_type: str,
    version: int,
    request: Request,
    actor: CurrentUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
    active: bool = True,
):
    try:
        await set_version_active(db, event_type=event_type, version=version, active=active)
    except SchemaRegistryError as exc:
        raise NotFoundError(str(exc)) from exc
    schema = await db.scalar(select(EventSchema).where(EventSchema.event_type == event_type))
    await audit(
        db, actor=actor.email, actor_role=actor.role.value,
        action="schema.set_version_active", resource_type="schema", resource_id=schema.id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"version": version, "active": active},
    )
    return await _schema_out(db, schema)


@router.post("/{event_type}/validate")
async def validate(
    event_type: str,
    payload: dict,
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    schema = await db.scalar(select(EventSchema).where(EventSchema.event_type == event_type))
    if schema is None or schema.active_version_id is None:
        return {"valid": True, "errors": [], "note": "no active schema; validation skipped"}
    version = await db.get(SchemaVersion, schema.active_version_id)
    errors = validate_payload(payload, version.json_schema)
    return {"valid": not errors, "errors": errors, "schema_version": version.version}


@router.post("/{event_type}/compatibility")
async def compatibility(
    event_type: str,
    candidate: dict,
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    schema = await db.scalar(select(EventSchema).where(EventSchema.event_type == event_type))
    if schema is None or schema.active_version_id is None:
        raise NotFoundError("no active schema version to compare against")
    active = await db.get(SchemaVersion, schema.active_version_id)
    ok, problems = check_backward_compatible(active.json_schema, candidate)
    return {"backward_compatible": ok, "problems": problems, "against_version": active.version}
