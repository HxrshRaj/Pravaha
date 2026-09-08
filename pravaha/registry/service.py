"""Lightweight schema registry.

Not a Confluent clone. Stores JSON Schema (draft 2020-12) documents describing
the *payload* of an event type, versioned. Supports:

* create schema / register new version
* retrieve active or specific version
* validate an event payload against the active version
* activate / deactivate a version
* BACKWARD compatibility check between versions (new schema can read data
  written by the previous schema): no new *required* fields, no removal of
  properties that were previously allowed, no narrowing of types.

Compatibility is intentionally conservative and explained in
docs/decisions/... rather than trying to cover every JSON Schema construct.
"""

from __future__ import annotations

from typing import Any

from jsonschema import Draft202012Validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.logging import get_logger
from pravaha.models import EventSchema, SchemaVersion
from pravaha.models.schemas import CompatibilityMode

log = get_logger(__name__)


class SchemaRegistryError(Exception):
    pass


class SchemaValidationError(SchemaRegistryError):
    def __init__(self, message: str, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


def _ensure_valid_schema_doc(doc: dict[str, Any]) -> None:
    try:
        Draft202012Validator.check_schema(doc)
    except Exception as exc:  # noqa: BLE001
        raise SchemaRegistryError(f"invalid JSON Schema document: {exc}") from exc


def validate_payload(payload: dict[str, Any], json_schema: dict[str, Any]) -> list[str]:
    """Return a list of human-readable validation errors ([] == valid)."""
    validator = Draft202012Validator(json_schema)
    errors: list[str] = []
    for err in sorted(validator.iter_errors(payload), key=lambda e: list(e.path)):
        loc = "/".join(str(p) for p in err.path) or "(root)"
        errors.append(f"{loc}: {err.message}")
    return errors


def check_backward_compatible(
    old: dict[str, Any], new: dict[str, Any]
) -> tuple[bool, list[str]]:
    """Can consumers using ``new`` read data written against ``old``?"""
    problems: list[str] = []

    old_required = set(old.get("required", []))
    new_required = set(new.get("required", []))
    added_required = new_required - old_required
    if added_required:
        problems.append(
            f"new required fields not present before: {sorted(added_required)}"
        )

    old_props: dict[str, Any] = old.get("properties", {})
    new_props: dict[str, Any] = new.get("properties", {})

    removed = set(old_props) - set(new_props)
    if removed and new.get("additionalProperties", True) is False:
        problems.append(
            f"properties removed while additionalProperties=false: {sorted(removed)}"
        )

    for name in set(old_props) & set(new_props):
        ot = _type_set(old_props[name])
        nt = _type_set(new_props[name])
        if ot and nt and not ot.issubset(nt):
            problems.append(
                f"property '{name}' type narrowed {sorted(ot)} -> {sorted(nt)}"
            )

    if old.get("additionalProperties", True) is not False and new.get(
        "additionalProperties", True
    ) is False:
        problems.append("additionalProperties tightened from allowed to false")

    return (len(problems) == 0, problems)


def _type_set(prop: dict[str, Any]) -> set[str]:
    t = prop.get("type")
    if t is None:
        return set()
    return {t} if isinstance(t, str) else set(t)


# --------------------------------------------------------------------------- CRUD


async def create_schema(
    session: AsyncSession,
    *,
    event_type: str,
    json_schema: dict[str, Any],
    description: str = "",
    compatibility: CompatibilityMode = CompatibilityMode.BACKWARD,
    created_by: str | None = None,
) -> tuple[EventSchema, SchemaVersion]:
    _ensure_valid_schema_doc(json_schema)
    existing = await session.scalar(
        select(EventSchema).where(EventSchema.event_type == event_type)
    )
    if existing is not None:
        raise SchemaRegistryError(f"schema for '{event_type}' already exists")

    schema = EventSchema(
        event_type=event_type, description=description, compatibility=compatibility
    )
    session.add(schema)
    await session.flush()

    version = SchemaVersion(
        schema_id=schema.id,
        version=1,
        json_schema=json_schema,
        is_active=True,
        created_by=created_by,
        notes="initial version",
    )
    session.add(version)
    await session.flush()
    schema.active_version_id = version.id
    await session.flush()
    log.info("registry.schema_created", event_type=event_type, version=1)
    return schema, version


async def register_version(
    session: AsyncSession,
    *,
    event_type: str,
    json_schema: dict[str, Any],
    notes: str = "",
    created_by: str | None = None,
    activate: bool = True,
    force: bool = False,
) -> SchemaVersion:
    _ensure_valid_schema_doc(json_schema)
    schema = await session.scalar(
        select(EventSchema).where(EventSchema.event_type == event_type)
    )
    if schema is None:
        raise SchemaRegistryError(f"no schema registered for '{event_type}'")

    latest = await session.scalar(
        select(SchemaVersion)
        .where(SchemaVersion.schema_id == schema.id)
        .order_by(SchemaVersion.version.desc())
    )
    next_version = (latest.version + 1) if latest else 1

    if latest and schema.compatibility == CompatibilityMode.BACKWARD and not force:
        ok, problems = check_backward_compatible(latest.json_schema, json_schema)
        if not ok:
            raise SchemaValidationError(
                f"schema v{next_version} is not backward-compatible with v{latest.version}",
                problems,
            )

    version = SchemaVersion(
        schema_id=schema.id,
        version=next_version,
        json_schema=json_schema,
        is_active=activate,
        created_by=created_by,
        notes=notes,
    )
    session.add(version)
    await session.flush()

    if activate:
        await _deactivate_others(session, schema.id, keep=version.id)
        schema.active_version_id = version.id
        await session.flush()

    log.info(
        "registry.version_registered",
        event_type=event_type,
        version=next_version,
        activated=activate,
    )
    return version


async def _deactivate_others(session: AsyncSession, schema_id: str, keep: str) -> None:
    rows = await session.scalars(
        select(SchemaVersion).where(SchemaVersion.schema_id == schema_id)
    )
    for row in rows:
        row.is_active = row.id == keep


async def set_version_active(
    session: AsyncSession, *, event_type: str, version: int, active: bool
) -> SchemaVersion:
    schema = await session.scalar(
        select(EventSchema).where(EventSchema.event_type == event_type)
    )
    if schema is None:
        raise SchemaRegistryError(f"no schema registered for '{event_type}'")
    row = await session.scalar(
        select(SchemaVersion).where(
            SchemaVersion.schema_id == schema.id, SchemaVersion.version == version
        )
    )
    if row is None:
        raise SchemaRegistryError(f"version {version} not found for '{event_type}'")
    row.is_active = active
    if active:
        await _deactivate_others(session, schema.id, keep=row.id)
        schema.active_version_id = row.id
    elif schema.active_version_id == row.id:
        schema.active_version_id = None
    await session.flush()
    return row


async def get_active_version(
    session: AsyncSession, event_type: str
) -> SchemaVersion | None:
    schema = await session.scalar(
        select(EventSchema).where(EventSchema.event_type == event_type)
    )
    if schema is None or schema.active_version_id is None:
        return None
    return await session.get(SchemaVersion, schema.active_version_id)


async def validate_event_payload(
    session: AsyncSession, event_type: str, payload: dict[str, Any]
) -> tuple[SchemaVersion | None, list[str]]:
    """Validate against the active schema version.

    Returns ``(version, errors)``. If no schema is registered for the type the
    event is allowed through with ``version=None`` and ``errors=[]`` (schema is
    opt-in per type); callers that require strict mode check for ``version``.
    """
    version = await get_active_version(session, event_type)
    if version is None:
        return None, []
    return version, validate_payload(payload, version.json_schema)
