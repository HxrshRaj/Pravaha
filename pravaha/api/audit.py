"""Helper to write an audit row + emit to the audit topic from API handlers."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.kafka.producer import get_producer
from pravaha.kafka.topics import AUDIT
from pravaha.logging import get_logger
from pravaha.models import AuditLog

log = get_logger(__name__)


async def audit(
    db: AsyncSession,
    *,
    actor: str,
    actor_role: str,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    result: str = "success",
    request_id: str | None = None,
    ip: str | None = None,
    metadata: dict[str, Any] | None = None,
    note: str = "",
) -> None:
    row = AuditLog(
        actor=actor,
        actor_role=actor_role,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        result=result,
        request_id=request_id,
        ip=ip,
        audit_metadata=metadata or {},
        note=note,
    )
    db.add(row)
    try:
        await get_producer().publish(
            AUDIT.name,
            {
                "kind": action,
                "actor": actor,
                "actor_role": actor_role,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "result": result,
                "request_id": request_id,
                "note": note,
                **(metadata or {}),
            },
            key=resource_type,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("audit.topic_emit_failed", action=action, error=str(exc))
