from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.deps import (
    CurrentUser,
    Page,
    get_db,
    pagination,
    require_engineer,
)
from pravaha.api.schemas import PageMeta, Paginated
from pravaha.models import AuditLog

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("", response_model=Paginated[dict])
async def list_audit(
    page: Page = Depends(pagination),
    _: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
    actor: str | None = Query(None),
    action: str | None = Query(None),
    resource_type: str | None = Query(None),
    since_hours: int = Query(168, ge=1, le=8760),
):
    start = datetime.now(UTC) - timedelta(hours=since_hours)
    q = select(AuditLog).where(AuditLog.created_at >= start)
    cq = select(func.count(AuditLog.id)).where(AuditLog.created_at >= start)
    for cond in (
        (AuditLog.actor == actor) if actor else None,
        (AuditLog.action == action) if action else None,
        (AuditLog.resource_type == resource_type) if resource_type else None,
    ):
        if cond is not None:
            q = q.where(cond)
            cq = cq.where(cond)
    q = q.order_by(AuditLog.created_at.desc()).limit(page.limit).offset(page.offset)
    total = int(await db.scalar(cq) or 0)
    rows = list(await db.scalars(q))
    return Paginated[dict](
        items=[
            {
                "id": r.id,
                "actor": r.actor,
                "actor_role": r.actor_role,
                "action": r.action,
                "resource_type": r.resource_type,
                "resource_id": r.resource_id,
                "result": r.result,
                "request_id": r.request_id,
                "metadata": r.audit_metadata,
                "note": r.note,
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ],
        meta=PageMeta(total=total, limit=page.limit, offset=page.offset, returned=len(rows)),
    )
