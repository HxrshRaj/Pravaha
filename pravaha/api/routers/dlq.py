from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.audit import audit
from pravaha.api.deps import (
    CurrentUser,
    Page,
    get_db,
    pagination,
    require_engineer,
    require_viewer,
)
from pravaha.api.errors import ConflictError, NotFoundError
from pravaha.api.schemas import PageMeta, Paginated
from pravaha.kafka.producer import get_producer
from pravaha.kafka.topics import RAW
from pravaha.models import DeadLetterEvent

router = APIRouter(prefix="/dlq", tags=["dlq"])


def _out(d: DeadLetterEvent) -> dict:
    return {
        "id": d.id,
        "event_id": d.event_id,
        "processor": d.processor,
        "source_topic": d.source_topic,
        "kafka_partition": d.kafka_partition,
        "kafka_offset": d.kafka_offset,
        "failure_reason": d.failure_reason,
        "error_detail": d.error_detail[:2000],
        "error_class": d.error_class,
        "attempt_count": d.attempt_count,
        "status": d.status,
        "created_at": d.created_at.isoformat(),
        "last_action_at": d.last_action_at.isoformat() if d.last_action_at else None,
        "last_action_by": d.last_action_by,
    }


@router.get("", response_model=Paginated[dict])
async def list_dlq(
    page: Page = Depends(pagination),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
    status: str | None = Query(None),
    processor: str | None = Query(None),
    reason: str | None = Query(None),
):
    q = select(DeadLetterEvent)
    cq = select(func.count(DeadLetterEvent.id))
    for cond in (
        (DeadLetterEvent.status == status) if status else None,
        (DeadLetterEvent.processor == processor) if processor else None,
        (DeadLetterEvent.failure_reason == reason) if reason else None,
    ):
        if cond is not None:
            q = q.where(cond)
            cq = cq.where(cond)
    q = q.order_by(DeadLetterEvent.created_at.desc()).limit(page.limit).offset(page.offset)
    total = int(await db.scalar(cq) or 0)
    rows = list(await db.scalars(q))
    return Paginated[dict](
        items=[_out(d) for d in rows],
        meta=PageMeta(total=total, limit=page.limit, offset=page.offset, returned=len(rows)),
    )


@router.get("/stats")
async def stats(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    rows = await db.execute(
        select(DeadLetterEvent.status, func.count(DeadLetterEvent.id)).group_by(
            DeadLetterEvent.status
        )
    )
    by_status = {s: int(c) for s, c in rows.all()}
    by_reason = {
        r: int(c)
        for r, c in (
            await db.execute(
                select(DeadLetterEvent.failure_reason, func.count(DeadLetterEvent.id))
                .where(DeadLetterEvent.status == "PENDING")
                .group_by(DeadLetterEvent.failure_reason)
            )
        ).all()
    }
    return {"by_status": by_status, "pending_by_reason": by_reason, "pending": by_status.get("PENDING", 0)}


@router.get("/{dlq_id}")
async def get_one(
    dlq_id: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    d = await db.get(DeadLetterEvent, dlq_id)
    if d is None:
        raise NotFoundError("DLQ entry not found")
    return {**_out(d), "raw_payload": d.raw_payload, "envelope": d.envelope}


@router.post("/{dlq_id}/retry")
async def retry(
    dlq_id: str,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    d = await db.get(DeadLetterEvent, dlq_id)
    if d is None:
        raise NotFoundError("DLQ entry not found")
    if d.status != "PENDING":
        raise ConflictError(f"entry is {d.status}, not PENDING")
    envelope = d.envelope or d.raw_payload
    if not isinstance(envelope, dict) or not envelope.get("event_id"):
        raise ConflictError("no replayable envelope stored for this entry")
    # Republish to events.raw so it re-enters normal processing. Idempotency
    # records (event_id, processor) mean already-processed consumers skip it.
    envelope = {**envelope}
    envelope.setdefault("metadata", {})
    if isinstance(envelope["metadata"], dict):
        envelope["metadata"]["_dlq_retry_of"] = dlq_id
    await get_producer().publish(RAW.name, {k: v for k, v in envelope.items() if not k.startswith("_")},
                                 key=envelope.get("partition_key") or envelope["event_id"])
    d.status = "RETRIED"
    d.last_action_at = datetime.now(UTC)
    d.last_action_by = actor.email
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="dlq.retry",
        resource_type="dead_letter_event", resource_id=dlq_id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"event_id": d.event_id, "processor": d.processor},
    )
    return {"ok": True, "id": dlq_id, "status": "RETRIED"}


@router.post("/{dlq_id}/discard")
async def discard(
    dlq_id: str,
    request: Request,
    confirm: bool = Query(False),
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    if not confirm:
        raise ConflictError("destructive action; pass ?confirm=true")
    d = await db.get(DeadLetterEvent, dlq_id)
    if d is None:
        raise NotFoundError("DLQ entry not found")
    d.status = "DISCARDED"
    d.last_action_at = datetime.now(UTC)
    d.last_action_by = actor.email
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="dlq.discard",
        resource_type="dead_letter_event", resource_id=dlq_id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"event_id": d.event_id},
    )
    return {"ok": True, "id": dlq_id, "status": "DISCARDED"}
