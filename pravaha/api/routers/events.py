from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.deps import (
    CurrentUser,
    Page,
    authed_producer,
    enforce_rate_limit,
    get_db,
    pagination,
    require_analyst,
)
from pravaha.api.errors import NotFoundError, ValidationFailedError
from pravaha.api.schemas import (
    BatchIngestResponse,
    EventOut,
    IngestResponse,
    PageMeta,
    Paginated,
)
from pravaha.config import settings
from pravaha.ingestion.service import IngestionRejected, ingest_event
from pravaha.kafka.producer import get_producer
from pravaha.models import Event, Producer

router = APIRouter(prefix="/events", tags=["events"])


def _event_out(e: Event) -> EventOut:
    return EventOut(
        event_id=e.event_id,
        event_type=e.event_type,
        event_version=e.event_version,
        producer=e.producer,
        producer_id=e.producer_id,
        event_time=e.event_time,
        ingestion_time=e.ingestion_time,
        partition_key=e.partition_key,
        kafka_partition=e.kafka_partition,
        kafka_offset=e.kafka_offset,
        kafka_topic=e.kafka_topic,
        correlation_id=e.correlation_id,
        trace_id=e.trace_id,
        region=e.region,
        lateness_at_ingest=e.lateness_at_ingest.value
        if hasattr(e.lateness_at_ingest, "value")
        else str(e.lateness_at_ingest),
        is_replay=e.is_replay,
        payload=e.payload,
        metadata=e.event_metadata,
        amount=e.amount,
    )


@router.post("", response_model=IngestResponse, status_code=202)
async def ingest_one(
    body: dict,
    request: Request,
    producer: Producer = Depends(authed_producer),
    db: AsyncSession = Depends(get_db),
):
    await enforce_rate_limit(request, f"ingest:{producer.id}", producer.rate_limit_per_min)
    try:
        result = await ingest_event(db, producer, body, producer_obj=get_producer())
    except IngestionRejected as exc:
        raise ValidationFailedError(str(exc), code=exc.code, details=exc.details) from exc
    return IngestResponse(**result.to_dict())


@router.post("/batch", response_model=BatchIngestResponse, status_code=202)
async def ingest_batch(
    body: dict,
    request: Request,
    producer: Producer = Depends(authed_producer),
    db: AsyncSession = Depends(get_db),
):
    events = body.get("events")
    if not isinstance(events, list) or not events:
        raise ValidationFailedError("body must be {\"events\": [...]} with at least one event")
    if len(events) > settings.ingest_max_batch_size:
        raise ValidationFailedError(
            f"batch too large ({len(events)} > {settings.ingest_max_batch_size})",
            code="BATCH_TOO_LARGE",
        )
    await enforce_rate_limit(
        request, f"ingest:{producer.id}", producer.rate_limit_per_min
    )

    from pravaha.ingestion.service import _known_event_types  # reuse cache within request

    known = await _known_event_types(db)
    results: list[IngestResponse] = []
    accepted = rejected = duplicate = 0
    producer_obj = get_producer()
    for raw in events:
        try:
            r = await ingest_event(
                db, producer, raw if isinstance(raw, dict) else {}, producer_obj=producer_obj, known_types=known
            )
            results.append(IngestResponse(**r.to_dict()))
            if r.outcome.value == "accepted":
                accepted += 1
            elif r.outcome.value == "duplicate":
                duplicate += 1
            else:
                rejected += 1
        except IngestionRejected as exc:
            rejected += 1
            results.append(
                IngestResponse(
                    outcome="rejected",
                    event_id=(raw or {}).get("event_id", "unknown"),
                    event_type=(raw or {}).get("event_type", "unknown"),
                    valid=False,
                    quality_flags=[],
                    schema_version=None,
                    kafka=None,
                    errors=[f"{exc.code}: {exc}"],
                )
            )
    return BatchIngestResponse(
        accepted=accepted, rejected=rejected, duplicate=duplicate, results=results
    )


@router.get("", response_model=Paginated[EventOut])
async def list_events(
    page: Page = Depends(pagination),
    _: CurrentUser = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
    event_type: str | None = Query(None),
    producer_id: str | None = Query(None),
    correlation_id: str | None = Query(None),
    region: str | None = Query(None),
    is_replay: bool | None = Query(None),
    time_from: datetime | None = Query(None),
    time_to: datetime | None = Query(None),
):
    q = select(Event)
    cq = select(func.count(Event.id))
    conds = []
    if event_type:
        conds.append(Event.event_type == event_type)
    if producer_id:
        conds.append(Event.producer_id == producer_id)
    if correlation_id:
        conds.append(Event.correlation_id == correlation_id)
    if region:
        conds.append(Event.region == region)
    if is_replay is not None:
        conds.append(Event.is_replay.is_(is_replay))
    if time_from:
        conds.append(Event.event_time >= time_from)
    if time_to:
        conds.append(Event.event_time <= time_to)
    for c in conds:
        q = q.where(c)
        cq = cq.where(c)

    order_col = Event.event_time if (page.sort in (None, "event_time")) else getattr(
        Event, page.sort, Event.event_time
    )
    q = q.order_by(order_col.desc() if page.order == "desc" else order_col.asc())
    q = q.limit(page.limit).offset(page.offset)

    total = int(await db.scalar(cq) or 0)
    rows = list(await db.scalars(q))
    return Paginated[EventOut](
        items=[_event_out(e) for e in rows],
        meta=PageMeta(total=total, limit=page.limit, offset=page.offset, returned=len(rows)),
    )


@router.get("/{event_id}", response_model=EventOut)
async def get_one_event(
    event_id: str,
    _: CurrentUser = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    e = await db.scalar(select(Event).where(Event.event_id == event_id))
    if e is None:
        raise NotFoundError(f"event {event_id} not found")
    return _event_out(e)


@router.get("/{event_id}/correlated", response_model=list[EventOut])
async def get_correlated(
    event_id: str,
    _: CurrentUser = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    e = await db.scalar(select(Event).where(Event.event_id == event_id))
    if e is None:
        raise NotFoundError(f"event {event_id} not found")
    rows = await db.scalars(
        select(Event)
        .where(Event.correlation_id == e.correlation_id)
        .order_by(Event.event_time.asc())
        .limit(500)
    )
    return [_event_out(x) for x in rows]
