from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.audit import audit
from pravaha.api.deps import CurrentUser, get_db, require_engineer, require_viewer
from pravaha.api.errors import ConflictError, NotFoundError, ValidationFailedError
from pravaha.api.schemas import CreateReplayRequest
from pravaha.models import ReplayJob
from pravaha.replay.engine import estimate_replay

router = APIRouter(prefix="/replay", tags=["replay"])


def _out(j: ReplayJob) -> dict:
    return {
        "id": j.id,
        "status": j.status,
        "requested_by": j.requested_by,
        "time_from": j.time_from.isoformat(),
        "time_to": j.time_to.isoformat(),
        "filter_event_type": j.filter_event_type,
        "filter_producer_id": j.filter_producer_id,
        "target_topic": j.target_topic,
        "estimated_count": j.estimated_count,
        "replayed_count": j.replayed_count,
        "failed_count": j.failed_count,
        "started_at": j.started_at.isoformat() if j.started_at else None,
        "finished_at": j.finished_at.isoformat() if j.finished_at else None,
        "error": j.error,
        "stats": j.stats,
        "created_at": j.created_at.isoformat(),
    }


@router.post("/estimate")
async def estimate(
    body: CreateReplayRequest,
    _: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    if body.time_to <= body.time_from:
        raise ValidationFailedError("time_to must be after time_from")
    tmp = ReplayJob(
        time_from=body.time_from,
        time_to=body.time_to,
        filter_event_type=body.filter_event_type,
        filter_producer_id=body.filter_producer_id,
        target_topic=body.target_topic,
    )
    count = await estimate_replay(db, tmp)
    return {"estimated_count": count}


@router.get("")
async def list_jobs(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    rows = await db.scalars(select(ReplayJob).order_by(ReplayJob.created_at.desc()).limit(100))
    return [_out(j) for j in rows]


@router.post("", status_code=201)
async def create_job(
    body: CreateReplayRequest,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    if body.time_to <= body.time_from:
        raise ValidationFailedError("time_to must be after time_from")
    if body.target_topic not in ("events.replay",):
        raise ValidationFailedError(
            "target_topic must be 'events.replay' (replay is isolated from live topics)"
        )
    job = ReplayJob(
        requested_by=actor.email,
        time_from=body.time_from,
        time_to=body.time_to,
        filter_event_type=body.filter_event_type,
        filter_producer_id=body.filter_producer_id,
        target_topic=body.target_topic,
        status="CREATED",
    )
    job.estimated_count = await estimate_replay(db, job)
    db.add(job)
    await db.flush()
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="replay.create",
        resource_type="replay_job", resource_id=job.id,
        request_id=getattr(request.state, "request_id", None),
        metadata={
            "time_from": body.time_from.isoformat(),
            "time_to": body.time_to.isoformat(),
            "estimated_count": job.estimated_count,
        },
    )
    return _out(job)


@router.get("/{job_id}")
async def get_job(
    job_id: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    j = await db.get(ReplayJob, job_id)
    if j is None:
        raise NotFoundError("replay job not found")
    return _out(j)


@router.post("/{job_id}/cancel")
async def cancel(
    job_id: str,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    j = await db.get(ReplayJob, job_id)
    if j is None:
        raise NotFoundError("replay job not found")
    if j.status in ("COMPLETED", "FAILED", "CANCELLED"):
        raise ConflictError(f"job already {j.status}")
    j.status = "CANCELLED"
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="replay.cancel",
        resource_type="replay_job", resource_id=job_id,
        request_id=getattr(request.state, "request_id", None),
    )
    return _out(j)
