from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.audit import audit
from pravaha.api.deps import CurrentUser, get_db, require_engineer, require_viewer
from pravaha.api.errors import ConflictError, NotFoundError, ValidationFailedError
from pravaha.api.schemas import CreatePipelineRequest, UpdatePipelineDraftRequest
from pravaha.models import (
    Pipeline,
    PipelineEdge,
    PipelineExecution,
    PipelineNode,
    PipelineVersion,
)
from pravaha.processing.pipeline_spec import validate_graph

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


def _pipeline_out(p: Pipeline, versions: list[PipelineVersion]) -> dict:
    return {
        "id": p.id,
        "name": p.name,
        "description": p.description,
        "active_version_id": p.active_version_id,
        "versions": [
            {
                "id": v.id,
                "version": v.version,
                "status": v.status,
                "published_at": v.published_at.isoformat() if v.published_at else None,
                "published_by": v.published_by,
                "node_count": len((v.graph or {}).get("nodes", [])),
            }
            for v in sorted(versions, key=lambda x: x.version)
        ],
    }


@router.get("")
async def list_pipelines(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    pipes = list(await db.scalars(select(Pipeline).order_by(Pipeline.created_at.desc())))
    out = []
    for p in pipes:
        vs = list(await db.scalars(select(PipelineVersion).where(PipelineVersion.pipeline_id == p.id)))
        out.append(_pipeline_out(p, vs))
    return out


@router.post("/validate")
async def validate(body: dict, _: CurrentUser = Depends(require_viewer)):
    graph = body.get("graph", body)
    res = validate_graph(graph)
    return {
        "ok": res.ok,
        "errors": res.errors,
        "warnings": res.warnings,
        "topological_order": res.topological_order,
    }


@router.post("", status_code=201)
async def create_pipeline(
    body: CreatePipelineRequest,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    if await db.scalar(select(Pipeline.id).where(Pipeline.name == body.name)):
        raise ConflictError("a pipeline with that name exists")
    graph = body.graph.model_dump()
    res = validate_graph(graph)
    # draft may be invalid, but warn
    pipeline = Pipeline(name=body.name, description=body.description)
    db.add(pipeline)
    await db.flush()
    version = PipelineVersion(pipeline_id=pipeline.id, version=1, status="DRAFT", graph=graph)
    db.add(version)
    await db.flush()
    await _sync_nodes_edges(db, version, graph)
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="pipeline.create",
        resource_type="pipeline", resource_id=pipeline.id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"name": body.name, "valid": res.ok},
    )
    return {"id": pipeline.id, "draft_version": 1, "validation": {"ok": res.ok, "errors": res.errors}}


@router.get("/{pipeline_id}")
async def get_pipeline(
    pipeline_id: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    p = await db.get(Pipeline, pipeline_id)
    if p is None:
        raise NotFoundError("pipeline not found")
    vs = list(await db.scalars(select(PipelineVersion).where(PipelineVersion.pipeline_id == p.id)))
    return _pipeline_out(p, vs)


@router.get("/{pipeline_id}/versions/{version}")
async def get_version(
    pipeline_id: str,
    version: int,
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    v = await db.scalar(
        select(PipelineVersion).where(
            PipelineVersion.pipeline_id == pipeline_id, PipelineVersion.version == version
        )
    )
    if v is None:
        raise NotFoundError("pipeline version not found")
    return {
        "id": v.id,
        "version": v.version,
        "status": v.status,
        "graph": v.graph,
        "published_at": v.published_at.isoformat() if v.published_at else None,
    }


@router.put("/{pipeline_id}/draft")
async def update_draft(
    pipeline_id: str,
    body: UpdatePipelineDraftRequest,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    p = await db.get(Pipeline, pipeline_id)
    if p is None:
        raise NotFoundError("pipeline not found")
    latest = await db.scalar(
        select(PipelineVersion)
        .where(PipelineVersion.pipeline_id == pipeline_id)
        .order_by(PipelineVersion.version.desc())
    )
    graph = body.graph.model_dump()
    if latest is None or latest.status != "DRAFT":
        new_version = (latest.version + 1) if latest else 1
        draft = PipelineVersion(
            pipeline_id=pipeline_id, version=new_version, status="DRAFT", graph=graph
        )
        db.add(draft)
        await db.flush()
    else:
        draft = latest
        draft.graph = graph
        await db.flush()
    await _sync_nodes_edges(db, draft, graph)
    res = validate_graph(graph)
    return {"version": draft.version, "status": "DRAFT", "validation": {"ok": res.ok, "errors": res.errors}}


@router.post("/{pipeline_id}/versions/{version}/publish")
async def publish(
    pipeline_id: str,
    version: int,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    v = await db.scalar(
        select(PipelineVersion).where(
            PipelineVersion.pipeline_id == pipeline_id, PipelineVersion.version == version
        )
    )
    if v is None:
        raise NotFoundError("pipeline version not found")
    if v.status == "PUBLISHED":
        raise ConflictError("version already published (published versions are immutable)")
    res = validate_graph(v.graph or {})
    if not res.ok:
        raise ValidationFailedError(
            "pipeline graph is invalid", code="PIPELINE_INVALID", details={"errors": res.errors}
        )
    v.status = "PUBLISHED"
    v.published_at = datetime.now(UTC)
    v.published_by = actor.email
    p = await db.get(Pipeline, pipeline_id)
    p.active_version_id = v.id
    # disable other published versions
    others = await db.scalars(
        select(PipelineVersion).where(
            PipelineVersion.pipeline_id == pipeline_id,
            PipelineVersion.id != v.id,
            PipelineVersion.status == "PUBLISHED",
        )
    )
    for o in others:
        o.status = "DISABLED"
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="pipeline.publish",
        resource_type="pipeline", resource_id=pipeline_id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"version": version},
    )
    return {"pipeline_id": pipeline_id, "version": version, "status": "PUBLISHED"}


@router.get("/{pipeline_id}/metrics")
async def metrics(
    pipeline_id: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    rows = await db.scalars(
        select(PipelineExecution)
        .where(PipelineExecution.pipeline_id == pipeline_id)
        .order_by(PipelineExecution.window_start.desc())
        .limit(120)
    )
    items = list(rows)
    return {
        "pipeline_id": pipeline_id,
        "windows": [
            {
                "window_start": i.window_start.isoformat(),
                "events_processed": i.events_processed,
                "events_failed": i.events_failed,
                "events_dlq": i.events_dlq,
                "retries": i.retries,
                "throughput_eps": i.throughput_eps,
                "latency_ms_p95": i.latency_ms_p95,
            }
            for i in items
        ],
        "totals": {
            "events_processed": sum(i.events_processed for i in items),
            "events_failed": sum(i.events_failed for i in items),
            "events_dlq": sum(i.events_dlq for i in items),
        },
    }


async def _sync_nodes_edges(db: AsyncSession, version: PipelineVersion, graph: dict) -> None:
    from sqlalchemy import delete

    await db.execute(delete(PipelineNode).where(PipelineNode.pipeline_version_id == version.id))
    await db.execute(delete(PipelineEdge).where(PipelineEdge.pipeline_version_id == version.id))
    for n in graph.get("nodes", []):
        db.add(
            PipelineNode(
                pipeline_version_id=version.id,
                node_key=n.get("key", ""),
                node_type=n.get("type", "filter"),
                config=n.get("config") or {},
            )
        )
    for e in graph.get("edges", []):
        db.add(
            PipelineEdge(
                pipeline_version_id=version.id,
                from_node=e.get("from", ""),
                to_node=e.get("to", ""),
            )
        )
    await db.flush()
