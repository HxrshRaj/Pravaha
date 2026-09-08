from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, BackgroundTasks, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.ai.investigator import create_investigation, run_investigation
from pravaha.ai.provider import available_providers
from pravaha.api.audit import audit
from pravaha.api.deps import (
    CurrentUser,
    get_db,
    require_analyst,
    require_viewer,
)
from pravaha.api.errors import NotFoundError
from pravaha.api.schemas import StartInvestigationRequest
from pravaha.config import settings
from pravaha.models import (
    AIEvidence,
    AIHypothesis,
    AIInvestigation,
    AIToolCall,
    AIUsage,
    AnomalyRecord,
)

router = APIRouter(prefix="/ai", tags=["ai"])


@router.get("/status")
async def status(_: CurrentUser = Depends(require_viewer)):
    providers = available_providers()
    return {
        "provider_order": settings.ai_providers,
        "available_providers": providers,
        "core_streaming_dependent_on_ai": False,
        "note": "AI unavailable does not affect event processing."
        if not providers or providers == ["mock"]
        else "AI providers configured.",
    }


@router.get("/investigations")
async def list_investigations(
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    q = select(AIInvestigation).order_by(AIInvestigation.created_at.desc()).limit(limit)
    if status:
        q = q.where(AIInvestigation.status == status)
    rows = await db.scalars(q)
    return [
        {
            "id": i.id,
            "anomaly_id": i.anomaly_id,
            "title": i.title,
            "status": i.status,
            "phase": i.phase,
            "provider": i.provider,
            "model": i.model,
            "root_cause": i.root_cause,
            "confidence": i.conclusion_confidence,
            "duration_ms": i.duration_ms,
            "created_at": i.created_at.isoformat(),
            "error": i.error,
        }
        for i in rows
    ]


@router.post("/investigations", status_code=202)
async def start_investigation(
    body: StartInvestigationRequest,
    request: Request,
    background: BackgroundTasks,
    actor: CurrentUser = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    anomaly = await db.get(AnomalyRecord, body.anomaly_id)
    if anomaly is None:
        raise NotFoundError("anomaly not found")
    inv_id = await create_investigation(body.anomaly_id, trigger="manual")
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="ai.investigation.start",
        resource_type="ai_investigation", resource_id=inv_id,
        request_id=getattr(request.state, "request_id", None),
        metadata={"anomaly_id": body.anomaly_id, "provider": body.provider},
    )
    background.add_task(run_investigation, inv_id, provider=body.provider)
    return {"investigation_id": inv_id, "status": "RUNNING", "detail": "running in background"}


@router.get("/investigations/{inv_id}")
async def get_investigation(
    inv_id: str, _: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)
):
    i = await db.get(AIInvestigation, inv_id)
    if i is None:
        raise NotFoundError("investigation not found")
    hyps = list(
        await db.scalars(
            select(AIHypothesis).where(AIHypothesis.investigation_id == inv_id).order_by(AIHypothesis.key)
        )
    )
    ev = list(
        await db.scalars(select(AIEvidence).where(AIEvidence.investigation_id == inv_id))
    )
    tcs = list(
        await db.scalars(
            select(AIToolCall).where(AIToolCall.investigation_id == inv_id).order_by(AIToolCall.seq)
        )
    )
    usage = list(
        await db.scalars(select(AIUsage).where(AIUsage.investigation_id == inv_id))
    )
    return {
        "id": i.id,
        "anomaly_id": i.anomaly_id,
        "title": i.title,
        "status": i.status,
        "phase": i.phase,
        "provider": i.provider,
        "model": i.model,
        "prompt_version": i.prompt_version,
        "summary": i.summary,
        "root_cause": i.root_cause,
        "impact": i.impact,
        "recommendations": i.recommendations,
        "confidence": i.conclusion_confidence,
        "error": i.error,
        "duration_ms": i.duration_ms,
        "created_at": i.created_at.isoformat(),
        "finished_at": i.finished_at.isoformat() if i.finished_at else None,
        "hypotheses": [
            {
                "key": h.key,
                "statement": h.statement,
                "explanation": h.explanation,
                "confidence": h.confidence,
                "status": h.status,
                "supporting": h.supporting,
                "contradicting": h.contradicting,
                "evidence_refs": h.evidence_refs,
            }
            for h in hyps
        ],
        "evidence": [
            {"ref": e.ref, "kind": e.kind, "source_tool": e.source_tool, "summary": e.summary}
            for e in ev
        ],
        "tool_calls": [
            {
                "seq": t.seq,
                "tool_name": t.tool_name,
                "arguments": t.arguments,
                "ok": t.ok,
                "error": t.error,
                "latency_ms": t.latency_ms,
                "result_summary": t.result_summary,
            }
            for t in tcs
        ],
        "usage": {
            "calls": len(usage),
            "prompt_tokens": sum(u.prompt_tokens for u in usage),
            "completion_tokens": sum(u.completion_tokens for u in usage),
            "total_tokens": sum(u.total_tokens for u in usage),
            "estimated_cost_usd": round(sum(u.estimated_cost_usd for u in usage), 6),
        },
    }


@router.get("/usage")
async def usage_summary(
    days: int = Query(7, ge=1, le=90),
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
):
    start = datetime.now(UTC) - timedelta(days=days)
    rows = await db.execute(
        select(
            AIUsage.provider,
            AIUsage.model,
            func.count(AIUsage.id),
            func.sum(AIUsage.prompt_tokens),
            func.sum(AIUsage.completion_tokens),
            func.sum(AIUsage.total_tokens),
            func.sum(AIUsage.estimated_cost_usd),
            func.avg(AIUsage.latency_ms),
        )
        .where(AIUsage.created_at >= start)
        .group_by(AIUsage.provider, AIUsage.model)
    )
    by_model = [
        {
            "provider": p,
            "model": m,
            "calls": int(c),
            "prompt_tokens": int(pt or 0),
            "completion_tokens": int(ct or 0),
            "total_tokens": int(tt or 0),
            "estimated_cost_usd": round(float(cost or 0.0), 6),
            "avg_latency_ms": round(float(lat or 0.0), 1),
        }
        for p, m, c, pt, ct, tt, cost, lat in rows.all()
    ]
    return {
        "days": days,
        "total_cost_usd": round(sum(x["estimated_cost_usd"] for x in by_model), 6),
        "total_tokens": sum(x["total_tokens"] for x in by_model),
        "by_model": by_model,
    }


@router.post("/investigations/{inv_id}/cancel")
async def cancel(
    inv_id: str,
    _: CurrentUser = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    i = await db.get(AIInvestigation, inv_id)
    if i is None:
        raise NotFoundError("investigation not found")
    if i.status in ("COMPLETED", "FAILED"):
        return {"ok": False, "status": i.status}
    i.status = "CANCELLED"
    i.finished_at = datetime.now(UTC)
    return {"ok": True, "status": "CANCELLED"}
