from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.api.audit import audit
from pravaha.api.deps import CurrentUser, get_db, require_engineer, require_viewer
from pravaha.api.errors import ConflictError, NotFoundError
from pravaha.api.schemas import CreateAlertRuleRequest, UpdateAlertRuleRequest
from pravaha.models import Alert, AlertRule

router = APIRouter(prefix="/alerts", tags=["alerts"])


def _rule_out(r: AlertRule) -> dict:
    return {
        "id": r.id,
        "name": r.name,
        "description": r.description,
        "metric": r.metric,
        "group_key": r.group_key,
        "operator": r.operator,
        "threshold": r.threshold,
        "duration_seconds": r.duration_seconds,
        "severity": r.severity,
        "cooldown_seconds": r.cooldown_seconds,
        "enabled": r.enabled,
        "last_fired_at": r.last_fired_at.isoformat() if r.last_fired_at else None,
    }


@router.get("/rules")
async def list_rules(_: CurrentUser = Depends(require_viewer), db: AsyncSession = Depends(get_db)):
    rows = await db.scalars(select(AlertRule).order_by(AlertRule.created_at.desc()))
    return [_rule_out(r) for r in rows]


@router.post("/rules", status_code=201)
async def create_rule(
    body: CreateAlertRuleRequest,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    if await db.scalar(select(AlertRule.id).where(AlertRule.name == body.name)):
        raise ConflictError("an alert rule with that name exists")
    rule = AlertRule(**body.model_dump())
    db.add(rule)
    await db.flush()
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="alert_rule.create",
        resource_type="alert_rule", resource_id=rule.id,
        request_id=getattr(request.state, "request_id", None), metadata=body.model_dump(),
    )
    return _rule_out(rule)


@router.patch("/rules/{rule_id}")
async def update_rule(
    rule_id: str,
    body: UpdateAlertRuleRequest,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    rule = await db.get(AlertRule, rule_id)
    if rule is None:
        raise NotFoundError("alert rule not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(rule, k, v)
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="alert_rule.update",
        resource_type="alert_rule", resource_id=rule_id,
        request_id=getattr(request.state, "request_id", None),
        metadata=body.model_dump(exclude_none=True),
    )
    return _rule_out(rule)


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(
    rule_id: str,
    request: Request,
    actor: CurrentUser = Depends(require_engineer),
    db: AsyncSession = Depends(get_db),
):
    rule = await db.get(AlertRule, rule_id)
    if rule is None:
        raise NotFoundError("alert rule not found")
    await db.delete(rule)
    await audit(
        db, actor=actor.email, actor_role=actor.role.value, action="alert_rule.delete",
        resource_type="alert_rule", resource_id=rule_id,
        request_id=getattr(request.state, "request_id", None),
    )


@router.get("")
async def list_alerts(
    _: CurrentUser = Depends(require_viewer),
    db: AsyncSession = Depends(get_db),
    since_minutes: int = Query(1440, ge=1, le=43200),
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    start = datetime.now(UTC) - timedelta(minutes=since_minutes)
    q = select(Alert).where(Alert.fired_at >= start)
    if status:
        q = q.where(Alert.status == status)
    q = q.order_by(Alert.fired_at.desc()).limit(limit)
    rows = await db.scalars(q)
    return [
        {
            "id": a.id,
            "rule_name": a.rule_name,
            "metric": a.metric,
            "severity": a.severity,
            "observed_value": a.observed_value,
            "threshold": a.threshold,
            "fired_at": a.fired_at.isoformat(),
            "resolved_at": a.resolved_at.isoformat() if a.resolved_at else None,
            "status": a.status,
            "context": a.context,
        }
        for a in rows
    ]
