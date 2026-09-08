"""Health, readiness and Prometheus metrics. Mounted unprefixed."""

from __future__ import annotations

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

from pravaha.db import session_scope
from pravaha.kafka.producer import get_producer
from pravaha.observability.metrics import DEP_UP
from pravaha.redis_client import redis_healthy

router = APIRouter(tags=["health"])


async def _check_postgres() -> bool:
    try:
        async with session_scope() as s:
            await s.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


async def _check_kafka() -> bool:
    try:
        return await get_producer().healthy()
    except Exception:  # noqa: BLE001
        return False


@router.get("/health")
@router.get("/api/v1/health")
async def health() -> dict:
    return {"status": "ok", "service": "pravaha-api"}


@router.get("/api/v1/ready")
@router.get("/ready")
async def ready(response: Response) -> dict:
    pg = await _check_postgres()
    rd = await redis_healthy()
    kf = await _check_kafka()
    DEP_UP.labels(dependency="postgres").set(1 if pg else 0)
    DEP_UP.labels(dependency="redis").set(1 if rd else 0)
    DEP_UP.labels(dependency="kafka").set(1 if kf else 0)
    deps = {"postgres": pg, "redis": rd, "kafka": kf}
    ok = pg and kf  # redis is degrade-able, not required for readiness
    if not ok:
        response.status_code = 503
    return {"ready": ok, "dependencies": deps}


@router.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
