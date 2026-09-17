"""FastAPI application factory."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

from pravaha.api.bootstrap import ensure_bootstrap_admin, ensure_kafka_topics
from pravaha.api.errors import install_exception_handlers
from pravaha.api.middleware import ContextMiddleware, SecurityHeadersMiddleware
from pravaha.config import settings
from pravaha.logging import configure_logging, get_logger
from pravaha.observability.metrics import DEP_UP  # noqa: F401

log = get_logger("pravaha.api")

API_PREFIX = "/api/v1"


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN201
    configure_logging()
    log.info("api.starting", env=settings.env)
    from pravaha.kafka.producer import get_producer

    try:
        await get_producer().start()
    except Exception as exc:  # noqa: BLE001
        log.warning("api.kafka_producer_start_deferred", error=str(exc))
    await ensure_kafka_topics()
    try:
        await ensure_bootstrap_admin()
    except Exception as exc:  # noqa: BLE001
        log.warning("api.bootstrap_admin_deferred", error=str(exc))
    yield
    log.info("api.stopping")
    try:
        await get_producer().stop()
    except Exception:  # noqa: BLE001
        pass
    from pravaha.db import dispose_engine
    from pravaha.redis_client import close_redis

    await close_redis()
    await dispose_engine()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Pravaha API",
        version="0.1.0",
        description="Real-Time Data Streaming & Event Intelligence Platform",
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
        docs_url="/docs",
        openapi_url="/openapi.json",
    )

    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(ContextMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID"],
    )

    install_exception_handlers(app)

    from pravaha.api.routers import (
        ai,
        alerts,
        analytics,
        anomalies,
        audit,
        auth,
        batch,
        consumers,
        data_quality,
        dlq,
        events,
        health,
        live,
        pipelines,
        producers,
        replay,
        schemas,
        system,
    )

    app.include_router(health.router)  # unprefixed /health, /metrics
    for r in (
        auth.router,
        events.router,
        producers.router,
        schemas.router,
        pipelines.router,
        analytics.router,
        anomalies.router,
        alerts.router,
        batch.router,
        consumers.router,
        dlq.router,
        replay.router,
        data_quality.router,
        ai.router,
        audit.router,
        system.router,
        live.router,
    ):
        app.include_router(r, prefix=API_PREFIX)

    @app.get("/", include_in_schema=False)
    async def root():  # noqa: ANN202
        return {"name": "pravaha", "version": "0.1.0", "docs": "/docs"}

    return app
