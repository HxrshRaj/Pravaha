"""Minimal stdlib HTTP health server for worker processes.

Serves /healthz (process alive), /readyz (deps reachable) and /metrics
(Prometheus text). Runs in a background thread so it never blocks the asyncio
event loop the worker uses.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from pravaha.logging import get_logger

log = get_logger(__name__)


def _readiness() -> tuple[bool, dict]:
    import asyncio

    async def _check() -> dict:
        from pravaha.kafka.producer import get_producer
        from pravaha.redis_client import redis_healthy

        out: dict = {}
        try:
            out["redis"] = await redis_healthy()
        except Exception:  # noqa: BLE001
            out["redis"] = False
        try:
            out["kafka"] = await get_producer().healthy()
        except Exception:  # noqa: BLE001
            out["kafka"] = False
        try:
            from sqlalchemy import text

            from pravaha.db import session_scope

            async with session_scope() as s:
                await s.execute(text("SELECT 1"))
            out["postgres"] = True
        except Exception:  # noqa: BLE001
            out["postgres"] = False
        return out

    try:
        deps = asyncio.run(_check())
    except RuntimeError:
        # already inside a loop (shouldn't happen in the thread) - best effort
        deps = {"redis": None, "kafka": None, "postgres": None}
    ready = all(v for v in deps.values() if v is not None) and bool(deps)
    return ready, deps


class _Handler(BaseHTTPRequestHandler):
    worker_name = "worker"

    def _send(self, code: int, body: bytes, content_type: str = "application/json") -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.rstrip("/") in ("/healthz", "/health"):
            self._send(200, json.dumps({"status": "ok", "worker": self.worker_name}).encode())
        elif self.path.rstrip("/") in ("/readyz", "/ready"):
            ready, deps = _readiness()
            self._send(
                200 if ready else 503,
                json.dumps({"ready": ready, "dependencies": deps}).encode(),
            )
        elif self.path.rstrip("/") == "/metrics":
            self._send(200, generate_latest(), CONTENT_TYPE_LATEST)
        else:
            self._send(404, b'{"error":"not found"}')

    def log_message(self, *_args) -> None:  # silence default stderr logging
        return


def start_health_server(port: int, worker_name: str) -> ThreadingHTTPServer:
    _Handler.worker_name = worker_name
    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True, name="health")
    t.start()
    log.info("worker.health_server_started", port=port, worker=worker_name)
    return server
