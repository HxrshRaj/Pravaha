"""HTTP middleware: request-id + structured access logs + Prometheus + body cap
+ security headers."""

from __future__ import annotations

import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from pravaha.api.errors import PayloadTooLargeError
from pravaha.config import settings
from pravaha.logging import bind_context, clear_context, get_logger
from pravaha.observability.metrics import HTTP_LATENCY, HTTP_REQUESTS_TOTAL

log = get_logger("pravaha.api.access")

_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cross-Origin-Opener-Policy": "same-origin",
}


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", request.url.path)


class ContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # noqa: ANN001
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        trace_id = request.headers.get("x-trace-id") or request_id
        request.state.request_id = request_id
        request.state.trace_id = trace_id
        bind_context(request_id=request_id, trace_id=trace_id, path=request.url.path)

        cl = request.headers.get("content-length")
        if cl and cl.isdigit() and int(cl) > settings.ingest_max_body_bytes:
            clear_context()
            raise PayloadTooLargeError(
                "request body exceeds limit",
                details={"limit_bytes": settings.ingest_max_body_bytes},
            )

        started = time.perf_counter()
        status_code = 500
        try:
            response: Response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            elapsed = time.perf_counter() - started
            path_t = _route_template(request)
            HTTP_REQUESTS_TOTAL.labels(
                method=request.method, path=path_t, status=str(status_code)
            ).inc()
            HTTP_LATENCY.labels(method=request.method, path=path_t).observe(elapsed)
            log.info(
                "http.request",
                method=request.method,
                path=request.url.path,
                route=path_t,
                status=status_code,
                duration_ms=round(elapsed * 1000, 2),
                client=request.client.host if request.client else None,
            )
            clear_context()


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # noqa: ANN001
        response: Response = await call_next(request)
        for k, v in _SECURITY_HEADERS.items():
            response.headers.setdefault(k, v)
        rid = getattr(request.state, "request_id", None)
        if rid:
            response.headers.setdefault("X-Request-ID", rid)
        return response
