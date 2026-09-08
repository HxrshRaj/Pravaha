"""Consistent API error model.

Every error response is::

    {"error": {"code": "...", "message": "...", "details": {...}, "request_id": "..."}}
"""

from __future__ import annotations

from typing import Any

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import ORJSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


class APIError(Exception):
    status_code = status.HTTP_400_BAD_REQUEST
    code = "BAD_REQUEST"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        status_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        if status_code:
            self.status_code = status_code
        self.details = details or {}


class NotFoundError(APIError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "NOT_FOUND"


class ConflictError(APIError):
    status_code = status.HTTP_409_CONFLICT
    code = "CONFLICT"


class UnauthorizedError(APIError):
    status_code = status.HTTP_401_UNAUTHORIZED
    code = "UNAUTHORIZED"


class ForbiddenError(APIError):
    status_code = status.HTTP_403_FORBIDDEN
    code = "FORBIDDEN"


class RateLimitedError(APIError):
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    code = "RATE_LIMITED"


class PayloadTooLargeError(APIError):
    status_code = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
    code = "PAYLOAD_TOO_LARGE"


class ValidationFailedError(APIError):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "VALIDATION_FAILED"


def _body(code: str, message: str, details: dict[str, Any], request_id: str) -> dict:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details,
            "request_id": request_id,
        }
    }


def _rid(request: Request) -> str:
    return getattr(request.state, "request_id", "") or request.headers.get("x-request-id", "")


def install_exception_handlers(app) -> None:  # noqa: ANN001
    @app.exception_handler(APIError)
    async def _api_error(request: Request, exc: APIError):  # noqa: ANN202
        return ORJSONResponse(
            status_code=exc.status_code,
            content=_body(exc.code, exc.message, exc.details, _rid(request)),
        )

    @app.exception_handler(RequestValidationError)
    async def _req_validation(request: Request, exc: RequestValidationError):  # noqa: ANN202
        return ORJSONResponse(
            status_code=422,
            content=_body(
                "VALIDATION_FAILED",
                "request validation failed",
                {"errors": exc.errors()},
                _rid(request),
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_exc(request: Request, exc: StarletteHTTPException):  # noqa: ANN202
        code = {
            401: "UNAUTHORIZED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            405: "METHOD_NOT_ALLOWED",
            429: "RATE_LIMITED",
        }.get(exc.status_code, "HTTP_ERROR")
        return ORJSONResponse(
            status_code=exc.status_code,
            content=_body(code, str(exc.detail), {}, _rid(request)),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception):  # noqa: ANN202
        from pravaha.logging import get_logger

        get_logger("pravaha.api").exception("api.unhandled_exception", path=request.url.path)
        return ORJSONResponse(
            status_code=500,
            content=_body("INTERNAL_ERROR", "an unexpected error occurred", {}, _rid(request)),
        )
