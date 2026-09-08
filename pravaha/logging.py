"""Structured logging setup based on structlog.

Every log line is emitted as a single JSON object (when ``PRAVAHA_LOG_JSON`` is
true) carrying whatever context has been bound: ``request_id``, ``trace_id``,
``event_id``, ``producer_id``, ``consumer_group``, ``pipeline_id`` etc.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from pravaha.config import settings

_configured = False


def configure_logging() -> None:
    global _configured
    if _configured:
        return

    level = getattr(logging, settings.log_level, logging.INFO)
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.log_json:
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    configure_logging()
    return structlog.get_logger(name)


def bind_context(**kwargs: Any) -> None:
    """Bind key/values onto the contextvar-scoped logger for this task/request."""
    structlog.contextvars.bind_contextvars(**{k: v for k, v in kwargs.items() if v is not None})


def clear_context() -> None:
    structlog.contextvars.clear_contextvars()
