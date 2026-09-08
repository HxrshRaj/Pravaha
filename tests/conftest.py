"""Shared pytest config.

Unit tests import pure logic only and need no infrastructure. Integration tests
(``tests/integration``) use testcontainers and are skipped automatically when
Docker is unavailable.
"""

from __future__ import annotations

import os

os.environ.setdefault("PRAVAHA_ENV", "test")
os.environ.setdefault("PRAVAHA_LOG_JSON", "false")
os.environ.setdefault("API_JWT_SECRET", "test-secret-key-not-for-prod")
os.environ.setdefault("KAFKA_TOPIC_PREFIX", "test.")

import pytest


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"
