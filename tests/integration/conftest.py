"""Integration test fixtures.

Spins up throwaway PostgreSQL + Redis + Kafka (KRaft) containers via
testcontainers, points ``pravaha.config.settings`` at them, runs Alembic, and
yields helpers. The whole module is skipped if Docker is not reachable so the
unit suite still runs anywhere.
"""

from __future__ import annotations

import os
import socket
import time

import pytest

pytestmark = pytest.mark.integration


def _docker_ok() -> bool:
    try:
        import docker  # noqa: PLC0415

        docker.from_env().ping()
        return True
    except Exception:  # noqa: BLE001
        return False


if not _docker_ok():  # pragma: no cover
    pytest.skip("Docker not available - skipping integration tests", allow_module_level=True)

from testcontainers.kafka import KafkaContainer  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402
from testcontainers.redis import RedisContainer  # noqa: E402


@pytest.fixture(scope="session")
def _infra():
    pg = PostgresContainer("postgres:16-alpine", dbname="pravaha", username="pravaha", password="pravaha")
    rd = RedisContainer("redis:7-alpine")
    kf = KafkaContainer("confluentinc/cp-kafka:7.7.1")
    pg.start()
    rd.start()
    kf.start()

    os.environ["POSTGRES_HOST"] = pg.get_container_host_ip()
    os.environ["POSTGRES_PORT"] = str(pg.get_exposed_port(5432))
    os.environ["POSTGRES_DB"] = "pravaha"
    os.environ["POSTGRES_USER"] = "pravaha"
    os.environ["POSTGRES_PASSWORD"] = "pravaha"
    os.environ["REDIS_HOST"] = rd.get_container_host_ip()
    os.environ["REDIS_PORT"] = str(rd.get_exposed_port(6379))
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = kf.get_bootstrap_server()
    os.environ["KAFKA_TOPIC_PREFIX"] = f"it{int(time.time())}."
    os.environ["KAFKA_DEFAULT_PARTITIONS"] = "3"

    # rebuild cached settings + engines against the new env
    from pravaha.config import get_settings

    get_settings.cache_clear()
    import importlib

    import pravaha.config as cfg

    importlib.reload(cfg)

    yield {"pg": pg, "rd": rd, "kf": kf}

    for c in (kf, rd, pg):
        try:
            c.stop()
        except Exception:  # noqa: BLE001
            pass


@pytest.fixture(scope="session")
def migrated(_infra):
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "alembic")
    command.upgrade(cfg, "head")
    return True


@pytest.fixture()
async def db_session(migrated):
    import importlib

    import pravaha.db as dbmod

    importlib.reload(dbmod)
    async with dbmod.session_scope() as s:
        yield s


@pytest.fixture(scope="session")
def free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port
