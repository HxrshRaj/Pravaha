"""One-time startup tasks: ensure Kafka topics exist and a bootstrap admin user."""

from __future__ import annotations

from sqlalchemy import select

from pravaha.auth.security import hash_password
from pravaha.config import settings
from pravaha.db import session_scope
from pravaha.logging import get_logger
from pravaha.models import User
from pravaha.models.users import Role

log = get_logger(__name__)


async def ensure_bootstrap_admin() -> None:
    async with session_scope() as s:
        existing = await s.scalar(
            select(User).where(User.email == settings.bootstrap_admin_email)
        )
        if existing is not None:
            return
        s.add(
            User(
                email=settings.bootstrap_admin_email,
                full_name="Bootstrap Admin",
                password_hash=hash_password(settings.bootstrap_admin_password),
                role=Role.ADMIN,
                is_active=True,
            )
        )
        log.info("bootstrap.admin_created", email=settings.bootstrap_admin_email)


async def ensure_kafka_topics() -> None:
    from pravaha.kafka.topics import ensure_topics

    try:
        created = await ensure_topics()
        if created:
            log.info("bootstrap.topics_created", topics=created)
    except Exception as exc:  # noqa: BLE001
        log.warning("bootstrap.topic_ensure_failed", error=str(exc))
