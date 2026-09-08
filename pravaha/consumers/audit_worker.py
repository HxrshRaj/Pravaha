"""Audit processor.

Consumer group ``pravaha.audit``. Consumes ``events.audit`` and appends rows to
``audit_logs``. API-side actions write to ``audit_logs`` directly *and* emit to
this topic so there is a single durable, append-only audit trail even if the API
DB write path changes.
"""

from __future__ import annotations

from pravaha.db import session_scope
from pravaha.kafka.consumer import ProcessingContext, StreamConsumer
from pravaha.logging import get_logger
from pravaha.models import AuditLog

log = get_logger(__name__)


class AuditProcessor(StreamConsumer):
    group_id = "pravaha.audit"
    processor = "audit"
    topics = ["events.audit"]
    envelope = False  # events.audit carries plain audit dicts, not envelopes

    async def handle(self, message: dict, ctx: ProcessingContext) -> None:
        data = message if isinstance(message, dict) else {}
        kind = data.get("kind", "audit.event")
        async with session_scope() as s:
            s.add(
                AuditLog(
                    actor=str(data.get("actor", "system")),
                    actor_role=str(data.get("actor_role", "")),
                    action=str(kind)[:64],
                    resource_type=str(data.get("resource_type", kind.split(".")[0]))[:48],
                    resource_id=str(data.get("resource_id") or data.get("alert_id") or "")[:64] or None,
                    result=str(data.get("result", "success"))[:16],
                    request_id=data.get("request_id"),
                    audit_metadata={k: v for k, v in data.items() if k not in ("actor", "actor_role")},
                    note=str(data.get("note", ""))[:2000],
                )
            )


async def main() -> None:
    from pravaha.logging import configure_logging

    configure_logging()
    await AuditProcessor().run()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
