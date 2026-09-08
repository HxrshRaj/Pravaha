"""Background worker that executes queued replay jobs.

Polls ``replay_jobs`` for rows in status CREATED and runs them one at a time via
:func:`run_replay_job`. Keeping replay in a dedicated worker (not the API
process) means a large replay never blocks request handling and progress is
observable through the job row.
"""

from __future__ import annotations

import asyncio
import contextlib

from sqlalchemy import select

from pravaha.db import get_session_factory, session_scope
from pravaha.kafka.producer import get_producer
from pravaha.logging import get_logger
from pravaha.models import ReplayJob
from pravaha.replay.engine import run_replay_job

log = get_logger(__name__)

POLL_INTERVAL_SECONDS = 3


class ReplayRunner:
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    async def run(self) -> None:
        producer = get_producer()
        await producer.start()
        log.info("replay_runner.started")
        try:
            while not self._stop.is_set():
                job_id = await self._next_job()
                if job_id is None:
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self._stop.wait(), timeout=POLL_INTERVAL_SECONDS)
                    continue
                try:
                    await run_replay_job(get_session_factory(), producer, job_id)
                except Exception as exc:  # noqa: BLE001
                    log.error("replay_runner.job_failed", job_id=job_id, error=str(exc))
        finally:
            await producer.stop()

    async def _next_job(self) -> str | None:
        async with session_scope() as s:
            row = await s.scalar(
                select(ReplayJob.id)
                .where(ReplayJob.status == "CREATED")
                .order_by(ReplayJob.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            return row

    def stop(self) -> None:
        self._stop.set()


async def main() -> None:
    from pravaha.logging import configure_logging

    configure_logging()
    await ReplayRunner().run()


if __name__ == "__main__":
    asyncio.run(main())
