"""Restart failed subsystem tasks with bounded delay; cancellation remains shutdown."""

import asyncio
import random

from .storage import now_ms


async def supervise(store, name, factory):
    failures = 0
    while True:
        previous = store.get("supervisor:" + name, {})
        store.put(
            "supervisor:" + name, previous | dict(state="RUNNING", last_attempt_ms=now_ms(), retries=failures)
        )
        try:
            await factory()
            raise RuntimeError("Long-running subsystem exited unexpectedly")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            delay = min(60, 2 ** min(failures, 6)) + random.random()
            store.put(
                "supervisor:" + name,
                dict(
                    state="RECOVERING",
                    last_error_type=type(exc).__name__,
                    last_error_ms=now_ms(),
                    retries=failures,
                    retry_seconds=delay,
                ),
            )
            await asyncio.sleep(delay)
