"""MaintenanceCoordinator ownership; the orchestration context supplies explicit runtime ports.

Trading algorithms are unchanged. This service owns scheduling/logic, while
shared session state remains on Scanner during the incremental extraction.
"""

import asyncio
import logging
from typing import Any, Protocol

from .storage import now_ms

log = logging.getLogger(__name__)
TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


class RuntimePorts(Protocol):
    settings: Any
    store: Any


class MaintenanceCoordinator:
    def __init__(self, runtime: RuntimePorts):
        self.runtime = runtime

    async def storage_loop(self):
        runtime = self.runtime
        from .retention import maintain as storage_maintain
        from .retention import storage_status
        from .storage import Store

        def maintain():
            store = Store(runtime.settings.data_dir)
            try:
                from .funnel import maintain as maintain_funnel

                maintain_funnel(store, now_ms())
                result = storage_maintain(store, runtime.settings)
                if now_ms() - store.get("storage_status", {}).get("at_ms", 0) >= 300_000:
                    storage_status(store, runtime.settings)
                store.put("storage_health", result)
                return result
            finally:
                store.close()

        while True:
            try:
                result = await asyncio.to_thread(maintain)
                runtime.store.put("storage_health", result)
            except Exception as exc:
                runtime.store.put("storage_health", dict(error_type=type(exc).__name__, at_ms=now_ms()))
            await asyncio.sleep(60)
