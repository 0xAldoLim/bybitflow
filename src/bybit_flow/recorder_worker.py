"""One bounded recorder subprocess. A stalled writer can be replaced safely.

Batch identifiers make publication idempotent if the writer commits and dies
before acknowledging. Partial files are overwritten only for that same batch.
"""

import asyncio
import multiprocessing
import sqlite3


def serve(connection, root, settings):
    from .config import Settings
    from .storage import Recorder, Store

    store = Store.__new__(Store)
    store.root = root
    store.db = sqlite3.connect(root / "research.sqlite", timeout=10)
    store.db.row_factory = sqlite3.Row
    recorder = Recorder(store, Settings(_env_file=None, **settings))
    try:
        while True:
            request = connection.recv()
            if request is None:
                return
            ident, batch = request
            try:
                recorder.flush(batch, store, ident=ident)
                connection.send(
                    dict(
                        ok=True,
                        disk_bytes=recorder.disk_bytes,
                        metrics={
                            key: getattr(recorder, key)
                            for key in (
                                "normalization_ms",
                                "parquet_write_ms",
                                "raw_write_ms",
                                "sqlite_write_ms",
                                "segment_flush_latency_ms",
                            )
                        },
                    )
                )
            except Exception as exc:
                connection.send(dict(ok=False, error_type=type(exc).__name__))
    except EOFError:
        pass
    finally:
        store.close()
        connection.close()


class ProcessWriter:
    def __init__(self, recorder):
        self.recorder = recorder
        self.process = self.connection = None

    def start(self):
        context = multiprocessing.get_context("spawn")
        self.connection, child = context.Pipe()
        self.process = context.Process(
            target=serve,
            args=(child, self.recorder.store.root, self.recorder.settings.model_dump()),
            daemon=True,
        )
        self.process.start()
        child.close()

    async def write(self, ident, batch):
        if self.process is None or not self.process.is_alive():
            self.close()
            self.start()
        # Serialization/pipe backpressure also stays off the websocket loop.
        try:
            await asyncio.wait_for(asyncio.to_thread(self.connection.send, (ident, batch)), 15)
        except (TimeoutError, OSError):
            self.close()
            raise
        deadline = asyncio.get_running_loop().time() + 60
        while not self.connection.poll():
            if not self.process.is_alive():
                self.close()
                raise OSError("Recorder worker exited before acknowledgement")
            if asyncio.get_running_loop().time() >= deadline:
                self.close()
                raise TimeoutError("Recorder writer stalled; worker replaced on retry")
            await asyncio.sleep(0.01)
        response = self.connection.recv()
        if not response["ok"]:
            from .storage import StorageBudgetExceeded

            if response["error_type"] == "StorageBudgetExceeded":
                raise StorageBudgetExceeded("Recording budget reached")
            raise OSError("Recorder worker failed: " + response["error_type"])
        self.recorder.disk_bytes = response["disk_bytes"]
        for key, value in response["metrics"].items():
            setattr(self.recorder, key, value)
        self.recorder.written += len(batch)

    def close(self):
        if self.process:
            if self.process.is_alive():
                self.process.terminate()
            self.process.join(timeout=2)
            if self.process.is_alive():
                self.process.kill()
                self.process.join(timeout=1)
        if self.connection:
            self.connection.close()
        self.process = self.connection = None
