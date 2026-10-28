"""Bounded SQLite measurements and an explicitly noncritical metadata writer."""

import asyncio
import json
import sqlite3
import threading
import time
from collections import OrderedDict, defaultdict, deque

DIAGNOSTIC_KEYS = frozenset(
    {
        "runtime_health",
        "scanner_performance",
        "clock_health",
        "clock_check_health",
        "quote_health",
        "scan_health",
        "operational_health",
        "storage_health",
    }
)


def diagnostic_key(key):
    return key in DIAGNOSTIC_KEYS or key.startswith("supervisor:")


def busy(exc):
    return isinstance(exc, sqlite3.OperationalError) and any(
        v in str(exc).lower() for v in ("locked", "busy")
    )


class Measurements:
    def __init__(self):
        self.samples = defaultdict(lambda: deque(maxlen=256))
        self.counts = defaultdict(int)
        self.lock = threading.Lock()

    def observe(self, name, value):
        with self.lock:
            self.samples[name].append(float(value))

    def increment(self, name):
        with self.lock:
            self.counts[name] += 1

    def snapshot(self):
        with self.lock:
            result = dict(self.counts)
            for key, values in self.samples.items():
                values = sorted(values)
                result[key] = sum(values) / len(values)
                result[key + "_max"] = max(values)
                result[key + "_p95"] = values[min(len(values) - 1, int(len(values) * 0.95))]
            return result


class MeasuredCursor(sqlite3.Cursor):
    def execute(self, sql, parameters=()):
        start = time.monotonic()
        try:
            return super().execute(sql, parameters)
        except sqlite3.OperationalError as exc:
            if busy(exc):
                self.connection.health.increment("sqlite_busy_events")
            raise
        finally:
            if (
                isinstance(sql, str)
                and sql.strip()
                and sql.lstrip().split(None, 1)[0].upper() in {"INSERT", "UPDATE", "DELETE", "REPLACE"}
            ):
                self.connection.health.observe("sqlite_write_latency_ms", (time.monotonic() - start) * 1000)
            elif isinstance(sql, str) and sql.lstrip().upper().startswith(("SELECT", "WITH", "PRAGMA")):
                self.connection.health.observe("sqlite_read_ms", (time.monotonic() - start) * 1000)

    def fetchall(self):
        start = time.monotonic()
        try:
            return super().fetchall()
        finally:
            self.connection.health.observe("sqlite_read_ms", (time.monotonic() - start) * 1000)

    def fetchone(self):
        start = time.monotonic()
        try:
            return super().fetchone()
        finally:
            self.connection.health.observe("sqlite_read_ms", (time.monotonic() - start) * 1000)


class MeasuredConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.health = Measurements()
        self.entered = []

    def execute(self, sql, parameters=()):
        return self.cursor(factory=MeasuredCursor).execute(sql, parameters)

    def __enter__(self):
        self.entered.append(time.monotonic())
        return super().__enter__()

    def __exit__(self, *args):
        start = self.entered.pop() if self.entered else time.monotonic()
        try:
            return super().__exit__(*args)
        finally:
            self.health.observe("sqlite_transaction_ms", (time.monotonic() - start) * 1000)


class MetadataWriter:
    """Coalesce diagnostics on the owning loop/connection; never contend with it.

    External writers still fail closed. No SQLite connection crosses threads, and
    an existing critical transaction is never committed by the diagnostic pump.
    """

    def __init__(self, db, measurements, capacity=64):
        self.db, self.metrics, self.capacity = db, measurements, capacity
        self.pending, self.overlay = OrderedDict(), {}
        self.sequence, self.stopping = 0, False
        self.wakeup = asyncio.Event()
        self.task = asyncio.create_task(self.run(), name="diagnostic-metadata")

    def offer(self, key, payload):
        if not diagnostic_key(key):
            raise ValueError("Critical metadata cannot be queued")
        self.sequence += 1
        value = (self.sequence, payload)
        if key not in self.pending and len(self.pending) >= self.capacity:
            old, dropped = self.pending.popitem(last=False)
            if self.overlay.get(old) == dropped:
                self.overlay.pop(old, None)
            self.metrics.increment("diagnostic_metadata_dropped")
        self.pending[key] = self.overlay[key] = value
        self.wakeup.set()

    def read(self, key):
        row = self.overlay.get(key)
        return (True, json.loads(row[1])) if row else (False, None)

    async def run(self):
        while True:
            await self.wakeup.wait()
            if self.stopping and not self.pending:
                return
            # Coalesce bursts and yield to recording/lifecycle work between rows.
            await asyncio.sleep(0.02)
            if self.db.in_transaction:
                continue
            key, value = self.pending.popitem(last=False)
            try:
                with self.db:
                    self.db.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (key, value[1]))
                if self.overlay.get(key) == value:
                    self.overlay.pop(key, None)
            except sqlite3.OperationalError as exc:
                if busy(exc):
                    self.metrics.increment("sqlite_busy_retries")
                    self.pending.setdefault(key, value)
                else:
                    self.metrics.increment("diagnostic_metadata_failures")
                    if self.overlay.get(key) == value:
                        self.overlay.pop(key, None)
            if not self.pending:
                self.wakeup.clear()
                if self.stopping:
                    return

    def abort(self):
        if self.pending:
            self.metrics.increment("diagnostic_shutdown_unflushed")
        self.task.cancel()
        self.pending.clear()
        self.overlay.clear()

    async def close(self, timeout=5):
        self.stopping = True
        self.wakeup.set()
        try:
            await asyncio.wait_for(self.task, timeout)
            return True
        except TimeoutError:
            self.abort()
            return False


async def measure_await(store, awaitable, metric):
    """Instrument without logging inputs, URLs, exception text or changing results."""
    metrics = getattr(getattr(store, "db", None), "health", None)
    start = time.monotonic()
    try:
        result = await awaitable
        if metrics and getattr(result, "status_code", 0) >= 400:
            metrics.increment(metric + "_errors")
            if result.status_code == 429:
                metrics.increment(metric + "_rate_limits")
        return result
    except Exception:
        if metrics:
            metrics.increment(metric + "_errors")
        raise
    finally:
        if metrics:
            metrics.observe(metric, (time.monotonic() - start) * 1000)
