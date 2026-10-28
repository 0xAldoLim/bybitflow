import asyncio
import json
import sqlite3
import time

import pytest

from bybit_flow.storage import Store


async def test_contended_diagnostics_are_nonblocking_and_critical_writes_fail_closed(tmp_path):
    store = Store(tmp_path)
    store.enable_runtime()
    blocker = sqlite3.connect(tmp_path / "research.sqlite")
    blocker.execute("BEGIN IMMEDIATE")
    start = time.monotonic()
    store.put("runtime_health", dict(healthy=True))
    assert time.monotonic() - start < 0.1
    assert store.get("runtime_health") == dict(healthy=True)
    with pytest.raises(sqlite3.OperationalError):
        store.put("critical-delivery-receipt", dict(status="sent"))
    assert time.monotonic() - start < 0.25
    assert store.get("critical-delivery-receipt") is None
    await asyncio.sleep(0.02)  # The collector event loop remains schedulable.
    blocker.rollback()
    blocker.close()
    await store.stop_runtime()
    assert json.loads(
        store.db.execute("SELECT payload FROM kv WHERE key='runtime_health'").fetchone()[0]
    ) == dict(healthy=True)
    assert store.db.health.snapshot()["sqlite_busy_events"] >= 1
    store.close()


def test_offline_critical_retry_still_acknowledges_after_writer_recovers(tmp_path, monkeypatch):
    store = Store(tmp_path)
    store.db.execute("PRAGMA busy_timeout=0")
    blocker = sqlite3.connect(tmp_path / "research.sqlite")
    blocker.execute("BEGIN IMMEDIATE")
    calls = []

    def recover(delay):
        calls.append(delay)
        blocker.rollback()

    monkeypatch.setattr("bybit_flow.storage.time.sleep", recover)
    store.put("critical", dict(value=1))
    assert store.get("critical") == dict(value=1) and len(calls) == 1
    assert store.db.health.snapshot()["sqlite_busy_retries"] == 1
    blocker.close()
    store.close()


async def test_diagnostic_queue_preserves_null_and_never_accepts_critical_keys(tmp_path):
    store = Store(tmp_path)
    store.enable_runtime()
    with pytest.raises(ValueError, match="Critical"):
        store._metadata.offer("bybitflow_v2_cutover", "{}")
    store.put("clock_health", None)
    assert store.get("clock_health", {"old": True}) is None
    await store.stop_runtime()
    assert store.get("clock_health") is None
    store.close()


def test_sqlite_telemetry_is_bounded_and_contains_no_sql_or_private_payload(tmp_path):
    store = Store(tmp_path)
    for n in range(300):
        store.put("private-token", {"secret": "not-for-diagnostics"})
        store.db.execute("SELECT 1").fetchone()
    metrics = store.db.health.snapshot()
    assert metrics["sqlite_write_latency_ms"] >= 0 and metrics["sqlite_read_ms"] >= 0
    assert "not-for-diagnostics" not in json.dumps(metrics)
    assert all(len(v) <= 256 for v in store.db.health.samples.values())
    store.close()


async def test_diagnostic_pump_never_contends_with_critical_owner(tmp_path):
    store = Store(tmp_path)
    store.enable_runtime()
    for index in range(100):
        store.put("runtime_health", {"index": index})
        store.put("critical-receipt", {"index": index})
        await asyncio.sleep(0.001)
    await store.stop_runtime()
    assert store.get("runtime_health") == {"index": 99}
    assert store.get("critical-receipt") == {"index": 99}
    assert store.db.health.snapshot().get("sqlite_busy_events", 0) == 0
    store.close()


async def test_diagnostic_pump_cannot_commit_an_open_critical_transaction(tmp_path):
    store = Store(tmp_path)
    store.enable_runtime()
    reader = sqlite3.connect(tmp_path / "research.sqlite")
    store.db.execute("INSERT INTO kv VALUES('critical-uncommitted','{}')")
    store.put("runtime_health", {"fresh": True})
    await asyncio.sleep(0.08)
    assert store.db.in_transaction
    assert reader.execute("SELECT 1 FROM kv WHERE key='critical-uncommitted'").fetchone() is None
    store.db.rollback()
    await store.stop_runtime()
    assert store.get("critical-uncommitted") is None
    assert store.get("runtime_health") == {"fresh": True}
    reader.close()
    store.close()


async def test_supervisor_diagnostics_cannot_kill_recovery_on_sqlite_contention(tmp_path):
    store = Store(tmp_path)
    store.enable_runtime()
    blocker = sqlite3.connect(tmp_path / "research.sqlite")
    blocker.execute("BEGIN IMMEDIATE")
    store.put("supervisor:storage", {"state": "RECOVERING"})
    store.put("storage_health", {"state": "STORAGE_BACKPRESSURE"})
    await asyncio.sleep(0.04)
    assert store.get("supervisor:storage")["state"] == "RECOVERING"
    blocker.rollback()
    blocker.close()
    await store.stop_runtime()
    assert store.get("storage_health")["state"] == "STORAGE_BACKPRESSURE"
    store.close()
