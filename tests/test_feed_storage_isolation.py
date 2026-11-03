import asyncio
import json
import sqlite3
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bybit_flow.native_streams import NativeStreams
from bybit_flow.orderflow import Book, Tape
from bybit_flow.storage import Store


async def test_health_contention_does_not_interrupt_recorded_feed(settings):
    store = Store(settings.data_dir)
    store.enable_runtime()
    streams = NativeStreams(settings, store, Mock(), SimpleNamespace(name="binance"))
    now = 100_000
    book = Book()
    book.apply(
        dict(type="snapshot", ts=now, data=dict(u=1, seq=1, b=[["100", "50"]], a=[["101", "50"]])), now
    )
    streams.books["TESTUSDT"] = book
    streams.tapes["TESTUSDT"] = Tape()
    streams.tapes["TESTUSDT"].last_receipt = now
    streams.sessions["TESTUSDT"] = "original-connection"
    streams.frames["TESTUSDT"] = deque(maxlen=120)
    streams.record = Mock()
    blocker = sqlite3.connect(settings.data_dir / "research.sqlite")
    blocker.execute("BEGIN IMMEDIATE")
    try:
        from unittest.mock import patch

        with patch("bybit_flow.native_streams.now_ms", return_value=now):
            streams.sample("TESTUSDT")
        assert book.valid
        assert streams.sessions["TESTUSDT"] == "original-connection"
        assert store.get("stream_health")["status"] == "HEALTHY"
        assert store.get("feed:binance:TESTUSDT")["status"] == "HEALTHY"
        assert store.get("liquidity:binance:TESTUSDT")["available"]
        assert len(streams.frames["TESTUSDT"]) == 1
        streams.record.assert_called_once()
        with pytest.raises(sqlite3.OperationalError):
            store.put("critical-delivery-receipt", {"status": "sent"})
        assert store.get("critical-delivery-receipt") is None
        await asyncio.sleep(0.03)
    finally:
        blocker.rollback()
        blocker.close()
        await store.stop_runtime()
        store.close()
    reader = sqlite3.connect(settings.data_dir / "research.sqlite")
    assert (
        json.loads(reader.execute("SELECT payload FROM kv WHERE key='stream_health'").fetchone()[0])["status"]
        == "HEALTHY"
    )
    reader.close()


def test_funnel_cleanup_search_does_not_hold_a_write_transaction(tmp_path):
    from bybit_flow.funnel import maintain

    store = Store(tmp_path)
    now = 10 * 86400000
    with store.db:
        store.db.executemany(
            "INSERT INTO funnel_events VALUES(?,?)",
            [("generated:keep", 1), ("confirmed:keep", 1), ("transient:old", 1), ("transient:recent", now)],
        )
        store.db.execute(
            "INSERT INTO funnel_minutes VALUES(?,?,?,?,?,?,?)", (0, "x", "all", "all", 1, 1, None)
        )
    observations = []

    def trace(sql):
        if "SELECT" in sql.upper() and "funnel_" in sql and "DELETE" not in sql.upper():
            observations.append(store.db.in_transaction)

    store.db.set_trace_callback(trace)
    maintain(store, now)
    assert observations and not any(observations)
    assert {r[0] for r in store.db.execute("SELECT event_key FROM funnel_events")} == {
        "generated:keep",
        "confirmed:keep",
        "transient:recent",
    }
    assert store.db.execute("SELECT count(*) FROM funnel_minutes").fetchone()[0] == 0
    store.close()


async def test_storage_forecast_refreshes_while_research_is_deferred(settings, monkeypatch):
    from bybit_flow import operational

    store = Store(settings.data_dir)
    store.db.health.increment("sqlite_busy_events")
    store.put("runtime_health", {"at_ms": 2 * operational.HOUR})
    store.put("storage_status", {"used_bytes": 91, "budget_bytes": 100})
    runtime = SimpleNamespace(
        store=store,
        settings=settings,
        rest_queue_depth=0,
        rest_inflight=0,
        streams=SimpleNamespace(reconnects={}),
        recorder=SimpleNamespace(healthy=True, pending_bytes=0, metrics=lambda: {}, pressure=lambda: 0),
    )
    calls = []

    def persist(root, sample, budget, collect_storage, checkpoint):
        calls.append((sample["state"], collect_storage, checkpoint))

    sleeps = 0

    async def sleep(delay):
        nonlocal sleeps
        sleeps += 1
        if sleeps > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(operational, "persist", persist)
    monkeypatch.setattr(operational, "now_ms", lambda: 2 * operational.HOUR)
    monkeypatch.setattr(operational, "memory_pressure", lambda: 0.95)
    monkeypatch.setattr(operational.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await operational.monitor(runtime)
    assert calls == [("RESEARCH_DEFERRED", True, False)]
    assert not operational.allowed(store, operational.Priority.FORWARD_RESEARCH, 2 * operational.HOUR)
    store.close()
