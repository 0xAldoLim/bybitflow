import asyncio
import json
import sqlite3
from collections import deque
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from bybit_flow.ml import cli
from bybit_flow.native_streams import NativeStreams
from bybit_flow.orderflow import Book, Tape
from bybit_flow.storage import Recorder, Store, now_ms


async def test_brief_recorder_burst_does_not_disconnect_or_reset_trade_coverage(settings, monkeypatch):
    settings = settings.model_copy(update={"queue_size": 100})
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    for i in range(80):
        recorder.offer("fixture", "BTCUSDT", i, {"i": i})
    streams = NativeStreams(settings, store, recorder, SimpleNamespace(name="binance", metadata={}))
    symbol = "TESTUSDT"
    streams.selected = (symbol,)
    streams.books[symbol], streams.tapes[symbol] = Book(), Tape()
    streams.liquidations[symbol], streams.frames[symbol] = deque(), deque()
    streams.sessions[symbol] = "same-connection"
    reads, connections = 0, 0

    class Socket:
        async def recv(self):
            nonlocal reads
            reads += 1
            if reads > 1:
                await asyncio.Event().wait()
            now = now_ms()
            return json.dumps(
                {
                    "data": {
                        "E": now,
                        "e": "aggTrade",
                        "T": now,
                        "s": symbol,
                        "a": 1,
                        "m": False,
                        "p": "100",
                        "q": "1",
                    }
                }
            )

    @asynccontextmanager
    async def connect(*args, **kwargs):
        nonlocal connections
        connections += 1
        yield Socket()

    monkeypatch.setattr("bybit_flow.native_streams.connect", connect)
    task = asyncio.create_task(streams.binance_market(symbol))
    try:
        for _ in range(100):
            if recorder.backpressure_waits:
                break
            await asyncio.sleep(0.001)
        assert recorder.backpressure_waits == 1 and reads == 0
        tape, coverage_start = streams.tapes[symbol], streams.tapes[symbol].coverage_start
        admitted = []
        while not recorder.queue.empty():
            admitted.append(recorder.queue.get_nowait())
        recorder.pending_bytes = 0  # Simulated writer acknowledgement, no dropped rows.
        for _ in range(100):
            if tape.trades:
                break
            await asyncio.sleep(0.001)
        assert len(tape.trades) == 1 and connections == 1
        assert tape.coverage_start == coverage_start
        assert recorder.events_dropped == recorder.overflow_count == 0
        assert len(admitted) == 81  # Initial burst plus subscription control.
        queued = list(recorder.queue._queue)
        assert sum("ws/publicTrade." in row["source"] for row in queued) == 1
        assert sum("raw/market" in row["source"] for row in queued) == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        store.close()


async def test_recorder_wait_is_bounded_and_cancellation_does_not_hide_a_gap(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings.model_copy(update={"queue_size": 100}))
    for i in range(80):
        recorder.offer("fixture", "BTCUSDT", i, {})
    try:
        with pytest.raises(RuntimeError, match="feed buffer budget"):
            await recorder.wait_for_capacity(timeout=0.02)
        task = asyncio.create_task(recorder.wait_for_capacity())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert recorder.queue.qsize() == 80 and recorder.events_dropped == 0
    finally:
        store.close()


def test_ml_yields_to_recording_and_automatically_resumes_without_losing_champion(settings, monkeypatch):
    store = Store(settings.data_dir)
    now, calls = 1_000_000, []
    monkeypatch.setattr(cli, "now_ms", lambda: now)
    monkeypatch.setattr(cli, "monitor", lambda *args: calls.append("monitor"))
    monkeypatch.setattr(
        cli, "train_ready", lambda *args: calls.append("train") or {"status": "collecting", "tracks": {}}
    )
    store.put("ml_champion", "preserved-model")
    store.put(
        "recorder_health",
        {"at_ms": now, "healthy": True, "state": "NORMAL", "queue_usage_percent": 75, "events_dropped": 100},
    )
    try:
        assert cli.worker_step(settings, store)["status"] == "DEFERRED_LIVE_PRESSURE"
        assert calls == []
        now += 61_000
        store.put(
            "recorder_health",
            {
                "at_ms": now,
                "healthy": True,
                "state": "NORMAL",
                "queue_usage_percent": 0,
                "events_dropped": 100,
            },
        )
        assert cli.worker_step(settings, store)["status"] == "collecting"
        assert calls == ["monitor", "train"]
        assert store.get("ml_champion") == "preserved-model"
    finally:
        store.close()


def test_historical_recorder_drops_do_not_permanently_pause_learning(settings, monkeypatch):
    store = Store(settings.data_dir)
    now = 1_000_000
    monkeypatch.setattr(cli, "now_ms", lambda: now)
    health = dict(at_ms=now, healthy=True, state="NORMAL", events_dropped=100)
    try:
        store.put("recorder_health", health)
        cli.prioritize_live_recording(store)
        store.put("recorder_health", health | {"events_dropped": 101})
        with pytest.raises(cli.LiveRecordingPressure):
            cli.prioritize_live_recording(store)
        cli.prioritize_live_recording(store)
        store.put("recorder_health", health | {"at_ms": now - 100_000, "healthy": False})
        cli.prioritize_live_recording(store)  # An offline desk must not block offline learning.
    finally:
        store.close()


def test_ml_pressure_status_writer_contention_defers_instead_of_crashing(settings, monkeypatch):
    store = Store(settings.data_dir)
    now = 1_000_000
    monkeypatch.setattr(cli, "now_ms", lambda: now)
    store.put("recorder_health", dict(at_ms=now, healthy=True, queue_usage_percent=75))
    original = store.put

    def put(key, value):
        if key == "ml_worker_state" and value.get("status") == "DEFERRED_LIVE_PRESSURE":
            raise sqlite3.OperationalError("database is locked")
        return original(key, value)

    monkeypatch.setattr(store, "put", put)
    try:
        assert cli.worker_step(settings, store)["status"] == "DEFERRED_SQLITE_BUSY"
    finally:
        store.close()


def test_real_recorder_health_drives_ml_deferral_and_resume(settings, monkeypatch):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings.model_copy(update={"queue_size": 100}))
    now, calls = 1_000_000, []
    monkeypatch.setattr(cli, "now_ms", lambda: now)
    monkeypatch.setattr("bybit_flow.storage.now_ms", lambda: now)
    monkeypatch.setattr(cli, "monitor", lambda *args: calls.append("monitor"))
    monkeypatch.setattr(
        cli, "train_ready", lambda *args: calls.append("train") or {"status": "collecting", "tracks": {}}
    )
    for i in range(75):
        recorder.offer("fixture", "BTCUSDT", now, {"i": i})
    try:
        store.put("recorder_health", recorder.metrics())
        assert cli.worker_step(settings, store)["status"] == "DEFERRED_LIVE_PRESSURE"
        assert calls == []
        assert store.get("ml_live_priority_checkpoint")["at_ms"] == now
        while not recorder.queue.empty():
            recorder.queue.get_nowait()
        recorder.pending_bytes = 0  # The writer has acknowledged the entire burst.
        now += 30_000
        store.put("recorder_health", recorder.metrics())
        assert cli.worker_step(settings, store)["status"] == "collecting"
        assert calls == ["monitor", "train"]
    finally:
        store.close()
