import asyncio
from collections import deque
from contextlib import nullcontext
from decimal import Decimal
from types import SimpleNamespace

import pytest

from bybit_flow.ml import cli, memory
from bybit_flow.native_streams import NativeStreams
from bybit_flow.orderflow import Book
from bybit_flow.storage import Store
from bybit_flow.streams import Streams


async def test_disconnected_bybit_rotation_releases_only_removed_market_history(settings, monkeypatch):
    store = SimpleNamespace(put=lambda *_: None)
    recorder = SimpleNamespace(offer=lambda *args, **kwargs: True, healthy=True)
    streams = Streams(settings, store, recorder)

    async def run():
        await asyncio.Event().wait()

    monkeypatch.setattr(streams, "run", run)
    await streams.select(["KEEPUSDT", "OLDUSDT"])
    retained = deque([{"event_ms": 1234}], maxlen=120)
    streams.frames = {"KEEPUSDT": retained, "OLDUSDT": deque([{"event_ms": 1234}])}
    streams.last_frame = {"KEEPUSDT": 1234, "OLDUSDT": 1234}
    try:
        await streams.select(["KEEPUSDT", "NEWUSDT"])
        assert streams.frames == {"KEEPUSDT": retained}
        assert streams.frames["KEEPUSDT"] is retained
        assert streams.last_frame == {"KEEPUSDT": 1234}
        await streams.select([])
        assert not streams.frames and not streams.last_frame
    finally:
        await streams.stop()


@pytest.mark.parametrize("venue", ["bybit", "binance"])
async def test_dashboard_history_keeps_visible_tail_and_records_every_frame(settings, monkeypatch, venue):
    recorded = []
    store = SimpleNamespace(put=lambda *_: None)

    def offer(source, symbol, event, payload, *args, **kwargs):
        if source.endswith("liquidity/frame"):
            recorded.append(payload)
        return True

    recorder = SimpleNamespace(offer=offer, healthy=True)
    symbol = "TESTUSDT"
    if venue == "bybit":
        streams = Streams(settings, store, recorder)
        streams.books[symbol] = Book()
    else:
        streams = NativeStreams(settings, store, recorder, SimpleNamespace(name=venue))

        async def run(_):
            await asyncio.Event().wait()

        monkeypatch.setattr(streams, "run_symbol", run)
        await streams.select([symbol])
        streams.sessions[symbol] = "fixture-connection"
    try:
        for index in range(725):
            now = 10_000 + index * 5_000
            message = dict(
                type="snapshot" if index == 0 else "delta",
                topic="orderbook.50." + symbol,
                ts=now,
                data=dict(u=index + 10, seq=index + 100, b=[["99", str(index + 1)]], a=[["101", "10"]]),
            )
            if venue == "bybit":
                streams.process(message, now)
            else:
                monkeypatch.setattr("bybit_flow.native_streams.now_ms", lambda: now)
                streams.books[symbol].apply(message, now)
                streams.tapes[symbol].last_receipt = now
                streams.sample(symbol)
        assert len(recorded) == 725
        assert list(streams.frames[symbol]) == recorded[-120:]
        assert len(streams.books[symbol].ofi_buckets) > 0
    finally:
        if venue != "bybit":
            await streams.select([])


def test_worker_releases_unused_memory_between_steps_without_changing_progress(settings, monkeypatch):
    class IdleReached(Exception):
        pass

    store = Store(settings.data_dir)
    store.put("ml_champion", "preserved-model")
    store.put("primary_materialization", dict(cursor_ms=1234, positions={"protected": {"entry": 99}}))
    calls = []
    monkeypatch.setattr("bybit_flow.ml.operations.heartbeat", lambda *_: nullcontext())

    def step(*_):
        calls.append("step")
        return dict(status="collecting")

    def release():
        calls.append("release")
        return dict(heap_trimmed=True)

    def sleep(_):
        calls.append("idle")
        if calls.count("idle") == 2:
            raise IdleReached

    monkeypatch.setattr(cli, "worker_step", step)
    monkeypatch.setattr(memory, "release_unused", release)
    monkeypatch.setattr(cli.time, "sleep", sleep)
    try:
        with pytest.raises(IdleReached):
            cli.run(["worker"], settings.model_copy(update={"ml_enabled": True}), store)
        assert calls == ["step", "release", "idle", "step", "idle"]
        assert store.get("ml_champion") == "preserved-model"
        assert store.get("primary_materialization") == {
            "cursor_ms": 1234,
            "positions": {"protected": {"entry": 99}},
        }
        assert store.get("ml_memory")["heap_trimmed"] is True
    finally:
        store.close()


def test_memory_reclamation_preserves_live_arrow_values_and_handles_unsupported_heap(monkeypatch):
    import pyarrow as pa

    values = pa.array([Decimal("1.25"), Decimal("2.50")])
    monkeypatch.setattr(memory.sys, "platform", "linux")

    def unavailable(_):
        raise OSError("No glibc allocator on this platform")

    monkeypatch.setattr(memory.ctypes, "CDLL", unavailable)
    result = memory.release_unused()
    assert values.to_pylist() == [Decimal("1.25"), Decimal("2.50")]
    assert result["heap_trimmed"] is False
