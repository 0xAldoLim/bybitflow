import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from bybit_flow.lifecycle import tick
from bybit_flow.native_streams import NativeStreams
from bybit_flow.streams import Streams


@pytest.mark.parametrize("venue", ["binance", "bybit"])
async def test_lifecycle_update_waiting_on_rotation_cannot_remove_new_scanner_selection(settings, venue):
    settings = settings.model_copy(update={"deep_symbols": 30})
    store, recorder = Mock(), Mock(healthy=True)
    store.active_signals.return_value = []
    if venue == "binance":
        streams = NativeStreams(settings, store, recorder, SimpleNamespace(name=venue))

        async def connected(_symbol):
            await asyncio.Event().wait()

        streams.run_symbol = connected
    else:
        streams = Streams(settings, store, recorder)
        streams.ws, streams.connected = AsyncMock(), True
    scanner = SimpleNamespace(
        store=store, recorder=recorder, streams=streams, exchange=venue, cross_venue=None
    )
    universe = ["KEPTUSDT"] + [f"NEW{i}USDT" for i in range(29)]
    try:
        await streams.select(["KEPTUSDT"])
        original_book, original_tape = streams.books["KEPTUSDT"], streams.tapes["KEPTUSDT"]
        async with streams.selection_lock:
            lifecycle = asyncio.create_task(tick(scanner, 2_000_000))
            await asyncio.sleep(0)
            # The scanner finishes its replacement while lifecycle is queued.
            await streams._select(universe)
        await lifecycle
        assert set(streams.selected) == set(universe)
        assert streams.books["KEPTUSDT"] is original_book
        assert streams.tapes["KEPTUSDT"] is original_tape
        # Explicit scanner replacement still rotates optional subscriptions.
        await streams.select(["KEPTUSDT"])
        assert streams.selected == ("KEPTUSDT",)
    finally:
        await streams.stop()


async def test_child_error_during_cancellation_cannot_keep_removed_stream_alive(settings):
    streams = NativeStreams(settings, Mock(), Mock(healthy=True), SimpleNamespace(name="binance"))
    started, failed = asyncio.Event(), False

    async def market(symbol):
        nonlocal failed
        if symbol == "REMOVEUSDT":
            started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if symbol == "REMOVEUSDT" and not failed:
                failed = True
                raise RuntimeError("child failed while closing") from None
            raise

    async def depth(_symbol):
        await asyncio.Event().wait()

    streams.binance_market, streams.binance_depth_recovery = market, depth
    await streams.select(["KEPTUSDT", "REMOVEUSDT"])
    await started.wait()
    removed = streams.tasks["REMOVEUSDT"]
    book, tape = streams.books["KEPTUSDT"], streams.tapes["KEPTUSDT"]
    try:
        await asyncio.wait_for(streams.select(["KEPTUSDT", "NEWUSDT"]), 1)
        assert removed.done()
        assert set(streams.selected) == {"KEPTUSDT", "NEWUSDT"}
        assert streams.books["KEPTUSDT"] is book and streams.tapes["KEPTUSDT"] is tape
        assert not streams.selection_lock.locked()
        assert not streams.stopping
    finally:
        removed.cancel()
        await asyncio.gather(removed, return_exceptions=True)
        await streams.stop()


async def test_child_failure_without_shutdown_still_reconnects(settings):
    streams = NativeStreams(settings, Mock(), Mock(healthy=True), SimpleNamespace(name="binance"))
    recovered, attempts = asyncio.Event(), 0

    async def market(_symbol):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary socket failure")
        recovered.set()
        await asyncio.Event().wait()

    async def depth(_symbol):
        await asyncio.Event().wait()

    streams.binance_market, streams.binance_depth_recovery = market, depth
    await streams.select(["KEPTUSDT"])
    try:
        await asyncio.wait_for(recovered.wait(), 3)
        assert not streams.tasks["KEPTUSDT"].done()
        assert streams.reconnects["KEPTUSDT"] == 1
    finally:
        await streams.stop()
