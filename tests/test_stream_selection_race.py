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
