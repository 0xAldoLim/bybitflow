import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from bybit_flow.exchanges import VenueAPI
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Recorder, Store


@pytest.mark.asyncio
@pytest.mark.parametrize("source,expected", [("auto", "bybit"), ("binance", "binance")])
async def test_failed_primary_can_change_new_scan_venue_without_replacing_active_plan(
    settings, signal, monkeypatch, source, expected
):
    settings.market_source = source
    store = Store(settings.data_dir)
    signal.source, signal.state = "binance", "ALERTED"
    store.signal(signal)
    original = signal.model_dump(mode="json")
    scanner = Scanner(settings, store, Recorder(store, settings))
    await scanner.api.close()
    old_api = VenueAPI("binance", settings)
    old_api.close = AsyncMock()
    old_streams = SimpleNamespace(
        selected=[signal.symbol], connected=False, tapes={}, select=AsyncMock(), stop=AsyncMock()
    )
    scanner.api, scanner.streams, scanner.source_ready = old_api, old_streams, True
    clock = [1000]
    sleeps = [0]

    async def step(seconds):
        sleeps[0] += 1
        if sleeps[0] == 3:
            raise asyncio.CancelledError
        clock[0] = 1000 if sleeps[0] == 1 else 122_000

    async def probe(name, settings):
        return dict(status="HEALTHY" if name == "bybit" else "UNAVAILABLE")

    monkeypatch.setattr("bybit_flow.scanner.asyncio.sleep", step)
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: clock[0])
    monkeypatch.setattr("bybit_flow.scanner.market_probe", probe)
    monkeypatch.setattr(
        "bybit_flow.feed_health.persistent_health",
        lambda *a: dict(active_lifecycle_stream_health=dict(fresh=0, total=1)),
    )
    with pytest.raises(asyncio.CancelledError):
        await scanner.source_watchdog()
    assert scanner.exchange == expected and scanner.source_ready
    assert store.active_signals()[0] == original
    old_streams.stop.assert_not_awaited()
    old_api.close.assert_not_awaited()
    if expected == "bybit":
        assert scanner.cross_venue.peers["binance"] == (old_api, old_streams)
        assert scanner.status["state"] == "warming"
        await scanner.api.close()
    await old_api.client.aclose()
    store.close()
