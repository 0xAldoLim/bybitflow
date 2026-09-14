import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from bybit_flow.native_streams import NativeStreams
from bybit_flow.scanner import Scanner


@pytest.mark.asyncio
async def test_failed_scan_retries_then_restores_normal_cadence(monkeypatch):
    scanner = Scanner.__new__(Scanner)
    scanner.settings = SimpleNamespace(market_source="auto", scan_seconds=900)
    scanner.store = Mock()
    scanner.context = {"BTCUSDT": {"stale": True}}
    scanner.streams = SimpleNamespace(select=AsyncMock())
    scanner.source_ready = True
    scanner.scan_once = AsyncMock(side_effect=[ConnectionError("temporary outage"), None])
    delays = []

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 1:
            assert scanner.context == {}
            assert scanner.source_ready is False
            scanner.streams.select.assert_awaited_once_with([])
        else:
            raise asyncio.CancelledError

    monkeypatch.setattr("bybit_flow.scanner.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await scanner.scan_loop()
    assert scanner.scan_once.await_count == 2
    assert delays == [60, 900]


@pytest.mark.asyncio
async def test_one_stale_symbol_does_not_reset_other_native_feeds(monkeypatch):
    scanner = Scanner.__new__(Scanner)
    scanner.settings = SimpleNamespace(market_source="auto", scan_enabled=True)
    scanner.store = Mock()
    scanner.api = SimpleNamespace(name="binance")
    scanner.recorder = SimpleNamespace(healthy=True, reason="ready")
    scanner.status = {"state": "collecting"}
    scanner.source_ready = True
    scanner.context = {"BTCUSDT": {"retained": True}}
    scanner.scan_lock = asyncio.Lock()
    streams = NativeStreams.__new__(NativeStreams)
    streams.selected = ("BTCUSDT", "QUIETUSDT")
    streams.connected_for = lambda symbol: symbol == "BTCUSDT"
    streams.select = AsyncMock()
    scanner.streams = streams
    calls = 0

    async def sleep(delay):
        nonlocal calls
        calls += 1
        if calls == 5:
            raise asyncio.CancelledError

    monkeypatch.setattr("bybit_flow.scanner.asyncio.sleep", sleep)
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: calls * 180_000)
    with pytest.raises(asyncio.CancelledError):
        await scanner.source_watchdog()
    streams.select.assert_not_awaited()
    assert scanner.source_ready and scanner.context["BTCUSDT"]["retained"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,preserved", [(ConnectionError("REST outage"), True), (ValueError("clock skew"), False)]
)
async def test_rest_failure_preserves_live_confirmation_history(monkeypatch, failure, preserved):
    scanner = Scanner.__new__(Scanner)
    scanner.settings = SimpleNamespace(market_source="auto", scan_seconds=300)
    scanner.store = Mock()
    scanner.source_ready = True
    scanner.recorder = SimpleNamespace(healthy=True)
    scanner.context = {"BTCUSDT": {"retained": True}}
    streams = NativeStreams.__new__(NativeStreams)
    streams.selected = ("BTCUSDT", "QUIETUSDT")
    streams.connected_for = lambda symbol: symbol == "BTCUSDT"
    streams.select = AsyncMock()
    scanner.streams = streams
    scanner.scan_once = AsyncMock(side_effect=failure)

    async def sleep(delay):
        assert delay == 60
        raise asyncio.CancelledError

    monkeypatch.setattr("bybit_flow.scanner.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await scanner.scan_loop()
    assert bool(scanner.context) is preserved
    assert scanner.source_ready is preserved
    assert scanner.status["live_feed_preserved"] is preserved
    if preserved:
        streams.select.assert_not_awaited()
    else:
        streams.select.assert_awaited_once_with([])
