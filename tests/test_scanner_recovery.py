import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from bybit_flow.native_streams import NativeStreams
from bybit_flow.scanner import Scanner


@pytest.mark.asyncio
async def test_failed_scan_retries_then_restores_normal_cadence(monkeypatch):
    scanner = Scanner.__new__(Scanner)
    scanner.lifecycle_bootstrapped = asyncio.Event()
    scanner.lifecycle_bootstrapped.set()
    scanner.pending_symbols = lambda: []
    scanner.settings = SimpleNamespace(market_source="auto", scan_seconds=900)
    scanner.store = Mock()
    scanner.context = {"BTCUSDT": {"stale": True}}
    scanner.streams = SimpleNamespace(select=AsyncMock(), selected=())
    scanner.source_ready = True
    scanner.scan_once = AsyncMock(side_effect=[ConnectionError("temporary outage"), None])
    delays = []

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 1:
            assert scanner.context == {"BTCUSDT": {"stale": True}}
            assert scanner.source_ready is True
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
    scanner.lifecycle_bootstrapped = asyncio.Event()
    scanner.lifecycle_bootstrapped.set()
    scanner.pending_symbols = lambda: []
    scanner.settings = SimpleNamespace(market_source="auto", scan_enabled=True)
    scanner.store = Mock()
    scanner.api = SimpleNamespace(name="binance")
    scanner.recorder = SimpleNamespace(healthy=True, reason="ready")
    scanner.status = {"state": "collecting"}
    scanner.source_ready = True
    scanner.context = {"BTCUSDT": {"retained": True}}
    scanner.scan_lock = asyncio.Lock()
    streams = NativeStreams.__new__(NativeStreams)
    streams.selection_lock = asyncio.Lock()
    streams.tapes = {}
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
    "failure,preserved", [(ConnectionError("REST outage"), True), (ValueError("clock skew"), True)]
)
async def test_rest_failure_preserves_live_confirmation_history(monkeypatch, failure, preserved):
    scanner = Scanner.__new__(Scanner)
    scanner.lifecycle_bootstrapped = asyncio.Event()
    scanner.lifecycle_bootstrapped.set()
    scanner.pending_symbols = lambda: []
    scanner.settings = SimpleNamespace(market_source="auto", scan_seconds=300)
    scanner.store = Mock()
    scanner.source_ready = True
    scanner.recorder = SimpleNamespace(healthy=True)
    scanner.context = {"BTCUSDT": {"retained": True}}
    streams = NativeStreams.__new__(NativeStreams)
    streams.selection_lock = asyncio.Lock()
    streams.tapes = {}
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
    streams.select.assert_awaited_once_with(["BTCUSDT", "QUIETUSDT"])


@pytest.mark.asyncio
async def test_selected_feed_gets_new_minutes_after_pending_expires():
    scanner = Scanner.__new__(Scanner)
    scanner.lifecycle_bootstrapped = asyncio.Event()
    scanner.lifecycle_bootstrapped.set()
    scanner.pending_symbols = lambda: []
    scanner.recorder = SimpleNamespace(healthy=True)
    scanner.discover_horizons = AsyncMock()
    scanner.settings = SimpleNamespace(execution_window_seconds=60)
    scanner.store = Mock()
    scanner.pending_symbols = lambda: []
    scanner.streams = SimpleNamespace(selected=("BTCUSDT",))
    scanner.context = {"BTCUSDT": {"instrument": "instrument", "asof": 1}}
    scanner.cached_candles = AsyncMock(return_value=["bars"])
    scanner.save_candidates = Mock()
    await scanner.refresh_once()
    scanner.save_candidates.assert_called_once()
    assert scanner.context["BTCUSDT"]["asof"] > 1
    # A failed refresh cannot discard the known context and kill subsequent retries.
    scanner.cached_candles.side_effect = TimeoutError
    previous = scanner.context["BTCUSDT"].copy()
    await scanner.refresh_once()
    assert scanner.context["BTCUSDT"] == previous


@pytest.mark.asyncio
async def test_native_removal_cancels_all_feeds_before_waiting():
    streams = NativeStreams.__new__(NativeStreams)
    streams.selection_lock = asyncio.Lock()
    streams.tapes = {}
    streams.settings = SimpleNamespace(deep_symbols=30)
    streams.selected = ("A", "B")
    streams.books = {"A": Mock(), "B": Mock()}
    streams.tapes = {"A": Mock(), "B": Mock()}
    streams.liquidations, streams.sessions, streams.last_sample, streams.frames = {}, {}, {}, {}
    streams.record = Mock()
    cancelled = set()
    both = asyncio.Event()

    async def feed(symbol):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.add(symbol)
            assert streams.selected == ()
            if len(cancelled) == 2:
                both.set()
            await both.wait()
            raise RuntimeError("already-failed connection during shutdown")

    streams.tasks = {s: asyncio.create_task(feed(s)) for s in streams.selected}
    await asyncio.sleep(0)
    await asyncio.wait_for(streams.select([]), timeout=1)
    assert cancelled == {"A", "B"}
    assert not streams.tasks and not streams.books and not streams.tapes
    assert not streams.connected_for("A")


@pytest.mark.asyncio
async def test_depth_recovery_preserves_executed_trade_window(monkeypatch):
    from collections import deque

    from bybit_flow.orderflow import Book, BookGap, Tape

    streams = NativeStreams.__new__(NativeStreams)
    streams.selection_lock = asyncio.Lock()
    streams.tapes = {}
    streams.api = SimpleNamespace(name="binance")
    streams.books = {"BTCUSDT": Book()}
    tape = Tape()
    tape.reset(1234)
    streams.tapes = {"BTCUSDT": tape}
    streams.frames = {"BTCUSDT": deque([{"old": True}])}
    streams.recorder = SimpleNamespace(healthy=True)
    streams.store, streams.record = Mock(), Mock()
    streams.binance_depth = AsyncMock(side_effect=[BookGap("resnapshot"), asyncio.CancelledError()])
    monkeypatch.setattr("bybit_flow.native_streams.asyncio.sleep", AsyncMock())
    with pytest.raises(asyncio.CancelledError):
        await streams.binance_depth_recovery("BTCUSDT")
    assert streams.binance_depth.await_count == 2
    assert streams.tapes["BTCUSDT"] is tape and tape.coverage_start == 1234
    assert not streams.frames["BTCUSDT"] and not streams.books["BTCUSDT"].valid
    assert streams.record.call_args.args[0] == "control/book_gap"


@pytest.mark.asyncio
async def test_depth_recovery_cannot_hide_recording_failure():
    from collections import deque

    from bybit_flow.orderflow import Book

    streams = NativeStreams.__new__(NativeStreams)
    streams.selection_lock = asyncio.Lock()
    streams.tapes = {}
    streams.books = {"BTCUSDT": Book()}
    streams.frames = {"BTCUSDT": deque()}
    streams.recorder = SimpleNamespace(healthy=False)
    streams.binance_depth = AsyncMock(side_effect=RuntimeError("recording failed"))
    with pytest.raises(RuntimeError):
        await streams.binance_depth_recovery("BTCUSDT")
    assert streams.binance_depth.await_count == 1
