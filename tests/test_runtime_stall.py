import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from bybit_flow.app import create_app
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Store


async def test_stuck_scan_times_out_releases_lock_and_retries_without_losing_active_plan(monkeypatch):
    import bybit_flow.scanner as module

    scanner = Scanner.__new__(Scanner)
    scanner.settings = SimpleNamespace(scan_seconds=300)
    scanner.store = Mock()
    scanner.recorder = SimpleNamespace(healthy=True)
    scanner.streams = SimpleNamespace(connected=True, selected=("KEPTUSDT",), select=AsyncMock())
    scanner.source_ready = True
    scanner.context = {"KEPTUSDT": {"original_plan": "unchanged"}}
    scanner.pending_symbols = lambda: ["KEPTUSDT"]
    scanner.lifecycle_bootstrapped = asyncio.Event()
    scanner.lifecycle_bootstrapped.set()
    scanner.scan_lock = asyncio.Lock()
    attempts, delays, budgets = 0, [], []
    original_timeout = asyncio.timeout

    def short_timeout(seconds):
        budgets.append(seconds)
        return original_timeout(0.01)

    async def scan():
        nonlocal attempts
        async with scanner.scan_lock:
            attempts += 1
            if attempts == 1:
                await asyncio.Event().wait()

    async def sleep(seconds):
        delays.append(seconds)
        if len(delays) == 2:
            raise asyncio.CancelledError

    scanner.scan_once = scan
    monkeypatch.setattr(module.asyncio, "timeout", short_timeout)
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(scanner.scan_loop(), 1)
    assert attempts == 2 and delays == [60, 300]
    assert budgets == [900, 900]
    assert not scanner.scan_lock.locked()
    assert scanner.context == {"KEPTUSDT": {"original_plan": "unchanged"}}
    scanner.streams.select.assert_awaited_once_with(["KEPTUSDT"], retain_selected=True)


async def test_source_recovery_waiting_for_scan_does_not_stop_health_heartbeat(monkeypatch):
    scanner = Scanner.__new__(Scanner)
    scanner.settings = SimpleNamespace(market_source="binance", scan_enabled=True)
    scanner.store = Mock()
    scanner.streams = SimpleNamespace(connected=False, selected=("BTCUSDT",), tapes={}, select=AsyncMock())
    scanner.api = SimpleNamespace(name="binance")
    scanner.status = {"state": "scanning"}
    scanner.recorder = SimpleNamespace(healthy=True, reason="recording")
    scanner.source_ready = True
    scanner.scan_lock = asyncio.Lock()
    await scanner.scan_lock.acquire()
    clock, ticks = 1000, 0

    async def sleep(_seconds):
        nonlocal clock, ticks
        ticks += 1
        if ticks == 4:
            raise asyncio.CancelledError
        clock = [1000, 122_000, 153_000][ticks - 1]

    monkeypatch.setattr("bybit_flow.scanner.asyncio.sleep", sleep)
    monkeypatch.setattr("bybit_flow.scanner_market_data.now_ms", lambda: clock)
    monkeypatch.setattr(
        "bybit_flow.feed_health.persistent_health",
        lambda *args: {"active_lifecycle_stream_health": {"fresh": 0, "total": 0}},
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(scanner.source_watchdog(), 1)
        updates = [
            call.args[1] for call in scanner.store.put.call_args_list if call.args[0] == "runtime_health"
        ]
        assert [row["at_ms"] for row in updates] == [1000, 122_000, 153_000]
        scanner.streams.select.assert_not_awaited()
    finally:
        scanner.scan_lock.release()


async def test_stale_runtime_cannot_report_healthy_and_recovers_on_current_heartbeat(settings, monkeypatch):
    settings.scan_enabled = True
    store = Store(settings.data_dir)
    now = 2_000_000
    monkeypatch.setattr("bybit_flow.app.now_ms", lambda: now)
    store.put("scanner_process_start", {"at_ms": now - 300_000})
    store.put("runtime_health", {"at_ms": now - 180_000})
    state = SimpleNamespace(
        store=store,
        recorder=SimpleNamespace(healthy=True),
        scanner=SimpleNamespace(status={"state": "scanning"}),
    )
    request = SimpleNamespace(app=SimpleNamespace(state=state))
    health = next(route.endpoint for route in create_app(settings).routes if route.path == "/healthz")
    try:
        assert (await health(request)).status_code == 503
        store.put("runtime_health", {"at_ms": now})
        assert (await health(request)).status_code == 200
    finally:
        store.close()
