import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

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
