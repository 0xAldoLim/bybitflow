from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from bybit_flow.notifications import embed
from bybit_flow.orderflow import Book, Tape
from bybit_flow.scanner import Scanner


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
async def test_outage_pauses_recovers_and_fresh_stop_still_invalidates(settings, signal, monkeypatch, direction):
    scanner = Scanner.__new__(Scanner)
    scanner.settings = settings
    scanner.store = Mock()
    scanner.recorder = Mock(healthy=True)
    scanner.api = SimpleNamespace(name="binance")
    scanner.notifier = SimpleNamespace(send_research=AsyncMock(return_value="sent"))
    scanner.context = {}
    book, tape = Book(), Tape()
    scanner.streams = SimpleNamespace(books={signal.symbol: book}, tapes={signal.symbol: tape})
    signal.source = "binance"
    signal.direction = direction
    signal.stop = 95 if direction == "LONG" else 105
    signal.state = "ALERTED"
    signal.expires_ms = 1000
    signal.holding_deadline_ms = 10_000_000
    now = 1_000_000
    await scanner.monitor_alerted(signal, now)
    assert signal.state == "ALERTED" and signal.coverage["monitoring"] == "paused"
    scanner.notifier.send_research.assert_not_awaited()
    await scanner.monitor_alerted(signal, now + 60_000)
    assert scanner.notifier.send_research.await_count == 1
    card = embed(signal, "http://localhost")["embeds"][0]
    assert card["title"].startswith("MONITORING PAUSED")
    assert "not a stop-loss hit" in card["fields"][0]["value"]
    await scanner.monitor_alerted(signal, now + 70_000)
    assert scanner.notifier.send_research.await_count == 1
    now += 80_000
    book.valid = True
    book.bids, book.asks = {Decimal("99.99"): Decimal(100)}, {Decimal("100.01"): Decimal(100)}
    book.event_ms = book.receipt_ms = tape.last_event = tape.last_receipt = now
    # Reconnected tape need not re-confirm an already published execution window.
    tape.coverage_start = now
    scanner.context[signal.symbol] = dict(asof=now, h4=[], instrument=SimpleNamespace(base="TEST"))
    monkeypatch.setattr("bybit_flow.scanner.facts_asof", lambda *args: [])
    monkeypatch.setattr("bybit_flow.scanner.candle_features", lambda *args: {"regime": "range"})
    await scanner.monitor_alerted(signal, now)
    assert signal.state == "ALERTED" and signal.coverage["monitoring"] == "active"
    assert scanner.notifier.send_research.await_count == 2
    assert embed(signal, "http://localhost")["embeds"][0]["title"].startswith("MONITORING RESUMED")
    # Stale below-stop prices must not be mistaken for a newly observed stop crossing.
    crossing = Decimal("90" if direction == "LONG" else "110")
    book.bids = {crossing: Decimal(100)}
    book.asks = {crossing + Decimal("0.01"): Decimal(100)}
    await scanner.monitor_alerted(signal, now + 20_000)
    assert signal.state == "ALERTED"
    book.event_ms = book.receipt_ms = now + 21_000
    await scanner.monitor_alerted(signal, now + 21_000)
    assert signal.state == "INVALIDATED"


@pytest.mark.asyncio
async def test_paused_tracking_ends_at_holding_deadline(settings, signal):
    scanner = Scanner.__new__(Scanner)
    scanner.settings = settings
    scanner.store = Mock()
    scanner.recorder = Mock(healthy=True)
    scanner.api = SimpleNamespace(name="binance")
    scanner.context = {}
    scanner.streams = SimpleNamespace(books={}, tapes={})
    scanner.notifier = SimpleNamespace(send_research=AsyncMock(return_value="sent"))
    signal.state = "ALERTED"
    signal.source = "binance"
    signal.holding_deadline_ms = 100_000
    await scanner.monitor_alerted(signal, 100_000)
    assert signal.state == "EXPIRED"
    assert embed(signal, "http://localhost")["embeds"][0]["title"].startswith("TRACKING ENDED")


def test_published_monitoring_cannot_be_displaced_by_newer_candidates(settings, signal):
    from bybit_flow.storage import Store

    store = Store(settings.data_dir)
    signal.state = "ALERTED"
    store.signal(signal)
    for i in range(4):
        other = signal.model_copy(deep=True)
        other.id = "new-" + str(i)
        other.created_ms += i + 1
        other.state = "EXPIRED" if i % 2 else "PENDING CONFIRMATION"
        store.signal(other)
    assert store.active_signals(limit=1)[0]["id"] == signal.id
    assert len(store.active_signals()) == 3
    store.close()
