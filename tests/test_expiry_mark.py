from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from bybit_flow.lifecycle import expiry_mark, tick
from bybit_flow.models import Candle, Trade
from bybit_flow.notifications import embed
from bybit_flow.observations import start as start_observation
from bybit_flow.orderflow import Book, Tape
from bybit_flow.reconciliation import advance, reconcile
from bybit_flow.storage import Store


@pytest.mark.asyncio
@pytest.mark.parametrize("direction,mark_price", [("LONG", 102), ("SHORT", 98)])
async def test_entered_setup_expires_with_favorable_cost_adjusted_mark(
    settings, signal, direction, mark_price
):
    deadline = 120_000
    signal.source = "binance"
    signal.state = "ALERTED"
    signal.direction = direction
    signal.stop = 95 if direction == "LONG" else 105
    signal.tp1 = 115 if direction == "LONG" else 85
    signal.tp2 = 120 if direction == "LONG" else 80
    signal.holding_deadline_ms = deadline
    signal.trigger_expires_ms = 110_000
    signal.risk["cost_per_base"] = 0.2
    store = Store(settings.data_dir)
    store.signal(signal)

    tape = Tape()
    tape.reset(0)
    tape.add(Trade(signal.symbol, 100_000, 100_000, "entry", "Buy", Decimal(100), Decimal(1)))
    tape.add(Trade(signal.symbol, 119_500, 119_500, "mark", "Buy", Decimal(mark_price), Decimal(1)))
    book = Book()
    book.valid = True
    book.bids = {Decimal(mark_price) - Decimal("0.01"): Decimal(10)}
    book.asks = {Decimal(mark_price) + Decimal("0.01"): Decimal(10)}
    book.event_ms = book.receipt_ms = deadline
    streams = SimpleNamespace(
        tapes={signal.symbol: tape},
        books={signal.symbol: book},
        selected=(signal.symbol,),
        select=AsyncMock(),
    )
    scanner = SimpleNamespace(
        store=store,
        settings=settings,
        exchange="binance",
        api=Mock(),
        streams=streams,
        recorder=Mock(healthy=True),
        reconcile_pending=set(),
    )
    await tick(scanner, deadline)

    saved = store.signals()[0]
    mark = saved["evidence"]["expiry_mark"]
    assert saved["state"] == "EXPIRED"
    assert saved["coverage"]["tracking_end"] is True
    assert mark["classification"] == "ESTIMATED_PROFIT"
    assert mark["estimated_net_r"] == pytest.approx(0.36)
    assert mark["account_fill_verified"] is False
    card = embed(signal.model_validate(saved), "http://localhost")["embeds"][0]
    assert card["title"].startswith("TRACKING ENDED")
    assert "Estimated profit" in next(f["value"] for f in card["fields"] if f["name"] == "At tracking end")
    store.close()


def test_stale_or_unentered_setup_cannot_claim_profit(signal):
    signal.state = "ALERTED"
    signal.holding_deadline_ms = 120_000
    signal.risk["cost_per_base"] = 0.2
    assert expiry_mark(signal, 102, 119_500, 120_000, 5_000, "LIVE_EXECUTED_TRADE") is None
    signal.evidence["observed_entry_ms"] = 100_000
    assert expiry_mark(signal, 102, 100_000, 120_000, 5_000, "LIVE_EXECUTED_TRADE") is None
    assert expiry_mark(signal, 102, 110_000, 120_000, 5_000, "LIVE_EXECUTED_TRADE") is None
    mark = expiry_mark(signal, 100.1, 119_500, 120_000, 5_000, "LIVE_EXECUTED_TRADE")
    assert mark["classification"] == "ESTIMATED_LOSS"  # Favorable gross move did not cover costs.
    signal.risk.clear()
    mark = expiry_mark(signal, 102, 119_500, 120_000, 5_000, "LIVE_EXECUTED_TRADE")
    assert mark["classification"] == "FAVORABLE_GROSS_COSTS_UNKNOWN"
    assert mark["estimated_net_r"] is None


def test_downtime_expiry_uses_complete_closed_candle_only(signal):
    signal.state = "ALERTED"
    signal.horizon_profile = "CORE_INTRADAY"
    signal.holding_deadline_ms = 120_000
    signal.evidence["observed_entry_ms"] = 1
    signal.risk["cost_per_base"] = 0.2
    bar = Candle(60_000, 60_000, 100, 103, 99, 102, 10, 1000)
    result = advance(signal, [bar], 60_000, 120_000, 180_000)
    assert result["state"] == "EXPIRED"
    assert result["expiry_mark"]["method"] == "CLOSED_1M_OHLC"
    assert result["expiry_mark"]["estimated_net_r"] == pytest.approx(0.36)
    assert "expiry_mark" not in advance(signal, [], 60_000, 120_000, 180_000)


@pytest.mark.asyncio
async def test_reconciled_expiry_persists_mark_before_discord_update(settings, signal):
    signal.source = "binance"
    signal.state = "ALERTED"
    signal.horizon_profile = "CORE_INTRADAY"
    signal.holding_deadline_ms = 120_000
    signal.evidence["observed_entry_ms"] = 1
    signal.coverage["monitor_cursor_event_ms"] = 60_000
    signal.risk["cost_per_base"] = 0.2
    store = Store(settings.data_dir)
    store.signal(signal)
    bar = Candle(60_000, 60_000, 100, 103, 99, 102, 10, 1000)
    notifier = SimpleNamespace(send_research=AsyncMock(return_value="terminal-persisted"))
    scanner = SimpleNamespace(
        store=store,
        settings=settings,
        exchange="binance",
        api=SimpleNamespace(candles=AsyncMock(return_value=[bar])),
        streams=SimpleNamespace(books={}, tapes={}),
        recorder=Mock(healthy=True),
        notifier=notifier,
        reconcile_pending={signal.id},
    )
    await reconcile(scanner, signal, 180_000)
    saved = store.signals()[0]
    assert saved["state"] == "EXPIRED"
    assert saved["evidence"]["expiry_mark"]["estimated_net_r"] == pytest.approx(0.36)
    assert embed(signal.model_validate(saved), "http://localhost")["embeds"][0]["title"].startswith(
        "TRACKING ENDED"
    )
    notifier.send_research.assert_awaited_once()
    store.close()


def test_entry_window_expiry_remains_unpriced_and_research_retains_partial_result(settings, signal):
    signal.state = "EXPIRED"
    signal.coverage["tracking_end"] = False
    assert embed(signal, "http://localhost")["embeds"][0]["title"].startswith("ENTRY EXPIRED")

    signal.coverage["tracking_end"] = True
    signal.coverage["monitoring"] = "active"
    signal.evidence["observed_entry_ms"] = 100_000
    signal.evidence["score_components"] = {"test": 1}
    signal.risk["cost_per_base"] = 0.2
    signal.evidence["expiry_mark"] = expiry_mark(signal, 102, 119_500, 120_000, 5_000, "LIVE_EXECUTED_TRADE")
    store = Store(settings.data_dir)
    start_observation(store, signal, 120_000, checkpoints=[10])
    row = store.db.execute("SELECT payload FROM observations WHERE signal_id=?", (signal.id,)).fetchone()
    assert row is not None
    import json

    observation = json.loads(row[0])
    assert observation["primary_outcome"] == "EXPIRED"
    assert observation["primary_net_r"] == pytest.approx(0.36)
    unverified = signal.model_copy(deep=True)
    unverified.id = "unverified-expiry"
    unverified.evidence.pop("expiry_mark")
    unverified.evidence["latest_observed_price"] = 104
    start_observation(store, unverified, 120_000, checkpoints=[10])
    raw = store.db.execute("SELECT payload FROM observations WHERE signal_id=?", (unverified.id,)).fetchone()[
        0
    ]
    assert json.loads(raw)["primary_net_r"] is None
    store.close()
