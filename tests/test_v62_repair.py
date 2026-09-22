import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
from pydantic import SecretStr

from bybit_flow.lifecycle import advance_trades
from bybit_flow.models import Candle, Trade
from bybit_flow.native_streams import NativeStreams
from bybit_flow.notifications import Notifier
from bybit_flow.orderflow import Book, Tape
from bybit_flow.production import evaluate_intraday_confirmation
from bybit_flow.reconciliation import reconcile
from bybit_flow.storage import Store
from bybit_flow.thesis_health import evaluate, summary


async def test_transient_stop_wick_and_durable_terminal(settings, signal):
    store = Store(settings.data_dir)
    signal.state = "ALERTED"
    store.signal(signal)
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            ("research:test-signal:initial", signal.id, "sent", "{}", "123", 1),
        )
    tape = Tape()
    for i, price in enumerate((100, 94, 101)):
        tape.add(
            Trade(signal.symbol, 2000 + i * 1000, 2000 + i * 1000, str(i), "Sell", Decimal(price), Decimal(1))
        )
    before = (signal.entry, signal.zone, signal.stop, signal.tp1, signal.tp2, signal.quality, signal.version)
    advance_trades(signal, tape, 10000)
    assert signal.state == "INVALIDATED"
    assert signal.evidence["terminal_event"]["effective_ms"] == 3000
    assert signal.evidence["terminal_event"]["method"] == "LIVE_EXECUTED_TRADE"
    store.signal(signal, signal.invalidation)
    stale = signal.model_copy(deep=True)
    stale.state = "ALERTED"
    store.signal(stale)
    assert not store.active_signals()
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    attempts = []

    def transport(request):
        attempts.append(request)
        return httpx.Response(503 if len(attempts) == 1 else 200, json={"id": "123"})

    notifier = Notifier(settings, store, transport=httpx.MockTransport(transport))
    await notifier.retry_terminals()
    with store.db:
        store.db.execute("UPDATE terminal_events SET next_ms=0")
    await notifier.retry_terminals()
    await notifier.retry_terminals()
    assert len(attempts) == 2 and all(r.method == "PATCH" for r in attempts)
    assert store.db.execute("SELECT notification_status FROM terminal_events").fetchone()[0] == "sent"
    assert before == (
        signal.entry,
        signal.zone,
        signal.stop,
        signal.tp1,
        signal.tp2,
        signal.quality,
        signal.version,
    )
    store.close()


async def test_restart_gap_original_source(settings, signal):
    store = Store(settings.data_dir)
    signal.state = "ALERTED"
    signal.source = "binance"
    signal.coverage["monitor_cursor_event_ms"] = 60000
    store.signal(signal)
    scanner = SimpleNamespace(
        store=store,
        settings=settings,
        exchange="binance",
        source_ready=True,
        recorder=Mock(healthy=True),
        streams=SimpleNamespace(books={}, tapes={}),
        api=SimpleNamespace(
            candles=AsyncMock(return_value=[Candle(60000, 60000, 100, 102, 94, 100, 1, 100)])
        ),
        reconcile_pending={signal.id},
        notifier=SimpleNamespace(send_research=AsyncMock()),
    )
    await reconcile(scanner, signal, 180000)
    assert signal.state == "INVALIDATED"
    event = json.loads(store.db.execute("SELECT payload FROM terminal_events").fetchone()[0])
    assert event["source"] == "binance" and event["method"] == "CLOSED_1M_OHLC"
    assert event["effective_ms"] == 120000 and not store.active_signals()
    store.close()


async def test_stream_timeout_reselect_and_reconcile(settings, signal, monkeypatch):
    store = Store(settings.data_dir)
    settings.deep_symbols = 1
    signal.source, signal.state = "binance", "ALERTED"
    store.signal(signal)
    streams = NativeStreams(settings, store, Mock(healthy=True), SimpleNamespace(name="binance"))
    connected = asyncio.Event()
    calls = 0

    async def market(symbol):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError()
        connected.set()
        await asyncio.Event().wait()

    streams.binance_market = market
    streams.binance_depth_recovery = AsyncMock(side_effect=lambda s: None)
    streams.required_symbols = {signal.symbol}
    await streams.select(["OPTIONAL"])
    await asyncio.wait_for(connected.wait(), 4)
    assert streams.selected == (signal.symbol,) and streams.reconnects[signal.symbol] == 1
    assert store.active_signals()[0]["state"] == "ALERTED"
    # A crashed task is also restarted when the selected symbols did not change.
    streams.tasks[signal.symbol].cancel()
    await asyncio.gather(streams.tasks[signal.symbol], return_exceptions=True)
    previous = streams.tasks[signal.symbol]
    await streams.select([])
    assert streams.tasks[signal.symbol] is not previous
    await streams.stop()
    assert not streams.tasks and not streams.selected
    book, tape = Book(), Tape()
    book.valid = True
    book.bids, book.asks = {Decimal(99): Decimal(1)}, {Decimal(101): Decimal(1)}
    book.event_ms = book.receipt_ms = tape.last_event = tape.last_receipt = 180000
    tape.coverage_start = 120000
    signal.coverage["monitor_cursor_event_ms"] = 60000
    scanner = SimpleNamespace(
        store=store,
        settings=settings,
        exchange="binance",
        source_ready=True,
        recorder=Mock(healthy=True),
        streams=SimpleNamespace(books={signal.symbol: book}, tapes={signal.symbol: tape}),
        api=SimpleNamespace(
            candles=AsyncMock(
                return_value=[
                    Candle(60000, 60000, 100, 102, 98, 100, 1, 100),
                    Candle(120000, 60000, 100, 102, 98, 100, 1, 100),
                ]
            )
        ),
        reconcile_pending={signal.id},
        notifier=SimpleNamespace(send_research=AsyncMock()),
    )
    assert not await reconcile(scanner, signal, 180000)
    assert await reconcile(scanner, signal, 180001)
    assert signal.coverage["monitor_cursor_event_ms"] == 180000
    store.close()


def test_wicky_reclaim_hold_flow_and_profile(signal):
    signal.horizon_profile, signal.family = "SHORT_INTRADAY", "liquidity_sweep"
    signal.evidence.update(
        wick_regime={"state": "WICKY"},
        structural_trigger=dict(valid=True, level=100, extreme=98, available_ms=300000),
        trigger_bar_end=300000,
    )
    flow = dict(available=True, delta_pct=30, cvd_slope=10, delta_persistence=0.8, initiative_long=True)
    reclaim = Candle(300000, 300000, 99, 102, 99, 101, 1, 100)
    failed = Candle(600000, 300000, 101, 102, 97, 99, 1, 100)
    held = Candle(600000, 300000, 101, 103, 100, 102, 1, 100)
    assert not evaluate_intraday_confirmation(signal, flow, [reclaim], 600000)["passed"]
    assert not evaluate_intraday_confirmation(signal, flow, [reclaim, failed], 900000)["passed"]
    assert not evaluate_intraday_confirmation(signal, flow, [reclaim, held], 900000)["passed"]
    signal.evidence["volume_profile"] = dict(
        available=True, coverage_complete=True, available_ms=900000, end_ms=900000, rejection_low=True
    )
    assert evaluate_intraday_confirmation(signal, flow, [reclaim, held], 900000)["passed"]


def test_horizon_health_and_accounting(settings, signal):
    store = Store(settings.data_dir)
    observation = dict(
        coverage_complete=True,
        flow=dict(delta_pct=-30, cvd_slope=-10, delta_persistence=0.9),
        obi=-0.3,
        structural_failure=True,
        structure_timeframe="15",
        window_end_ms=1,
    )
    for horizon, toxic in [("SHORT_INTRADAY", True), ("CORE_INTRADAY", False), ("SWING", False)]:
        signal.horizon_profile = horizon
        health = {}
        for now in range(1, 180002, 60000):
            observation["window_end_ms"] = now
            health = evaluate(signal, dict(observation), health, now)
        assert health["withdraw"] == toxic
        signal.id = horizon
        store.signal(signal)
        if horizon != "SWING":
            store.put("thesis_health:" + signal.id, health)
    result = summary(store)
    assert (
        result["current"] + result["degraded"] + result["paused_for_coverage"] + result["unavailable"]
        == result["eligible"]
        == 3
    )
    store.close()
