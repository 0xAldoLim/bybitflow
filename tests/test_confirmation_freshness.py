from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

from bybit_flow.native_streams import NativeStreams
from bybit_flow.orderflow import Book, Tape
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Store


async def test_live_updates_during_one_candidate_do_not_make_next_candidate_stale(
    settings, signal, monkeypatch
):
    clock = {"now": 1_801_000}
    end = 1_800_000
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: clock["now"])
    monkeypatch.setattr("bybit_flow.native_streams.now_ms", lambda: clock["now"])
    monkeypatch.setattr("bybit_flow.scanner.candle_features", lambda *args: {"atr": 1})
    monkeypatch.setattr("bybit_flow.scanner.evaluate_risk", lambda *args: {"reasons": [], "accepted": False})
    monkeypatch.setattr("bybit_flow.scanner.score", lambda *args, **kwargs: None)
    monkeypatch.setattr("bybit_flow.ml.inference.apply", lambda *args: None)
    store = Store(settings.data_dir)
    scanner = Scanner.__new__(Scanner)
    scanner.settings, scanner.store = settings, store
    scanner.recorder = Mock(healthy=True)
    scanner.api = SimpleNamespace(name="binance")
    scanner.context = {}
    streams = NativeStreams.__new__(NativeStreams)
    streams.settings = settings
    streams.selected = ("FIRSTUSDT", "SECONDUSDT", "STALEUSDT")
    streams.books, streams.tapes, streams.liquidations = {}, {}, {}
    plans = []
    for i, symbol in enumerate(streams.selected):
        plan = signal.model_copy(deep=True)
        plan.id, plan.symbol, plan.source = symbol, symbol, "binance"
        plan.created_ms = 1000 - i  # Stable evaluation order.
        plan.expires_ms = clock["now"] + 900_000
        plan.trigger_expires_ms = clock["now"] + 120_000
        plan.evidence = {
            "trigger_bar_end": end,
            "execution_window_ms": 60_000,
            "execution_window_end_ms": end,
        }
        store.signal(plan)
        plans.append(plan)
        book = Book()
        book.valid = True
        book.bids, book.asks = {Decimal("99.99"): Decimal(100)}, {Decimal("100.01"): Decimal(100)}
        book.receipt_ms = book.event_ms = clock["now"] - (20_000 if symbol == "STALEUSDT" else 0)
        tape = Tape()
        tape.coverage_start = end - 60_000
        tape.last_receipt = tape.last_event = book.receipt_ms
        streams.books[symbol], streams.tapes[symbol], streams.liquidations[symbol] = book, tape, []
        scanner.context[symbol] = dict(
            m15=[SimpleNamespace(end=end)],
            asof=clock["now"],
            instrument=SimpleNamespace(tick=Decimal("0.01"), base=symbol),
            derivatives=dict(funding_rate=0, ticker_observed_ms=clock["now"]),
        )
    scanner.streams = streams

    def flow(*args):
        # Models a real feed callback running while footprint yields to a worker.
        clock["now"] += 1000
        for symbol in streams.selected[:2]:
            streams.books[symbol].receipt_ms = streams.books[symbol].event_ms = clock["now"]
            streams.tapes[symbol].last_receipt = streams.tapes[symbol].last_event = clock["now"]
        return {"available": False}

    calculate = Mock(side_effect=flow)
    monkeypatch.setattr("bybit_flow.scanner.footprint", calculate)
    await scanner.evaluate()
    saved = {row["symbol"]: row for row in store.signals()}
    assert calculate.call_count == 2
    for plan in plans[:2]:
        row = saved[plan.symbol]
        assert row["coverage"]["trade_window_complete"]
        assert row["coverage"]["reasons"] == []
        assert row["coverage"]["checked_ms"] <= clock["now"]
        assert "executed order flow did not confirm family trigger" in row["gates"]
        assert row["state"] == "PENDING CONFIRMATION"  # Rejected flow is still rejected.
        assert (row["entry"], row["stop"], row["tp1"], row["trigger_expires_ms"]) == (
            plan.entry,
            plan.stop,
            plan.tp1,
            plan.trigger_expires_ms,
        )
    assert not saved["STALEUSDT"]["coverage"]["trade_window_complete"]
    assert "COVERAGE_BOOK_STALE" in saved["STALEUSDT"]["coverage"]["reason_codes"]
    store.close()


async def test_generation_readiness_uses_receipt_time_after_candle_io(settings, signal, monkeypatch):
    clock = {"now": 1_801_000}
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: clock["now"])
    store = Store(settings.data_dir)
    scanner = Scanner.__new__(Scanner)
    scanner.settings = settings.model_copy(
        update={"execution_window_seconds": 60, "horizon_profiles": ["CORE_INTRADAY"]}
    )
    scanner.store, scanner.recorder = store, Mock(healthy=True)
    scanner.api = SimpleNamespace(name="binance")
    scanner.pending_symbols = lambda: []
    inst = SimpleNamespace(symbol=signal.symbol)
    scanner.context = {signal.symbol: {"instrument": inst, "asof": clock["now"]}}
    book, tape = Book(), Tape()
    book.valid = True
    tape.coverage_start = 1_680_000
    scanner.streams = SimpleNamespace(
        selected=(signal.symbol,), books={signal.symbol: book}, tapes={signal.symbol: tape}
    )

    async def fetch(*args):
        clock["now"] += 1000
        book.receipt_ms = book.event_ms = clock["now"]
        tape.last_receipt = tape.last_event = clock["now"]
        return [SimpleNamespace(end=1_800_000, interval=3_600_000)]

    scanner.cached_candles = fetch
    generate = Mock(return_value=[])
    monkeypatch.setattr("bybit_flow.scanner.candidates", generate)
    await scanner.refresh_once()
    generate.assert_called_once()
    assert generate.call_args.args[4] == clock["now"]
    assert store.get("flow_warmup:binance:" + signal.symbol)["ready"]
    assert scanner.context[signal.symbol]["asof"] == clock["now"]
    store.close()
