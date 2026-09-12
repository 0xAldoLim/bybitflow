from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

from bybit_flow.native_streams import NativeStreams
from bybit_flow.orderflow import Book, Tape
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Store


async def test_stale_other_symbol_does_not_block_confirmation_evidence(settings, signal, monkeypatch):
    now, end = 2_000_000, 1_800_000
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: now)
    monkeypatch.setattr("bybit_flow.native_streams.now_ms", lambda: now)
    monkeypatch.setattr("bybit_flow.scanner.candle_features", lambda *args: {"atr": 1})
    flow = {"available": False}
    calculate_flow = Mock(return_value=flow)
    monkeypatch.setattr("bybit_flow.scanner.footprint", calculate_flow)
    monkeypatch.setattr("bybit_flow.scanner.evaluate_risk", lambda *args: {"reasons": [], "accepted": False})
    monkeypatch.setattr("bybit_flow.scanner.score", lambda *args: None)
    monkeypatch.setattr("bybit_flow.ml.inference.apply", lambda *args: None)
    store = Store(settings.data_dir)
    signal.source = "binance"
    signal.expires_ms = now + 900_000
    signal.evidence = {"trigger_bar_end": end}
    store.signal(signal)
    scanner = Scanner.__new__(Scanner)
    scanner.settings, scanner.store = settings, store
    scanner.api = SimpleNamespace(name="binance")
    scanner.recorder = Mock(healthy=True)
    scanner.context = {
        signal.symbol: {
            "m15": [SimpleNamespace(end=end)],
            "asof": now,
            "instrument": SimpleNamespace(tick=Decimal("0.01"), base="TEST"),
            "derivatives": {"funding_rate": 0, "ticker_observed_ms": now},
        }
    }
    streams = NativeStreams.__new__(NativeStreams)
    streams.settings = settings
    streams.selected = (signal.symbol, "STALEUSDT")
    book = Book()
    book.valid = True
    book.bids, book.asks = {Decimal("99.99"): Decimal(100)}, {Decimal("100.01"): Decimal(100)}
    book.receipt_ms = book.event_ms = now
    tape = Tape()
    tape.coverage_start = end - 900_000
    tape.last_event = tape.last_receipt = now
    streams.books = {signal.symbol: book, "STALEUSDT": Book()}
    streams.tapes = {signal.symbol: tape, "STALEUSDT": Tape()}
    streams.liquidations = {signal.symbol: []}
    scanner.streams = streams
    assert not streams.connected
    assert streams.connected_for(signal.symbol)
    await scanner.evaluate()
    calculate_flow.assert_called_once()
    saved = store.signals()[0]
    assert saved["coverage"]["trade_window_complete"]
    assert saved["coverage"]["reasons"] == []
    assert "executed order flow did not confirm family trigger" in saved["gates"]
    # Losing this signal's own history must still block evidence evaluation.
    tape.coverage_start = end
    calculate_flow.reset_mock()
    await scanner.evaluate()
    calculate_flow.assert_not_called()
    saved = store.signals()[0]
    assert not saved["coverage"]["trade_window_complete"]
    assert "full closed 15-minute trade window not yet retained" in saved["coverage"]["reasons"]
    store.close()
