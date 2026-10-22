"""Representation regressions: exact values, retention bounds and rotation ownership."""

import asyncio
import gc
import json
import weakref
from collections import deque
from decimal import ROUND_DOWN, Decimal, FloatOperation, InvalidOperation, localcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from bybit_flow import exchanges, models
from bybit_flow import streams as bybit_streams
from bybit_flow.backtest import PaperPosition
from bybit_flow.native_streams import NativeStreams
from bybit_flow.orderflow import Book, BookGap, Tape, footprint


@pytest.fixture(autouse=True)
def clear_trade_decimal_cache():
    models._trade_decimal_text.cache_clear()
    yield
    models._trade_decimal_text.cache_clear()


@pytest.mark.parametrize(
    "value", ["100.00", "100.0", "1E+2", "-0.000", "0.12345678901234567890123456789", " 1_000.00 "]
)
def test_shared_wire_decimal_preserves_exact_representation(value):
    with localcontext() as context:
        context.prec = 3
        context.rounding = ROUND_DOWN
        initial_flags = dict(context.flags)
        expected = Decimal(value)
        first = models.trade_decimal(value)
        second = models.trade_decimal(json.loads(json.dumps(value)))
        assert first is second
        assert first.as_tuple() == expected.as_tuple()
        assert str(first) == str(expected)
        assert dict(context.flags) == initial_flags
    # Numerically equal spellings must not erase trailing zeros/exponents.
    assert str(models.trade_decimal("100.00")) == "100.00"
    assert str(models.trade_decimal("1E+2")) == "1E+2"


@pytest.mark.parametrize(
    "value", ["NaN", "sNaN", "Infinity", "-Infinity", "invalid", "", "1e999999999999999999999999", 0.1]
)
@pytest.mark.parametrize("trap", [False, True])
def test_nonfinite_invalid_and_float_inputs_preserve_context_on_every_call(value, trap):
    def observe(constructor):
        with localcontext() as context:
            context.clear_flags()
            context.traps[InvalidOperation] = trap
            context.traps[FloatOperation] = trap
            try:
                result = constructor(value)
                outcome = ("value", result.as_tuple())
            except Exception as exc:
                outcome = (type(exc), exc.args)
            return outcome, dict(context.flags)

    expected = observe(Decimal)
    assert observe(models.trade_decimal) == expected
    assert observe(models.trade_decimal) == expected
    assert models._trade_decimal_text.cache_info().currsize == 0


def test_cache_ownership_is_bounded_and_eviction_does_not_mutate_retained_trades():
    retained = models.trade_decimal("123.4500")
    for i in range(models.TRADE_DECIMAL_CACHE_SIZE + 20):
        models.trade_decimal(f"{i}.123456")
    info = models._trade_decimal_text.cache_info()
    assert info.currsize == info.maxsize == models.TRADE_DECIMAL_CACHE_SIZE
    assert retained.as_tuple() == Decimal("123.4500").as_tuple()
    long_value = "1." + "0" * 70
    before = models._trade_decimal_text.cache_info()
    assert models.trade_decimal(long_value).as_tuple() == Decimal(long_value).as_tuple()
    assert models._trade_decimal_text.cache_info() == before


@pytest.mark.parametrize("venue", ["binance", "bybit", "okx"])
def test_all_trade_fields_and_contract_multiplier_match_uncached_decoder(monkeypatch, venue):
    raw = {
        "binance": dict(s="TESTUSDT", T=2000, a=123, p="100.0000", q="0.012300", m=True),
        "bybit": dict(s="TESTUSDT", T=2000, i="123", p="100.0000", v="0.012300", S="Sell"),
        "okx": dict(
            instId="TEST-USDT-SWAP", ts="2000", tradeId="123", px="100.0000", sz="0.012300", side="sell"
        ),
    }[venue]
    instrument = SimpleNamespace(
        contract_multiplier=Decimal("0.001234"), exchange_symbol="TEST-USDT-SWAP", symbol="TESTUSDT"
    )
    actual = exchanges.trade_from_wire(venue, raw, 2050, instrument)
    monkeypatch.setattr(exchanges, "trade_decimal", Decimal)
    expected = exchanges.trade_from_wire(venue, raw, 2050, instrument)
    assert actual == expected
    assert actual.price.as_tuple() == expected.price.as_tuple()
    assert actual.size.as_tuple() == expected.size.as_tuple()


def make_trade(ident, event=None):
    return exchanges.trade_from_wire(
        "binance",
        dict(
            s="TESTUSDT",
            T=event if event is not None else ident * 1000,
            a=ident,
            p="100.00",
            q="2.500",
            m=False,
        ),
        ident * 1000 + 50,
    )


def test_tape_fifo_deduplication_coverage_and_reset_unchanged():
    tape = Tape(3, source="binance", symbol="TESTUSDT")
    assert tape.add(make_trade(1))
    assert not tape.add(make_trade(1))
    for ident in (2, 3, 4):
        assert tape.add(make_trade(ident))
    assert [t.trade_id for t in tape.trades] == ["2", "3", "4"]
    assert list(tape.ids) == ["2", "3", "4"]
    assert tape.coverage_start == 1001
    # An evicted ID may be accepted again; current duplicate IDs may not.
    assert tape.add(make_trade(1, 5000))
    assert not tape.add(make_trade(4, 6000))
    assert list(tape.ids) == ["3", "4", "1"]
    assert tape.coverage_start == 2001
    with pytest.raises(BookGap, match="Out-of-order"):
        tape.add(make_trade(6, 4000))
    assert not tape.ids and not tape.trades
    assert tape.coverage_start == 6050
    assert tape.last_event == tape.last_receipt == 0
    assert tape.add(make_trade(1, 7000))
    tape.reset(8000)
    assert not tape.ids and not tape.trades and tape.coverage_start == 8000


def test_flow_event_features_and_paper_outcome_match_without_sharing(monkeypatch, signal):
    def evaluate():
        tape = Tape(3000, source="binance", symbol="TESTUSDT")
        position = PaperPosition(signal, 10, fee_bps=6, slippage_bps=2)
        for i in range(4000):
            raw = dict(
                s="TESTUSDT",
                T=500 + i * 50,
                a=i,
                p="94.00" if i == 3999 else str(100 + i % 5 / 100),
                q=str(1 + i % 33),
                m=bool(i % 2),
            )
            trade = exchanges.trade_from_wire("binance", raw, raw["T"] + 10)
            tape.add(trade)
            position.on_trade(trade)
        return (
            list(tape.trades),
            list(tape.ids),
            tape.coverage_start,
            footprint(tape.window(140000, 200000), Decimal("0.01"), 1),
            tape.event_features.snapshot(140000, 200000, 200510, True),
            vars(position),
        )

    actual = evaluate()
    monkeypatch.setattr(exchanges, "trade_decimal", Decimal)
    assert evaluate() == actual


def test_bybit_collector_retains_identical_trades_and_recorded_wire_payload(monkeypatch, settings):
    def collect():
        messages = []
        recorder = SimpleNamespace(
            healthy=True,
            offer=lambda *args, **kwargs: messages.append(json.dumps(args[3], sort_keys=True)) or True,
        )
        stream = bybit_streams.Streams(settings, SimpleNamespace(), recorder)
        stream.books["TESTUSDT"], stream.tapes["TESTUSDT"] = Book(), Tape(10)
        msg = dict(
            topic="publicTrade.TESTUSDT",
            ts=2000,
            data=[
                dict(T=2000, i="1", S="Buy", p="100.000", v="2.5000"),
                dict(T=2000, i="block", S="Buy", p="100.000", v="2.5000", BT=True),
            ],
        )
        stream.process(msg, 2050)
        return list(stream.tapes["TESTUSDT"].trades), messages

    actual = collect()
    monkeypatch.setattr(bybit_streams, "trade_decimal", Decimal)
    assert collect() == actual
    assert len(actual[0]) == 1


@pytest.mark.parametrize("venue", ["binance", "bybit"])
async def test_rotation_collects_removed_market_children_and_retains_selected_and_lifecycle(
    settings, monkeypatch, venue
):
    recorder = SimpleNamespace(healthy=True, offer=lambda *args, **kwargs: True)
    store = SimpleNamespace(put=lambda *args: None)
    if venue == "binance":
        stream = NativeStreams(settings, store, recorder, SimpleNamespace(name=venue))

        async def worker(symbol):
            await asyncio.Event().wait()

        monkeypatch.setattr(stream, "run_symbol", worker)
        stream.required_symbols = {"ACTIVEUSDT"}
    else:
        stream = bybit_streams.Streams(settings, store, recorder)
        stream.ws, stream.connected = AsyncMock(), True
    try:
        await stream.select(["KEEPUSDT", "OLDUSDT", "ACTIVEUSDT"])
        old_refs = [
            weakref.ref(mapping["OLDUSDT"]) for mapping in (stream.books, stream.tapes, stream.liquidations)
        ]
        stream.frames["OLDUSDT"] = deque([{"large-child": [0] * 10000}], maxlen=120)
        old_refs.append(weakref.ref(stream.frames["OLDUSDT"]))
        kept = [(stream.books[s], stream.tapes[s]) for s in ("KEEPUSDT", "ACTIVEUSDT")]
        # Lifecycle includes its required subscription in Bybit's supplied union.
        await stream.select(["KEEPUSDT", "NEWUSDT"] + (["ACTIVEUSDT"] if venue == "bybit" else []))
        gc.collect()
        assert all(ref() is None for ref in old_refs)
        assert [(stream.books[s], stream.tapes[s]) for s in ("KEEPUSDT", "ACTIVEUSDT")] == kept
        assert not any(
            "OLDUSDT" in mapping
            for mapping in (stream.books, stream.tapes, stream.liquidations, stream.frames)
        )
    finally:
        await stream.stop()
