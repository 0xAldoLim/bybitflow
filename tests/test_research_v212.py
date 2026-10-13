from copy import deepcopy

import pytest

from bybit_flow.models import Candle
from bybit_flow.orderflow import Book
from bybit_flow.quality_v211 import entry_quality, structural
from bybit_flow.research_v212 import (
    POLICY,
    capture_initial,
    execution_change,
    relative_volume,
    remember_volume,
    suppression,
)
from bybit_flow.storage import Store
from bybit_flow.strategy import candidates


def book(at=1000):
    b = Book()
    b.apply(
        dict(
            type="snapshot",
            ts=at,
            data=dict(s="TESTUSDT", b=[["99.99", "10000"]], a=[["100.01", "10000"]], u=1, seq=1),
        ),
        at,
    )
    return b


@pytest.mark.parametrize("direction,price", [("LONG", 100.01), ("SHORT", 99.99)])
def test_initial_quote_once_and_legacy_unchanged(settings, signal, instrument, direction, price):
    signal.direction = direction
    if direction == "SHORT":
        signal.stop = 105
        signal.tp1 = 85
    original = deepcopy(signal.evidence)
    assert not capture_initial(signal, book(), settings, 1000, instrument)
    assert signal.evidence == original
    signal.evidence["research_instrumentation"] = POLICY
    assert capture_initial(signal, book(), settings, 1000, instrument)
    saved = deepcopy(signal.evidence["initial_executable_quote"])
    assert saved["executable_price"] == price
    assert saved["cost_components"]["spread_added_per_base"] == 0
    assert not capture_initial(signal, book(2000), settings, 2000, instrument)
    assert signal.evidence["initial_executable_quote"] == saved
    assert signal.entry == 100 and signal.risk == {}


def test_stale_initial_quote_not_frozen(settings, signal, instrument):
    signal.evidence["research_instrumentation"] = POLICY
    assert not capture_initial(signal, book(), settings, settings.book_stale_ms + 1001, instrument)
    assert "initial_executable_quote" not in signal.evidence


def test_execution_deterioration_is_measurement(settings, signal, instrument):
    signal.evidence["research_instrumentation"] = POLICY
    signal.evidence["execution_features"] = {"atr": 2}
    capture_initial(signal, book(), settings, 1000, instrument)
    initial = deepcopy(signal.evidence["initial_executable_quote"])
    signal.risk = {"cost_per_base": initial["cost_per_base"]}
    q = entry_quality(
        signal,
        100.5,
        2000,
        current={"cost_per_base": initial["cost_per_base"] + 0.1},
        quote={"spread_bps": 3},
    )
    d = execution_change(signal, q)
    assert d["state"] == "EXECUTION_DEGRADED"
    assert d["net_rr_change"] == pytest.approx(q["cost_adjusted_rr_now"] - initial["net_rr"])
    assert d["initial_to_current_price_move_atr"] == pytest.approx((100.5 - 100.01) / 2)
    assert d["elapsed_ms"] == 1000 and signal.evidence["initial_executable_quote"] == initial
    assert d["research_only"] and signal.gates == []


def observations(interval, days=31, bucket=0):
    return [Candle(i * 86400000 + bucket, interval, 100, 101, 99, 100, 10 + i, 1000) for i in range(days)]


@pytest.mark.parametrize(
    "field", ["initial_executable_quote", "stop_plan", "execution_features", "h1", "m15"]
)
def test_null_optional_research_mapping_is_missing_evidence(signal, field):
    quality = dict(available_ms=2000)
    baseline = execution_change(signal, quality)
    signal.evidence[field] = None
    before = signal.model_dump()
    assert execution_change(signal, quality) == baseline
    assert signal.model_dump() == before
    assert baseline["state"] == "EXECUTION_UNAVAILABLE" and baseline["research_only"]


@pytest.mark.parametrize("field", ["current_execution_cost_estimate", "quote_provenance"])
def test_null_optional_quote_mapping_does_not_block_research(signal, field):
    quality = dict(available_ms=2000)
    baseline = execution_change(signal, quality)
    quality[field] = None
    assert execution_change(signal, quality) == baseline


@pytest.mark.parametrize("interval", [900000, 3600000, 14400000, 86400000])
def test_relative_volume_sessions_causal_bounded(settings, interval):
    store = Store(settings.data_dir)
    bars = observations(interval)
    now = bars[-1].end
    remember_volume(store, "binance", "TESTUSDT", interval, bars, now)
    q = relative_volume(store, "binance", "TESTUSDT", bars[-1], now)
    assert q["state"] == "KNOWN" and 3 <= q["reference_sample_count"] <= 20
    assert q["reference_end_ms"] <= bars[-1].start
    before = deepcopy(q)
    future = Candle(now + 86400000, interval, 100, 101, 99, 100, 999999, 1000)
    remember_volume(store, "binance", "TESTUSDT", interval, [future], now)
    assert relative_volume(store, "binance", "TESTUSDT", bars[-1], now) == before
    assert relative_volume(store, "bybit", "TESTUSDT", bars[-1], now)["state"] == "UNKNOWN"
    assert relative_volume(store, "binance", "OTHERUSDT", bars[-1], now)["state"] == "UNKNOWN"
    other = Candle(bars[-1].start + interval, interval, 100, 101, 99, 100, 100, 1000)
    if interval < 86400000:
        assert relative_volume(store, "binance", "TESTUSDT", other, other.end)["state"] == "UNKNOWN"
    store.close()


def test_relative_volume_minimum_and_completed_only(settings):
    store = Store(settings.data_dir)
    bars = observations(900000, 3)
    remember_volume(store, "binance", "TESTUSDT", 900000, bars, bars[-1].end)
    assert relative_volume(store, "binance", "TESTUSDT", bars[-1], bars[-1].end)["state"] == "UNKNOWN"
    assert (
        relative_volume(store, "binance", "TESTUSDT", bars[-1], bars[-1].start)["current_completed_volume"]
        is None
    )
    store.close()


def test_second_excursion_is_only_an_observation(settings, instrument, monkeypatch):
    bars = [Candle(i * 60000, 60000, 100, 102, 98, 100, 10, 1000) for i in range(80)]
    bars[76] = Candle(76 * 60000, 60000, 100, 102, 97, 100, 10, 1000)
    bars[79] = Candle(79 * 60000, 60000, 100, 102, 96, 100, 10, 1000)
    f = dict(regime="range", low=98, high=102, atr=2, ma20=100, sweep_long=False, sweep_short=False)
    first = structural(bars, "range_rejection", "LONG", f, f, bars[-1].end)
    later = structural(bars, "range_rejection", "LONG", f, f, bars[-1].end, _research_start_after=76)
    assert not first["candidate_event"] and later["candidate_event"]
    monkeypatch.setattr("bybit_flow.strategy.candle_features", lambda *args: f.copy())
    captured = []
    plans = candidates(
        instrument,
        bars,
        bars,
        bars,
        bars[-1].end,
        families=("range_rejection",),
        quality_policy="trade-validity-v21.1",
        structural_observations=captured,
    )
    assert not any(p.direction == "LONG" for p in plans)
    assert any(r.get("diagnostic_suppression") for r in captured)
    store = Store(settings.data_dir)
    kwargs = dict(
        source="binance",
        symbol="TESTUSDT",
        horizon="CORE_INTRADAY",
        family="range_rejection",
        direction="LONG",
        now=bars[-1].end,
        reason="test",
    )
    assert suppression(store, first, later, **kwargs)["classification"] == "POTENTIAL_DISTINCT_EVENT"
    assert suppression(store, first, first, **kwargs)["classification"] == "TRUE_DUPLICATE"
    assert len(store.get("structural-suppressions-v212")) == 2
    assert store.db.execute("SELECT count(*) FROM signals").fetchone()[0] == 0
    assert store.db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    rows = store.get("structural-suppressions-v212")
    store.put("structural-suppressions-v212", [rows[0]] * 1000)
    suppression(store, first, dict(later, structure_event_id="another"), **kwargs)
    assert len(store.get("structural-suppressions-v212")) == 1000
    store.close()


async def test_relative_volume_warmup_never_replaces_production_candles(settings, instrument):
    from unittest.mock import AsyncMock

    from bybit_flow.scanner import Scanner
    from bybit_flow.storage import Recorder

    store = Store(settings.data_dir)
    scanner = Scanner(settings, store, Recorder(store, settings))
    bars = [Candle(i * 900000, 900000, 100, 101, 99, 100, 10, 1000) for i in range(960)]
    asof = bars[-1].end
    key = (instrument.symbol, "15")
    original = (asof // 900000, bars[-80:])
    scanner.candle_cache[key] = original
    scanner.candle_cache_limits[key] = (original[0], 80)
    scanner.api.candles = AsyncMock(return_value=bars)
    await scanner.volume_research(instrument, "15", original[1], asof)
    assert scanner.candle_cache[key] is original
    assert scanner.candle_cache_limits[key] == (original[0], 80)
    assert relative_volume(store, instrument.exchange, instrument.symbol, bars[-1], asof)["state"] == "KNOWN"
    await scanner.volume_research(instrument, "15", original[1], asof)
    assert scanner.api.candles.await_count == 1
    await scanner.api.close()
    store.close()
