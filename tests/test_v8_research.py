"""Focused V8 causality, corroboration and bounded-computation checks."""

import asyncio
from collections import OrderedDict, deque
from dataclasses import replace
from types import SimpleNamespace

import pytest
from conftest import synthetic_bars

from bybit_flow import anchored, breadth, context_ev, liquidation, ofi, opportunity, spot_perp, volatility
from bybit_flow.ml.ablation import report as ablation_report
from bybit_flow.ml.features import snapshot
from bybit_flow.models import Candle
from bybit_flow.orderflow import Book, BookGap
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Store, now_ms


def test_context_ev_uses_only_prior_cache_and_shrinks(signal):
    keys = list(context_ev._keys(signal))
    parent = dict(samples=100, effective_samples=100, mean_net_r=0, dispersion_r=1)
    child = dict(samples=50, effective_samples=50, mean_net_r=10, dispersion_r=1)
    cache = dict(
        policy=context_ev.POLICY,
        available_ms=100,
        max_label_available_ms=90,
        groups={keys[0]: parent, keys[1]: child},
    )
    result = context_ev.assess(cache, signal, 200)
    assert result["matched_level"] == 2
    assert result["shrunk_expectancy_r"] == 5
    assert result["score_effect"] == 0 and not result["production_gate"]
    cache["groups"][keys[1]]["samples"] = 2
    assert context_ev.assess(cache, signal, 200)["matched_level"] == 1
    assert context_ev.assess(cache, signal, 99)["status"] == "INSUFFICIENT"


def test_liquidation_amount_needs_independent_corroboration():
    at = 1_000_000
    events = [
        dict(
            event_ms=at - 20_000,
            receipt_ms=at - 19_000,
            liquidated_side="LONG",
            price="100",
            notional="10000",
        )
    ]
    baseline = deque([100] * 25, maxlen=120)
    common = dict(
        price=98,
        baseline=baseline,
        derivatives={"oi_change_pct": -2},
        book={"depletion_60s": {"bid": 1000}, "replenishment_60s": {"bid": 10}},
    )
    assert liquidation.assess(events, at, "binance", baseline=baseline)["state"] == "NORMAL"
    continuation = liquidation.assess(events, at, "binance", flow={"delta_pct": -30}, **common)
    assert continuation["state"] == "DELEVERAGING_CONTINUATION"
    exhaustion = liquidation.assess(
        events,
        at,
        "binance",
        price=101,
        baseline=baseline,
        derivatives={"oi_change_pct": -2},
        flow={"absorption_long": True, "cvd_acceleration": 1},
    )
    assert exhaustion["state"] == "LONG_LIQUIDATION_EXHAUSTION"


def test_breadth_requires_fresh_universe():
    bars = synthetic_bars(31)
    at = bars[-1].end
    rows = [
        dict(symbol=symbol, source="binance", eligible=True, h1=bars, h4=bars, m15=bars)
        for symbol in ("BTCUSDT", "ETHUSDT", "AUSDT", "BUSDT", "CUSDT", "DUSDT", "EUSDT", "FUSDT")
    ]
    assert breadth.assess(rows[:3], at, "binance")["state"] == "INSUFFICIENT"
    result = breadth.assess(rows, at, "binance")
    assert result["denominator"] == 8
    assert result["source_ms"] <= at


def test_breadth_broad_risk_on_vs_btc_led(monkeypatch):
    bars = [
        Candle(
            i * 3_600_000,
            3_600_000,
            100 * 1.01**i,
            101 * 1.01**i,
            99 * 1.01**i,
            100 * 1.01**i,
            100,
            10000 * 1.01**i,
        )
        for i in range(31)
    ]
    at = bars[-1].end
    rows = [
        dict(
            symbol=symbol,
            source="binance",
            eligible=True,
            h1=bars,
            h4=bars,
            h1_features={"regime": "trending up"},
            h4_features={"regime": "trending up"},
        )
        for symbol in ("BTCUSDT", "ETHUSDT", "AUSDT", "BUSDT", "CUSDT", "DUSDT", "EUSDT", "FUSDT")
    ]
    assert breadth.assess(rows, at, "binance")["state"] == "BROAD_RISK_ON"
    monkeypatch.setattr(breadth, "_beta", lambda *_: 1.0)
    for row in rows[1:]:
        alt = bars[:-1] + [replace(bars[-1], close=bars[-2].close * 1.001)]
        row["h1"] = row["h4"] = alt
    assert breadth.assess(rows, at, "binance")["state"] == "BTC_LED_RALLY"


def test_ofi_book_reset_and_trade_delta_independence():
    assert ofi.l1_event((100, 2), (101, 2), (100, 3), (101, 1)) > 0
    assert ofi.l1_event((100, 2), (101, 2), (100, 1), (101, 3)) < 0
    book = Book()
    book.apply(
        dict(type="snapshot", ts=1000, data={"u": 1, "seq": 1, "b": [["100", "2"]], "a": [["101", "2"]]}),
        1000,
    )
    for index in range(2, 8):
        at = index * 1000
        book.apply(
            dict(
                type="delta", ts=at, data={"u": index, "seq": index, "b": [["100", str(index + 2)]], "a": []}
            ),
            at,
        )
    result = ofi.assess(book, 7000, "bybit")
    assert not result["ofi_available"] and result["ofi_l1_60s"] > 0
    assert result["ofi_coverage_seconds"] == 6
    assert "delta_pct" not in result
    book.apply(
        dict(type="snapshot", ts=8000, data={"u": 8, "seq": 8, "b": [["100", "2"]], "a": [["101", "2"]]}),
        8000,
    )
    assert not ofi.assess(book, 8000, "bybit")["ofi_available"]
    with pytest.raises(BookGap):
        book.apply(dict(type="delta", ts=9000, data={"u": 8, "seq": 7, "b": [["100", "4"]], "a": []}), 9000)
    assert not ofi.assess(book, 9000, "bybit")["ofi_available"]


def test_anchored_and_volatility_use_closed_history():
    bars = synthetic_bars(70, interval=60_000)
    at = bars[-1].end
    assert anchored.avwap(bars, bars[-10].start, at, "binance")["bars"] == 10
    assert anchored.avwap(bars, at + 60_000, at, "binance")["state"] == "UNAVAILABLE"
    assert anchored.time_acceptance(bars, 100, at, "binance")["source_ms"] == at
    assert volatility.assess(bars, at, "binance")["source_ms"] == at
    assert volatility.assess(bars + synthetic_bars(1, 60_000, at), at, "binance")["state"] == "INSUFFICIENT"


def test_spot_perp_comparison_is_missing_aware_and_source_preserving():
    at = 100_000
    spot = dict(
        source_ms=at - 1000,
        available_ms=at,
        price=100,
        return_1m=0.001,
        return_5m=0.002,
        delta=10,
        flow_trust=1,
    )
    perp = dict(
        source="bybit",
        source_ms=at - 1000,
        available_ms=at,
        price=100.1,
        return_1m=0.001,
        return_5m=0.002,
        delta=20,
        flow_trust=0.8,
    )
    result = spot_perp.compare(spot, perp, at)
    assert result["state"] == "SPOT_PERP_CONFIRMED"
    assert result["perp_source"] == "bybit"
    assert spot_perp.compare(spot | {"available_ms": at - 91_000}, perp, at)["state"] == "UNAVAILABLE"
    leveraged = spot_perp.compare(
        spot | {"return_1m": 0.0001},
        perp | {"return_1m": 0.005, "price": 101, "oi_change_1m_pct": 2},
        at,
        previous={"spot_perp_basis_bps": 0, "available_ms": at - 1000},
    )
    assert leveraged["state"] == "LEVERAGED_PERP_ONLY_MOVE"


def test_opportunity_redundancy_does_not_rescore(signal):
    bars = synthetic_bars(31)
    a = signal.model_copy(deep=True)
    b = signal.model_copy(deep=True)
    b.id, b.symbol = "second", "SECONDUSDT"
    for row in (a, b):
        row.state = "CONFIRMED"
        row.quality = 70
        row.evidence["market_factor"] = {"beta_to_btc": 1.2}
    ranked = opportunity.rank([a, b], bars[-1].end, {a.symbol: bars, b.symbol: bars})
    assert ranked[a.id]["redundancy_penalty"] > 0
    assert a.quality == b.quality == 70
    b.evidence["market_factor"] = {"beta_to_btc": 0.1}
    assert opportunity.rank([a, b], bars[-1].end)[a.id]["redundancy_penalty"] == 0


@pytest.mark.asyncio
async def test_derivative_ttl_feature_cache_and_rest_bound(bars):
    scanner = Scanner.__new__(Scanner)

    class API:
        name = "binance"
        calls = 0

        async def history(self, *_):
            self.calls += 1
            return [{"timestamp": "1"}]

    scanner.api = API()
    scanner.derivative_cache = {}
    scanner.derivative_cache_hits = scanner.derivative_cache_misses = 0
    scanner.feature_cache = OrderedDict()
    scanner.feature_cache_hits = scanner.feature_cache_misses = 0
    scanner.rest_semaphore = asyncio.Semaphore(3)
    scanner.rest_reduced_semaphore = asyncio.Semaphore(1)
    scanner.rest_reduced_until = 0
    scanner.rest_inflight = scanner.rest_queue_depth = 0
    at = now_ms()
    assert (await scanner.cached_derivative_history("open-interest", "BTCUSDT", 0, at, 180_000))[0]
    await scanner.cached_derivative_history("open-interest", "BTCUSDT", 0, at + 1, 180_000)
    assert scanner.api.calls == 1 and scanner.derivative_cache_hits == 1
    at = bars[-1].end
    scanner.cached_features("BTCUSDT", "60", bars, at)
    scanner.cached_features("BTCUSDT", "60", bars, at)
    assert scanner.feature_cache_hits == 1
    scanner.cached_features("BTCUSDT", "60", bars + synthetic_bars(1, 3_600_000, at), at + 3_600_000)
    assert scanner.feature_cache_misses == 2
    peak = active = 0

    async def work():
        nonlocal peak, active
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0)
        active -= 1

    await asyncio.gather(*(scanner.bounded_rest(work()) for _ in range(12)))
    assert peak <= 3 and scanner.rest_queue_depth == 0


def test_v8_snapshot_future_observation_is_missing(signal):
    signal.evidence["volatility"] = dict(source="bybit", source_ms=1500, available_ms=2500, rv_ratio=2)
    frozen = snapshot(signal, 2000, "decision")
    assert frozen["schema_version"] == "candidate-v10"
    assert frozen["values"]["rv_ratio"] is None
    assert frozen["feature_metadata"]["rv_ratio"]["missing"]


def test_ablation_reports_insufficient_without_consuming_holdout(tmp_path):
    store = Store(tmp_path)
    try:
        result = ablation_report(store, "binance", now_ms())
        assert result["status"] == "INSUFFICIENT_EVIDENCE"
        assert not result["holdout_consumed"]
        assert store.db.execute("SELECT count(*) FROM ml_holdouts").fetchone()[0] == 0
    finally:
        store.close()


@pytest.mark.asyncio
async def test_optional_v8_worker_failure_is_isolated(monkeypatch):
    from bybit_flow import v8_runtime

    class MemoryStore:
        def __init__(self):
            self.values = {}

        def get(self, key, default=None):
            return self.values.get(key, default)

        def put(self, key, value):
            self.values[key] = value

        def active_signals(self):
            return []

    scanner = SimpleNamespace(
        exchange="binance",
        streams=SimpleNamespace(selected=["BTCUSDT"], books={}, tapes={}, liquidations={}),
        context={},
        candle_cache={},
        v8_cache={"OLDUSDT": {"liquidation": {"state": "NORMAL"}, "ofi": {"ofi_available": True}}},
        liquidation_baseline={},
        store=MemoryStore(),
        settings=SimpleNamespace(book_stale_ms=5000),
        pending_symbols=lambda: [],
    )
    monkeypatch.setattr(v8_runtime.volatility, "assess", lambda *_: (_ for _ in ()).throw(ValueError()))
    await v8_runtime.refresh(scanner)
    assert scanner.store.get("v8_research")["errors"] == {"BTCUSDT": "ValueError", "ETHUSDT": "ValueError"}
    assert scanner.store.get("v8_research")["liquidation_available"] == 0
    assert scanner.store.get("v8_research")["ofi_symbols_ready"] == 0
    assert scanner.v8_cache == {}
