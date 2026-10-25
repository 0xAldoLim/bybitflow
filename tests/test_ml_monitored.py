import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from bybit_flow.lifecycle import advance_trades
from bybit_flow.ml.bootstrap import CandleCache
from bybit_flow.ml.cli import train_ready
from bybit_flow.ml.features import snapshot
from bybit_flow.ml.inference import apply, select_compatible
from bybit_flow.ml.monitored import POLICY, backfill, dataset, evaluate, pending
from bybit_flow.ml.registry import Registry
from bybit_flow.ml.store import FeatureStore, canonical, digest
from bybit_flow.ml.training import read_dataset
from bybit_flow.models import Candle, Trade
from bybit_flow.storage import Store


def plan(signal, horizon="SWING"):
    s = signal.model_copy(deep=True)
    s.source, s.created_ms, s.state = "binance", 120_000, "ALERTED"
    s.horizon_profile = horizon
    s.expected_hold_max = 2880 if horizon == "SWING" else 120
    s.trigger_expires_ms = 1_020_000
    s.holding_deadline_ms = s.created_ms + s.expected_hold_max * 60_000
    s.expires_ms = s.holding_deadline_ms
    s.risk = dict(accepted=True, cost_per_base=0.2, net_rr=2.96)
    return s


def bar(start, opening=100, high=102, low=98, close=100):
    return Candle(start, 60_000, opening, high, low, close, 100, 10_000)


def touch(s, at=130_000):
    trade = Trade(s.symbol, at, at, "entry-print", "Buy", Decimal("100"), Decimal("1"), s.source)
    advance_trades(s, SimpleNamespace(trades=[trade]), at)
    assert s.evidence["observed_entry_method"] == "LIVE_EXECUTED_TRADE"
    assert s.evidence["observed_entry_price"] == 100


@pytest.mark.parametrize("horizon", ["SWING", "CORE_INTRADAY", "SHORT_INTRADAY"])
def test_known_entry_then_offline_stop_becomes_loss_without_local_tape(signal, horizon):
    s = plan(signal, horizon)
    row = snapshot(s, 121_000, "decision") | dict(id="snapshot")
    touch(s)
    # No recorder rows exist during this interval; recovery uses public history.
    stop_at = 86_520_000 if horizon == "SWING" else 600_000
    bars = [bar(t) for t in range(120_000, stop_at, 60_000)] + [bar(stop_at, low=94)]
    outcome = evaluate(row, s.model_dump(), bars, "binance", stop_at + 120_000)
    assert outcome["complete"] and outcome["classification"] == "loss"
    assert outcome["outcome"] == "STOP" and outcome["net_r"] == pytest.approx(-1.04)
    assert outcome["entry_basis"] == "recorded-live-price-touch"
    assert not outcome["account_fill_verified"] and not outcome["local_continuous_recording_required"]


def test_missing_history_and_ambiguous_order_never_manufacture_loss(signal):
    s = plan(signal)
    row = snapshot(s, 121_000, "decision") | dict(id="snapshot")
    touch(s)
    missing = evaluate(row, s.model_dump(), [bar(120_000), bar(240_000, low=94)], "binance", 360_000)
    assert missing["outcome"] == "INCOMPLETE" and missing["net_r"] is None
    both = evaluate(row, s.model_dump(), [bar(120_000), bar(180_000, low=94, high=116)], "binance", 300_000)
    assert both["outcome"] == "AMBIGUOUS" and not both["complete"]
    partial = evaluate(row, s.model_dump(), [bar(120_000, low=94)], "binance", 300_000)
    assert partial["outcome"] == "AMBIGUOUS" and partial["net_r"] is None
    with pytest.raises(ValueError, match="original venue"):
        evaluate(row, s.model_dump(), [bar(120_000)], "okx", 300_000)


def test_assumed_entry_and_original_deadline_are_explicit(signal):
    s = plan(signal, "CORE_INTRADAY")
    s.holding_deadline_ms = 420_000
    row = snapshot(s, 121_000, "decision") | dict(id="snapshot")
    result = evaluate(
        row, s.model_dump(), [bar(t, close=101) for t in range(120_000, 420_000, 60_000)], "binance", 500_000
    )
    assert result["complete"] and result["outcome"] == "TIME_EXIT"
    assert result["exit_ms"] == 420_000  # Do not extend by the later decision timestamp.
    assert result["entry_basis"] == "historical-zone-touch"
    assert not result["account_fill_verified"]


@pytest.mark.parametrize("outcome,low,high", [("TARGET", 84, 102), ("STOP", 98, 106)])
def test_recovered_short_target_or_stop(signal, outcome, low, high):
    s = plan(signal)
    s.direction, s.stop, s.tp1 = "SHORT", 105, 85
    row = snapshot(s, 121_000, "decision") | dict(id="snapshot")
    touch(s)
    result = evaluate(
        row, s.model_dump(), [bar(120_000), bar(180_000, low=low, high=high)], "binance", 300_000
    )
    assert result["complete"] and result["outcome"] == outcome
    assert (result["net_r"] > 0) == (outcome == "TARGET")


def test_unfilled_setup_is_not_a_failed_trade(signal):
    s = plan(signal, "CORE_INTRADAY")
    s.holding_deadline_ms = 420_000
    row = snapshot(s, 121_000, "decision") | dict(id="snapshot")
    bars = [bar(t, opening=110, low=108, high=112, close=111) for t in range(120_000, 420_000, 60_000)]
    result = evaluate(row, s.model_dump(), bars, "binance", 500_000)
    assert result["outcome"] == "NO_ENTRY" and result["net_r"] is None and not result["complete"]


@pytest.mark.asyncio
async def test_incomplete_history_cache_is_refetched(settings, monkeypatch):
    store = Store(settings.data_dir)
    monkeypatch.setattr("bybit_flow.ml.bootstrap.asyncio.sleep", AsyncMock())
    candles = AsyncMock(
        side_effect=[[bar(120_000), bar(240_000)], [bar(t) for t in (120_000, 180_000, 240_000)]]
    )
    market = SimpleNamespace(name="binance", candles=candles)
    cache = CandleCache(store, POLICY)
    state = dict(requests=0, cache_hits=0, cached_ranges=0)
    first = await cache.get(market, "TESTUSDT", 120_000, 300_000, state, require_complete=True)
    assert len(first) == 2
    second = await cache.get(market, "TESTUSDT", 120_000, 300_000, state, require_complete=True)
    assert len(second) == 3 and candles.await_count == 2
    assert await cache.get(market, "TESTUSDT", 120_000, 300_000, state, require_complete=True) == second
    assert state["cache_hits"] == 1 and candles.await_count == 2
    store.close()


@pytest.mark.asyncio
async def test_restart_retries_gap_then_trains_on_recovered_loss_and_keeps_prints(
    settings, signal, monkeypatch
):
    store = Store(settings.data_dir)
    s = plan(signal)
    ident = FeatureStore(store).capture(s, 121_000, "decision")
    touch(s)
    s.state = "INVALIDATED"
    s.evidence.update(
        primary_outcome="STOP", terminal_event=dict(effective_ms=240_000, method="CLOSED_1M_OHLC")
    )
    store.signal(s)
    original_plan = s.model_dump(mode="json")
    FeatureStore(store).label(
        ident, dict(policy="prints-v1", complete=False, classification="incomplete", net_r=None), 140_000
    )
    clock = [300_000]
    monkeypatch.setattr("bybit_flow.ml.monitored.now_ms", lambda: clock[0])
    market = SimpleNamespace(name="binance")
    calls = []

    async def history(self, market, symbol, start, end, state, **kwargs):
        calls.append((start, end))
        return [bar(120_000), bar(180_000, low=94)] if len(calls) > 1 else [bar(180_000, low=94)]

    monkeypatch.setattr("bybit_flow.ml.monitored.CandleCache.get", history)
    assert len(pending(store, "binance", 300_000)) == 1  # Swing can finish before 48 hours.
    result = await backfill(store, settings, "binance", market=market)
    assert result["pending"] == 1
    assert not store.db.execute("SELECT 1 FROM ml_labels WHERE policy=?", (POLICY,)).fetchone()
    store.close()
    store = Store(settings.data_dir)
    assert (await backfill(store, settings, "binance", market=market))["complete"] == 0
    assert len(calls) == 1  # Restarting the worker cannot reset the durable retry cooldown.
    clock[0] = 600_001
    assert (await backfill(store, settings, "binance", market=market))["complete"] == 1
    rows = dataset(store, "binance", clock[0])
    assert len(rows) == 1 and rows[0]["id"] == ident
    assert rows[0]["label"]["classification"] == "loss"
    assert rows[0]["label_available_ms"] == 600_001
    path = FeatureStore(store).write_dataset(rows)
    assert read_dataset(path, track="monitored")[0]["id"] == ident
    with pytest.raises(ValueError, match="label policies"):
        read_dataset(path, track="primary")
    assert not FeatureStore(store).dataset(300_000, source="binance")
    assert (
        json.loads(store.db.execute("SELECT payload FROM signals WHERE id=?", (s.id,)).fetchone()[0])
        == original_plan
    )
    assert not pending(store, "binance", clock[0])
    assert (await backfill(store, settings, "binance", market=market))["complete"] == 0
    assert len(calls) == 2
    store.close()


def test_automatic_monitored_fit_dispatched_and_not_below_minimum(settings, monkeypatch):
    store = Store(settings.data_dir)
    store.put("ml_monitored_trainability", dict(binance=dict(trainable=499)))
    calls = []

    def fit(store, source, two_stage):
        calls.append((source, two_stage))
        return dict(id="m" * 32)

    monkeypatch.setattr("bybit_flow.ml.monitored.train", fit)
    assert train_ready(settings, store)["status"] == "collecting"
    store.put("ml_monitored_trainability", dict(binance=dict(trainable=500)))
    result = train_ready(settings, store)
    assert result["tracks"]["monitored:binance"]["status"] == "challenger"
    assert calls == [("binance", settings.ml_two_stage)]
    store.close()


@pytest.mark.asyncio
async def test_windows_busy_recovery_lock_defers(settings, monkeypatch):
    store = Store(settings.data_dir)

    def busy(path):
        error = OSError("Recovery already running")
        error.winerror = 33
        raise error

    monkeypatch.setattr("bybit_flow.ml.locking.exclusive", busy)
    assert (await backfill(store, settings, "binance"))["status"] == "DEFERRED_BUSY"
    store.close()


def test_monitored_challenger_never_filters_or_claims_execution(settings, signal, monkeypatch):
    store = Store(settings.data_dir)
    settings.ml_enabled = settings.ml_filter_research = True
    model = dict(
        id="c" * 32,
        track="monitored",
        label_fidelity="MONITORED_OHLC_PROXY",
        model=dict(thresholds=dict(min_probability=0.7, min_quality=95)),
    )
    monkeypatch.setattr("bybit_flow.ml.inference.select_compatible", lambda *a: (model, False, []))
    monkeypatch.setattr("bybit_flow.ml.inference.predict", lambda *a: [0.01])
    monkeypatch.setattr("bybit_flow.ml.inference.explain", lambda *a: {})
    levels = signal.entry, signal.stop, signal.tp1, signal.quality
    apply(signal, settings, store, 2000)
    assert signal.validation_status == "monitored_challenger"
    assert signal.evidence["ml"]["production_authority"] == "none"
    assert not signal.gates and signal.calibrated_probability is None
    assert levels == (signal.entry, signal.stop, signal.tp1, signal.quality)
    store.close()


def test_monitored_priority_and_promotion_barrier(settings, signal):
    store = Store(settings.data_dir)
    signal.source = "binance"
    row = snapshot(signal, 120_000, "decision")
    registry = Registry(store)
    m = dict(
        id="c" * 32,
        created_ms=100_000,
        track="monitored",
        feature_schema_version=signal.feature_schema_version,
        label_policy=POLICY,
        source="binance",
        stage="decision",
        periods=dict(holdout=dict(end=100_000)),
        model={},
        minimum_coverage=0,
        strategy_versions=[signal.version],
    )
    registry.register(m)
    registry.register(
        m
        | dict(
            id="b" * 32,
            track="bootstrap",
            feature_schema_version="bootstrap-core-v1",
            label_policy="ohlc-path-v1",
        )
    )
    selected, champion, _ = select_compatible(registry, signal, row, 120_000)
    assert selected["id"] == m["id"] and not champion
    with pytest.raises(ValueError, match="cannot be promoted"):
        registry.promote(m["id"], "Named reviewer")
    primary = m | dict(id="a" * 32, track="primary", label_policy="prints-v1")
    with store.db:
        store.db.execute(
            "INSERT INTO ml_models VALUES(?,?,?,?)",
            (primary["id"], 90_000, canonical(primary), digest(primary)),
        )
    assert select_compatible(registry, signal, row, 120_000)[0]["id"] == primary["id"]
    store.close()


def queued(store, signal, name, *, invalid=False, horizon="SWING"):
    s = plan(signal, horizon)
    s.id, s.symbol = name, name.upper() + "USDT"
    if invalid:
        s.zone = (110, 112)  # An invalid frozen plan cannot become an invented trade.
    ident = FeatureStore(store).capture(s, 121_000, "decision")
    if not invalid:
        touch(s)
    s.state = "INVALIDATED"
    s.evidence.update(primary_outcome="STOP", terminal_event=dict(effective_ms=240_000))
    store.signal(s)
    return ident, s


async def test_invalid_old_batch_no_longer_starves_newer_interrupted_swing(settings, signal, monkeypatch):
    store = Store(settings.data_dir)
    first, _ = queued(store, signal, "badone", invalid=True)
    second, _ = queued(store, signal, "badtwo", invalid=True)
    good, s = queued(store, signal, "goodswing")
    original = store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (good,)).fetchone()[0]
    monkeypatch.setattr("bybit_flow.ml.monitored.now_ms", lambda: 300_000)
    calls = []

    async def history(self, market, symbol, start, end, state, **kwargs):
        calls.append(symbol)
        return [bar(120_000), bar(180_000, low=94)]

    monkeypatch.setattr("bybit_flow.ml.monitored.CandleCache.get", history)
    market = SimpleNamespace(name="binance")
    # Process all three to prove invalid inputs do not consume REST or block a good long setup.
    result = await backfill(store, settings, "binance", limit=3, market=market)
    assert result["excluded"] == 2 and result["complete"] == 1
    assert calls == [s.symbol]
    assert len(dataset(store, "binance", 300_000)) == 1
    assert store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (good,)).fetchone()[0] == original
    assert not pending(store, "binance", 300_000)
    for ident in (first, second):
        label = json.loads(
            store.db.execute(
                "SELECT payload FROM ml_labels WHERE snapshot_id=? AND policy=?", (ident, POLICY)
            ).fetchone()[0]
        )
        assert not label["complete"] and label["outcome"] == "INVALID_FROZEN_PLAN"
    store.close()


async def test_unavailable_old_history_yields_to_other_horizons_and_retries_after_restart(
    settings, signal, monkeypatch
):
    store = Store(settings.data_dir)
    old, old_signal = queued(store, signal, "missing", horizon="SWING")
    new, new_signal = queued(store, signal, "fresh", horizon="SHORT_INTRADAY")
    clock = [300_000]
    monkeypatch.setattr("bybit_flow.ml.monitored.now_ms", lambda: clock[0])
    # Make one old row due first without mutating its immutable feature payload.
    from bybit_flow.ml.monitored import save_attempt

    save_attempt(
        store, {"id": new, "source": "binance", "signal": new_signal.model_dump()}, "PENDING", retry=False
    )
    calls = []

    async def history(self, market, symbol, start, end, state, **kwargs):
        calls.append(symbol)
        return [] if symbol == old_signal.symbol else [bar(120_000), bar(180_000, low=94)]

    monkeypatch.setattr("bybit_flow.ml.monitored.CandleCache.get", history)
    market = SimpleNamespace(name="binance")
    assert (await backfill(store, settings, "binance", limit=1, market=market))["pending"] == 1
    store.close()
    store = Store(settings.data_dir)
    assert (await backfill(store, settings, "binance", limit=1, market=market))["complete"] == 1
    assert calls == [old_signal.symbol, new_signal.symbol]
    assert not pending(store, "binance", clock[0])
    clock[0] += 300_001
    assert [r["id"] for r in pending(store, "binance", clock[0])] == [old]
    store.close()


async def test_long_history_resumes_cached_chunks_instead_of_redownloading(settings, monkeypatch):
    from bybit_flow.ml.monitored import path_history

    store = Store(settings.data_dir)
    cache = CandleCache(store, POLICY)
    calls, fail = [], [True]
    monkeypatch.setattr("bybit_flow.ml.bootstrap.asyncio.sleep", AsyncMock())

    async def candles(symbol, timeframe, end, *, limit, start):
        calls.append(start)
        if start == 480 * 60_000 and fail[0]:
            raise httpx.ReadTimeout("fixture")
        return [bar(t) for t in range(start, end, 60_000)]

    market = SimpleNamespace(name="binance", candles=candles)
    state = dict(requests=0, cache_hits=0, cached_ranges=0)
    with pytest.raises(httpx.ReadTimeout):
        await path_history(cache, market, "TESTUSDT", 0, 961 * 60_000, state)
    fail[0] = False
    result = await path_history(cache, market, "TESTUSDT", 0, 961 * 60_000, state)
    assert len(result) == 961 and calls.count(0) == 1
    assert state["cache_hits"] == 1
    store.close()


async def test_recovered_swing_stops_fetching_once_original_outcome_is_proven(settings, signal, monkeypatch):
    store = Store(settings.data_dir)
    s = plan(signal)
    ident = FeatureStore(store).capture(s, 121_000, "decision")
    frozen = store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
    row = dict(id=ident, **json.loads(frozen))
    touch(s)
    store.signal(s)  # The original live lifecycle has not seen the offline stop.
    asof = s.holding_deadline_ms + 60_000
    monkeypatch.setattr("bybit_flow.ml.monitored.now_ms", lambda: asof)
    full = [bar(t, low=94 if t == 180_000 else 98) for t in range(120_000, s.holding_deadline_ms, 60_000)]
    calls = []

    async def history(self, market, symbol, start, end, state, **kwargs):
        calls.append((start, end))
        return [b for b in full if start <= b.start < end]

    monkeypatch.setattr("bybit_flow.ml.monitored.CandleCache.get", history)
    result = await backfill(store, settings, "binance", market=SimpleNamespace(name="binance"))
    assert result["complete"] == 1 and len(calls) == 1
    label = dataset(store, "binance", asof)[0]["label"]
    assert label == evaluate(row, s.model_dump(), full, "binance", asof, asof)
    assert label["classification"] == "loss" and label["outcome"] == "STOP"
    assert calls[0][1] < s.holding_deadline_ms
    assert store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0] == frozen
    store.close()
