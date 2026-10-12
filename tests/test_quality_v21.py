import json
from copy import deepcopy

import pytest

from bybit_flow.horizons import assign
from bybit_flow.models import Candle
from bybit_flow.quality_v21 import (
    POLICY,
    apply,
    counterfactual,
    entry_quality,
    liquidity_update,
    overlaps_existing,
    path_quality,
    structural,
)
from bybit_flow.storage import Store
from bybit_flow.trade_quality import load_rows, metrics, summarize


def tagged(signal):
    signal.evidence["trigger_bar_end"] = signal.created_ms
    assign(signal, "CORE_INTRADAY")
    signal.evidence["production_policies"] = {"trade_quality": POLICY}
    signal.risk = dict(cost_per_base=0.2, accepted=True, net_rr=2.8)
    signal.coverage["trade_window_complete"] = True
    return signal


def flat_bars():
    return [Candle(i * 60000, 60000, 100, 102, 98, 100 + i % 2 * 0.1, 10, 1000) for i in range(80)]


def replace_bar(bars, index, low, high, close):
    old = bars[index]
    bars[index] = Candle(old.start, old.interval, 100, high, low, close, 10, 1000)


def structure(bars, family, direction="LONG"):
    return structural(bars, family, direction, {"regime": "range"}, {"low": 98, "high": 102}, bars[-1].end)


def test_frozen_range_reclaim_and_future_candle_exclusion():
    bars = flat_bars()
    replace_bar(bars, -1, 97, 101, 99)
    result = structure(bars, "range_rejection")
    assert result["reclaimed"] and not result["reasons"]
    assert result["frozen_low"] == 98 and result["level_known_ms"] <= bars[-1].start
    future = Candle(bars[-1].end, 60000, 99, 999, 1, 90, 10, 1000)
    assert (
        structural(
            bars + [future],
            "range_rejection",
            "LONG",
            {"regime": "range"},
            {"low": 98, "high": 102},
            bars[-1].end,
        )
        == result
    )
    replace_bar(bars, -1, 98.1, 101, 99)
    assert "FAILED_RECLAIM" in structure(bars, "range_rejection")["reasons"]


def test_acceptance_two_closes_and_failed_retest():
    bars = flat_bars()
    replace_bar(bars, -3, 101, 104, 103)
    replace_bar(bars, -2, 102, 105, 104)
    replace_bar(bars, -1, 101.9, 104, 103)
    result = structure(bars, "breakout_retest")
    assert result["accepted"] and result["retest_held"] and not result["reasons"]
    assert result["frozen_high"] == 102
    replace_bar(bars, -3, 101, 104, 101)
    assert "FAILED_ACCEPTANCE" in structure(bars, "breakout_retest")["reasons"]
    replace_bar(bars, -3, 101, 104, 103)
    replace_bar(bars, -1, 101, 104, 101)
    assert not structure(bars, "breakout_retest")["retest_held"]


@pytest.mark.parametrize("horizon", ["SHORT_INTRADAY", "CORE_INTRADAY", "SWING", "EXTENDED_SWING"])
def test_entry_valid_chased_and_stale_by_original_window(signal, horizon):
    tagged(signal)
    assign(signal, horizon)
    assert entry_quality(signal, 100, 1100)["entry_not_chased"]
    assert not entry_quality(signal, 110, 1100)["entry_not_chased"]
    assert not entry_quality(signal, 100, signal.trigger_expires_ms or signal.expires_ms)["entry_not_chased"]


def test_old_active_plan_unchanged_by_new_policy(signal):
    before = deepcopy(signal.model_dump())
    apply(signal, 120, 999999999, {"state": "LIQUIDITY_STRESSED"})
    assert signal.model_dump() == before
    assert overlaps_existing(signal, [before])


def test_flow_contradiction_and_cost_gate_retain_plan(signal):
    tagged(signal)
    signal.evidence["flow"] = dict(
        available=True, delta_pct=30, price_change=-0.2, quality=dict(flow_trust_score=0.9)
    )
    levels = (signal.entry, signal.zone, signal.stop, signal.tp1, signal.tp2)
    apply(signal, 100, 1200)
    assert "MECHANISM_CONTRADICTED" in signal.gates
    assert signal.final_tier == "REJECTED"
    assert levels == (signal.entry, signal.zone, signal.stop, signal.tp1, signal.tp2)
    signal.gates = []
    signal.evidence["flow"]["price_change"] = 0.2
    signal.risk["cost_per_base"] = 5
    apply(signal, 100, 1300)
    assert "COST_DOMINATES_RISK" in signal.gates


def sample(spread=1, depth=100):
    return dict(spread=spread, depth=depth, impact=1, epoch=1, session="ASIA")


def test_liquidity_causal_hysteresis_and_outage_reset():
    state = {}
    for i in range(30):
        state = liquidity_update(state, sample(), (i + 1) * 60000)
    future = dict(sample(999, 1), at_ms=999999999)
    polluted = deepcopy(state)
    polluted["rows"].append(future)
    for minute in range(31, 34):
        next_state = liquidity_update(state, sample(10, 1), minute * 60000)
        if minute == 31:
            assert next_state == liquidity_update(polluted, sample(10, 1), minute * 60000)
        state = next_state
    assert state["state"] == "LIQUIDITY_STRESSED"
    # Same minute cannot fabricate recovery persistence.
    assert liquidity_update(state, sample(), 33 * 60000 + 100) == state
    state = liquidity_update(state, sample(), 34 * 60000)
    assert state["state"] == "LIQUIDITY_STRESSED"
    state = liquidity_update(state, sample(), 35 * 60000)
    assert state["state"] == "LIQUIDITY_NORMAL"
    assert liquidity_update(state, sample(20, 0.1), 60 * 60000)["bad_run"] == 1


def test_volume_strength_beats_raw_barrier_count(signal):
    small = dict(
        raw_volume_profile=[dict(price=101 + i, buy=1, sell=0) for i in range(3)]
        + [dict(price=90, buy=97, sell=0)]
    )
    large = dict(raw_volume_profile=[dict(price=105, buy=90, sell=0), dict(price=90, buy=10, sell=0)])
    assert (
        path_quality(signal, small, [101, 102, 103])["path_difficulty_score"]
        < path_quality(signal, large, [105])["path_difficulty_score"]
    )
    assert path_quality(signal, {}, [105])["path_difficulty_score"] is None


def test_pullback_counts_separate_touches_without_banning_later_attempts():
    bars = flat_bars()
    for i in range(60, 80):
        replace_bar(bars, i, 101, 103, 102)
    replace_bar(bars, 65, 98, 103, 102)
    replace_bar(bars, 75, 98, 103, 102)
    context = {"regime": "trending up", "previous_regime": "trending up"}
    result = structural(bars, "trend_pullback", "LONG", context, {"low": 98, "high": 103}, bars[-1].end)
    assert result["pullback_attempt_index"] >= 2
    assert "REGIME_SETUP_MISMATCH" not in result["reasons"]


def test_new_opportunity_identity_does_not_change_every_scan(instrument, monkeypatch):
    from bybit_flow.strategy import candidates

    bars = flat_bars()
    replace_bar(bars, -1, 97, 101, 99)
    feature = dict(regime="range", atr=2, low=98, high=102, ma20=100, sweep_long=True, sweep_short=False)
    monkeypatch.setattr("bybit_flow.strategy.candle_features", lambda *args: dict(feature))
    first = candidates(
        instrument,
        bars,
        bars,
        bars,
        bars[-1].end,
        families=["range_rejection"],
        execution_window_ms=60000,
        quality_policy=True,
    )
    second = candidates(
        instrument,
        bars,
        bars,
        bars,
        bars[-1].end + 60000,
        families=["range_rejection"],
        execution_window_ms=60000,
        quality_policy=True,
    )
    assert first and second
    assert [s.id for s in first] == [s.id for s in second]


def test_rejected_snapshot_uses_existing_label_path(settings, signal):
    tagged(signal)
    counterfactual(signal, ["FAILED_RECLAIM"], 1200)
    store = Store(settings.data_dir)
    try:
        store.signal(signal)
        store.signal(signal)
        saved = store.db.execute(
            "SELECT payload FROM ml_snapshots WHERE signal_id=? AND stage='decision'", (signal.id,)
        ).fetchall()
        assert len(saved) == 1
        assert json.loads(saved[0][0])["signal"]["evidence"]["v21_counterfactual"]["stop"] == signal.stop
        assert store.db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
        rows, truncated = load_rows(store.db, 9999999999999, days=365)
        assert not rows and not truncated  # Old decision outside requested time range.
    finally:
        store.close()


def test_audit_separates_policy_visibility_and_unknown_cost(signal):
    tagged(signal)
    s = signal.model_dump()
    s["risk"] = {}
    row = dict(
        snapshot_id="one",
        receipt=["sent", "123"],
        snapshot=dict(signal=s, decision_ms=86400000),
        labels=[
            dict(
                policy="monitored-ohlc-v1",
                payload=dict(complete=True, net_r=-1, outcome="STOP", exit_ms=90000000),
            )
        ],
    )
    report = summarize([row], 99999999)
    assert report["overall"]["prints-v1"]["delivered"]["complete_outcomes"] == 0
    assert report["overall"]["monitored-ohlc-v1"]["delivered"]["losses"] == 1
    assert metrics([row], "monitored-ohlc-v1")["mean_cost_pct_original_risk"] is None
    row["receipt"] = ["sent", None]
    assert summarize([row], 99999999)["delivered"] == 0


def test_audit_projection_keeps_decision_evidence_and_original_snapshot(settings, signal):
    import time

    from bybit_flow.ml.store import FeatureStore

    tagged(signal)
    signal.evidence["flow"] = dict(profile=[dict(price=100, buy=20, sell=10)], delta_pct=30)
    signal.evidence["v2"] = dict(mechanism=dict(state="INEFFICIENT_AGGRESSION", score=0))
    store = Store(settings.data_dir)
    try:
        now = time.time_ns() // 1000000
        ident = FeatureStore(store).capture(signal, now, "decision")
        snapshot = json.loads(
            store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
        )
        # Duplicated feature diagnostics must not consume the bounded audit payload.
        snapshot["feature_metadata"] = {"unused_replay": "x" * (33 * 1024 * 1024)}
        store.db.execute("UPDATE ml_snapshots SET payload=? WHERE id=?", (json.dumps(snapshot), ident))
        original = store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
        rows, truncated = load_rows(store.db, now + 1)
        assert not truncated and len(rows) == 1
        assert "feature_metadata" not in rows[0]["snapshot"]
        evidence = rows[0]["snapshot"]["signal"]["evidence"]
        assert evidence["flow"]["delta_pct"] == 30 and "profile" not in evidence["flow"]
        assert evidence["v2"]["mechanism"]["score"] == 0
        assert (
            original
            == store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
        )
    finally:
        store.close()
