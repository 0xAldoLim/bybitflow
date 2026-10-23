"""Predeclared mechanics, target/stop geometry and historical policy isolation."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal

import pytest

from bybit_flow import quality
from bybit_flow.impact_efficiency import assess as impact
from bybit_flow.models import Candle
from bybit_flow.orderflow import Book
from bybit_flow.quality_v22 import POLICY, apply
from bybit_flow.risk import evaluate_risk
from bybit_flow.setup_scoring import combine, edge_age, score
from bybit_flow.stop_viability import assess as stop
from bybit_flow.target_reachability import (
    candidate_targets,
    movement_capacity,
    path,
    reachability,
    select_target,
)

NOW = 120000


def tagged(signal, settings, instrument):
    s = deepcopy(signal)
    s.feature_schema_version = "candidate-v20"
    s.horizon_profile = "CORE_INTRADAY"
    s.trigger_expires_ms = 1000000
    s.holding_deadline_ms = 14520000
    s.stop, s.tp1, s.tp2 = 98, 106, 108
    s.evidence.update(
        production_policies=dict(trade_quality=POLICY),
        price_tick="0.01",
        flow_confirmed=True,
        frozen_structure=dict(
            available_ms=60000,
            setup_interval_ms=60000,
            frozen_reference=99,
            frozen_high=105,
            frozen_low=95,
            reasons=[],
        ),
        structural_trigger=dict(valid=True, available_ms=60000, level=99, extreme=98.5),
        setup_features=dict(atr=2, asof=60000),
        execution_features=dict(atr=0.5, asof=60000),
        execution=dict(short_horizon_noise=1, stop_noise_ratio=2),
        range=dict(low=95, high=105),
        v8_structure_response=dict(supportive=True),
        flow=dict(
            available=True,
            delta_pct=50,
            delta_notional=5000,
            price_change=0.3,
            vwap=100,
            delta_persistence=1,
            impact_persistence=1,
            stacked_buy=3,
            stacked_sell=0,
            initiative_long=True,
            initiative_short=False,
            cvd_slope=100,
        ),
        flow_quality=dict(
            available_ms=NOW,
            window_end_ms=NOW,
            flow_trust_score=1,
            price_displacement_bps=30,
            impact_persistence=1,
            net_signed_notional=5000,
        ),
    )
    s.coverage["trade_window_complete"] = True
    b = book()
    s.evidence["book"] = b.features(NOW)
    s.risk = evaluate_risk(s, instrument, settings, b)
    return s, b


def book(now=NOW, bid="99.99", ask="100.01", quantity="100000"):
    b = Book()
    b.bids, b.asks = {Decimal(bid): Decimal(quantity)}, {Decimal(ask): Decimal(quantity)}
    b.valid, b.event_ms, b.receipt_ms = True, now, now
    return b


def history(count=100, interval=60000, high=101, low=99):
    return [Candle(i * interval, interval, 100, high, low, 100, 100, 10000) for i in range(count)]


def test_successful_aggression_outscores_dramatic_ineffective_flow(signal, settings, instrument):
    s, _ = tagged(signal, settings, instrument)
    good = impact(s, NOW)
    weak = deepcopy(s)
    weak.evidence["flow"].update(delta_pct=90, price_change=0.005)
    weak.evidence["flow_quality"].update(price_displacement_bps=0.5)
    poor = impact(weak, NOW)
    assert good["score"] > 65 and poor["score"] < 40
    assert poor["state"] == "STRONG_AGGRESSION_WEAK_RESPONSE"
    # Correlated transformations do not create additional conviction.
    weak.evidence["flow"].update(cvd=1e12, stacked_buy=20, cvd_slope=1e10, trades=10000)
    assert impact(weak, NOW)["score"] == poor["score"]


def test_materially_adverse_response_is_contradicted(signal, settings, instrument):
    s, _ = tagged(signal, settings, instrument)
    s.evidence["flow"]["price_change"] = -0.1
    s.evidence["flow_quality"]["price_displacement_bps"] = -10
    r = impact(s, NOW)
    assert r["contradicted"] and r["score"] == 0


def test_absorption_reversal_remains_valid_alternative(signal, settings, instrument):
    s, b = tagged(signal, settings, instrument)
    s.family = "liquidity_sweep"
    s.evidence["flow"].update(
        delta_pct=-60,
        price_change=0.05,
        absorption_long=True,
        defended_notional=dict(LONG=5000),
        sell_notional=10000,
    )
    s.evidence["flow_quality"].update(price_displacement_bps=5, net_signed_notional=-5000)
    result = impact(s, NOW)
    assert result["pathway"] == "ABSORPTION" and result["score"] > 65
    apply(s, 100.01, NOW, book=b, settings=settings, minimum_rr=settings.min_net_rr)
    assert "MECHANISM_CONTRADICTED" not in s.gates


def test_unknown_or_future_flow_never_becomes_adverse(signal, settings, instrument):
    s, _ = tagged(signal, settings, instrument)
    s.evidence["flow_quality"]["available_ms"] = NOW + 1
    result = impact(s, NOW)
    assert result["state"] == "UNKNOWN" and not result["contradicted"] and result["score"] is None


def test_capacity_uses_only_fully_past_windows_and_is_symmetric(signal):
    signal.evidence["setup_features"] = dict(atr=2)
    bars = history()
    now = bars[-1].end
    long = movement_capacity(signal, bars, now, 5)
    signal.direction = "SHORT"
    short = movement_capacity(signal, bars, now, 5)
    assert long["state"] == "READY" and long["q75_atr"] == short["q75_atr"] == 0.5
    future = history(1, high=10000, low=1)[0]
    future = replace(future, start=now)
    assert movement_capacity(signal, bars + [future], now, 5) == short


@pytest.mark.parametrize("horizon", ["SHORT_INTRADAY", "CORE_INTRADAY", "SWING", "EXTENDED_SWING"])
def test_new_generation_preserves_v211_structural_geometry(instrument, monkeypatch, horizon):
    from test_quality_v211 import bar, flat

    from bybit_flow.horizons import assign
    from bybit_flow.strategy import candidates

    bars = flat()
    bar(bars, -1, 97, 101, 99)
    feature = dict(regime="range", atr=2, low=98, high=102, ma20=100, sweep_long=True, sweep_short=False)
    monkeypatch.setattr("bybit_flow.strategy.candle_features", lambda *a: dict(feature))
    monkeypatch.setattr("bybit_flow.quality_v211.trend_qualification", lambda *a: {})
    args = dict(families=["range_rejection"], execution_window_ms=60000)
    old = candidates(
        instrument, bars, bars, bars, bars[-1].end, quality_policy="trade-validity-v21.1", **args
    )
    new = candidates(instrument, bars, bars, bars, bars[-1].end, quality_policy=POLICY, **args)
    assert new and len(new) == len(old)
    for before, after in zip(old, new):
        assign(after, horizon)
        assert (after.entry, after.stop, after.tp1, after.tp2) == (
            before.entry,
            before.stop,
            before.tp1,
            before.tp2,
        )
        assert after.evidence["production_policies"]["trade_quality"] == POLICY
        assert after.evidence["production_policies"]["scoring"] == "bybitflow-v22-setup-1"
        assert after.feature_schema_version == "candidate-v20"


def test_capacity_does_not_drop_a_whole_bar_after_one_second(signal):
    signal.evidence["setup_features"] = dict(atr=2)
    bars = history()
    now = bars[-1].end
    full = movement_capacity(signal, bars, now, 4)
    later = movement_capacity(signal, bars, now + 1000, 4 - 1 / 60)
    assert full["state"] == later["state"] == "READY"
    assert full["q75_atr"] == later["q75_atr"]
    assert later["measured_horizon_minutes"] == 4 - 1 / 60


def test_future_stop_plan_cannot_supply_noise_or_wick_evidence(signal, settings, instrument):
    s, _ = tagged(signal, settings, instrument)
    s.evidence["stop_plan"] = dict(
        available_ms=NOW + 1,
        execution_noise_q80=100,
        wick_regime=dict(adverse_wick_q80=100),
    )
    result = stop(s, 100, NOW)
    assert result["ordinary_noise"] == 1
    assert result["stop_wick_q80"] is None


def test_edge_age_keeps_strongest_observation_instead_of_latest(signal, settings, instrument):
    s, _ = tagged(signal, settings, instrument)
    s.evidence["impact_efficiency"] = dict(coverage_complete=True, score=80, window_end_ms=NOW)
    assert edge_age(s, NOW)["strongest_impact_age_ms"] == 0
    s.evidence["impact_efficiency"] = dict(coverage_complete=True, score=20, window_end_ms=NOW + 60000)
    assert edge_age(s, NOW + 60000)["strongest_impact_age_ms"] == 60000
    s.evidence["impact_efficiency"].update(score=90)
    assert edge_age(s, NOW + 60000)["strongest_impact_age_ms"] == 0


@pytest.mark.parametrize("distance,state", [(0.8, "REACHABLE"), (1.5, "STRETCH"), (2.5, "LOW_REACHABILITY")])
def test_reachability_states(signal, distance, state):
    signal.evidence["setup_features"] = dict(atr=2)
    capacity = dict(state="READY", available_ms=NOW, q75_atr=0.5, q90_atr=1)
    assert reachability(signal, 100 + distance, 100, capacity, NOW)["state"] == state
    assert reachability(signal, 103, 100, capacity | dict(available_ms=NOW + 1), NOW)["state"] == "UNKNOWN"


def test_capacity_gaps_and_small_samples_are_unknown(signal):
    signal.evidence["setup_features"] = dict(atr=2)
    bars = history(25)
    assert movement_capacity(signal, bars, bars[-1].end, 10)["state"] == "UNKNOWN"
    bars = history()
    for i in range(0, len(bars), 5):
        bars[i] = replace(bars[i], start=bars[i].start + 10)
    assert movement_capacity(signal, bars, bars[-1].end, 5)["state"] == "UNKNOWN"


def target_signal(signal):
    signal.stop, signal.tp1 = 99.8, 106
    signal.evidence.update(structural_tp1=106, price_tick="0.01", setup_features=dict(atr=2))
    signal.risk.update(fee_bps_assumption=0, slippage_bps_assumption=0, funding_reserve_bps=0)
    return signal


def test_nearer_viable_execution_target_preserves_structural_objective(signal):
    s = target_signal(signal)
    capacity = dict(state="READY", available_ms=NOW, q75_atr=0.5, q90_atr=1, current_setup_atr=2)
    r = select_target(s, capacity, NOW, 2)
    assert r["execution_tp1"] == 101 and r["structural_tp1"] == 106
    assert s.tp1 == 106  # Pure selector cannot modify a live plan.
    s.stop = 98
    assert select_target(s, capacity, NOW, 2)["state"] == "UNTRADEABLE"


def test_reachable_structural_target_is_not_mechanically_shortened(signal):
    s = target_signal(signal)
    s.tp1 = s.evidence["structural_tp1"] = 100.8
    capacity = dict(state="READY", available_ms=NOW, q75_atr=0.5, q90_atr=1, current_setup_atr=2)
    assert select_target(s, capacity, NOW, 2)["execution_tp1"] == 100.8
    assert select_target(s, dict(state="UNKNOWN", available_ms=NOW), NOW, 2)["execution_tp1"] == 100.8


def test_target_bound_reserves_capacity_and_rejects_backward_levels(signal):
    s = target_signal(signal)
    s.evidence["level_policy"] = dict(
        available_ms=NOW, candidates=[dict(price=90 + i / 100, available_ms=NOW) for i in range(64)]
    )
    capacity = dict(state="READY", available_ms=NOW, q75_atr=0.5, q90_atr=1, current_setup_atr=2)
    choices = candidate_targets(s, capacity, NOW)
    assert len(choices) <= 24 and all(p > s.entry for p, _ in choices)
    assert {101, 102, 106} <= {p for p, _ in choices}
    s.tp2 = 100.5
    assert select_target(s, capacity, NOW, 2)["state"] == "UNTRADEABLE"


def test_volume_weighted_friction_deduplicates_aliases(signal):
    signal.evidence.update(
        price_tick="0.01",
        volume_profile=dict(
            available=True,
            coverage_complete=True,
            available_ms=NOW,
            source_ms=NOW,
            poc=101,
            vah=101,
            val=101,
            hvn=[101] * 20,
            raw_volume_profile=[dict(price=101, buy=90, sell=0), dict(price=120, buy=10, sell=0)],
        ),
    )
    r = path(signal, 115, 100, NOW)
    assert r["barrier_count"] == 1 and r["state"] == "HEAVY_FRICTION" and r["score"] == 10


@pytest.mark.parametrize(
    "noise,state", [(0.8, "HEALTHY_STOP"), (1.7, "TIGHT_BUT_VALID"), (2.5, "NOISE_CONFLICT")]
)
def test_stop_states_without_widening(signal, settings, instrument, noise, state):
    s, _ = tagged(signal, settings, instrument)
    s.evidence["execution"]["short_horizon_noise"] = noise
    s.tp1 = 120
    assert stop(s, 100, NOW)["state"] == state
    assert s.stop == 98


def test_stop_untradeable_when_noise_safe_geometry_breaks_rr(signal, settings, instrument):
    s, _ = tagged(signal, settings, instrument)
    s.tp1 = 103
    s.evidence["execution"]["short_horizon_noise"] = 3
    assert stop(s, 100, NOW, 2)["state"] == "UNTRADEABLE_GEOMETRY"
    s.stop = 90
    s.evidence["execution"]["short_horizon_noise"] = 1
    assert stop(s, 100, NOW)["state"] == "STRUCTURALLY_TOO_WIDE"


def test_formula_and_single_headline(signal, settings, instrument):
    assert combine(80, 60, 100) == 68.2
    s, b = tagged(signal, settings, instrument)
    apply(s, 100.01, NOW, minimum_rr=settings.min_net_rr, book=b, settings=settings)
    assert s.evidence["score_profile"] == "bybitflow-v22-setup-1"
    assert s.quality == combine(
        s.evidence["v22"]["evidence_score"],
        s.evidence["v22"]["tradeability_score"],
        s.evidence["v22"]["confidence_cap"],
    )
    assert s.qualification["probability"] is None
    assert s.evidence["v22"]["weaknesses"]


def test_provisional_rejection_does_not_freeze_confirmation_window(signal, settings, instrument):
    s, b = tagged(signal, settings, instrument)
    s.state = "PENDING CONFIRMATION"
    # Mirrors scanner ordering: provisional scoring precedes final V8 and
    # trade-quality gates. It must not be mistaken for a confirmed decision.
    score(s, True, NOW)
    assert s.evidence.get("score_components")
    s.gates = ["UNCONFIRMED_LIQUIDITY_SWEEP"]
    apply(s, 100.01, NOW, book=b, settings=settings)
    first = deepcopy(s.evidence["v22_initial_assessment"])
    assert "score_components" not in s.evidence
    assert s.evidence["v22_counterfactual"]["reasons"]
    s.gates = []
    apply(s, 100.01, NOW, book=b, settings=settings)
    assert s.evidence["score_components"] and not s.gates
    assert s.evidence["v22_initial_assessment"] == first


@pytest.mark.parametrize("policy", ["trade-validity-v21", "trade-validity-v21.1"])
def test_historical_policy_dispatch_is_exact(signal, policy):
    from bybit_flow import quality_v21, quality_v211

    signal.evidence["production_policies"] = dict(trade_quality=policy)
    signal.risk.update(cost_per_base=0.2, net_rr=2.8)
    expected = deepcopy(signal)
    module = quality_v21 if policy.endswith("v21") else quality_v211
    module.apply(expected, 100, 1100)
    quality.apply(signal, 100, 1100)
    assert signal.model_dump() == expected.model_dump()
    before = deepcopy(signal.model_dump())
    apply(signal, 100, 1100)
    assert signal.model_dump() == before
