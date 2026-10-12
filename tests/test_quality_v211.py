"""Policy separation, causal events and non-additive filter attribution."""

from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pytest

from bybit_flow import quality
from bybit_flow.horizons import assign
from bybit_flow.ml.monitored import research_path
from bybit_flow.models import Candle
from bybit_flow.orderflow import Book
from bybit_flow.quality_v211 import (
    POLICY,
    apply,
    current_cost,
    entry_quality,
    liquidity_update,
    overlaps_existing,
    structural,
    trend_qualification,
)
from bybit_flow.risk import evaluate_risk, execution_cost
from bybit_flow.trade_quality import counterfactual_attribution, execution_stress, summarize


def tagged(s):
    s.evidence["production_policies"] = {"trade_quality": POLICY}
    s.evidence["price_tick"] = "0.01"
    s.risk = dict(
        cost_per_base=0.2, net_rr=2.8, fee_bps_assumption=5, slippage_bps_assumption=1, funding_reserve_bps=1
    )
    s.trigger_expires_ms = 100000
    return s


def flat(count=80):
    return [Candle(i * 60000, 60000, 100, 102, 98, 100 + i % 2 * 0.1, 10, 1000) for i in range(count)]


def bar(bars, index, low, high, close):
    old = bars[index]
    bars[index] = Candle(old.start, old.interval, close, high, low, close, 10, 1000)


def structure(bars, family, direction="LONG", **kwargs):
    return structural(
        bars,
        family,
        direction,
        {"regime": "range"},
        {"low": 98, "high": 102, "atr": 2},
        bars[-1].end,
        **kwargs,
    )


@pytest.mark.parametrize(
    "direction,price,relation,code",
    [
        ("LONG", 100, "INSIDE_ZONE", None),
        ("LONG", 102, "CHASE_SIDE", "ENTRY_PRICE_CHASED"),
        ("LONG", 98, "ADVERSE_SIDE", "ENTRY_ZONE_BROKEN_ADVERSE"),
        ("SHORT", 98, "CHASE_SIDE", "ENTRY_PRICE_CHASED"),
        ("SHORT", 102, "ADVERSE_SIDE", "ENTRY_ZONE_BROKEN_ADVERSE"),
    ],
)
def test_directional_entry(signal, direction, price, relation, code):
    tagged(signal)
    signal.direction = direction
    if direction == "SHORT":
        signal.stop, signal.tp1 = 105, 85
    q = entry_quality(signal, price, 1100)
    assert q["zone_relation"] == relation
    assert q["entry_not_chased"] == (relation != "CHASE_SIDE")
    assert q["acceptable"] == (code is None)
    if code:
        assert code in q["reasons"]


@pytest.mark.parametrize(
    "cost,state,code",
    [
        (None, "COST_UNKNOWN", "EXECUTION_COST_UNKNOWN"),
        (-1, "COST_INVALID", "EXECUTION_COST_INVALID"),
        (float("nan"), "COST_INVALID", "EXECUTION_COST_INVALID"),
    ],
)
def test_cost_failure_is_not_chasing(signal, cost, state, code):
    tagged(signal).risk["cost_per_base"] = cost
    q = entry_quality(signal, 100, 1100)
    assert q["cost_state"] == state and q["reasons"] == [code]
    assert q["entry_not_chased"] and not q["acceptable"]


def test_rr_degradation_and_gross_net_separation(signal):
    tagged(signal)
    signal.zone = (95, 110)
    q = entry_quality(signal, 108, 1100)
    assert q["reasons"] == ["RR_DEGRADED"] and q["entry_not_chased"]
    assert q["gross_rr_now"] == pytest.approx(7 / 13)
    assert q["cost_adjusted_rr_now"] == pytest.approx(6.8 / 13.2)
    assert q["original_gross_rr"] == 3


@pytest.mark.parametrize(
    "at,state,code",
    [(100000, "TRIGGER_EXPIRED", "ENTRY_TRIGGER_EXPIRED"), (3601000, "PLAN_EXPIRED", "ENTRY_PLAN_EXPIRED")],
)
def test_expiry_dimensions(signal, at, state, code):
    q = entry_quality(tagged(signal), 100, at)
    assert q["time_state"] == state and q["reasons"] == [code]


@pytest.mark.parametrize(
    "price,state",
    [(95, "STOP_ALREADY_BREACHED"), (116, "TARGET_ALREADY_PASSED"), (float("nan"), "INVALID_PRICE")],
)
def test_geometry_not_chased(signal, price, state):
    q = entry_quality(tagged(signal), price, 1100)
    assert q["geometry_state"] == state
    assert "ENTRY_PRICE_CHASED" not in q["reasons"]


@pytest.mark.parametrize(
    "stop,target,state", [(101, 115, "NON_POSITIVE_RISK"), (95, 99, "NON_POSITIVE_REWARD")]
)
def test_original_invalid_geometry(signal, stop, target, state):
    tagged(signal)
    signal.stop, signal.tp1 = stop, target
    assert entry_quality(signal, 100, 1100)["geometry_state"] == state


def test_ticks_and_stale_quotes(signal):
    tagged(signal)
    assert entry_quality(signal, 101.00000000000001, 1100)["zone_relation"] == "INSIDE_ZONE"
    assert entry_quality(signal, 101.01, 1100)["zone_relation"] == "CHASE_SIDE"
    q = entry_quality(signal, 100, 1100, quote=dict(fresh=False, book_timestamp_ms=100))
    assert q["reasons"] == ["EXECUTION_QUOTE_STALE"] and q["quote_age_ms"] == 1000
    assert entry_quality(signal, 100, 1100, quote={})["acceptable"]


def test_cost_components_and_residual_impact(signal, settings, instrument):
    tagged(signal)
    book = Book()
    book.bids = {Decimal("99"): Decimal("1000")}
    book.asks = {Decimal("101"): Decimal("1000")}
    book.valid = True
    r = evaluate_risk(signal, instrument, settings, book)
    c = execution_cost(100, 95, 115, settings.taker_fee_bps, settings.slippage_bps, r["funding_reserve_bps"])
    assert r["cost_per_base"] == c["cost_per_base"]
    assert c["spread_added_per_base"] == 0
    signal.risk = r
    now = current_cost(signal, 101, book)
    # VWAP is best ask: its half-spread must not appear as residual impact.
    assert now["residual_entry_impact_per_base"] == 0 and now["additional_impact_per_base"] == 0
    frozen = r["cost_per_base"]
    apply(signal, 101, 1100)
    apply(signal, 100, 1200)
    assert signal.evidence["frozen_entry_cost"]["cost_per_base"] == frozen
    assert execution_cost(100, 95, 115, 5, 1, 1, 0.5)["additional_impact_per_base"] == pytest.approx(0.49)
    signal.risk.pop("notional")
    assert current_cost(signal, 101, book)["impact_state"] == "UNKNOWN"


@pytest.mark.parametrize("delay", [0, 1, 3])
def test_reclaim_frozen_and_causal(delay):
    bars = flat()
    start = len(bars) - delay - 1
    bar(bars, start, 97, 101, 97.9)
    for i in range(start + 1, len(bars) - 1):
        # Touching the boundary avoids two established outside closes.
        bar(bars, i, 97.5, 101, 98)
    bar(bars, -1, 97.5, 101, 99)
    result = structure(bars, "range_rejection")
    assert result["reclaimed"] and result["reclaim_delay_bars"] == delay
    assert result["reference_low"] == 98 and result["reference_end_ms"] <= bars[start].start
    future = Candle(bars[-1].end, 60000, 100, 999, 1, 100, 10, 1000)
    assert (
        structural(
            bars + [future],
            "range_rejection",
            "LONG",
            {"regime": "range"},
            {"low": 98, "high": 102, "atr": 2},
            bars[-1].end,
        )
        == result
    )


def test_late_reclaim_and_range_acceptance():
    bars = flat()
    bar(bars, -5, 97, 101, 97.5)
    for i in range(-4, -1):
        bar(bars, i, 97.5, 101, 98)
    bar(bars, -1, 98, 101, 99)
    assert "FAILED_RECLAIM" in structure(bars, "range_rejection")["reasons"]
    bars = flat()
    bar(bars, -3, 97, 101, 97.5)
    bar(bars, -2, 97, 101, 97.5)
    bar(bars, -1, 97, 101, 99)
    assert structure(bars, "range_rejection")["range_state"] == "RANGE_ACCEPTANCE_OUTSIDE"


@pytest.mark.parametrize("delay", [1, 4])
def test_breakout_retest_bounded(delay):
    bars = flat()
    start = len(bars) - delay - 2
    bar(bars, start, 102.1, 105, 103)
    bar(bars, start + 1, 102.1, 105, 104)
    for i in range(start + 2, len(bars)):
        bar(bars, i, 102.1, 106, 105)
    bar(bars, -1, 101.9, 105, 103)
    result = structure(bars, "breakout_retest")
    assert result["accepted"] and result["retest_held"] and not result["reasons"]
    assert result["retest_delay_bars"] == delay and result["frozen_high"] == 102
    assert result["reference_end_ms"] <= bars[start].start
    # Future retests may not change the earlier decision.
    assert (
        structural(
            bars + [Candle(bars[-1].end, 60000, 100, 100, 90, 95, 1, 100)],
            "breakout_retest",
            "LONG",
            {"regime": "range"},
            {"low": 98, "high": 102, "atr": 2},
            bars[-1].end,
        )
        == result
    )


def test_retest_expired_failed_and_no_repeat():
    bars = flat()
    for i in range(-6, 0):
        bar(bars, i, 102.1, 105, 103)
    result = structure(bars, "breakout_retest")
    assert result["research_state"] == "ACCEPTED_NO_RETEST" and result["reasons"] == ["RETEST_EXPIRED"]
    next_bar = Candle(bars[-1].end, 60000, 103, 105, 101.9, 103, 10, 1000)
    assert not structure(bars + [next_bar], "breakout_retest")["retest_held"]
    bar(bars, -4, 101, 104, 101.5)
    assert structure(bars[:-3], "breakout_retest")["breakout_state"] == "RETEST_FAILED"
    bars = flat()
    bar(bars, -4, 102.1, 105, 103)
    bar(bars, -3, 102.1, 105, 104)
    bar(bars, -2, 101.9, 105, 103)
    bar(bars, -1, 101.9, 105, 103)
    assert not structure(bars, "breakout_retest")["candidate_event"]


def test_insufficient_structure_safe():
    result = structure(flat(4), "range_rejection")
    assert result["reasons"] == ["STRUCTURAL_HISTORY_INSUFFICIENT"] and result["required_bars"] == 24


@pytest.mark.parametrize(
    "previous,earlier,state",
    [
        ((99, 101), (98, 102), "ID_NR4"),
        ((100, 102), (98, 101), "NR4_ONLY"),
        ((98, 102), (97, 103), "INSIDE_ONLY"),
    ],
)
def test_compression_subtypes(previous, earlier, state):
    bars = flat()
    bar(bars, -5, 99, 100.5, 100)
    if state != "INSIDE_ONLY":
        bar(bars, -5, 97, 104, 100)
    bar(bars, -3, *earlier, 100)
    bar(bars, -2, *previous, 100)
    assert structure(bars, "liquidity_sweep")["compression_state"] == state


def test_bounded_trend_qualification_and_ordinals(monkeypatch):
    bars = flat()
    calls = []

    def classifier(prefix, at):
        calls.append(at)
        return dict(regime="trending up" if len(prefix) >= 70 else "range")

    monkeypatch.setattr("bybit_flow.features.candle_features", classifier)
    qualified = trend_qualification(bars, bars[-1].end)
    assert len(calls) == 20 and qualified["trending up"] == bars[70].end
    for i in range(71, 80):
        bar(bars, i, 101, 103, 102)
    bar(bars, 72, 98, 103, 102)
    bar(bars, 77, 98, 103, 102)
    context = dict(regime="trending up", previous_regime="trending up", trend_qualification=qualified)
    first = structural(
        bars[:73], "trend_pullback", "LONG", context, {"low": 98, "high": 103, "atr": 2}, bars[72].end
    )
    second = structural(
        bars, "trend_pullback", "LONG", context, {"low": 98, "high": 103, "atr": 2}, bars[-1].end
    )
    assert first["pullback_attempt_index_since_qualification"] == 1
    assert second["pullback_attempt_index_since_qualification"] == 2 and not second["reasons"]


def test_policy_routing_preserves_old(signal):
    from bybit_flow.quality_v21 import entry_quality as old

    signal.evidence["production_policies"] = {"trade_quality": "trade-validity-v21"}
    signal.risk = dict(cost_per_base=0.2)
    before = deepcopy(signal.model_dump())
    assert quality.entry_quality(signal, 98, 1100) == old(signal, 98, 1100)
    assert signal.model_dump() == before
    apply(signal, 120, 999999999)
    assert signal.model_dump() == before


@pytest.mark.parametrize("horizon", ["SHORT_INTRADAY", "CORE_INTRADAY", "SWING", "EXTENDED_SWING"])
def test_new_candidate_policy_and_event_identity(instrument, monkeypatch, horizon):
    from bybit_flow.strategy import candidates

    bars = flat()
    bar(bars, -1, 97, 101, 99)
    feature = dict(regime="range", atr=2, low=98, high=102, ma20=100, sweep_long=True, sweep_short=False)
    monkeypatch.setattr("bybit_flow.strategy.candle_features", lambda *a: dict(feature))
    monkeypatch.setattr("bybit_flow.quality_v211.trend_qualification", lambda *a: {})
    args = dict(families=["range_rejection"], execution_window_ms=60000, quality_policy=POLICY)
    first = candidates(instrument, bars, bars, bars, bars[-1].end, **args)
    second = candidates(instrument, bars, bars, bars, bars[-1].end + 10000, **args)
    assert first and [s.id for s in first] == [s.id for s in second]
    assign(first[0], horizon)
    assign(second[0], horizon)
    assert first[0].feature_schema_version == "candidate-v20"
    assert first[0].evidence["production_policies"]["trade_quality"] == POLICY
    old = first[0].model_dump()
    old["evidence"]["production_policies"]["trade_quality"] = "trade-validity-v21"
    assert overlaps_existing(second[0], [old])


def test_liquidity_variants_preserve_joint_production():
    state = {}
    for i in range(30):
        state = liquidity_update(
            state,
            dict(spread=1 + i % 2, depth=100 + i % 2, impact=1, epoch=1, session="ASIA"),
            (i + 1) * 60000,
        )
    result = liquidity_update(
        state, dict(spread=10, depth=200, impact=1, epoch=1, session="ASIA"), 31 * 60000
    )
    assert result["subtype"] == "SPREAD_STRESS_ONLY" and result["state"] == "LIQUIDITY_NORMAL"
    assert result["variants"]["either"] and not result["variants"]["joint"]
    assert not liquidity_update({}, dict(spread=1, depth=1, epoch=1, session="ASIA"), 60000)["variants"][
        "either"
    ]


def row(signal, ident, reasons, net):
    s = deepcopy(tagged(signal).model_dump())
    s["id"] = ident
    s["evidence"]["v21_counterfactual"] = dict(reasons=reasons)
    return dict(
        snapshot_id=ident,
        candidate_identity=ident,
        snapshot=dict(signal=s, decision_ms=86400000),
        labels=[dict(policy="monitored-ohlc-v1", payload=dict(complete=True, net_r=net, exit_ms=90000000))],
        receipt=None,
    )


def test_unique_shared_sole_combinations_and_ablations(signal):
    loser = row(signal, "loss", ["RR_DEGRADED", "LIQUIDITY_STRESS", "MECHANISM_CONTRADICTED"], -1)
    winner = row(signal, "win", ["RR_DEGRADED", "LIQUIDITY_STRESS"], 2)
    sole = row(signal, "sole", ["RR_DEGRADED"], -1)
    result = counterfactual_attribution([loser, winner, sole, deepcopy(loser)], "monitored-ohlc-v1")
    assert result["unique"]["signals"] == 3 and result["unique"]["losses"] == 2
    rr = result["by_reason"]["RR_DEGRADED"]
    assert (
        rr["sole_reason_avoided_losers"] == 1
        and rr["shared_avoided_losers"] == 1
        and rr["shared_missed_winners"] == 1
    )
    assert result["leave_one_filter_out"]["RR_DEGRADED"]["uniquely_changed_decisions"] == 1
    assert len(result["reason_combinations"]) == 3
    report = summarize([loser, winner, sole], 99999999)
    assert report["research_cohorts"]["monitored-ohlc-v1"]["validity_policy"][POLICY]["signals"] == 3
    assert report["failure_taxonomy"]["CHASED_ENTRY"]["status"] == "obsolete for v21.1"


def test_cost_stress_formula_no_spread(signal):
    value = row(signal, "one", ["RR_DEGRADED"], 1)
    report = execution_stress([value], "monitored-ohlc-v1")
    assert report["cost_multipliers"]["2"]["mean_net_r"] == pytest.approx(0.96)
    assert report["delayed_execution"] == "UNKNOWN"


def test_markout_availability_and_excursion_bounds(signal):
    s = tagged(signal).model_dump()
    s["execution_timeframe"], s["setup_timeframe"] = "1", "5"
    r = dict(signal=s, decision_ms=60000)
    bars = flat(10)
    result = research_path(
        r, dict(proxy_entry_price=100, entry_available_ms=60000, exit_ms=300000), bars, 180000
    )
    assert result["markouts"]["entry"]["execution_1"]["state"] == "OBSERVED_PROXY"
    assert result["markouts"]["entry"]["setup_1"]["state"] == "UNKNOWN"
    assert result["full_mfe_r"] is None and result["observed_mfe_lower_bound_r"] == 0.4


def test_scanner_acceptability_not_compatibility(signal, settings):
    from bybit_flow.scanner import Scanner

    tagged(signal)
    signal.zone = (95, 110)
    book = Book()
    book.valid = True
    book.bids = {Decimal("107.99"): Decimal("1000")}
    book.asks = {Decimal("108"): Decimal("1000")}
    book.receipt_ms = book.event_ms = 1100
    scanner = SimpleNamespace(settings=settings)
    assert not Scanner.entry_ready(scanner, signal, book, 1100)
    assert signal.coverage["entry_quality_now"]["entry_not_chased"]
    assert not signal.coverage["entry_quality_now"]["acceptable"]


def test_actual_response_fields_used(signal):
    tagged(signal).coverage["trade_window_complete"] = True
    signal.evidence["flow"] = dict(
        available=True,
        delta_pct=30,
        quality=dict(
            flow_trust_score=0.9, price_displacement_bps=-2, gross_notional=1000, flow_efficiency=0.002
        ),
    )
    apply(signal, 100, 1100)
    assert "MECHANISM_CONTRADICTED" in signal.gates
    assert signal.evidence["aggression_response"]["aggressive_notional"] == 1000


def test_invalid_current_assumptions_not_unknown(signal):
    tagged(signal).risk["fee_bps_assumption"] = -1
    current = current_cost(signal, 100)
    assert entry_quality(signal, 100, 1100, current=current)["reasons"] == ["EXECUTION_COST_INVALID"]


def test_pending_observations_are_bounded_and_never_create_cards(instrument, settings):
    from bybit_flow.scanner import Scanner
    from bybit_flow.storage import Store

    bars = flat()
    bar(bars, -1, 102.1, 105, 103)
    accepting = structure(bars, "breakout_retest")
    assert accepting["breakout_state"] == "BREAKOUT_ACCEPTING" and not accepting["candidate_event"]
    bars.append(Candle(bars[-1].end, 60000, 104, 105, 102.1, 104, 10, 1000))
    pending = structure(bars, "breakout_retest")
    assert pending["breakout_state"] == "RETEST_PENDING" and not pending["candidate_event"]
    store = Store(settings.data_dir)
    scanner = SimpleNamespace(store=store)
    for i in range(405):
        observation = dict(pending, structure_event_id=str(i), family="breakout_retest", direction="LONG")
        Scanner.remember_structures(scanner, instrument, "CORE_INTRADAY", [observation], i)
    assert len(store.get("structural-observations-v211")) == 400
    assert store.db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    store.close()


def test_core_reference_variant_frozen_at_scheduled_boundary():
    from dataclasses import replace

    bars = [replace(b, start=i * 3600000, interval=3600000) for i, b in enumerate(flat())]
    result = structure(bars, "liquidity_sweep", core_bars=bars)
    variant = result["core_24h_6h_variant"]
    assert variant["available"] and variant["reference_end_ms"] == 78 * 3600000
    assert variant["frozen_until_ms"] == 84 * 3600000 and variant["high"] == 102
    bar(bars, -1, 50, 999, 100)
    assert structure(bars, "liquidity_sweep", core_bars=bars)["core_24h_6h_variant"] == variant


async def test_same_event_delivered_once(settings, signal, notification_clock):
    import httpx
    from pydantic import SecretStr

    from bybit_flow.notifications import Notifier
    from bybit_flow.storage import Store

    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/signals")
    tagged(signal)
    signal.state = "CONFIRMED"
    store = Store(settings.data_dir)
    store.signal(signal)
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(200, json={"id": "initial-card", "channel_id": "signals-channel"})

    notifier = Notifier(settings, store, httpx.MockTransport(transport))
    assert await notifier.send_research(signal) == "sent"
    again = signal.model_copy(deep=True)
    await notifier.send_research(again)
    assert len(calls) == 1
    assert (
        store.db.execute(
            "SELECT count(*) FROM outbox WHERE key LIKE '%:initial' AND status='sent'"
        ).fetchone()[0]
        == 1
    )
    store.close()


@pytest.mark.parametrize("policy,checked_ms", [(POLICY, 2000), ("trade-validity-v21", 1100)])
def test_execution_check_after_async_book_update_preserves_old_policy(
    signal, settings, monkeypatch, policy, checked_ms
):
    from bybit_flow.scanner import Scanner

    tagged(signal).evidence["production_policies"]["trade_quality"] = policy
    book = Book()
    book.valid = True
    book.bids = {Decimal("99.99"): Decimal("1000")}
    book.asks = {Decimal("100"): Decimal("1000")}
    book.receipt_ms = book.event_ms = 2000
    scanner = SimpleNamespace(settings=settings, store=SimpleNamespace(get=lambda *args: {}))
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: 2000)
    assert not book.fresh(1100, settings.book_stale_ms)
    Scanner.apply_trade_quality(scanner, signal, book, 1100)
    q = signal.evidence["entry_quality"]
    assert q["available_ms"] == checked_ms
    assert "EXECUTION_QUOTE_STALE" not in signal.gates
    if policy == POLICY:
        assert q["quote_state"] == "QUOTE_FRESH" and q["quote_age_ms"] == 0 and q["acceptable"]
    else:
        assert "quote_state" not in q and q["entry_not_chased"]
