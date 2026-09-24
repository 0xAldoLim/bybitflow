from decimal import Decimal

import pytest

from bybit_flow.cross_venue import compare, substitution
from bybit_flow.funnel import reject, status
from bybit_flow.levels import build_stop_plan, targets
from bybit_flow.models import Candle
from bybit_flow.notifications import embed
from bybit_flow.production import (
    evaluate_intraday_confirmation,
    market_alignment,
    participation,
    structure_response,
)
from bybit_flow.scoring import score
from bybit_flow.storage import Store


def setup(signal, direction="LONG", mature=True):
    sign = 1 if direction == "LONG" else -1
    signal.direction = direction
    signal.horizon_profile = "CORE_INTRADAY"
    signal.family = "liquidity_sweep"
    signal.version += ":production-v2"
    signal.evidence.update(
        structural_trigger=dict(valid=True, level=100),
        trigger_bar_end=60000,
        session_metrics=dict(
            baseline_samples=20 if mature else 0,
            volume_percentile=0.8,
            trade_count_percentile=0.8,
            trade_intensity_percentile=0.8,
            delta_magnitude_percentile=0.8,
        ),
    )
    flow = dict(
        available=True,
        delta_pct=sign * 25,
        delta_persistence=0.8,
        cvd_slope=sign * 10,
        initiative_long=sign > 0,
        initiative_short=sign < 0,
    )
    signal.evidence["flow"] = flow
    bar = Candle(0, 60000, 100, 102, 98, 100 + sign, 10, 1000)
    return flow, [bar]


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
@pytest.mark.parametrize("mature", [True, False])
def test_valid_production_reversal_without_champion(signal, direction, mature):
    flow, bars = setup(signal, direction, mature)
    result = evaluate_intraday_confirmation(signal, flow, bars, 60000)
    assert result["passed"] and signal.model_version is None
    assert result["participation_mode"] == ("PERCENTILE" if mature else "RAW_FALLBACK")


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_contrarian_is_valid_with_independent_evidence(signal, direction):
    flow, bars = setup(signal, direction)
    sign = 1 if direction == "LONG" else -1
    signal.evidence["market_factor"] = dict(
        available=True,
        available_ms=60000,
        beta_to_btc=1,
        correlation_to_btc=0.8,
        beta_stability_btc=0.1,
        residual_btc=sign * 0.004,
        expected_return_btc=-sign * 0.002,
    )
    btc = dict(regime="trending down" if sign > 0 else "trending up", efficiency=0.6)
    aligned = market_alignment(signal, btc, btc, 60000, structure_response(signal, bars, 60000))
    assert aligned["alignment"] == "IDIOSYNCRATIC_DIVERGENCE" and not aligned["blocked"]
    assert evaluate_intraday_confirmation(signal, flow, bars, 60000, aligned)["passed"]
    signal.evidence["market_factor"]["residual_btc"] = 0
    weak = market_alignment(signal, btc, btc, 60000, True)
    assert weak["blocked"] and weak["reason"] == "CONTRARIAN_EVIDENCE_INSUFFICIENT"


@pytest.mark.parametrize(
    "change", [dict(delta_pct=1), dict(delta_persistence=0.2), dict(cvd_slope=-1), dict(available=False)]
)
def test_weak_aligned_and_wick_only_do_not_confirm(signal, change):
    flow, bars = setup(signal, mature=False)
    flow.update(change)
    result = evaluate_intraday_confirmation(
        signal, flow, bars, 60000, dict(alignment="MARKET_ALIGNED", blocked=False)
    )
    assert not result["passed"] and "UNCONFIRMED_LIQUIDITY_SWEEP" in result["reason_codes"]


def test_opposing_flow_never_overridden_by_profile(signal):
    flow, bars = setup(signal)
    flow.update(delta_pct=-30, cvd_slope=-10)
    signal.evidence["volume_profile"] = dict(
        available=True, coverage_complete=True, available_ms=60000, end_ms=60000, rejection_low=True
    )
    result = evaluate_intraday_confirmation(signal, flow, bars, 60000)
    assert result["profile_support"] and result["flow_support"] == "FLOW_OPPOSING" and not result["passed"]


def test_participation_does_not_double_count_correlated_metrics():
    metrics = dict(baseline_samples=20, volume_percentile=0.9, aggressive_buy_notional_percentile=0.9)
    assert not participation(metrics, 1, 0.6)["passed"]
    metrics["trade_count_percentile"] = 0.6
    assert participation(metrics, 1, 0.6)["passed"]
    assert participation(dict(baseline_samples=0), 1, 0.6)["passed"] is None


def test_original_trigger_and_post_trigger_response_required(signal):
    flow, bars = setup(signal)
    signal.evidence["structural_trigger"]["valid"] = False
    assert not evaluate_intraday_confirmation(signal, flow, bars, 60000)["passed"]
    signal.evidence["structural_trigger"]["valid"] = True
    signal.evidence["trigger_bar_end"] = 120000
    assert not evaluate_intraday_confirmation(signal, flow, bars, 60000)["passed"]


def remote_rows(trust=0.85, displacement=-6, include_okx=True):
    rows = []
    for name in ["binance", "bybit", "okx"] if include_okx else ["binance", "bybit"]:
        rows.append(
            dict(
                exchange=name,
                event_ms=60000,
                mid=100,
                spread_bps=1,
                window_start=0,
                window_end=60000,
                flow=dict(
                    available=True,
                    delta_pct=-30,
                    quality=dict(
                        flow_trust_score=0.2 if name == "binance" else trust,
                        effective_delta_notional=-1000,
                        price_displacement_bps=displacement,
                        book_response_consistency=0.8,
                    ),
                ),
            )
        )
    return compare(rows, 61000)


def test_two_trusted_remote_venues_substitute_only_low_quality_local_flow(signal):
    flow, bars = setup(signal, "SHORT", mature=False)
    signal.source = "binance"
    signal.version += ":flow-quality-v1:cross-venue-flow-substitution-v1"
    flow["quality"] = dict(flow_quality_state="REPETITIVE_TWO_SIDED_CHURN", flow_trust_score=0.2)
    flow.update(delta_pct=0, cvd_slope=0)
    remote = substitution(signal, flow, remote_rows(), 0, 60000, 61000, True)
    assert remote["passed"] and remote["confirming_venues"] == ["bybit", "okx"]
    signal.evidence["flow_substitution"] = remote
    signal.evidence["flow_confirmation_mode"] = "CROSS_VENUE_SUBSTITUTION"
    result = evaluate_intraday_confirmation(signal, flow, bars, 60000)
    assert result["passed"] and result["flow_confirmation_mode"] == "CROSS_VENUE_SUBSTITUTION"
    assert "LOW_INFORMATION_EXECUTED_FLOW" not in result["reason_codes"]


@pytest.mark.parametrize(
    "comparison", [remote_rows(include_okx=False), remote_rows(trust=0.2), remote_rows(displacement=0)]
)
def test_remote_volume_or_one_venue_does_not_rescue(signal, comparison):
    flow, _ = setup(signal, "SHORT")
    signal.source = "binance"
    flow["quality"] = dict(flow_quality_state="LOW_INFORMATION_VOLUME", flow_trust_score=0.2)
    assert not substitution(signal, flow, comparison, 0, 60000, 61000, True)["passed"]


def test_trusted_opposing_local_flow_is_not_substituted(signal):
    flow, _ = setup(signal, "SHORT")
    signal.source = "binance"
    flow.update(delta_pct=30, cvd_slope=10)
    flow["quality"] = dict(
        flow_quality_state="INFORMATIVE_DIRECTIONAL_FLOW",
        flow_trust_score=0.85,
        effective_delta_notional=1000,
    )
    assert (
        substitution(signal, flow, remote_rows(), 0, 60000, 61000, True)["reason"]
        == "CROSS_VENUE_DISAGREEMENT"
    )


def test_substitution_cannot_override_missing_local_structure(signal):
    flow, _ = setup(signal, "SHORT")
    signal.source = "binance"
    flow["quality"] = dict(flow_quality_state="LOW_INFORMATION_VOLUME", flow_trust_score=0.2)
    result = substitution(signal, flow, remote_rows(), 0, 60000, 61000, False)
    assert not result["passed"] and result["reason"] == "LOCAL_STRUCTURE_UNCONFIRMED"


def test_remote_confirmation_scores_one_orderflow_category_and_is_visible(signal):
    flow, _ = setup(signal, "SHORT")
    signal.source = "binance"
    signal.version += ":flow-quality-v1:flow-score-v2"
    signal.state = "CONFIRMED"
    signal.risk = {"accepted": True, "net_rr": 2}
    flow["quality"] = dict(flow_quality_state="REPETITIVE_TWO_SIDED_CHURN", flow_trust_score=0.2)
    signal.evidence["flow_quality"] = flow["quality"]
    signal.evidence["flow_confirmation_mode"] = "CROSS_VENUE_SUBSTITUTION"
    signal.evidence["flow_substitution"] = substitution(signal, flow, remote_rows(), 0, 60000, 61000, True)
    score(signal, True, False)
    earned = signal.evidence["score_components"]["orderflow"]["earned"]
    flow.update(delta_pct=-99, absorption_short=True, defended_notional={"SHORT": 1000000})
    score(signal, True, False)
    assert signal.evidence["score_components"]["orderflow"]["earned"] == earned
    assert earned <= 25
    card = embed(signal, "http://127.0.0.1:8000")
    assert any(
        "MIXED LOCAL / CONFIRMED CROSS-VENUE" in field["value"] for field in card["embeds"][0]["fields"]
    )


def test_future_confirmation_evidence_does_not_change_decision(signal):
    flow, bars = setup(signal)
    original = evaluate_intraday_confirmation(signal, flow, bars, 60000)
    bars.append(Candle(60000, 60000, 100, 120, 1, 2, 100, 10000))
    signal.evidence["volume_profile"] = dict(
        available=True, coverage_complete=True, available_ms=120000, end_ms=120000, rejection_low=True
    )
    assert evaluate_intraday_confirmation(signal, flow, bars, 60000) == original


def swing_bars():
    return [Candle(i * 60000, 60000, 100, 101, 99, 100, 10, 1000) for i in range(65)]


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
@pytest.mark.parametrize(
    "family", ["liquidity_sweep", "range_rejection", "trend_pullback", "breakout_retest"]
)
def test_swing_stop_structure_noise_and_targets(direction, family):
    bars = swing_bars()
    plan = build_stop_plan(100, direction, family, bars, bars, 2, Decimal(".01"), bars[-1].end)
    sign = 1 if direction == "LONG" else -1
    assert plan["policy"] == "swing-stop-v2" and not plan["reasons"]
    assert sign * (plan["anchor"] - plan["stop"]) >= plan["buffer"]
    assert plan["buffer"] == 1.5 and plan["execution_noise_q80"] == 2
    levels = targets(100, plan["stop"], direction, family, bars, Decimal(".01"), bars[-1].end)
    assert sign * (levels["tp1"] - 100) == 3 * abs(100 - plan["stop"])


def test_profile_stop_adverse_same_thesis_and_future_excluded():
    bars = swing_bars()
    now = bars[-1].end
    profile = dict(
        available=True, coverage_complete=True, end_ms=now, available_ms=now, excess_low_price=98.5
    )
    args = (100, "LONG", "liquidity_sweep", bars, bars, 2, Decimal(".01"), now)
    plan = build_stop_plan(*args, profile)
    assert plan["anchor"] == 98.5 and plan["profile_reference_type"] == "excess_low_price"
    assert build_stop_plan(*args, profile) == plan
    profile["available_ms"] = now + 1
    fallback = build_stop_plan(*args, profile)
    assert fallback == build_stop_plan(*args)
    future = Candle(now, 60000, 100, 1000, 1, 100, 10, 1000)
    assert (
        build_stop_plan(
            100, "LONG", "liquidity_sweep", bars + [future], bars + [future], 2, Decimal(".01"), now
        )
        == fallback
    )


def test_overwide_stop_is_rejected_not_tightened():
    bars = swing_bars()
    plan = build_stop_plan(110, "LONG", "liquidity_sweep", bars, bars, 2, Decimal(".01"), bars[-1].end)
    assert plan["stop"] == 97.5
    assert plan["reasons"] == ["SWING_STRUCTURAL_STOP_TOO_WIDE"]


def test_funnel_new_reasons_and_normal_delivery_path(settings, signal):
    from bybit_flow.funnel import emit

    store = Store(settings.data_dir)
    for reason in (
        "UNCONFIRMED_LIQUIDITY_SWEEP",
        "CONTRARIAN_EVIDENCE_INSUFFICIENT",
        "SWING_STRUCTURAL_STOP_TOO_WIDE",
    ):
        reject(store, signal, reason, 60000)
    for metric in ("flow_confirmed", "confirmed", "alert_claimed", "discord_sent"):
        emit(store, metric, 60000, signal=signal)
    result = status(store, 60000)
    assert result["cumulative"]["discord_sent"] == 1
    assert result["cumulative"]["risk_rejected"] == 1
    assert result["cumulative"]["market_alignment_blocked"] == 1
    store.close()


def test_existing_plan_unchanged_by_new_generation(signal, instrument):
    from bybit_flow.strategy import candidates

    original = signal.model_dump_json()
    bars = swing_bars()
    plans = candidates(instrument, bars, bars, bars, bars[-1].end, structural_targets=True, horizon="SWING")
    assert signal.model_dump_json() == original
    for plan in plans:
        assert plan.evidence["stop_plan"]["policy"] == "swing-stop-v2"
        assert ":production-v2" in plan.version


@pytest.mark.asyncio
async def test_scanner_retries_then_alerts_without_ml_champion(settings, signal, instrument, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from bybit_flow.features import candle_features
    from bybit_flow.models import Trade
    from bybit_flow.orderflow import Book, Tape
    from bybit_flow.scanner import Scanner

    bars = swing_bars()
    end = bars[-1].end
    now = end + 1000
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: now)
    monkeypatch.setattr("bybit_flow.scanner.SpreadHistory.assess", lambda *args: dict(reasons=[]))
    signal.family = "range_rejection"
    signal.horizon_profile = "CORE_INTRADAY"
    signal.version += ":production-v2:hardening-v1"
    signal.created_ms = end
    signal.expires_ms = end + 900000
    signal.trigger_expires_ms = end + 120000
    signal.holding_deadline_ms = end + 14400000
    signal.primary_tracking_deadline = signal.holding_deadline_ms
    signal.evidence.update(
        trigger_bar_end=end,
        execution_window_ms=60000,
        execution_window_end_ms=end,
        structural_trigger=dict(valid=True, level=99),
        context_features=candle_features(bars, end),
        setup_features=candle_features(bars, end),
    )
    # A reclaim through the frozen level satisfies response independently of candle color.
    bars[-1] = Candle(end - 60000, 60000, 99.8, 101, 98.9, 100, 10, 1000)
    book = Book()
    book.valid = True
    book.bids = {Decimal("99.99"): Decimal(10000)}
    book.asks = {Decimal("100.01"): Decimal(10000)}
    book.receipt_ms = book.event_ms = now
    tape = Tape()
    tape.reset(end - 120000)
    for i in range(60):
        tape.add(
            Trade(
                signal.symbol,
                end - 60000 + i * 1000,
                end - 60000 + i * 1000,
                str(i),
                "Buy",
                Decimal("99.6") + Decimal(i) / 100,
                Decimal(1),
            )
        )
    tape.last_event = tape.last_receipt = now
    store = Store(settings.data_dir)
    store.signal(signal)
    scanner = Scanner.__new__(Scanner)
    scanner.settings = settings.model_copy(update={"ml_enabled": True, "ml_filter_research": True})
    scanner.store = store
    scanner.recorder = Mock(healthy=True)
    scanner.api = SimpleNamespace(name="bybit")
    scanner.context = {
        signal.symbol: dict(
            m15=bars,
            h1=bars,
            h4=bars,
            asof=now,
            instrument=instrument,
            derivatives=dict(funding_rate=0, ticker_observed_ms=now),
        )
    }
    scanner.candle_cache = {(signal.symbol, "60"): (now, bars)}
    scanner.streams = SimpleNamespace(
        connected=True,
        books={signal.symbol: book},
        tapes={signal.symbol: tape},
        liquidations={signal.symbol: []},
    )
    scanner.notifier = SimpleNamespace(send_research=AsyncMock(return_value="sent"))
    # Fail first on opposing prints; do not freeze the failed decision or extend deadlines.
    original = list(tape.trades)
    tape.trades = [
        Trade(t.symbol, t.event_ms, t.receipt_ms, t.trade_id, "Sell", t.price, t.size) for t in original
    ]
    await scanner.evaluate()
    pending = store.signals()[0]
    assert pending["state"] == "PENDING CONFIRMATION" and not pending["evidence"].get("score_components")
    assert pending["trigger_expires_ms"] == signal.trigger_expires_ms
    tape.trades = original
    await scanner.evaluate()
    saved = store.signals()[0]
    assert saved["state"] == "ALERTED", saved["gates"]
    assert saved["evidence"]["confirmation"]["passed"]
    assert saved["model_version"] is None
    assert saved["stop"] == signal.stop and saved["holding_deadline_ms"] == signal.holding_deadline_ms
    scanner.notifier.send_research.assert_awaited_once()
    store.close()


def test_nullable_provenance_is_missing_not_confirmation_runtime_error(signal):
    from bybit_flow.ml.features import snapshot

    signal.evidence["flow"] = dict(delta_pct=30, available_ms=None)
    row = snapshot(signal, 60000, "decision")
    assert row["values"]["delta_pct"] is None


def test_factor_disagreement_and_future_factor_do_not_hard_block(signal):
    _, _ = setup(signal, "SHORT")
    signal.evidence["market_factor"] = dict(
        available=True,
        available_ms=60000,
        beta_to_btc=1,
        correlation_to_btc=0.8,
        beta_stability_btc=0.1,
        residual_btc=0,
        expected_return_btc=0.002,
    )
    btc = dict(regime="trending up", efficiency=0.6)
    eth = dict(regime="trending down", efficiency=0.6)
    result = market_alignment(signal, btc, eth, 60000)
    assert result["alignment"] == "FACTOR_RELATIONSHIP_UNCERTAIN" and not result["blocked"]
    signal.evidence["market_factor"]["available_ms"] = 120000
    assert not market_alignment(signal, btc, btc, 60000)["blocked"]
