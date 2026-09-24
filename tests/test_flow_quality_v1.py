"""Focused causal flow-quality and higher-timeframe prior cases."""

from decimal import Decimal

from bybit_flow.app import create_app
from bybit_flow.cross_venue import compare
from bybit_flow.diagnostics import flow_quality_status
from bybit_flow.flow_quality import assess
from bybit_flow.models import Trade
from bybit_flow.production import market_alignment, participation
from bybit_flow.storage import Store


def prints(prices, sides, sizes=None):
    sizes = sizes or [1] * len(prices)
    return [
        Trade("TESTUSDT", i * 1000, i * 1000, str(i), side, Decimal(str(price)), Decimal(str(size)))
        for i, (price, side, size) in enumerate(zip(prices, sides, sizes))
    ]


def quality(rows, raw=None, book=None, baseline=(), cross=None):
    raw = raw or {"available": True, "bucket": "0.01", "poc": 100}
    return assess(rows, Decimal("0.01"), book or {}, raw, 120000, 0, 120000, baseline, cross)


def test_repetitive_churn_cannot_supply_trusted_participation():
    rows = prints([100] * 100, ["Buy", "Sell"] * 50)
    result = quality(rows)
    assert result["flow_quality_state"] == "REPETITIVE_TWO_SIDED_CHURN"
    assert result["mirror_pair_count"] >= 8
    assert result["flow_trust_score"] < 0.6
    assert result["effective_volume_ratio"] < 0.3
    metrics = {
        "effective_baseline_samples": 20,
        "volume_percentile": 1.0,
        "trade_count_percentile": 1.0,
        "delta_magnitude_percentile": 1.0,
    }
    assert not participation(metrics, -1, 0.7, trusted=True)["passed"]


def test_directional_flow_and_absorption_are_distinct():
    rows = prints([100 + i * 0.02 for i in range(40)], ["Buy"] * 40)
    directional = quality(rows, book={"depletion_60s": {"ask": 500}})
    assert directional["flow_quality_state"] == "INFORMATIVE_DIRECTIONAL_FLOW"
    assert directional["flow_trust_score"] >= 0.8
    sellers = prints([100, 99.8] + [100] * 38, ["Sell"] * 35 + ["Buy"] * 5)
    absorbed = quality(
        sellers,
        raw={
            "available": True,
            "bucket": "0.01",
            "poc": 100,
            "absorption_long": True,
            "potential_trapped_sellers": True,
        },
        book={"replenishment_60s": {"bid": 1000}},
    )
    assert absorbed["flow_quality_state"] == "GENUINE_ABSORPTION"
    assert absorbed["flow_trust_score"] >= 0.8


def test_low_printed_volume_can_be_liquidity_repricing():
    rows = prints([100, 100.2, 100.4], ["Buy"] * 3, [0.01] * 3)
    result = quality(
        rows,
        book={"depletion_60s": {"ask": 50}, "spread_bps": 6, "depth": {"10": {"ask": 1}}},
        cross={"available": True, "receipt_ms": 119000, "wick_context": "MARKET_WIDE_SWEEP"},
    )
    assert result["flow_quality_state"] == "LIQUIDITY_DRIVEN_REPRICING"


def aligned_signal(signal, direction, btc_regime, strong=False):
    sign = 1 if direction == "LONG" else -1
    signal.version += ":flow-quality-v1"
    signal.horizon_profile = "SHORT_INTRADAY"
    signal.context_timeframe, signal.setup_timeframe, signal.execution_timeframe = "60", "15", "5"
    signal.direction = direction
    opposite = "trending down" if btc_regime == "trending up" else "trending up"
    signal.evidence["factor_timeframes"] = {
        "60": dict(
            available=True,
            available_ms=1000,
            beta_to_btc=1.0,
            correlation_to_btc=0.8,
            beta_stability_btc=0.1,
            residual_btc=sign * (0.004 if strong else 0.0001),
            expected_return_btc=0.002,
        )
    }
    signal.evidence["factor_regimes"] = {
        "60": {"btc": {"regime": btc_regime, "efficiency": 0.7}},
        "15": {"btc": {"regime": btc_regime, "efficiency": 0.6}},
        "5": {"btc": {"regime": opposite, "efficiency": 0.4}},
    }
    signal.evidence["flow"] = dict(
        available=True,
        delta_pct=sign * 35 if strong else sign * 12,
        cvd_slope=sign * 10,
        delta_persistence=0.8,
        initiative_long=sign > 0,
        initiative_short=sign < 0,
        quality={
            "flow_trust_score": 1.0 if strong else 0.85,
            "flow_quality_state": "INFORMATIVE_DIRECTIONAL_FLOW",
            "price_displacement_bps": sign * 10 if strong else sign * 1,
        },
    )
    signal.evidence["auction"] = {"acceptance_above" if sign > 0 else "acceptance_below": strong}
    signal.evidence["session_metrics"] = {"effective_baseline_samples": 0}
    return signal


def test_bullish_htf_prior_blocks_ordinary_alt_short_and_allows_real_divergence(signal):
    weak = aligned_signal(signal, "SHORT", "trending up")
    result = market_alignment(weak, {}, {}, 2000, True)
    assert result["timeframe_context"] == "HTF_TREND_WITH_LTF_PULLBACK"
    assert result["blocked"] and result["reason"] == "HTF_MARKET_CONFLICT_WITHOUT_STRONG_DIVERGENCE"
    aligned_signal(signal, "SHORT", "trending up", strong=True)
    result = market_alignment(signal, {}, {}, 2000, True)
    assert result["alignment"] == "IDIOSYNCRATIC_DIVERGENCE" and not result["blocked"]
    signal.evidence["flow"]["quality"].update(
        flow_trust_score=0.2, flow_quality_state="REPETITIVE_TWO_SIDED_CHURN"
    )
    assert market_alignment(signal, {}, {}, 2000, True)["blocked"]


def test_bearish_htf_prior_blocks_ordinary_alt_long(signal):
    aligned_signal(signal, "LONG", "trending down")
    result = market_alignment(signal, {}, {}, 2000, True)
    assert result["blocked"] and result["reason"] == "HTF_MARKET_CONFLICT_WITHOUT_STRONG_DIVERGENCE"


def test_cross_venue_flow_does_not_replace_contrarian_factor_or_acceptance(signal):
    aligned_signal(signal, "SHORT", "trending up", strong=False)
    signal.evidence["flow"]["quality"].update(
        flow_trust_score=0.2, flow_quality_state="REPETITIVE_TWO_SIDED_CHURN"
    )
    signal.evidence["flow_substitution"] = {"passed": True}
    signal.evidence["flow_confirmation_mode"] = "CROSS_VENUE_SUBSTITUTION"
    weak = market_alignment(signal, {}, {}, 2000, True)
    assert weak["blocked"] and weak["reason"] == "HTF_MARKET_CONFLICT_WITHOUT_STRONG_DIVERGENCE"
    signal.evidence["factor_timeframes"]["60"]["residual_btc"] = -0.004
    signal.evidence["auction"]["acceptance_below"] = True
    strong = market_alignment(signal, {}, {}, 2000, True)
    assert strong["alignment"] == "IDIOSYNCRATIC_DIVERGENCE" and not strong["blocked"]


def test_large_low_trust_venue_does_not_overrule_trusted_cross_venue_flow():
    rows = []
    for name, raw_delta, effective_delta, trust in (
        ("binance", 15, 100, 1.0),
        ("bybit", 12, 75, 0.85),
        ("okx", -95, -10000, 0.2),
    ):
        rows.append(
            dict(
                exchange=name,
                event_ms=119000,
                mid=100,
                spread_bps=1,
                window_start=0,
                window_end=60000,
                flow=dict(
                    available=True,
                    delta_pct=raw_delta,
                    quality=dict(
                        flow_trust_score=trust,
                        effective_delta_notional=effective_delta,
                        price_displacement_bps=4 if effective_delta > 0 else -1,
                    ),
                ),
            )
        )
    result = compare(rows, 120000)
    assert result["trusted_consensus_delta_sign"] == "POSITIVE"
    assert result["cross_venue_trusted_flow_agreement"]


def test_retired_tradingview_endpoint_preserves_historical_rows(settings, signal):
    signal.source = "tradingview"
    signal.state = "RESOLVED"
    store = Store(settings.data_dir)
    with store.db:
        store.db.execute(
            "INSERT INTO signals VALUES(?,?,?,?,?)",
            (signal.id, signal.symbol, signal.created_ms, signal.state, signal.model_dump_json()),
        )
    app = create_app(settings)
    assert not any(route.path.startswith("/webhooks/tradingview") for route in app.routes)
    assert store.db.execute("SELECT state FROM signals WHERE id=?", (signal.id,)).fetchone()[0] == "RESOLVED"
    assert not hasattr(settings, "tv_enabled")
    store.close()


def test_flow_quality_doctor_reports_measured_windows(settings):
    store = Store(settings.data_dir)
    store.put(
        "flow-quality-baseline-v1:binance:TESTUSDT:ASIA:SHORT_INTRADAY",
        [
            {
                "window_end_ms": 119000,
                "flow_quality_state": "INFORMATIVE_DIRECTIONAL_FLOW",
                "flow_trust_score": 1.0,
                "effective_volume_ratio": 0.9,
            }
        ],
    )
    store.put(
        "feed:binance:TESTUSDT",
        {"at_ms": 119000, "status": "HEALTHY", "book_event_ms": 119000, "trade_event_ms": 119000},
    )
    quality, venues, alignment = flow_quality_status(store, settings, 120000)
    assert quality["windows_1h"] == 1 and quality["informative_pct"] == 100
    assert venues["binance"]["book_continuity"] == 1
    assert alignment["contrarian_weak_blocked"] == 0
    store.close()
