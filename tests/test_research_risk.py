from dataclasses import replace
from decimal import Decimal as D

import pytest
from test_market_data import message

from bybit_flow.backtest import PaperPosition, performance
from bybit_flow.calibration import clustered_statistics, walk_forward_predictions
from bybit_flow.features import candle_features, pivots, validate_bars
from bybit_flow.models import Candle, Trade
from bybit_flow.orderflow import Book
from bybit_flow.risk import evaluate_risk
from bybit_flow.scoring import score


def test_pivot_confirmation_delay():
    bars = [Candle(i * 1000, 1000, 2, h, 1, 2, 1, 2) for i, h in enumerate([3, 4, 7, 4, 3, 2])]
    assert not pivots(bars[:4], 2, 2)
    p = pivots(bars[:5], 2, 2)[0]
    assert p["pivot_ms"] == 3000 and p["available_ms"] == 5000


def test_future_extension_does_not_change_past_features(bars):
    original = candle_features(bars[:100], bars[99].end)
    future = [replace(c, high=c.high * 100, close=c.close * 50) for c in bars[100:]]
    joined = bars[:100] + future
    assert candle_features([c for c in joined if c.end <= bars[99].end], bars[99].end) == original


def test_gapped_or_unclosed_bars_rejected(bars):
    with pytest.raises(ValueError):
        validate_bars(bars, bars[-1].start)
    with pytest.raises(ValueError):
        candle_features(bars[:50] + bars[51:], bars[-1].end)


def test_position_sizing_depends_on_stop_not_leverage(settings, instrument, signal):
    book = Book()
    book.apply(message(), 1000)
    cfg = settings.model_copy(update={"equity": 10000, "leverage": 3})
    p = {"positions": [], "daily_loss_fraction": 0, "weekly_loss_fraction": 0}
    a = evaluate_risk(signal, instrument, cfg, book, p, 0.0001)
    b = evaluate_risk(signal, instrument, cfg.model_copy(update={"leverage": 1}), book, p, 0.0001)
    assert a["accepted"] and a["quantity"] == b["quantity"]
    assert a["estimated_loss"] <= 25
    assert a["estimated_margin"] < b["estimated_margin"]
    assert a["net_rr"] < a["gross_rr"]


def test_portfolio_limits_and_missing_snapshot(settings, instrument, signal):
    book = Book()
    book.apply(message(), 1000)
    cfg = settings.model_copy(update={"equity": 10000})
    assert not evaluate_risk(signal, instrument, cfg, book)["accepted"]
    p = {"positions": [{"risk_fraction": 0.006}], "daily_loss_fraction": 0.02, "weekly_loss_fraction": 0.05}
    r = evaluate_risk(signal, instrument, cfg, book, p)
    assert {"daily loss limit", "weekly loss limit", "combined correlated risk limit"} <= set(r["reasons"])


def test_inadequate_liquidation_buffer_rejected(settings, instrument, signal):
    signal.stop, signal.tp1 = 80, 180
    book = Book()
    book.apply(message(), 1000)
    assert (
        "illustrative liquidation buffer inadequate"
        in evaluate_risk(signal, instrument, settings, book)["reasons"]
    )


def test_high_quality_is_never_probability(signal):
    signal.risk = {"accepted": True}
    score(signal, True, True)
    assert signal.qualification["probability"] is None
    assert signal.final_tier == "RESEARCH" and signal.quality < 90
    signal.gates = ["stale"]
    score(signal, True, True)
    assert signal.raw_tier == "F" and signal.final_tier == "REJECTED"


def test_partial_missed_gap_and_costed_fills(signal):
    p = PaperPosition(signal, requested_qty=1, participation=0.1)
    p.on_trade(Trade("TESTUSDT", 1100, 1100, "0", "Buy", D(100), D(10)))
    assert p.quantity == 0  # latency
    p.on_trade(Trade("TESTUSDT", 2000, 2000, "1", "Buy", D(100), D(5)))
    assert p.quantity == 0.5
    p.on_trade(Trade("TESTUSDT", 70000, 70000, "2", "Sell", D(90), D(10)))
    o = p.outcome()
    assert o["complete"] and o["net_r"] < -2  # gap below stop, plus fees/slippage
    missed = PaperPosition(signal, 1)
    missed.on_trade(Trade("TESTUSDT", 80000, 80000, "3", "Buy", D(100), D(10)))
    assert missed.outcome()["exit_reason"] == "missed_fill"


def test_funding_sign_and_gap_exclusion(signal):
    p = PaperPosition(signal, 1, participation=1)
    p.on_trade(Trade("TESTUSDT", 2000, 2000, "1", "Buy", D(100), D(1)))
    p.on_funding(3000, 0.001, 100)
    assert p.funding == 0.1
    p.data_gaps.append("disconnected")
    p.on_trade(Trade("TESTUSDT", 70000, 70000, "2", "Buy", D(115), D(1)))
    assert not p.outcome()["complete"]
    assert performance([p.outcome()])["trades"] == 0


def test_cluster_uncertainty_uses_independent_weeks():
    stats = clustered_statistics([{"net_r": 1, "cluster": 1}] * 500)
    assert stats["effective_samples"] == 1 and stats["probability"] is None
    assert "ev_ci_r" not in stats
    rows = [{"net_r": 1 if i % 2 else -1, "cluster": i} for i in range(30)]
    assert clustered_statistics(rows, replicates=100) == clustered_statistics(rows, replicates=100)


def test_walk_forward_does_not_use_unfinished_future_labels():
    rows = [
        dict(
            entry_ms=i * 100,
            exit_ms=1000000,
            family="x",
            direction="LONG",
            regime="range",
            liquidity_bucket="liquid",
            score=80,
            cluster=i,
            net_r=1,
        )
        for i in range(30)
    ]
    predictions = walk_forward_predictions(rows, min_train=1, purge_ms=0)
    assert all(p["train_size"] == 0 and p["prediction"] is None for p in predictions)
