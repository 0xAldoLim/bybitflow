import json
from types import SimpleNamespace

import pytest

from bybit_flow.acceptance import (
    DAY_MS,
    M5,
    MAKER_FEE_BPS,
    cross_sectional_rows,
    evaluate,
    fade_maker,
    first_touch,
    funding_rows,
    maker_entry,
    net_r,
    taker_cost_bps,
    variants_of,
)


def bar(i, o, h, low, c):
    return [i * M5, o, h, low, c, 1.0, 1.0]


def plan(entry=100.0, stop=98.0, tp1=106.0):
    return SimpleNamespace(entry=entry, stop=stop, tp1=tp1)


def test_limit_order_needs_a_trade_through_and_pays_maker_on_a_target():
    rows = [
        bar(0, 100.5, 101, 100.0, 100.8),  # Touches 100 exactly: not a fill.
        bar(1, 100.8, 101, 99.8, 100.5),  # Trades through: filled at 100.
        bar(2, 100.5, 106.5, 100.2, 106),  # Target.
        bar(3, 106, 106, 106, 106),
        bar(4, 106, 106, 106, 106),
    ]
    filled = maker_entry(rows, 0, plan(), 1, 0.0, 12, 10 * M5)
    assert filled["plan"] == ("UP", 3.0) and filled["even"] == ("UP", 1)
    assert filled["cost_bps"] == 2 * MAKER_FEE_BPS and filled["stop_bps"] == pytest.approx(200)


def test_fill_bar_can_stop_but_never_credit_the_target():
    rows = [
        bar(0, 100.5, 107, 97.5, 99),  # Fills, and the same bar reaches both stop and target.
        bar(1, 99, 99, 99, 99),
        bar(2, 99, 99, 99, 99),
        bar(3, 99, 99, 99, 99),
    ]
    filled = maker_entry(rows, 0, plan(), 1, 0.0, 12, 10 * M5)
    assert filled["plan"] == ("DN", -1)
    assert filled["cost_bps"] == MAKER_FEE_BPS + taker_cost_bps()


def test_unfilled_and_deeper_limit_orders():
    rows = [bar(i, 101, 102, 100.5, 101.5) for i in range(20)]
    assert maker_entry(rows, 0, plan(), 1, 0.0, 12, 10 * M5) is None
    rows[3] = bar(3, 101, 101, 99.4, 100)  # Reaches 99.5 = entry minus 0.25 of the 2.0 risk.
    deeper = maker_entry(rows, 0, plan(), 1, 0.25, 12, 10 * M5)
    assert deeper["stop_bps"] == pytest.approx(1.5 / 99.5 * 1e4)
    short = maker_entry(rows, 0, plan(entry=101.9, stop=103.9, tp1=96), -1, 0.0, 12, 10 * M5)
    assert short is not None and short["stop_bps"] == pytest.approx(2 / 101.9 * 1e4)


def test_variants_carry_their_own_cost_and_only_count_rows_that_have_them():
    base = dict(symbol="X", horizon="SWING", family="trend_pullback", stop_bps=100.0, features={})
    rows = []
    for i in range(400):
        row = base | dict(
            t=(i // 4) * DAY_MS + i,
            as_published=dict(even=("UP", 1), plan=("UP", 2)),
            mirrored=dict(even=("DN", -1), plan=("DN", -1)),
        )
        if i % 2:
            row["maker_close"] = dict(even=("UP", 1), plan=("UP", 2), stop_bps=200.0, cost_bps=4.0)
        rows.append(row)
    assert variants_of(rows) == ["as_published", "maker_close", "mirrored"]
    assert net_r(rows[1], "maker_close", 15) == pytest.approx(2 - 4 / 200)
    assert net_r(rows[1], "as_published", 15) == pytest.approx(2 - 15 / 100)
    report = evaluate(rows, 15)
    assert report["rules_tried"] == 3
    assert report["segments"]["SWING|trend_pullback|maker_close"]["all"]["n"] == 200
    assert report["segments"]["SWING|trend_pullback|as_published"]["all"]["n"] == 400


def write_symbol(directory, symbol, drift, days=40):
    """5m candles with a steady drift, so 7-day ranks are unambiguous."""
    rows, price = [], 100.0
    for i in range(days * 288):
        nxt = price * (1 + drift)
        rows.append([i * M5, price, max(price, nxt) * 1.001, min(price, nxt) * 0.999, nxt, 1.0, 1.0])
        price = nxt
    (directory / f"{symbol}.json").write_text(json.dumps(rows))
    return rows


def test_cross_sectional_longs_the_strongest_and_shorts_the_weakest(tmp_path):
    symbols = [f"S{i:02d}USDT" for i in range(12)]
    for i, symbol in enumerate(symbols):
        write_symbol(tmp_path, symbol, (i - 5.5) * 2e-6)
    rows = cross_sectional_rows(symbols, tmp_path)
    assert rows and {r["family"] for r in rows} == {"cross_momentum_7d"}
    assert {r["symbol"] for r in rows if r["direction"] == "LONG"} == set(symbols[-3:])
    assert {r["symbol"] for r in rows if r["direction"] == "SHORT"} == set(symbols[:3])
    assert all(r["t"] % DAY_MS == 0 for r in rows)


def test_funding_extreme_fades_the_crowd(tmp_path):
    write_symbol(tmp_path, "TESTUSDT", 0.0)
    interval = 8 * 3_600_000
    funding = [[(n + 1) * interval, 0.0001 + (n % 7) * 1e-6] for n in range(110)]
    funding[100][1] = 0.01  # Longs pay heavily.
    funding[105][1] = -0.01  # Shorts pay heavily.
    (tmp_path / "_funding_TESTUSDT.json").write_text(json.dumps(funding))
    rows = funding_rows("TESTUSDT", tmp_path)
    by_time = {r["t"]: r["direction"] for r in rows}
    assert by_time[funding[100][0]] == "SHORT" and by_time[funding[105][0]] == "LONG"
    assert funding_rows("MISSINGUSDT", tmp_path) == []


def test_fade_trades_against_the_signal_with_resting_orders():
    # Original plan is LONG at 100 with stop 98: the fade sells 100, stop 102, target 98.
    rows = [
        bar(0, 99.5, 100.0, 99.0, 99.8),  # Touches 100 exactly: a sell limit is not filled.
        bar(1, 99.8, 100.4, 99.6, 100.0),  # Trades through: short from 100.
        bar(2, 100.0, 100.5, 98.0, 98.5),  # Touches the 98 target exactly: not filled yet.
        bar(3, 98.5, 99.0, 97.8, 98.0),  # Trades through the target.
        bar(4, 98, 98, 98, 98),
        bar(5, 98, 98, 98, 98),
    ]
    faded = fade_maker(rows, 0, plan(), 1, 12, 20 * M5)
    assert faded["plan"] == ("UP", 1) and faded["even"] == ("UP", 1)
    assert faded["cost_bps"] == 2 * MAKER_FEE_BPS and faded["stop_bps"] == pytest.approx(200)


def test_fade_stop_pays_taker_cost_and_fill_bar_can_stop_it():
    rows = [bar(0, 99.5, 102.5, 99.0, 102.0)] + [bar(i, 102, 102, 102, 102) for i in range(1, 6)]
    faded = fade_maker(rows, 0, plan(), 1, 12, 20 * M5)
    assert faded["plan"] == ("DN", -1)
    assert faded["cost_bps"] == MAKER_FEE_BPS + taker_cost_bps()
    assert fade_maker(rows, 0, plan(), 1, 12, 20 * M5, taker_side=9.5)["cost_bps"] == MAKER_FEE_BPS + 9.5
    quiet = [bar(i, 99, 99.5, 98.5, 99) for i in range(20)]
    assert fade_maker(quiet, 0, plan(), 1, 12, 20 * M5) is None


def test_strict_target_requires_a_trade_through():
    touch = [[0, 100, 102, 99.5, 101, 1, 1], [M5, 101, 102.1, 100.5, 102, 1, 1]]
    assert first_touch(touch[:1], 0, 100, 1, 1, 1, 2) == ("UP", 1)
    assert first_touch(touch[:1], 0, 100, 1, 1, 1, 2, strict_target=True) == ("OPEN", 0.0)
    assert first_touch(touch, 0, 100, 1, 1, 1, 2, strict_target=True) == ("UP", 1)
