"""Do the scanner's recorded order-flow, book and derivatives readings predict outcomes?

Candle replays cannot answer this: these readings exist only where the live
system recorded them. Every decision-stage snapshot in the production database
(about 28,900 over eleven weeks, 157 symbols) is labelled from public candles
and each declared reading is tested against the outcome.

Protocol, fixed before any outcome was computed:

- Outcome: entering at the next 5-minute open after the decision, does price
  reach +1R before -1R, where R is the distance to the frozen stop. A bar that
  touches both counts as the stop.
- Readings are made direction-aware where they have a direction, so "with the
  trade" always means the same thing for longs and shorts.
- First 75% of decisions by time is the test sample; the last 25% is held out.
- A reading passes only if top third versus bottom third differ in the test
  sample with |z| >= the Bonferroni threshold for the number of readings, with
  errors clustered by symbol-day, and the held-out sample shows the same sign
  with |z| >= 2.
- One model check: a ridge on all readings, fitted walk-forward, graded by
  whether its best tenth out of sample has a positive net R after costs.

Decisions are not independent (several per structural event), which is why the
errors are clustered and why this is evidence about readings, not a trade count.
"""

import bisect
import collections
import json
import math
import os
import statistics as st
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CANDLES = os.path.join(HERE, "..", "backtest", "candles-decisions")
COST_BPS = 15.0
DAY = 86_400_000

# name -> (source key or callable, directional?) ; directional readings are multiplied by the trade sign.
WITH_AGAINST = {
    "absorption": ("absorption_long", "absorption_short"),
    "initiative": ("initiative_long", "initiative_short"),
    "stacked": ("stacked_buy", "stacked_sell"),
    "flow_efficiency": ("flow_buy_efficiency", "flow_sell_efficiency"),
    "book_replenishment": ("book_replenishment_60s.bid", "book_replenishment_60s.ask"),
    "trapped_opponents": ("potential_trapped_sellers", "potential_trapped_buyers"),
    "sweep": ("sweep_long", "sweep_short"),
}
DIRECTIONAL = (
    "delta_pct delta_divergence flow_delta_persistence flow_cvd_acceleration book_obi_touch book_obi_5bps "
    "book_obi_10bps book_obi_25bps book_obi_persistence book_depth.10.imbalance book_visible_flow_imbalance "
    "funding_rate derivatives_basis market_factor_residual_return auction_poc_shift h1_slope h4_slope "
    "m15_slope h1_premium_discount h4_premium_discount m15_premium_discount session_metrics_delta_relative"
).split()
PLAIN = (
    "book_spread_bps oi_change_pct derivatives_crowding_score derivatives_deleveraging_score "
    "range_location range_width_atr range_compression range_boundary_interactions auction_value_overlap "
    "market_factor_beta_to_btc market_factor_correlation_to_btc h1_efficiency h4_efficiency m15_efficiency "
    "h1_volume_expansion h4_volume_expansion m15_volume_expansion h1_realized_volatility stop_atr net_rr "
    "execution_stop_noise_ratio execution_volatility_ratio execution_depth_consumed flow_same_side_run "
    "session_metrics_depth_relative session_metrics_spread_relative session_metrics_trade_intensity_relative "
    "session_metrics_turnover_relative utc_hour quality_score score_orderflow score_structure score_regime "
    "score_derivatives score_execution participation_volume_percentile participation_obi_percentile "
    "participation_delta_magnitude_percentile flow_trust_score flow_absorption_ratio size_concentration"
).split()


def number(value):
    if isinstance(value, bool):
        return float(value)
    return float(value) if isinstance(value, (int, float)) and math.isfinite(value) else None


def readings(record):
    values, sign = record["values"], 1 if record["direction"] == "LONG" else -1
    out = {}
    for name, (long_key, short_key) in WITH_AGAINST.items():
        a, b = number(values.get(long_key)), number(values.get(short_key))
        if a is not None and b is not None:
            out[name + "_with_trade"], out[name + "_against_trade"] = (a, b) if sign > 0 else (b, a)
    for key in DIRECTIONAL:
        v = number(values.get(key))
        if v is not None:
            out[key + " (trade direction)"] = sign * v
    for key in PLAIN:
        v = number(values.get(key))
        if v is not None:
            out[key] = v
    for key in ("flow_confirmed", "risk_accepted"):
        if record.get(key) is not None:
            out[key] = float(bool(record[key]))
    out["no_gates_raised"] = float(not record.get("gates"))
    return out


def label(records):
    """Attach the candle outcome to each decision."""
    cache, out, skipped = {}, [], collections.Counter()
    for r in records:
        symbol = r["exchange_symbol"] if r["source"] == "binance" and r["exchange_symbol"] else r["symbol"]
        if symbol not in cache:
            path = os.path.join(CANDLES, symbol + ".json")
            rows = json.load(open(path)) if os.path.exists(path) else None
            cache[symbol] = (rows, [x[0] for x in rows]) if rows else (None, None)
        rows, times = cache[symbol]
        if not rows:
            skipped["no candles"] += 1
            continue
        i = bisect.bisect_right(times, r["decision_ms"])
        if i >= len(rows) - 12 or i == 0 or rows[i][0] - r["decision_ms"] > 600_000:
            skipped["outside candle range"] += 1
            continue
        sign = 1 if r["direction"] == "LONG" else -1
        entry = rows[i][1]
        risk = sign * (entry - r["stop"])
        target = sign * (r["tp1"] - entry)
        if risk <= 0 or target <= 0 or risk / entry > 0.25:
            skipped["stop or target already crossed"] += 1
            continue
        even, plan = None, None
        deadline = r["decision_ms"] + (r.get("hold") or 240) * 60_000
        for t, o, h, low, _c, _v, _q in rows[i : i + 2880]:
            stop_hit = (low <= entry - sign * risk) if sign > 0 else (h >= entry - sign * risk)
            if even is None:
                if stop_hit:
                    even = 0.0
                elif (h >= entry + sign * risk) if sign > 0 else (low <= entry + sign * risk):
                    even = 1.0
            if plan is None:
                if t > deadline:
                    plan = sign * (o - entry) / risk
                elif stop_hit:
                    plan = -1.0
                elif (h >= r["tp1"]) if sign > 0 else (low <= r["tp1"]):
                    plan = target / risk
            if even is not None and plan is not None:
                break
        if even is None:
            skipped["unresolved"] += 1
            continue
        stop_bps = risk / entry * 1e4
        out.append(
            dict(
                t=r["decision_ms"],
                cluster=(symbol, r["decision_ms"] // DAY),
                win=even,
                net_r=(plan if plan is not None else 0.0) - COST_BPS / stop_bps,
                x=readings(r),
                family=r["family"],
                horizon=r["horizon"],
            )
        )
    return out, skipped


def clustered_mean(rows, key):
    groups = collections.defaultdict(list)
    for r in rows:
        groups[r["cluster"]].append(r[key])
    n = sum(len(v) for v in groups.values())
    mean = sum(sum(v) for v in groups.values()) / n
    residual = [sum(x - mean for x in v) for v in groups.values()]
    k = len(residual)
    se = math.sqrt(sum(e * e for e in residual) * k / (k - 1)) / n if k > 1 else float("inf")
    return mean, se, n


def tercile_test(rows, name):
    have = sorted((r for r in rows if name in r["x"]), key=lambda r: r["x"][name])
    if len(have) < 600 or have[0]["x"][name] == have[-1]["x"][name]:
        return None
    values = [r["x"][name] for r in have]
    if len(set(values)) <= 2:  # A flag: compare its two states.
        low, high = [r for r in have if r["x"][name] == values[0]], [r for r in have if r["x"][name] != values[0]]
    else:
        k = len(have) // 3
        low, high = have[:k], have[-k:]
    if len(low) < 150 or len(high) < 150:
        return None
    (a, sa, na), (b, sb, nb) = clustered_mean(low, "win"), clustered_mean(high, "win")
    return dict(n=len(have), low=a, high=b, diff=b - a, z=(b - a) / math.sqrt(sa * sa + sb * sb), n_low=na, n_high=nb)


def main():
    records = [json.loads(line) for line in open(os.path.join(HERE, "decisions.jsonl"), encoding="utf-8")]
    rows, skipped = label(records)
    rows.sort(key=lambda r: r["t"])
    cut = rows[int(len(rows) * 0.75)]["t"]
    test, hold = [r for r in rows if r["t"] < cut], [r for r in rows if r["t"] >= cut]
    names = sorted({k for r in rows for k in r["x"]})
    threshold = st.NormalDist().inv_cdf(1 - 0.05 / (2 * len(names)))
    base, base_se, _ = clustered_mean(rows, "win")
    net, net_se, _ = clustered_mean(rows, "net_r")
    print(f"decisions labelled: {len(rows)} of {len(records)}; skipped {dict(skipped)}")
    print(f"clusters (symbol-days): {len({r['cluster'] for r in rows})}; test {len(test)}, held out {len(hold)}")
    print(f"all decisions: +1R first {100 * base:.1f}% (±{196 * base_se:.1f}), plan net {net:+.3f}R (±{1.96 * net_se:.3f})")
    print(f"readings tested: {len(names)}; required |z| in test: {threshold:.2f}; then same sign and |z|>=2 held out\n")
    results = []
    for name in names:
        a, b = tercile_test(test, name), tercile_test(hold, name)
        if a:
            passed = bool(b and abs(a["z"]) >= threshold and abs(b["z"]) >= 2 and a["z"] * b["z"] > 0)
            results.append((abs(a["z"]), name, a, b, passed))
    results.sort(reverse=True)
    print(f"{'reading':52s}{'n':>7}{'low third':>10}{'high third':>11}{'z test':>8}{'z held':>8}  result")
    for _score, name, a, b, passed in results:
        held = f"{b['z']:+8.1f}" if b else "     n/a"
        print(
            f"{name[:52]:52s}{a['n']:>7}{100 * a['low']:>9.1f}%{100 * a['high']:>10.1f}%{a['z']:>+8.1f}{held}"
            f"  {'PASSES' if passed else ''}"
        )
    print("\nPASSED:", ", ".join(n for _s, n, _a, _b, p in results if p) or "none")
    json.dump(
        [dict(name=n, test=a, holdout=b, passed=p) for _s, n, a, b, p in results],
        open(os.path.join(HERE, "decision_feature_results.json"), "w"),
        indent=1,
    )
    model_check(rows, names)


def model_check(rows, names):
    import numpy as np

    usable = [n for n in names if sum(n in r["x"] for r in rows) >= 0.5 * len(rows)]
    medians = {n: st.median(r["x"][n] for r in rows[: len(rows) // 2] if n in r["x"]) for n in usable}

    def matrix(sample):
        return np.array([[r["x"].get(n, medians[n]) for n in usable] for r in sample], float)

    scored, n = [], len(rows)
    for fold in range(3):
        a, b = n * (3 + fold) // 6, n * (4 + fold) // 6
        x, y = matrix(rows[:a]), np.array([max(-1.5, min(6.0, r["net_r"])) for r in rows[:a]])
        mean, scale = x.mean(axis=0), x.std(axis=0)
        scale[scale == 0] = 1.0
        z = (x - mean) / scale
        weights = np.linalg.solve(z.T @ z + 500.0 * np.eye(z.shape[1]), z.T @ (y - y.mean()))
        prediction = ((matrix(rows[a:b]) - mean) / scale) @ weights + y.mean()
        scored += list(zip(prediction.tolist(), rows[a:b]))
    scored.sort(key=lambda item: item[0])
    print(f"\nModel check: ridge on {len(usable)} readings with at least 50% coverage, walk-forward, n={len(scored)}")
    for k in range(10):
        chunk = [r for _p, r in scored[len(scored) * k // 10 : len(scored) * (k + 1) // 10]]
        mean, se, count = clustered_mean(chunk, "net_r")
        win, _, _ = clustered_mean(chunk, "win")
        print(
            f"  predicted decile {k + 1:2d}: n={count:5d}  +1R first {100 * win:5.1f}%"
            f"  net {mean:+.3f}R  (lower bound {mean - 1.96 * se:+.3f})"
        )


if __name__ == "__main__":
    sys.exit(main())
