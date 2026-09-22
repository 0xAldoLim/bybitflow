"""Measured research hypotheses, never transferred equity probabilities."""

import math
import random
from statistics import mean, median, pstdev

from .features import candle_features


def range_context(bars, now):
    f = candle_features(bars, now)
    window = bars[-22:-2]
    lo, hi = min(c.low for c in window), max(c.high for c in window)
    last, atr = bars[-1], max(f["atr"], 1e-12)
    width = max(hi - lo, 1e-12)
    above, below = last.high > hi, last.low < lo
    inside = lo <= last.close <= hi
    prior_width = (
        max(c.high for c in bars[-41:-21]) - min(c.low for c in bars[-41:-21]) if len(bars) >= 41 else width
    )
    return dict(
        high=hi,
        low=lo,
        midpoint=(hi + lo) / 2,
        width_atr=width / atr,
        preceding_impulse=abs(window[0].close - bars[-31].close) / atr if len(bars) >= 31 else None,
        boundary_interactions=sum(c.high >= hi - 0.15 * atr or c.low <= lo + 0.15 * atr for c in window),
        time_inside=sum(lo <= c.close <= hi for c in window) / len(window),
        compression=width / max(prior_width, 1e-12),
        maturity=len(window),
        deviation_above=above,
        deviation_below=below,
        reentry=inside and (above or below),
        failed_acceptance=inside and (above or below),
        accepted_breakout=last.close > hi and bars[-2].close > hi or last.close < lo and bars[-2].close < lo,
        location=(last.close - lo) / width,
        breakout_distance_atr=max(last.close - hi, lo - last.close, 0) / atr,
    )


def auction_context(flow, previous, price):
    if not flow.get("available") or flow.get("val") is None:
        return {"state": "UNCLEAR", "available": False}
    lo, hi, poc = flow["val"], flow["vah"], flow["poc"]
    prevlo, prevhi = previous.get("val"), previous.get("vah")
    overlap = None
    if prevlo is not None and prevhi is not None:
        overlap = max(0, min(hi, prevhi) - max(lo, prevlo)) / max(hi - lo, prevhi - prevlo, 1e-12)
    shift = poc - previous["poc"] if previous.get("poc") is not None else None
    state = "DISCOVERY_UP" if price > hi else "DISCOVERY_DOWN" if price < lo else "BALANCE"
    if prevhi is not None and flow.get("high", price) > prevhi and price <= prevhi:
        state = "REJECTION_HIGH"
    elif prevlo is not None and flow.get("low", price) < prevlo and price >= prevlo:
        state = "REJECTION_LOW"
    elif overlap is not None and overlap < 0.25 and lo <= price <= hi:
        state = "TRANSITION"
    return dict(
        available=True,
        state=state,
        poc=poc,
        vah=hi,
        val=lo,
        hvn=flow.get("hvn", []),
        lvn=flow.get("lvn", []),
        poc_shift=shift,
        value_shift=shift,
        value_overlap=overlap,
        acceptance_above=price > hi,
        acceptance_below=price < lo,
        return_to_value=lo <= price <= hi,
        method="executed volume; contiguous 70% value area",
    )


def factor_context(asset, btc, eth, asof=None):
    def returns(bars):
        closed = [b for b in bars if asof is None or b.end <= asof]
        return {b.end: math.log(b.close / a.close) for a, b in zip(closed, closed[1:])}

    own = returns(asset)
    result = {"available": False, "method": "aligned closed-bar log returns, trailing 60 observations"}
    residuals = []
    for name, bars in (("btc", btc), ("eth", eth)):
        other = returns(bars)
        times = sorted(set(own) & set(other))[-60:]
        if len(times) < 20:
            continue
        x, y = [other[t] for t in times], [own[t] for t in times]

        def beta(xs, ys):
            mx, my = mean(xs), mean(ys)
            variance = sum((v - mx) ** 2 for v in xs)
            return sum((a - mx) * (b - my) for a, b in zip(xs, ys)) / variance if variance > 1e-18 else None

        b = beta(x[:-1], y[:-1])
        if b is None:
            continue
        corr = b * pstdev(x[:-1]) / max(pstdev(y[:-1]), 1e-12)
        expected = b * x[-1]
        residual = y[-1] - expected
        half = len(x) // 2
        b1, b2 = beta(x[:half], y[:half]), beta(x[half:-1], y[half:-1])
        result.update(
            {
                "samples_" + name: len(times) - 1,
                "fit_end_ms_" + name: times[-2],
                "available_ms_" + name: times[-1],
                "factor_return_" + name: x[-1],
                "actual_return_" + name: y[-1],
                "beta_to_" + name: b,
                "correlation_to_" + name: corr,
                "expected_return_" + name: expected,
                "residual_" + name: residual,
                "beta_stability_" + name: abs(b1 - b2) if b1 is not None and b2 is not None else None,
            }
        )
        residuals.append(residual)
    if residuals:
        result.update(
            available=True,
            available_ms=max(result[k] for k in result if k.startswith("available_ms_")),
            residual_return=mean(residuals),
            relative_strength=max(0, mean(residuals)),
            relative_weakness=min(0, mean(residuals)),
        )
    return result


def flow_response(trades, book_features, previous=None):
    if not trades:
        return {"available": False}
    trades = sorted(trades, key=lambda t: (t.event_ms, t.trade_id))
    signed = [float(t.price * t.size) * (1 if t.side == "Buy" else -1) for t in trades]
    buy = sum(max(x, 0) for x in signed)
    sell = -sum(min(x, 0) for x in signed)
    change = float(trades[-1].price - trades[0].price)
    duration = max(1, (trades[-1].event_ms - trades[0].event_ms) / 1000)
    run = best = 1
    side_runs = {"Buy": 0, "Sell": 0}
    side_runs[trades[0].side] = 1
    for a, b in zip(trades, trades[1:]):
        run = run + 1 if a.side == b.side else 1
        best = max(best, run)
        side_runs[b.side] = max(side_runs[b.side], run)
    width = max(1, math.ceil(len(signed) / 4))
    chunks = [sum(signed[i : i + width]) for i in range(0, len(signed), width)]
    delta = buy - sell
    prior = previous or {}
    slope = delta / duration
    return dict(
        available=True,
        aggressive_buy_notional=buy,
        aggressive_sell_notional=sell,
        price_change=change,
        signed_aggressor_notional=delta,
        directional_price_change=change * (1 if delta >= 0 else -1),
        impact_per_signed_notional=change / delta if abs(delta) > 1e-12 else None,
        flow_efficiency=abs(change) / (buy + sell) if buy + sell else None,
        impact_persistence=sum(x * delta > 0 for x in chunks) / len(chunks),
        absorption_ratio=(buy + sell) / max(abs(change) / float(trades[0].price), 1e-9),
        reversal_after_flow=change * delta < 0,
        price_change_per_signed_notional=change / delta if abs(delta) > 1e-12 else None,
        buy_efficiency=max(change, 0) / buy if buy else None,
        sell_efficiency=max(-change, 0) / sell if sell else None,
        delta_persistence=sum(x * delta > 0 for x in chunks) / len(chunks),
        aggressive_notional_persistence=sum(x * delta > 0 for x in chunks) / len(chunks),
        cvd_slope=slope,
        cvd_acceleration=slope - prior["cvd_slope"] if "cvd_slope" in prior else None,
        same_side_run=best,
        same_side_buy_run=side_runs["Buy"],
        same_side_sell_run=side_runs["Sell"],
        trade_intensity=len(trades) / duration,
        high=max(float(t.price) for t in trades),
        low=min(float(t.price) for t in trades),
        absorption_duration=duration,
        replenishment_strength=sum(book_features.get("replenishment_60s", {}).values()),
    )


def derivatives_context(derivatives, bars):
    result = dict(derivatives)
    change = bars[-1].close / bars[-2].close - 1
    oi = derivatives.get("oi_change_pct")
    rate = derivatives.get("funding_rate")
    result.update(
        price_up_oi_up=change > 0 and oi > 0 if oi is not None else None,
        price_up_oi_down=change > 0 and oi < 0 if oi is not None else None,
        price_down_oi_up=change < 0 and oi > 0 if oi is not None else None,
        price_down_oi_down=change < 0 and oi < 0 if oi is not None else None,
        crowding_score=min(1, abs(rate) / 0.001) if rate is not None else None,
        deleveraging_score=min(1, max(0, -oi) / 10) if oi is not None else None,
        basis=(derivatives["mark"] / derivatives["index"] - 1)
        if derivatives.get("mark") and derivatives.get("index")
        else None,
        interpretation="joint observed states; no universal directional meaning",
    )
    return result


def session_baseline(store, signal, flow, book, now, window_end_ms=None):
    key = (
        f"session-baseline-v3:{signal.source}:{signal.symbol}:{signal.horizon_profile}:{signal.entry_session}"
    )
    history = store.get(key, [])
    window_end_ms = window_end_ms if window_end_ms is not None else now // 60000 * 60000
    history = [
        h
        for h in history
        if h["at_ms"] < now and h.get("window_end_ms", h["at_ms"] // 60000 * 60000) < window_end_ms
    ]
    current = dict(
        spread=book.get("spread_bps"),
        depth=sum(book.get("depth", {}).get("10", {}).get(s, 0) for s in ("bid", "ask")),
        trade_intensity=flow.get("trade_intensity"),
        delta=flow.get("delta_pct"),
        turnover=flow.get("buy_notional", 0) + flow.get("sell_notional", 0),
        volume=flow.get("buy_base", 0) + flow.get("sell_base", 0),
        trade_count=flow.get("trades"),
        aggressive_buy_notional=flow.get("buy_notional"),
        aggressive_sell_notional=flow.get("sell_notional"),
        delta_magnitude=abs(flow["delta_pct"]) if flow.get("delta_pct") is not None else None,
        cvd_slope=flow.get("cvd_slope"),
        replenishment=flow.get("replenishment_strength"),
        obi=book.get("obi_10bps"),
    )
    result = {
        "baseline_samples": len(history),
        "normalization": "prior complete execution windows from selected native markets, same symbol, venue, horizon and session",
        "policy": "participation-percentiles-v2",
        "available_ms": now,
        "production_gate": True,
        "window_end_ms": window_end_ms,
        "window_start_ms": min((h.get("window_end_ms", h["at_ms"]) for h in history), default=window_end_ms),
    }
    for name, value in current.items():
        prior = [
            h[name]
            for h in history
            if h.get(name) is not None
            and h["at_ms"] < now
            and h.get("window_end_ms", h["at_ms"] // 60000 * 60000) < window_end_ms
        ]
        result[name] = value
        result[name + "_percentile"] = (
            sum(v <= value for v in prior) / len(prior) if len(prior) >= 20 and value is not None else None
        )
        result[name + "_relative"] = (
            value / median(prior)
            if len(prior) >= 20 and value is not None and abs(median(prior)) > 1e-12
            else None
        )
    if not history or window_end_ms > history[-1].get("window_end_ms", history[-1]["at_ms"] // 60000 * 60000):
        store.put(key, (history + [current | {"at_ms": now, "window_end_ms": window_end_ms}])[-120:])
    return result


def execution_context(signal, bars, book, settings):
    f = candle_features(bars, max(signal.created_ms, bars[-1].end))
    noise = median(c.high - c.low for c in bars[-20:])
    stop = abs(signal.entry - signal.stop)
    vol = [abs(math.log(b.close / a.close)) for a, b in zip(bars, bars[1:])]
    baseline = median(vol[-40:-1]) if len(vol) > 1 else 0
    ratio = vol[-1] / max(baseline, 1e-12)
    regime = (
        "VOLATILITY_SHOCK"
        if ratio >= 5
        else "HIGH_VOL"
        if ratio >= 2
        else "LOW_VOL"
        if ratio < 0.5
        else "NORMAL_VOL"
    )
    impact = book.impact(signal.direction, settings.hypothetical_notional)
    bf = book.features(signal.created_ms)
    depth = bf.get("depth", {}).get("10", {}).get("ask" if signal.direction == "LONG" else "bid", 0)
    return dict(
        stop_distance=stop,
        stop_atr=stop / max(f["atr"], 1e-12),
        short_horizon_noise=noise,
        stop_noise_ratio=stop / max(noise, 1e-12),
        volatility_regime=regime,
        volatility_ratio=ratio,
        spread_cost_bps=bf.get("spread_bps"),
        depth_consumed=settings.hypothetical_notional / depth if depth else None,
        slippage_bps=impact["impact_bps"] if impact else None,
        fill_probability=None,
        time_to_fill=None,
        partial_fill_risk="unknown",
        adverse_selection="elevated" if regime == "VOLATILITY_SHOCK" else "unmeasured",
        method="visible market sweep estimate; passive fill probabilities require calibration",
    )


def select_deep(ranked, core, pinned, capacity, seed, exploration=0.125):
    fixed = list(dict.fromkeys(pinned + core))[:capacity]
    eligible = [r for r in ranked if r.get("eligible") and r["symbol"] not in fixed]
    slots = capacity - len(fixed)
    explore = min(len(eligible), max(1, round(slots * exploration))) if slots and eligible else 0
    exploit = eligible[: max(0, slots - explore)]
    pool = eligible[len(exploit) :]
    sampled = random.Random(seed).sample(pool, min(explore, len(pool)))
    selected = fixed + [r["symbol"] for r in exploit + sampled]
    metadata = {
        s: dict(
            selection_reason="CORE" if s in core else "ACTIVE",
            selection_probability=1.0,
            preliminary_rank=None,
        )
        for s in fixed
    }
    for rank, r in enumerate(ranked):
        s = r["symbol"]
        if s in selected and s not in metadata:
            metadata[s] = dict(
                selection_reason="EXPLORE" if r in sampled else "EXPLOIT",
                selection_probability=len(sampled) / len(pool) if r in sampled else 1.0,
                preliminary_rank=rank + 1,
            )
    return selected, metadata
