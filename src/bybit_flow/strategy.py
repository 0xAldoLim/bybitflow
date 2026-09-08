"""Four independent, experimental setup families; same function in scanner and replay."""

import hashlib
from decimal import ROUND_HALF_UP, Decimal

from .features import candle_features
from .models import Signal

FAMILIES = ("liquidity_sweep", "trend_pullback", "range_rejection", "breakout_retest")


def candidates(instrument, h4, h1, m15, asof, families=FAMILIES):
    f4, f1, f15 = [candle_features(b, asof) for b in (h4, h1, m15)]
    if f4["regime"] in {"high-volatility disorder", "uncertain"}:
        return []
    last, prior = h1[-1], h1[-2]
    atr = f1["atr"]
    plans = []
    for direction in ("LONG", "SHORT"):
        long = direction == "LONG"
        supportive = f4["regime"] == ("trending up" if long else "trending down")
        ranging = f4["regime"] == "range" and f1["regime"] == "range"
        level = f1["low"] if long else f1["high"]
        sweep = f1["sweep_long" if long else "sweep_short"]
        rules = {
            "liquidity_sweep": (supportive or ranging) and sweep,
            "trend_pullback": supportive
            and (last.low <= f1["ma20"] < last.close if long else last.high >= f1["ma20"] > last.close)
            and abs(last.close - f1["ma20"]) < 0.7 * atr,
            "range_rejection": ranging
            and (
                last.low <= level + 0.1 * atr and last.close > level + 0.2 * atr
                if long
                else last.high >= level - 0.1 * atr and last.close < level - 0.2 * atr
            ),
            # Prior candle must have closed outside a previously established range.
            "breakout_retest": supportive
            and (
                prior.close > max(c.high for c in h1[-22:-2])
                and last.low <= max(c.high for c in h1[-22:-2]) + 0.1 * atr
                and last.close > max(c.high for c in h1[-22:-2])
                if long
                else prior.close < min(c.low for c in h1[-22:-2])
                and last.high >= min(c.low for c in h1[-22:-2]) - 0.1 * atr
                and last.close < min(c.low for c in h1[-22:-2])
            ),
        }
        for family in families:
            if not rules[family]:
                continue
            sign = 1 if long else -1
            entry = m15[-1].close
            stop = min(last.low, level) - 0.15 * atr if long else max(last.high, level) + 0.15 * atr
            distance = sign * (entry - stop)
            if distance <= 0 or distance > 3 * atr:
                continue
            target = f1["high"] if long else f1["low"]
            if not ranging:
                # Measured R projection, explicitly not a claimed liquidity target.
                target = entry + sign * 3 * distance

            def rounded(p):
                return float(
                    (Decimal(str(p)) / instrument.tick).to_integral_value(rounding=ROUND_HALF_UP)
                    * instrument.tick
                )

            ident = hashlib.sha256(
                f"{instrument.symbol}|{direction}|{family}|{last.end}|rules-0.1.0".encode()
            ).hexdigest()[:20]
            plans.append(
                Signal(
                    id=ident,
                    symbol=instrument.symbol,
                    direction=direction,
                    family=family,
                    created_ms=asof,
                    expires_ms=last.end + 3_600_000,
                    regime=f4["regime"],
                    entry=rounded(entry),
                    zone=(rounded(last.close - 0.3 * atr), rounded(last.close + 0.3 * atr)),
                    stop=rounded(stop),
                    tp1=rounded(target),
                    tp2=rounded(entry + sign * 4 * distance),
                    invalidation=f"Price breaches {rounded(stop)}; data gap or regime loss also invalidates",
                    reason=f"{family}: causal 1H rule in {f4['regime']}; executed-flow confirmation required",
                    evidence={
                        "h4": f4,
                        "h1": f1,
                        "m15": f15,
                        "trigger_bar_end": last.end,
                        "target_method": "opposite range boundary"
                        if ranging
                        else "3R/4R measured projections",
                    },
                )
            )
    return plans


def confirm(signal, flow, m15):
    if not flow.get("available"):
        return False
    long = signal.direction == "LONG"
    if (long and flow["delta_pct"] < -20 and not flow["absorption_long"]) or (
        not long and flow["delta_pct"] > 20 and not flow["absorption_short"]
    ):
        return False
    # Distinct families use distinct execution triggers; none claims validation.
    if signal.family in {"liquidity_sweep", "range_rejection"}:
        return flow["absorption_long" if long else "absorption_short"] and (
            m15[-1].close > m15[-1].open if long else m15[-1].close < m15[-1].open
        )
    return flow["initiative_long" if long else "initiative_short"] and (
        flow["stacked_buy" if long else "stacked_sell"] >= 3
    )
