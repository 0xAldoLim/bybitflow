"""Four independent, experimental setup families; same function in scanner and replay."""

import hashlib
from decimal import ROUND_HALF_UP, Decimal

from .features import candle_features
from .models import Signal

FAMILIES = ("liquidity_sweep", "trend_pullback", "range_rejection", "breakout_retest")


def candidates(
    instrument,
    context_bars,
    setup_bars,
    execution_bars,
    asof,
    families=FAMILIES,
    execution_window_ms=900_000,
    structural_targets=False,
    volume_profile=None,
    horizon=None,
):
    context_bars, setup_bars, execution_bars = [
        [b for b in bars if b.end <= asof] for bars in (context_bars, setup_bars, execution_bars)
    ]
    context_features, setup_features, execution_features = [
        candle_features(b, asof) for b in (context_bars, setup_bars, execution_bars)
    ]
    if context_features["regime"] in {"high-volatility disorder", "uncertain"}:
        return []
    last, prior = setup_bars[-1], setup_bars[-2]
    atr = setup_features["atr"]
    plans = []
    for direction in ("LONG", "SHORT"):
        long = direction == "LONG"
        supportive = context_features["regime"] == ("trending up" if long else "trending down")
        ranging = context_features["regime"] == "range" and setup_features["regime"] == "range"
        level = setup_features["low"] if long else setup_features["high"]
        sweep = setup_features["sweep_long" if long else "sweep_short"]
        rules = {
            "liquidity_sweep": (supportive or ranging) and sweep,
            "trend_pullback": supportive
            and (
                last.low <= setup_features["ma20"] < last.close
                if long
                else last.high >= setup_features["ma20"] > last.close
            )
            and abs(last.close - setup_features["ma20"]) < 0.7 * atr,
            "range_rejection": ranging
            and (
                last.low <= level + 0.1 * atr and last.close > level + 0.2 * atr
                if long
                else last.high >= level - 0.1 * atr and last.close < level - 0.2 * atr
            ),
            # Prior candle must have closed outside a previously established range.
            "breakout_retest": supportive
            and (
                prior.close > max(c.high for c in setup_bars[-22:-2])
                and last.low <= max(c.high for c in setup_bars[-22:-2]) + 0.1 * atr
                and last.close > max(c.high for c in setup_bars[-22:-2])
                if long
                else prior.close < min(c.low for c in setup_bars[-22:-2])
                and last.high >= min(c.low for c in setup_bars[-22:-2]) - 0.1 * atr
                and last.close < min(c.low for c in setup_bars[-22:-2])
            ),
        }
        for family in families:
            if not rules[family]:
                continue
            sign = 1 if long else -1
            entry = execution_bars[-1].close
            stop = min(last.low, level) - 0.15 * atr if long else max(last.high, level) + 0.15 * atr
            stop_plan = None
            if horizon in {"SHORT_INTRADAY", "CORE_INTRADAY"}:
                from .production import intraday_stop

                stop_plan = intraday_stop(
                    entry,
                    direction,
                    min(last.low, level) if long else max(last.high, level),
                    setup_bars,
                    execution_bars,
                    atr,
                    horizon,
                    asof,
                )
                stop = stop_plan["stop"]
            if horizon == "SWING":
                from .levels import build_stop_plan

                stop_plan = build_stop_plan(
                    entry,
                    direction,
                    family,
                    setup_bars,
                    execution_bars,
                    atr,
                    instrument.tick,
                    asof,
                    volume_profile,
                )
                stop = stop_plan["stop"]
            distance = sign * (entry - stop)
            if distance <= 0 or (distance > 3 * atr and stop_plan is None):
                continue
            target = setup_features["high"] if long else setup_features["low"]
            if not ranging:
                # Measured R projection, explicitly not a claimed liquidity target.
                target = entry + sign * 3 * distance

            def rounded(p):
                return float(
                    (Decimal(str(p)) / instrument.tick).to_integral_value(rounding=ROUND_HALF_UP)
                    * instrument.tick
                )

            version = (
                "rules-0.2.0"
                if execution_window_ms == 900_000
                else f"rules-0.3.0-flow-{execution_window_ms // 1000}s"
            )
            slot = execution_bars[-1].end if execution_window_ms == 900_000 else asof // 60_000 * 60_000
            ident = hashlib.sha256(
                f"{instrument.symbol}|{direction}|{family}|{last.end}|{slot}|{version}".encode()
            ).hexdigest()[:20]
            plans.append(
                Signal(
                    id=ident,
                    version=version,
                    symbol=instrument.symbol,
                    direction=direction,
                    family=family,
                    created_ms=asof,
                    expires_ms=last.end + 3_600_000,
                    trigger_expires_ms=execution_bars[-1].end + 900_000,
                    holding_deadline_ms=execution_bars[-1].end + 14_400_000,
                    regime=context_features["regime"],
                    entry=rounded(entry),
                    zone=(rounded(last.close - 0.3 * atr), rounded(last.close + 0.3 * atr)),
                    stop=rounded(stop),
                    tp1=rounded(target),
                    tp2=rounded(entry + sign * 4 * distance),
                    invalidation=f"Price breaches {rounded(stop)}; data gap or regime loss also invalidates",
                    reason=f"{family}: causal 1H rule in {context_features['regime']}; executed-flow confirmation required",
                    evidence={
                        "production_policies": dict(
                            confirmation="intraday-confirmation-v2"
                            if horizon in {"SHORT_INTRADAY", "CORE_INTRADAY"}
                            and family in {"liquidity_sweep", "range_rejection"}
                            else "family-executed-flow-v1",
                            market_alignment="market-alignment-v2",
                            scoring="native-evidence-4",
                            levels="structural-levels-v3",
                            profile="executed-profile-v2",
                            stop=(stop_plan or {}).get(
                                "policy", "swing-stop-v2" if horizon == "SWING" else "structural-stop-v1"
                            ),
                        )
                        if horizon
                        else {},
                        "stop_plan": stop_plan,
                        "wick_regime": (stop_plan or {}).get("wick_regime", {}),
                        "volume_profile": volume_profile or {"available": False},
                        "structural_trigger": dict(
                            valid=True,
                            family=family,
                            level=level,
                            extreme=last.low if long else last.high,
                            available_ms=last.end,
                        ),
                        "h4": context_features,
                        "h1": setup_features,
                        "m15": execution_features,
                        "price_tick": str(instrument.tick),
                        "trigger_bar_end": last.end,
                        "execution_window_ms": execution_window_ms,
                        "execution_window_end_ms": slot,
                        "target_method": "opposite range boundary"
                        if ranging
                        else "3R/4R measured projections",
                    },
                )
            )
    for signal in plans:
        if horizon:
            signal.version += ":production-v2:flow-score-v2:lifecycle-v62:cross-venue-flow-substitution-v1"
            signal.id = hashlib.sha256(
                (
                    signal.id + ":production-v2:flow-score-v2:lifecycle-v62:cross-venue-flow-substitution-v1"
                ).encode()
            ).hexdigest()[:24]
        if (signal.evidence.get("stop_plan") or {}).get("reasons"):
            signal.gates = list(signal.evidence["stop_plan"]["reasons"])
            signal.state = "INVALIDATED"
            signal.invalidation = "; ".join(signal.gates)
        if structural_targets:
            from .levels import targets

            selected = targets(
                signal.entry,
                signal.stop,
                signal.direction,
                signal.family,
                setup_bars,
                instrument.tick,
                asof,
                volume_profile,
            )
            signal.tp1, signal.tp2 = selected["tp1"], selected["tp2"]
            signal.evidence["level_policy"] = selected
            signal.evidence["target_method"] = selected["target_method"]
            signal.version += ":structural-levels-v3"
            signal.id = hashlib.sha256((signal.id + ":structural-levels-v3").encode()).hexdigest()[:24]
        signal.source = instrument.exchange
        signal.evidence["source_exchange"] = instrument.exchange
        signal.evidence["exchange_symbol"] = instrument.exchange_symbol or instrument.symbol
        if instrument.exchange != "bybit":
            signal.id = hashlib.sha256((instrument.exchange + ":" + signal.id).encode()).hexdigest()[:24]
    return plans


def confirm(signal, flow, execution_bars):
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
            execution_bars[-1].close > execution_bars[-1].open
            if long
            else execution_bars[-1].close < execution_bars[-1].open
        )
    return flow["initiative_long" if long else "initiative_short"] and (
        flow["stacked_buy" if long else "stacked_sell"] >= 3
    )
