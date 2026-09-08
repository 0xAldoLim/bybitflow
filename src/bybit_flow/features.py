"""Causal candle features. Inputs must contain only closed, contiguous bars."""

import math
from statistics import mean

from .models import DAY


def validate_bars(bars, asof):
    if any(
        not all(math.isfinite(v) for v in (c.open, c.high, c.low, c.close, c.volume, c.turnover))
        or c.turnover < 0
        for c in bars
    ):
        raise ValueError("Non-finite or negative candle values")
    if any(
        c.end > asof
        or min(c.open, c.high, c.low, c.close) <= 0
        or c.high < max(c.open, c.close)
        or c.low > min(c.open, c.close)
        or c.volume < 0
        for c in bars
    ):
        raise ValueError("Invalid or unclosed candle")
    if any(b.start != a.end or a.interval != b.interval for a, b in zip(bars, bars[1:])):
        raise ValueError("Candle gap or duplicate")


def pivots(bars, left=3, right=3):
    out = []
    for k in range(left, len(bars) - right):
        window = bars[k - left : k + right + 1]
        for kind, attr, fn in (("high", "high", max), ("low", "low", min)):
            values = [getattr(c, attr) for c in window]
            value = getattr(bars[k], attr)
            if value == fn(values) and values.count(value) == 1:
                out.append(
                    dict(
                        kind=kind,
                        price=value,
                        pivot_ms=bars[k].end,
                        available_ms=bars[k + right].end,
                        index=k,
                    )
                )
    return out


def candle_features(bars, asof):
    validate_bars(bars, asof)
    if len(bars) < 60:
        raise ValueError("Need at least 60 closed candles")
    tr = [max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close)) for p, c in zip(bars, bars[1:])]
    atr = mean(tr[-14:])
    if atr <= 0:
        raise ValueError("Zero ATR")
    closes = [c.close for c in bars]
    ma = mean(closes[-20:])
    slope = (ma - mean(closes[-25:-5])) / (5 * atr)
    path = sum(abs(b - a) for a, b in zip(closes[-21:], closes[-20:]))
    efficiency = abs(closes[-1] - closes[-21]) / path if path else 0
    rv = math.sqrt(mean(math.log(b / a) ** 2 for a, b in zip(closes[-21:], closes[-20:])))
    swings = pivots(bars)
    highs = [s for s in swings if s["kind"] == "high"]
    lows = [s for s in swings if s["kind"] == "low"]
    structure = "uncertain"
    if len(highs) >= 2 and len(lows) >= 2:
        if highs[-1]["price"] > highs[-2]["price"] and lows[-1]["price"] > lows[-2]["price"]:
            structure = "up"
        elif highs[-1]["price"] < highs[-2]["price"] and lows[-1]["price"] < lows[-2]["price"]:
            structure = "down"
        else:
            structure = "range"
    if mean(tr[-5:]) > 2 * mean(tr[-55:-5]) or atr / closes[-1] > 0.08:
        regime = "high-volatility disorder"
    elif structure == "up" and slope > 0.04 and efficiency > 0.25:
        regime = "trending up"
    elif structure == "down" and slope < -0.04 and efficiency > 0.25:
        regime = "trending down"
    elif efficiency < 0.25 and abs(slope) < 0.15:
        regime = "range"
    else:
        regime = "uncertain"
    last, prev = bars[-1], bars[-2]
    # Only levels known BEFORE the triggering candle are usable for a sweep/break.
    known = [s for s in swings if s["available_ms"] <= last.start]
    kh = [s for s in known if s["kind"] == "high"]
    kl = [s for s in known if s["kind"] == "low"]
    high = kh[-1]["price"] if kh else max(c.high for c in bars[-21:-1])
    low = kl[-1]["price"] if kl else min(c.low for c in bars[-21:-1])
    eq = []
    for levels in (kh, kl):
        if len(levels) >= 2 and abs(levels[-1]["price"] - levels[-2]["price"]) <= 0.1 * atr:
            eq.append(dict(kind=levels[-1]["kind"], price=mean([levels[-1]["price"], levels[-2]["price"]])))
    fvg = []
    for i in range(max(2, len(bars) - 20), len(bars)):
        a, b, c = bars[i - 2 : i + 1]
        local_atr = mean(tr[max(0, i - 14) : i])
        if abs(b.close - b.open) < local_atr or b.high - b.low < 1.2 * local_atr:
            continue
        if c.low - a.high > 0.1 * local_atr and not any(x.low <= a.high for x in bars[i + 1 :]):
            fvg.append(dict(side="LONG", low=a.high, high=c.low, available_ms=c.end))
        if a.low - c.high > 0.1 * local_atr and not any(x.high >= a.low for x in bars[i + 1 :]):
            fvg.append(dict(side="SHORT", low=c.high, high=a.low, available_ms=c.end))
    bos = "up" if prev.close <= high < last.close else "down" if prev.close >= low > last.close else None
    blocks = []
    if bos and abs(last.close - last.open) >= atr:
        for c in reversed(bars[-11:-1]):
            if (bos == "up" and c.close < c.open) or (bos == "down" and c.close > c.open):
                blocks.append(
                    dict(
                        side=bos,
                        low=c.low,
                        high=c.high,
                        available_ms=last.end,
                        invalidation=c.low if bos == "up" else c.high,
                    )
                )
                break
    day = (last.end - 1) // DAY * DAY
    week = ((day // DAY + 3) // 7 * 7 - 3) * DAY  # Monday 00:00 UTC
    periods = {}
    for name, start, end in (
        ("session", day, last.end),
        ("prior_day", day - DAY, day),
        ("prior_week", week - 7 * DAY, week),
    ):
        subset = [c for c in bars if c.start >= start and c.end <= end]
        complete = bool(subset) and subset[0].start == start and subset[-1].end == end
        periods[name] = (
            dict(high=max(c.high for c in subset), low=min(c.low for c in subset), complete=complete)
            if subset
            else None
        )
    session = [c for c in bars if c.start >= day]
    vol = sum(c.volume for c in session)
    vwap = sum(c.turnover for c in session) / vol if vol else None
    return dict(
        asof=last.end,
        regime=regime,
        atr=atr,
        ma20=ma,
        slope=slope,
        efficiency=efficiency,
        realized_volatility=rv,
        structure=structure,
        swings=swings[-20:],
        internal=pivots(bars, 2, 2)[-12:],
        high=high,
        low=low,
        equal_levels=eq,
        bos=bos,
        choch=bos if bos and structure in {"up", "down"} and bos != structure else None,
        sweep_long=last.low < low and last.close > low,
        sweep_short=last.high > high and last.close < high,
        potential_sweep_long=last.low < low,
        potential_sweep_short=last.high > high,
        fvg=fvg,
        order_blocks=blocks,
        premium_discount=(last.close - low) / (high - low) if high > low else None,
        periods=periods,
        session_vwap=vwap,
        session_vwap_complete=bool(session) and session[0].start == day,
        volume_expansion=last.volume / mean(c.volume for c in bars[-21:-1])
        if sum(c.volume for c in bars[-21:-1])
        else 0,
    )
