"""Causal family-specific structural targets; original structural stops retained."""

from decimal import ROUND_HALF_UP, Decimal


def targets(entry, stop, direction, family, bars, tick, asof, profile=None):
    if not bars or any(bar.end > asof for bar in bars):
        raise ValueError("Structural targets require closed decision-time bars")
    sign = 1 if direction == "LONG" else -1
    risk = abs(entry - stop)
    candidates = []
    valid_profile = bool(
        profile
        and profile.get("available")
        and profile.get("coverage_complete")
        and profile.get("available_ms", asof + 1) <= asof
        and profile.get("end_ms", asof + 1) <= asof
    )
    if valid_profile:
        for name in (
            "poc",
            "vah",
            "val",
            "excess_high_price",
            "excess_low_price",
            "rejection_high_price",
            "rejection_low_price",
        ):
            if profile.get(name) is not None:
                candidates.append((profile[name], "executed volume " + name.upper(), profile["available_ms"]))
        for name in ("hvn", "lvn"):
            candidates.extend(
                (p, "executed volume " + name.upper(), profile["available_ms"]) for p in profile.get(name, [])
            )
    # A pivot is known only after its right-hand confirmation candle closes.
    for left, center, right in zip(bars[-62:-2], bars[-61:-1], bars[-60:]):
        if sign > 0 and center.high > left.high and center.high >= right.high:
            candidates.append((center.high, "confirmed opposing swing", right.end))
        elif sign < 0 and center.low < left.low and center.low <= right.low:
            candidates.append((center.low, "confirmed opposing swing", right.end))
    if family in {"range_rejection", "liquidity_sweep"}:
        boundary = max(b.high for b in bars[-20:-1]) if sign > 0 else min(b.low for b in bars[-20:-1])
        candidates.append((boundary, "opposing established range", bars[-2].end))
    # The same downstream fee/slippage/funding/RR gate still accepts or rejects.
    usable = sorted((x for x in candidates if sign * (x[0] - entry) >= 2.5 * risk), key=lambda x: sign * x[0])
    first = (
        usable[0]
        if usable
        else (entry + sign * 3 * risk, "3R measured fallback: no suitable causal structure", asof)
    )
    second = next(
        (x for x in usable if sign * (x[0] - first[0]) >= 0.5 * risk),
        (entry + sign * max(4 * risk, sign * (first[0] - entry) + risk), "measured extension fallback", asof),
    )

    def rounded(price):
        return float((Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick)

    return dict(
        policy="structural-levels-v3",
        tp1=rounded(first[0]),
        tp2=rounded(second[0]),
        target_method=first[1],
        tp2_method=second[1],
        available_ms=asof,
        source_timeframe_ms=bars[-1].interval,
        profile_evidence=profile if valid_profile else "unavailable; no profile level invented",
        profile_location={
            name: (
                "ABOVE_VALUE"
                if price > profile["vah"]
                else "BELOW_VALUE"
                if price < profile["val"]
                else "IN_VALUE"
            )
            for name, price in [("entry", entry), ("stop", stop), ("tp1", first[0]), ("tp2", second[0])]
        }
        if valid_profile
        else None,
        candidates=[dict(price=p, kind=k, available_ms=t) for p, k, t in usable],
        stop_policy="supplied immutable stop; targets computed from its risk distance",
    )


def build_stop_plan(
    entry, direction, family, setup_bars, execution_bars, setup_atr, tick, asof, profile=None
):
    """New Swing plans only. Never called on an existing signal."""
    import math
    from decimal import ROUND_CEILING, ROUND_FLOOR

    from .features import pivots

    bars = [b for b in setup_bars if b.end <= asof]
    execution = [b for b in execution_bars if b.end <= asof][-32:]
    if not bars or not execution or setup_atr <= 0:
        raise ValueError("Stop construction requires closed causal bars and positive ATR")
    sign = 1 if direction == "LONG" else -1
    attr = "low" if sign > 0 else "high"
    adverse = min if sign > 0 else max
    last = bars[-1]
    confirmed = [p for p in pivots(bars) if p["kind"] == attr and p["available_ms"] <= asof]
    structural = (
        confirmed[-1]["price"] if confirmed else adverse(getattr(b, attr) for b in bars[-21:-1] or bars)
    )
    # The actual deviation/retest/pullback extreme and its confirmed support define failure.
    anchor = adverse(getattr(last, attr), structural)
    kind = {
        "liquidity_sweep": "swept extreme and confirmed structure",
        "range_rejection": "range deviation extreme",
        "trend_pullback": "pullback and confirmed structural swing",
        "breakout_retest": "failed retest and confirmed structure",
    }[family]
    reference = reference_type = reference_ms = None
    valid = bool(
        profile
        and profile.get("available")
        and profile.get("coverage_complete")
        and profile.get("available_ms", asof + 1) <= asof
        and profile.get("end_ms", asof + 1) <= asof
        and profile.get("end_ms", 0) >= last.start
    )
    if valid:
        for name in (
            ("excess_low_price", "rejection_low_price")
            if sign > 0
            else ("excess_high_price", "rejection_high_price")
        ):
            price = profile.get(name)
            # Same-thesis context: adverse extreme close to the structural anchor, not an unrelated distant auction.
            if price is not None and 0 < sign * (anchor - price) <= 0.75 * setup_atr:
                anchor, reference, reference_type, reference_ms = price, price, name, profile["available_ms"]
    ranges = sorted(b.high - b.low for b in execution)
    noise = ranges[max(0, math.ceil(0.8 * len(ranges)) - 1)]
    buffer = min(0.75 * setup_atr, max(0.25 * setup_atr, 0.75 * noise))
    raw = anchor - sign * buffer
    stop = float(
        (Decimal(str(raw)) / tick).to_integral_value(rounding=ROUND_FLOOR if sign > 0 else ROUND_CEILING)
        * tick
    )
    distance = sign * (entry - stop)
    reasons = ["SWING_STRUCTURAL_STOP_TOO_WIDE"] if distance > 3 * setup_atr else []
    if distance <= 0 or stop <= 0:
        reasons.append("SWING_STRUCTURAL_STOP_INVALID")
    return dict(
        policy="swing-stop-v2",
        stop=stop,
        anchor=anchor,
        anchor_type=kind,
        stop_anchor_price=anchor,
        stop_anchor_type=kind,
        stop_anchor_available_ms=last.end,
        buffer=buffer,
        stop_buffer=buffer,
        buffer_method="max(0.25 setup ATR,0.75 execution q80), capped at 0.75 setup ATR; outward tick rounding",
        execution_noise_q80=noise,
        setup_atr=setup_atr,
        stop_distance_atr=distance / setup_atr,
        stop_distance_bps=distance / entry * 10000,
        stop_noise_ratio=distance / max(noise, float(tick)),
        profile_reference=reference,
        profile_reference_type=reference_type,
        profile_stop_reference=reference,
        profile_stop_reference_type=reference_type,
        profile_stop_available_ms=reference_ms,
        available_ms=asof,
        window_start_ms=execution[0].start,
        window_end_ms=execution[-1].end,
        sample_count=len(execution),
        reasons=reasons,
    )
