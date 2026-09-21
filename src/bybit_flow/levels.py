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
        for name in ("poc", "vah", "val"):
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
        policy="structural-targets-v2",
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
        stop_policy="existing structural invalidation plus 0.15 setup ATR; unchanged",
    )
