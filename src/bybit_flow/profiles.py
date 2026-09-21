"""Decision-time executed-volume profiles. Missing coverage is never invented."""

from statistics import median

from .orderflow import footprint


def build(tape, tick, atr, start, end, available_ms, source, symbol, previous=None):
    result = dict(
        policy="executed-profile-v2",
        available=False,
        source=source,
        symbol=symbol,
        start_ms=start,
        window_start_ms=start,
        window_end_ms=end,
        end_ms=end,
        available_ms=available_ms,
        tick_size=str(tick),
        volume_units="base",
        coverage_complete=False,
        value_area_fraction=0.7,
        method="contiguous executed-volume value area",
    )
    if tape is None or end > available_ms or start >= end:
        return result | dict(reason="invalid bounds or tape unavailable")
    complete = tape.coverage_start <= start and tape.last_event >= end - 10000
    if not complete:
        return result | dict(reason="insufficient continuous executed-trade coverage")
    trades = [t for t in tape.window(start, end) if t.receipt_ms <= available_ms]
    if len(trades) > 120000:
        return result | dict(reason="profile exceeds bounded worker capacity", trades=len(trades))
    if not trades or trades[-1].event_ms < end - 10000 or not complete:
        return result | dict(reason="insufficient continuous executed-trade coverage", trades=len(trades))
    profile = footprint(trades, tick, atr)
    bins = profile["profile"]
    volumes = [row["buy"] + row["sell"] for row in bins]
    typical = median(volumes)
    close = float(trades[-1].price)
    context = dict(
        close=close,
        excess_low=len(bins) >= 3 and volumes[0] < 0.25 * typical,
        excess_high=len(bins) >= 3 and volumes[-1] < 0.25 * typical,
        rejection_high=float(max(t.price for t in trades)) > profile["vah"] and close < profile["vah"],
        rejection_low=float(min(t.price for t in trades)) < profile["val"] and close > profile["val"],
        acceptance="ABOVE_VALUE"
        if close > profile["vah"]
        else "BELOW_VALUE"
        if close < profile["val"]
        else "IN_VALUE",
        interpretation="descriptive executed auction; acceptance/excess hypotheses are not validated profitability labels",
        migration_available=False,
        poc_shift=None,
        vah_shift=None,
        val_shift=None,
        value_overlap=None,
        value_migration_direction="UNAVAILABLE",
    )
    if (
        previous
        and previous.get("available")
        and previous.get("coverage_complete")
        and previous.get("source") == source
        and previous.get("symbol") == symbol
        and previous.get("end_ms", end) >= start
        and previous["end_ms"] < end
        and previous.get("available_ms", available_ms + 1) <= available_ms
    ):
        overlap = max(0, min(profile["vah"], previous["vah"]) - max(profile["val"], previous["val"]))
        context.update(
            migration_available=True,
            poc_shift=profile["poc"] - previous["poc"],
            vah_shift=profile["vah"] - previous["vah"],
            val_shift=profile["val"] - previous["val"],
            value_overlap=overlap
            / max(profile["vah"] - profile["val"], previous["vah"] - previous["val"], float(tick)),
            prior_window=dict(
                start_ms=previous["start_ms"],
                end_ms=previous["end_ms"],
                available_ms=previous["available_ms"],
            ),
            reference_note="successive rolling windows may overlap; no independent-sample claim",
        )
    for side in ("low", "high"):
        price = float((min if side == "low" else max)(t.price for t in trades))
        for kind in ("excess", "rejection"):
            context[kind + "_" + side + "_price"] = price if context[kind + "_" + side] else None
    if context["migration_available"]:
        shifts = [context[k] for k in ("poc_shift", "vah_shift", "val_shift")]
        context["value_migration_direction"] = (
            "UP"
            if all(x >= 0 for x in shifts) and any(x > 0 for x in shifts)
            else "DOWN"
            if all(x <= 0 for x in shifts) and any(x < 0 for x in shifts)
            else "FLAT"
        )
    # Compact levels only; the immutable recording holds the full executions.
    return (
        result
        | context
        | {k: profile[k] for k in ("poc", "vah", "val", "hvn", "lvn", "bucket", "trades")}
        | dict(
            sample_count=len(trades),
            available=True,
            coverage_complete=True,
            price_min=min(float(t.price) for t in trades),
            price_max=max(float(t.price) for t in trades),
            bin_rule="floor(price / bucket) * bucket; bucket=max(tick,floor(ATR/100/tick)*tick)",
            last_event_ms=trades[-1].event_ms,
            last_receipt_ms=max(t.receipt_ms for t in trades),
        )
    )
