"""Corroborated liquidation state from reported public events, never a census."""

from statistics import median

POLICY = "liquidation-state-v1"


def assess(events, asof_ms, source, *, price=None, flow=None, derivatives=None, book=None, baseline=None):
    base = dict(
        policy=POLICY,
        state="UNAVAILABLE",
        source=source,
        source_ms=None,
        available_ms=asof_ms,
        venue_limitation={
            "binance": "sampled forceOrder; approximate last-fill notional, not a census",
            "bybit": "reported allLiquidation bankruptcy-price notional",
            "okx": "unavailable; no validated collector",
        }.get(source, "unknown venue"),
    )
    if events is None or source not in {"binance", "bybit"}:
        return base
    rows = [
        row
        for row in events
        if asof_ms - 900_000 <= row.get("event_ms", 0) <= asof_ms
        and row.get("receipt_ms", asof_ms + 1) <= asof_ms
    ][-1000:]

    def amount(row):
        try:
            return max(0.0, float(row.get("notional", row.get("bankruptcy_notional", 0))))
        except (TypeError, ValueError):
            return 0.0

    def side(row):
        return row.get("liquidated_side", row.get("liquidated_position"))

    totals = {
        (direction, minutes): sum(
            amount(row)
            for row in rows
            if side(row) == direction and row["event_ms"] >= asof_ms - minutes * 60_000
        )
        for direction in ("LONG", "SHORT")
        for minutes in (1, 5, 15)
    }
    recent = [row for row in rows if row["event_ms"] >= asof_ms - 60_000]
    total_1m = sum(amount(row) for row in recent)
    total_5m = totals["LONG", 5] + totals["SHORT", 5]
    prior_1m = sum(amount(row) for row in rows if asof_ms - 120_000 <= row["event_ms"] < asof_ms - 60_000)
    historical = [float(value) for value in (baseline or [])[-120:] if value is not None and value >= 0]
    percentile = (
        sum(value <= total_1m for value in historical) / len(historical) if len(historical) >= 20 else None
    )
    intensity = total_1m / max(1.0, median(historical)) if len(historical) >= 20 else None
    first = recent[0] if recent else None
    flow = flow or {}
    first_price = flow.get("liquidation_reference_price") or (
        float(first.get("price", 0)) if first and source == "binance" else 0
    )
    response = (price / first_price - 1) * 10_000 if price and first_price > 0 else None
    derivatives = derivatives or {}
    book = book or {}
    delta = flow.get("delta_pct")
    oi = derivatives.get("oi_change_pct")
    bid_removal = book.get("depletion_60s", {}).get("bid")
    bid_addition = book.get("replenishment_60s", {}).get("bid")
    depth_change = (
        (bid_addition - bid_removal) / max(1, bid_addition + bid_removal)
        if bid_addition is not None and bid_removal is not None
        else None
    )
    heavy = bool(
        total_1m
        and (percentile is not None and percentile >= 0.9 or intensity is not None and intensity >= 2)
    )
    long_dominant = totals["LONG", 1] >= 2 * max(1, totals["SHORT", 1])
    short_dominant = totals["SHORT", 1] >= 2 * max(1, totals["LONG", 1])
    falling_oi = oi is not None and oi <= -1
    sell_flow = delta is not None and delta <= -15
    buy_flow = delta is not None and delta >= 15
    state = "NORMAL"
    if (
        heavy
        and long_dominant
        and falling_oi
        and flow.get("absorption_long")
        and response is not None
        and response >= 0
        and flow.get("cvd_acceleration", 0) > 0
    ):
        state = "LONG_LIQUIDATION_EXHAUSTION"
    elif (
        heavy
        and short_dominant
        and falling_oi
        and flow.get("absorption_short")
        and response is not None
        and response <= 0
        and flow.get("cvd_acceleration", 0) < 0
    ):
        state = "SHORT_LIQUIDATION_EXHAUSTION"
    elif heavy and long_dominant and falling_oi and sell_flow and response is not None and response <= -15:
        state = (
            "DELEVERAGING_CONTINUATION"
            if depth_change is not None and depth_change < -0.2
            else "LONG_LIQUIDATION_CASCADE"
        )
    elif heavy and short_dominant and falling_oi and buy_flow and response is not None and response >= 15:
        state = "SHORT_LIQUIDATION_CASCADE"
    elif heavy and falling_oi and response is not None and delta is not None and response * delta < 0:
        state = "LIQUIDATION_REVERSAL"
    clusters = 0
    last_event = None
    for row in recent:
        if last_event is None or row["event_ms"] - last_event > 10_000:
            clusters += 1
        last_event = row["event_ms"]
    total_15m = totals["LONG", 15] + totals["SHORT", 15]
    return base | dict(
        state=state,
        source_ms=max((row["event_ms"] for row in rows), default=None),
        long_liquidation_notional_1m=totals["LONG", 1],
        short_liquidation_notional_1m=totals["SHORT", 1],
        long_liquidation_notional_5m=totals["LONG", 5],
        short_liquidation_notional_5m=totals["SHORT", 5],
        long_liquidation_notional_15m=totals["LONG", 15],
        short_liquidation_notional_15m=totals["SHORT", 15],
        liquidation_imbalance=(totals["SHORT", 1] - totals["LONG", 1]) / total_1m if total_1m else 0.0,
        liquidation_intensity=intensity,
        liquidation_intensity_percentile=percentile,
        liquidation_cluster_count=clusters,
        liquidation_acceleration=(total_1m - prior_1m) / max(1, prior_1m),
        liquidation_price_response_bps=response,
        post_liquidation_delta=flow.get("post_liquidation_delta"),
        post_liquidation_cvd_slope=flow.get("cvd_slope"),
        oi_change_during_liquidation=oi if recent else None,
        depth_change_during_liquidation=depth_change if recent else None,
        observed_events=len(rows),
        baseline_samples=len(historical),
        total_5m=total_5m,
        total_15m=total_15m,
    )
