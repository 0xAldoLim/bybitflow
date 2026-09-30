"""Causal anchored VWAP and closed-bar time-at-location research."""

POLICY = "anchored-acceptance-v1"


def avwap(bars, anchor_ms, asof_ms, source, *, anchor_price=None, atr=None, known_ms=None):
    result = dict(
        policy=POLICY,
        state="UNAVAILABLE",
        source=source,
        source_ms=None,
        available_ms=asof_ms,
        anchor_ms=anchor_ms,
        anchor_price=anchor_price,
        avwap=None,
    )
    if anchor_ms is None or anchor_ms > asof_ms or (known_ms is not None and known_ms > asof_ms):
        return result
    closed = [bar for bar in bars if bar.start >= anchor_ms and bar.end <= asof_ms]
    if len(closed) < 2 or sum(bar.volume for bar in closed) <= 0:
        return result
    cumulative_volume = cumulative_turnover = 0.0
    values = []
    for bar in closed:
        cumulative_volume += bar.volume
        cumulative_turnover += bar.turnover
        values.append(cumulative_turnover / cumulative_volume if cumulative_volume else None)
    latest = values[-1]
    previous = values[-2]
    above = sum(bar.close > value for bar, value in zip(closed, values) if value is not None)
    below = sum(bar.close < value for bar, value in zip(closed, values) if value is not None)
    duration = closed[0].interval
    return result | dict(
        state="AVAILABLE",
        source_ms=closed[-1].end,
        anchor_price=anchor_price if anchor_price is not None else closed[0].open,
        avwap=latest,
        distance_to_avwap_atr=(closed[-1].close - latest) / atr if atr and atr > 0 else None,
        avwap_slope=(latest - previous) / atr if atr and atr > 0 else latest - previous,
        bars_above_avwap=above,
        bars_below_avwap=below,
        time_above_avwap_ms=above * duration,
        time_below_avwap_ms=below * duration,
        reclaim_avwap=closed[-2].close <= previous < closed[-1].close,
        failed_reclaim_avwap=closed[-2].close > previous >= closed[-1].close,
        acceptance_ratio=above / len(closed),
        bars=len(closed),
    )


def time_acceptance(bars, level, asof_ms, source, *, band=0.0):
    result = dict(policy=POLICY, state="UNCLEAR", source=source, source_ms=None, available_ms=asof_ms)
    if level is None or level <= 0 or band < 0:
        return result
    closed = [bar for bar in bars if bar.end <= asof_ms][-20:]
    if len(closed) < 5:
        return result
    above = sum(bar.close > level + band for bar in closed)
    below = sum(bar.close < level - band for bar in closed)
    inside = len(closed) - above - below
    crossed_up = closed[-2].high > level + band and closed[-1].close < level - band
    crossed_down = closed[-2].low < level - band and closed[-1].close > level + band
    state = (
        "REJECTED_ABOVE"
        if crossed_up
        else "REJECTED_BELOW"
        if crossed_down
        else "ACCEPTED_ABOVE"
        if above / len(closed) >= 0.7
        else "ACCEPTED_BELOW"
        if below / len(closed) >= 0.7
        else "BALANCED"
    )
    duration = closed[0].interval
    return result | dict(
        state=state,
        source_ms=closed[-1].end,
        level=level,
        bars_above=above,
        bars_below=below,
        bars_inside=inside,
        time_above_ms=above * duration,
        time_below_ms=below * duration,
        time_inside_ms=inside * duration,
        acceptance_ratio=max(above, below) / len(closed),
        rejection_ratio=(1 if state.startswith("REJECTED") else 0) / len(closed),
    )


def assess(bars, asof_ms, source, *, atr=None, anchors=None, levels=None):
    from .models import DAY

    if not bars or bars[-1].end > asof_ms:
        return dict(policy=POLICY, state="UNAVAILABLE", source=source, available_ms=asof_ms)
    day = (bars[-1].end - 1) // DAY * DAY
    week = ((day // DAY + 3) // 7 * 7 - 3) * DAY
    known = {"daily_open": dict(anchor_ms=day), "weekly_open": dict(anchor_ms=week)}
    known.update(anchors or {})
    anchored = {
        name: avwap(
            bars,
            value.get("anchor_ms"),
            asof_ms,
            source,
            atr=atr,
            anchor_price=value.get("anchor_price"),
            known_ms=value.get("known_ms"),
        )
        for name, value in known.items()
    }
    acceptance = {
        name: time_acceptance(bars, value, asof_ms, source, band=(atr or 0) * 0.1)
        for name, value in (levels or {}).items()
    }
    setup = anchored.get("setup_trigger", {})
    if setup.get("avwap") is not None:
        acceptance["setup_avwap"] = time_acceptance(bars, setup["avwap"], asof_ms, source)
    return dict(
        policy=POLICY,
        state="AVAILABLE" if any(row["state"] == "AVAILABLE" for row in anchored.values()) else "UNAVAILABLE",
        source=source,
        source_ms=bars[-1].end,
        available_ms=asof_ms,
        anchors=anchored,
        acceptance=acceptance,
        distance_setup_avwap_atr=setup.get("distance_to_avwap_atr"),
        setup_avwap_slope=setup.get("avwap_slope"),
        setup_avwap_acceptance_ratio=setup.get("acceptance_ratio"),
        time_above_value_ratio=acceptance.get("poc", {}).get("bars_above", 0) / len(bars[-20:])
        if "poc" in acceptance
        else None,
        time_below_value_ratio=acceptance.get("poc", {}).get("bars_below", 0) / len(bars[-20:])
        if "poc" in acceptance
        else None,
    )
