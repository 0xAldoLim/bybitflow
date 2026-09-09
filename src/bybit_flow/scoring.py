WEIGHTS = {
    "regime": 15,
    "structure": 20,
    "orderflow": 25,
    "derivatives": 10,
    "execution": 15,
    "fundamentals": 10,
    "cross_market": 5,
}


def tier(score):
    return next(
        name
        for cutoff, name in (
            (95, "SSS"),
            (90, "SS"),
            (85, "S"),
            (75, "A"),
            (65, "B"),
            (50, "C"),
            (35, "D"),
            (0, "F"),
        )
        if score >= cutoff
    )


def unit(value, scale=1):
    import math

    return (
        max(0.0, min(1.0, value / scale)) if isinstance(value, (float, int)) and math.isfinite(value) else 0.0
    )


def score(signal, flow_confirmed, derivatives_available, fundamental=None, cross=None):
    # Price-event group receives one score; sweep/BOS/wick are never summed as independent votes.
    e = signal.evidence
    h4, h1, flow, d = (e.get(k, {}) for k in ("h4", "h1", "flow", "derivatives"))
    sign = 1 if signal.direction == "LONG" else -1
    structure = h1.get("sweep_long" if sign > 0 else "sweep_short", False) or h1.get("bos") == (
        "up" if sign > 0 else "down"
    )
    distance = abs(signal.entry - signal.stop)
    same_funding = sign * (d.get("funding_rate") or 0)
    # Due-diligence coverage, not token intrinsic value or a directional override.
    facts = fundamental if fundamental is not None else e.get("fundamentals", [])
    categories = {
        f.get("category")
        for f in facts
        if f.get("source")
        and f.get("definition")
        and f.get("known_ms", float("inf")) <= signal.created_ms < f.get("expires_ms", 0)
    }
    required = {"economic_purpose", "value_accrual", "dilution", "security", "governance"}
    contexts = cross if cross is not None else e.get("cross_market", {})
    supportive = "trending up" if sign > 0 else "trending down"
    cross_fraction = sum(v == supportive for k, v in contexts.items() if k != signal.symbol) / max(
        1, len([k for k in contexts if k != signal.symbol])
    )
    fractions = {
        "regime": unit(h4.get("efficiency"), 0.5),
        "structure": unit(h1.get("atr", 0) * 2 / distance if distance else 0)
        * int(bool(structure) or signal.family in {"trend_pullback", "breakout_retest"}),
        "orderflow": min(
            unit(abs(flow.get("delta_pct", 0)), 25),
            unit(flow.get("stacked_buy" if sign > 0 else "stacked_sell", 0), 4),
        )
        if flow_confirmed
        else 0,
        "derivatives": unit(1 - max(0, same_funding) / 0.001)
        if derivatives_available and d.get("oi_change_pct") is not None and d.get("funding_rate") is not None
        else 0,
        "execution": unit(signal.risk.get("net_rr"), 2.5) if signal.risk.get("accepted") else 0,
        "fundamentals": len(categories & required) / len(required),
        "cross_market": cross_fraction,
    }
    signal.quality = round(sum(WEIGHTS[k] * v for k, v in fractions.items()), 1)
    signal.raw_tier = "F" if signal.gates else tier(signal.quality)
    signal.evidence["score_components"] = {
        k: {"weight": WEIGHTS[k], "earned": WEIGHTS[k] * v} for k, v in fractions.items()
    }
    signal.final_tier = "REJECTED" if signal.gates else "RESEARCH"
    signal.qualification = {
        "status": "Uncalibrated",
        "probability": None,
        "reason": "No independently validated deployment model; all high-tier alerts locked",
        "data_cap": "Missing observations earn no points; no weight redistribution",
    }
    signal.evidence["score_profile"] = "native-evidence-1; fundamentals means sourced diligence coverage"
    return signal
