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
            (20, "E"),
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
    reversal = signal.family in {"liquidity_sweep", "range_rejection"}
    if reversal:
        opposing = flow.get("sell_notional" if sign > 0 else "buy_notional", 0)
        defended = flow.get("defended_notional", {}).get(signal.direction, 0)
        flow_strength = min(
            unit(abs(flow.get("delta_pct", 0)), 50), unit(defended / opposing if opposing else 0, 0.25)
        )
        flow_strength *= int(bool(flow.get("absorption_long" if sign > 0 else "absorption_short")))
    else:
        flow_strength = min(
            unit(abs(flow.get("delta_pct", 0)), 25),
            unit(flow.get("stacked_buy" if sign > 0 else "stacked_sell", 0), 4),
        )
    fractions = {
        "regime": unit(h4.get("efficiency"), 0.5),
        "structure": unit(h1.get("atr", 0) * 2 / distance if distance else 0)
        * int(
            bool(structure)
            or signal.family in {"trend_pullback", "breakout_retest"}
            or (signal.family == "range_rejection" and signal.regime == "range")
        ),
        "orderflow": flow_strength if flow_confirmed else 0,
        "derivatives": unit(1 - max(0, same_funding) / 0.001)
        if derivatives_available and d.get("oi_change_pct") is not None and d.get("funding_rate") is not None
        else 0,
        "execution": unit(signal.risk.get("net_rr"), 2.5) if signal.risk.get("accepted") else 0,
        "fundamentals": len(categories & required) / len(required),
        "cross_market": cross_fraction,
    }
    if signal.horizon_profile != "LEGACY" and e.get("range"):
        r, auction, execution = (e.get(k, {}) for k in ("range", "auction", "execution"))
        # Original categories and weights stay intact; measured evidence refines each category.
        location = (
            unit(abs(r.get("location", 0.5) - 0.5) * 2)
            if reversal
            else unit(r.get("breakout_distance_atr", 0) + 0.5)
        )
        e["location_quality"] = location
        fractions["structure"] *= 0.7 + 0.3 * location
        persistence = unit(flow.get("delta_persistence", 0))
        book = e.get("book", {})
        dom = unit((1 + sign * book.get("obi_persistence", 0)) / 2)
        dom *= unit(book.get("obi_samples", 0), 30)
        short = signal.horizon_profile == "SHORT_INTRADAY"
        fractions["orderflow"] *= (
            (0.65 + 0.2 * persistence + 0.15 * dom) if short else (0.85 + 0.15 * persistence)
        )
        if auction.get("available"):
            aligned = auction["state"] in (
                {"DISCOVERY_UP", "REJECTION_LOW"} if sign > 0 else {"DISCOVERY_DOWN", "REJECTION_HIGH"}
            )
            fractions["structure"] *= 0.85 + 0.15 * int(aligned or auction["state"] == "BALANCE" and reversal)
        fractions["execution"] *= unit(execution.get("stop_noise_ratio", 0), 1)
        factor = e.get("market_factor", {})
        if factor.get("available"):
            fractions["cross_market"] *= 0.75 + 0.25 * int(sign * factor["residual_return"] >= 0)
        if signal.horizon_profile in {"SWING", "EXTENDED_SWING"}:
            fractions["derivatives"] *= unit(1 - max(0, same_funding) * signal.expected_hold_max / 480 / 0.01)
        e["score_reasons"] = [
            f"{k}: {v:.3f} of category weight from observed evidence" for k, v in fractions.items()
        ]
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
    signal.evidence["score_profile"] = (
        "native-evidence-2; family-specific flow; fundamentals means sourced diligence coverage"
    )
    return signal
