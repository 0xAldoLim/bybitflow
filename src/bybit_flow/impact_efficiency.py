"""Executed aggression and resulting price response are one mechanism, not votes."""

from .setup_common import causal, clamp, envelope, number, positive

POLICY = "impact-efficiency-v1"
REVERSALS = {"liquidity_sweep", "range_rejection"}


def assess(signal, now, ofi=None, history=()):
    e = signal.evidence
    flow = e.get("flow") or {}
    quality = e.get("flow_quality") or flow.get("quality") or {}
    sign = 1 if signal.direction == "LONG" else -1
    result = envelope(
        signal,
        POLICY,
        now,
        state="UNKNOWN",
        score=None,
        mechanism_score=None,
        coverage_complete=False,
        contradicted=False,
        pathway="UNKNOWN",
    )
    covered = bool(
        signal.coverage.get("trade_window_complete")
        and flow.get("available")
        and causal(quality, signal, now, 120000)
    )
    trust = quality.get("flow_trust_score")
    if not covered or not number(trust) or trust < 0.6:
        return result | dict(
            state="UNTRUSTED" if number(trust) and trust < 0.6 else "UNKNOWN", flow_trust=trust
        )
    if not number(flow.get("delta_pct")):
        return result | dict(reason="Directional aggression unavailable", flow_trust=trust)
    delta = sign * flow["delta_pct"]
    notional = quality.get("net_signed_notional", flow.get("delta_notional"))
    signed_notional = sign * notional if number(notional) else None
    displacement = quality.get("price_displacement_bps")
    change = flow.get("price_change")
    atr = e.get("execution_features", {}).get("atr")
    price = flow.get("vwap", signal.entry)
    if not number(displacement) and number(change) and positive(price):
        displacement = 10000 * change / price
    signed_bps = sign * displacement if number(displacement) else None
    response_atr = (
        signed_bps * price / 10000 / atr
        if number(signed_bps) and positive(price) and positive(atr)
        else sign * change / atr
        if number(change) and positive(atr)
        else None
    )
    if response_atr is None:
        return result | dict(flow_trust=trust)
    persistence = quality.get("impact_persistence", flow.get("impact_persistence"))
    if not number(persistence):
        return result | dict(reason="Price-response persistence unavailable", flow_trust=trust)
    persistence = clamp(persistence, 0, 1)
    book = e.get("book") or {}
    side = "ask" if sign > 0 else "bid"
    depth = book.get("depth", {}).get("10", {}).get(side)
    replenishment = book.get("replenishment_60s", {}).get(side)
    depletion = book.get("depletion_60s", {}).get(side)
    replenishment_ratio = replenishment / depletion if number(replenishment) and positive(depletion) else None
    depth_ratio = abs(signed_notional) / depth if positive(depth) and number(signed_notional) else None
    efficiency = max(0, response_atr) / depth_ratio if positive(depth_ratio) else None
    ofi = ofi or e.get("event_ofi") or {}
    ready_ofi = ofi.get("state") == "READY" and causal(ofi, signal, now, 5000)
    touch = ofi.get("windows", {}).get("60", {}).get("touch", {})
    ofi_directional = touch.get("directional") if ready_ofi else None
    ofi_state = (
        "UNKNOWN"
        if not number(ofi_directional)
        else "SUPPORTIVE"
        if ofi_directional > 0
        else "OPPOSING"
        if ofi_directional < 0
        else "NEUTRAL"
    )
    # A smooth ATR response curve replaces the old saturated conviction score.
    response_score = 100 * max(0, response_atr) / (max(0, response_atr) + 0.15)
    directional = 0.70 * response_score + 0.30 * 100 * persistence
    directional = 0.75 * directional + 0.25 * min(response_score, 100 * persistence)
    directional *= min(1, max(0, delta) / 20) * trust
    if ofi_state == "OPPOSING":
        directional *= 0.75
    if replenishment_ratio is not None and replenishment_ratio > 1:
        directional *= max(0.6, 1 / (1 + 0.2 * (replenishment_ratio - 1)))
    defended = flow.get("defended_notional", {}).get(signal.direction)
    absorbed_volume = flow.get("sell_notional" if sign > 0 else "buy_notional")
    structural_response = bool(
        e.get("v8_structure_response", {}).get("supportive")
        or e.get("confirmation", {}).get("reclaim_hold_support")
        and e.get("confirmation", {}).get("structure_support")
    )
    absorption = bool(
        signal.family in REVERSALS
        and flow.get("absorption_long" if sign > 0 else "absorption_short")
        and structural_response
        and positive(defended)
        and positive(absorbed_volume)
        and response_atr >= 0
    )
    absorbed_score = (
        (
            100
            * min(1, defended / absorbed_volume / 0.25)
            * trust
            * (0.65 + 0.35 * min(1, max(0, response_atr) / 0.15))
        )
        if absorption
        else 0
    )
    repeated = [
        r
        for r in history[-3:]
        if causal(r, signal, now, 180000)
        and r.get("window_end_ms", 0) >= e.get("structural_trigger", {}).get("available_ms", 0)
    ]
    repeated = list({r.get("window_end_ms"): r for r in repeated if number(r.get("window_end_ms"))}.values())
    failed = sum(
        r.get("directional_aggression", 0) >= 20
        and (r.get("displacement_atr") or 0) <= 0.02
        and (r.get("opposing_replenishment_ratio") or 0) >= 1
        for r in repeated
    )
    materially_adverse = delta >= 10 and response_atr <= -0.05
    no_progress = delta >= 20 and failed >= 2 and response_atr <= 0.02 and (replenishment_ratio or 0) >= 1
    contradicted = bool((materially_adverse or no_progress) and not absorption)
    best = max(directional, absorbed_score)
    state = (
        "MECHANISM_CONTRADICTED"
        if contradicted
        else "DEFENDED_ABSORPTION_RESPONSE"
        if absorbed_score > directional
        else "STRONG_AGGRESSION_WEAK_RESPONSE"
        if delta >= 20 and response_atr <= 0.05
        else "EFFICIENT_DIRECTIONAL_IMPACT"
        if delta > 0 and response_atr > 0.15 and persistence >= 0.75
        else "DIRECTIONAL_RESPONSE"
        if response_atr > 0
        else "NEUTRAL_RESPONSE"
    )
    return result | dict(
        state=state,
        score=0.0 if contradicted else round(clamp(best), 3),
        mechanism_score=0.0 if contradicted else round(clamp(best), 3),
        coverage_complete=True,
        contradicted=contradicted,
        pathway="ABSORPTION" if absorbed_score > directional else "DIRECTIONAL",
        directional_score=directional,
        absorption_score=absorbed_score,
        directional_aggression=delta,
        directional_notional=signed_notional,
        signed_price_displacement=signed_bps,
        displacement_bps=signed_bps,
        displacement_atr=response_atr,
        response_efficiency=efficiency,
        impact_persistence=persistence,
        flow_trust=trust,
        ofi_state=ofi_state,
        ofi_directional=ofi_directional,
        depth_consumed=depth_ratio,
        opposing_replenishment=replenishment,
        opposing_replenishment_ratio=replenishment_ratio,
        spread_bps=book.get("spread_bps"),
        window_end_ms=quality.get("window_end_ms"),
        repeated_failed_windows=failed,
        assumption="One aggression concept; maximum valid directional/absorption path; no additive delta/CVD/stack votes",
    )
