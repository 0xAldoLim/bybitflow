"""Interpret shared causal observations, rather than recomputing market engines.

Versioned deterministic mappings are implementation inferences. Scores are not
probabilities; existing confirmation, execution and V8 gates remain mandatory.
"""

import math

from .scoring import tier, unit
from .v2 import SCORE_PROFILE


def usable(value, source, now, ttl=960_000):
    return bool(
        value
        and value.get("source", source) == source
        and isinstance(value.get("available_ms"), (int, float))
        and 0 <= now - value["available_ms"] <= ttl
        and isinstance(value.get("source_ms", value.get("end_ms")), (int, float))
        and 0 <= now - value.get("source_ms", value.get("end_ms")) <= ttl
    )


def location(signal, now):
    e, sign = signal.evidence, 1 if signal.direction == "LONG" else -1
    r, trigger = e.get("range", {}), e.get("structural_trigger", {})
    reversal = signal.family in {"liquidity_sweep", "range_rejection"}
    lower, upper = r.get("low"), r.get("high")
    position = (
        (signal.entry - lower) / (upper - lower)
        if lower is not None and upper is not None and upper > lower
        else None
    )
    context = e.get("context_features", {})
    structural = bool(trigger.get("valid") and trigger.get("available_ms", now + 1) <= now)
    if reversal:
        geometry = unit(1 - 2 * position if sign > 0 else 2 * position - 1) if position is not None else 0
    elif signal.family == "breakout_retest":
        geometry = float(structural)
    else:
        aligned = context.get("regime") == ("trending up" if sign > 0 else "trending down")
        geometry = unit(context.get("efficiency"), 0.5) * int(aligned)
    profile = e.get("volume_profile", {})
    profile_ready = bool(
        profile.get("available")
        and profile.get("coverage_complete")
        and usable(profile, signal.source, now, 120_000)
    )
    poc, val, vah = (profile.get(k) if profile_ready else None for k in ("poc", "val", "vah"))
    atr = e.get("setup_features", {}).get("atr") or 0
    profile_position = (
        (signal.entry - val) / (vah - val) if val is not None and vah is not None and vah > val else None
    )
    response = e.get("auction", {}).get("state")
    favorable_response = response in (
        {"REJECTION_LOW", "DISCOVERY_UP"} if sign > 0 else {"REJECTION_HIGH", "DISCOVERY_DOWN"}
    )
    profile_fraction = (
        unit(1 - 2 * profile_position if sign > 0 else 2 * profile_position - 1)
        if reversal and profile_position is not None
        else float(favorable_response and profile_ready)
    )
    state = "UNKNOWN"
    if profile_position is not None:
        state = (
            "BELOW_VALUE"
            if profile_position < 0
            else "ABOVE_VALUE"
            if profile_position > 1
            else "VALUE_LOWER_EDGE"
            if profile_position < 0.25
            else "VALUE_UPPER_EDGE"
            if profile_position > 0.75
            else "VALUE_CENTER"
        )
    # A balanced center has low reversal location; no blanket penalty to trend pullbacks.
    return dict(
        score=100 * (0.65 * geometry * int(structural) + 0.35 * profile_fraction),
        state=state,
        range_position=position,
        profile_position=profile_position,
        profile_ready=profile_ready,
        distance_to_poc_atr=(signal.entry - poc) / atr if poc is not None and atr else None,
        structural_reference=trigger.get("level"),
        explanation=f"{state.replace('_', ' ').lower()}; original {signal.family.replace('_', ' ')} reference",
    )


def mechanism(signal, flow_confirmed, now=None):
    e, sign = signal.evidence, 1 if signal.direction == "LONG" else -1
    flow = e.get("flow", {})
    trust = unit(flow.get("quality", {}).get("flow_trust_score"))
    atr = e.get("execution_features", {}).get("atr") or 0
    response = sign * flow.get("price_change", 0) / atr if atr else 0
    directional = min(
        unit(sign * flow.get("delta_pct", 0), 25),
        unit(flow.get("delta_persistence"), 0.75),
        unit(response, 0.15),
    )
    size = e.get("trade_size_state", {})
    rotation = e.get("event_rotation_state", {})
    now = now if now is not None else e.get("flow_quality", {}).get("window_end_ms", signal.created_ms)
    size_support = bool(
        size.get("available")
        and size.get("coverage_complete")
        and usable(size, signal.source, now, 120_000)
        and size.get("large_trade_direction") == ("BUY" if sign > 0 else "SELL")
    )
    rotation_support = bool(
        rotation.get("available")
        and rotation.get("coverage_complete")
        and usable(rotation, signal.source, now, 120_000)
        and rotation.get("direction") == ("UP" if sign > 0 else "DOWN")
    )
    # Unknown evidence cannot earn more credit than known opposing evidence.
    # These describe the same prints: one shared modifier, never additive votes.
    directional *= 1 if size_support and rotation_support else 0.85
    absorption = bool(flow.get("absorption_long" if sign > 0 else "absorption_short"))
    opposing = flow.get("sell_notional" if sign > 0 else "buy_notional", 0)
    defended = flow.get("defended_notional", {}).get(signal.direction, 0)
    absorbed = min(
        unit(defended / opposing if opposing else 0, 0.25), unit(flow.get("delta_persistence"), 0.75)
    ) * int(absorption)
    state = "NEUTRAL"
    basis = "No independently supported directional mechanism"
    if not flow_confirmed or trust < 0.6:
        state, basis = "UNCONFIRMED", "Insufficient trusted confirmation for a mechanism classification"
    elif absorbed > 0 and directional <= absorbed:
        state = "DEFENDED_BID_ABSORPTION" if sign > 0 else "DEFENDED_ASK_ABSORPTION"
        basis = "Opposing aggression met recorded replenishment; existing family response confirmed"
    elif directional > 0:
        state = ("CONVICTION_" if directional >= 0.8 else "MOMENTUM_") + ("BUY" if sign > 0 else "SELL")
        basis = "Directional executed flow moved price with persistent response"
    elif abs(flow.get("delta_pct", 0)) >= 20:
        state = "INEFFICIENT_AGGRESSION"
        basis = "Aggression lacked a supported directional price response"
    fraction = (
        max(directional, absorbed) if signal.family in {"liquidity_sweep", "range_rejection"} else directional
    )
    if e.get("flow_confirmation_mode") == "CROSS_VENUE_SUBSTITUTION":
        remote = e.get("flow_substitution", {})
        observations = remote.get("observations", [])
        fraction = (
            min((unit(sign * r.get("price_displacement_bps", 0), 8) for r in observations), default=0)
            if remote.get("passed")
            else 0
        )
        trust = min((unit(r.get("flow_trust_score")) for r in observations), default=0)
        state, basis = (
            "CROSS_VENUE_RESPONSE",
            "Trusted independent-venue response; native tape features remain separate",
        )
    # Alias descriptions of a price/flow event never accumulate independent credit.
    return dict(
        score=100 * fraction * min(1, trust / 0.6) * int(flow_confirmed),
        state=state,
        trust=trust,
        directional_response_atr=response,
        absorption_fraction=absorbed,
        initiative_fraction=directional,
        explanation=basis,
        credit_policy="maximum supported pathway; no additive delta/CVD/stack votes",
    )


def trade_offer(signal, now):
    e, risk = signal.evidence, signal.risk
    profile = e.get("volume_profile", {})
    ready = bool(
        profile.get("available")
        and profile.get("coverage_complete")
        and usable(profile, signal.source, now, 120_000)
    )
    levels = []
    if ready:
        levels += [("POC", profile.get("poc"))]
        levels += [("HVN", p) for p in profile.get("hvn", [])[:128]]
    anchored = e.get("anchored", {})
    if anchored.get("production_ready") and usable(anchored, signal.source, now):
        levels += [("AVWAP", r.get("avwap")) for r in anchored.get("anchors", {}).values()]
    low, high = sorted((signal.entry, signal.tp1))
    # One reference price is one barrier even if POC/HVN/AVWAP describe it repeatedly.
    barriers = sorted(
        {float(p) for _, p in levels if isinstance(p, (int, float)) and math.isfinite(p) and low < p < high}
    )
    path = 1 / (1 + len(barriers)) if ready else 0
    distance = abs(signal.entry - signal.stop)
    cost = risk.get("cost_per_base")
    cost_fraction = cost / distance if isinstance(cost, (int, float)) and distance else None
    rr = unit(risk.get("net_rr"), 3)
    costs = unit(1 - cost_fraction / 0.5) if cost_fraction is not None else 0
    noise = unit(e.get("execution", {}).get("stop_noise_ratio"))
    score = 100 * (0.35 * rr + 0.20 * costs + 0.20 * noise + 0.25 * path) * int(bool(risk.get("accepted")))
    state = (
        "UNKNOWN_PATH"
        if not ready
        else "OPEN_PATH"
        if not barriers
        else "MODERATE_FRICTION"
        if len(barriers) <= 2
        else "HEAVY_FRICTION"
    )
    return dict(
        score=score,
        path_state=state,
        barriers=barriers[:32],
        barrier_count=len(barriers),
        net_rr=risk.get("net_rr"),
        cost_risk_fraction=cost_fraction,
        profile_ready=ready,
        explanation=f"{state.replace('_', ' ').lower()}; {risk.get('net_rr', 0):.2f}R after estimated costs",
        limitation="Observed reference friction only; no fill or target probability; original plan unchanged",
    )


def liquidation_sequence(row, flow, previous, now):
    """Interpret the existing sampled liquidation stream; never infer missing events."""
    base = dict(
        policy="liquidation-sequence-v1",
        source=row.get("source"),
        source_ms=row.get("source_ms"),
        receipt_ms=row.get("available_ms"),
        available_ms=now,
        coverage_complete=bool(row.get("production_ready")),
        quality="SAMPLED_EVENTS",
        state="UNAVAILABLE",
        fade_confirmed=False,
        continuation_risk=False,
    )
    if (
        not row.get("production_ready")
        or not row.get("trusted_flow")
        or not usable(row, row.get("source"), now, 180_000)
    ):
        return base
    long_n, short_n = row.get("long_liquidation_notional_1m", 0), row.get("short_liquidation_notional_1m", 0)
    heavy = (row.get("liquidation_intensity_percentile") or 0) >= 0.9 or (
        row.get("liquidation_intensity") or 0
    ) >= 2
    prior_valid = bool(
        previous.get("source") == row.get("source")
        and isinstance(previous.get("available_ms"), (int, float))
        and 0 <= now - previous["available_ms"] <= 180_000
        and previous.get("episode_ms")
    )
    current_sign = -1 if long_n > short_n else 1 if short_n > long_n else 0
    # Freeze the initiating forced side; changing sampled dominance is not decay.
    forced_sign = previous.get("forced_sign", 0) if prior_valid else current_sign
    acceleration = row.get("liquidation_acceleration") or 0
    aligned = forced_sign * (flow.get("delta_pct") or 0) > 0
    flow_acceleration = forced_sign * (flow.get("cvd_acceleration") or 0) > 0
    state = "NO_EVENT"
    episode = previous.get("episode_ms") if prior_valid else None
    if heavy:
        episode = episode or now
        state = "LIQUIDATION_ACCELERATION" if acceleration > 0 else "LIQUIDATION_PEAK"
    continuation = bool(episode and acceleration > 0 and aligned and flow_acceleration)
    opposite = (
        forced_sign * (flow.get("delta_pct") or 0) < 0
        and forced_sign * (row.get("liquidation_price_response_bps") or 0) < 0
    )
    # A large sampled event alone never authorizes a fade. It requires an observed
    # earlier shock, decay and opposite trusted price/flow response.
    fade = bool(episode and prior_valid and acceleration <= 0 and opposite)
    if continuation:
        state = "CONTINUATION_RISK"
    elif fade:
        state = "FADE_CONFIRMING"
    elif episode and not heavy:
        state = "AGGRESSIVE_FLOW_DECAY" if not aligned else "POST_LIQUIDATION_VACUUM"
    return base | dict(
        state=state,
        episode_ms=episode,
        forced_sign=forced_sign,
        fade_confirmed=fade,
        continuation_risk=continuation,
        limitation="Existing venue sampled-event/OI readiness; no complete liquidation census",
    )


def combine(location_score, mechanism_score, offer_score, confidence):
    weighted = 0.30 * location_score + 0.45 * mechanism_score + 0.25 * offer_score
    return round(
        min(0.80 * weighted + 0.20 * min(location_score, mechanism_score, offer_score), confidence), 1
    )


def score(signal, flow_confirmed, now):
    where, process, offer = (
        location(signal, now),
        mechanism(signal, flow_confirmed, now),
        trade_offer(signal, now),
    )
    e = signal.evidence
    native_covered = bool(signal.coverage.get("trade_window_complete") and e.get("flow", {}).get("available"))
    structural = bool(
        e.get("structural_trigger", {}).get("valid")
        and e.get("structural_trigger", {}).get("available_ms", now + 1) <= now
    )
    risk_ready = bool(signal.risk.get("accepted"))
    # Required evidence never becomes credible solely because optional observations exist.
    confidence = (
        min(
            100 * process["trust"],
            80
            + 10 * where["profile_ready"]
            + 10 * int(bool(e.get("event_rotation_state", {}).get("available"))),
        )
        if native_covered and structural and risk_ready and flow_confirmed
        else 0
    )
    sequence = e.get("liquidation_sequence", {})
    if (
        sequence.get("continuation_risk")
        and sequence.get("forced_sign") == (-1 if signal.direction == "LONG" else 1)
        and signal.family in {"liquidity_sweep", "range_rejection"}
    ):
        process["score"] = min(process["score"], 40)
        process["explanation"] += "; opposing forced flow is still accelerating"
    signal.quality = combine(where["score"], process["score"], offer["score"], confidence)
    signal.raw_tier = "F" if signal.gates else tier(signal.quality)
    signal.final_tier = "REJECTED" if signal.gates else "RESEARCH"
    e["v2"] = dict(
        score_profile=SCORE_PROFILE,
        formula="v2-score-1",
        source=signal.source,
        source_ms=e.get("flow_quality", {}).get("window_end_ms", now),
        receipt_ms=now,
        available_ms=now,
        coverage_complete=native_covered,
        quality="DETERMINISTIC_UNVALIDATED",
        source_mode="substitution"
        if e.get("flow_confirmation_mode") == "CROSS_VENUE_SUBSTITUTION"
        else "native",
        location=where,
        mechanism=process,
        trade_offer=offer,
        liquidation_sequence=sequence,
        evidence_confidence=confidence,
        quality_score=signal.quality,
        component_note="Shared v1 evidence retained; new versioned interpretation, not a win probability",
    )
    e["score_profile"] = SCORE_PROFILE
    e["score_components"] = {
        k: dict(weight=w, earned=w * value / 100, score=value)
        for k, w, value in (
            ("location", 30, where["score"]),
            ("mechanism", 45, process["score"]),
            ("trade_offer", 25, offer["score"]),
        )
    }
    e["score_reasons"] = [
        "Location: " + where["explanation"],
        "Mechanism: " + process["explanation"],
        "Trade offer: " + offer["explanation"],
        f"Evidence confidence cap: {confidence:.1f}/100",
    ]
    signal.qualification = dict(
        status="Uncalibrated",
        probability=None,
        reason="Versioned deterministic v2 quality; no validated v20 deployment model",
        data_cap="Missing observations add no credit; evidence confidence caps quality",
    )
    return signal
