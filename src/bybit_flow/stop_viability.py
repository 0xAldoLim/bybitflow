"""Assess the supplied structural stop. Never move a stop or change sizing."""

from .setup_common import clamp, envelope, number, positive


def assess(signal, price, now, minimum_rr=1.5, cost=None):
    e = signal.evidence
    sign = 1 if signal.direction == "LONG" else -1
    distance = sign * (price - signal.stop) if number(price) else None
    setup_atr = e.get("setup_features", {}).get("atr")
    execution_atr = e.get("execution_features", {}).get("atr")
    execution = e.get("execution") or {}
    plan = e.get("stop_plan") or {}
    if plan.get("available_ms", signal.created_ms) > now:
        plan = {}
    wick = e.get("wick_regime") or plan.get("wick_regime") or {}
    if wick.get("available_ms", signal.created_ms) > now:
        wick = {}
    if e.get("execution_features", {}).get("asof", signal.created_ms) > now:
        execution, execution_atr = {}, None
    if e.get("setup_features", {}).get("asof", signal.created_ms) > now:
        setup_atr = None
    # Existing noise estimate and historical adverse wicks measure different risks.
    noise_values = [
        execution.get("short_horizon_noise"),
        plan.get("execution_noise_q80"),
        wick.get("adverse_wick_q80"),
    ]
    noise = max((v for v in noise_values if positive(v)), default=None)
    ratio = distance / noise if number(distance) and positive(noise) else None
    spread = e.get("book", {}).get("spread_bps")
    impact = (
        (e.get("entry_quality") or {})
        .get("current_execution_cost_estimate", {})
        .get("residual_entry_impact_per_base")
    )
    cost = cost if number(cost) else signal.risk.get("cost_per_base")
    max_atr = {"SHORT_INTRADAY": 1.5, "CORE_INTRADAY": 2.25, "SWING": 3.0}.get(signal.horizon_profile)
    limit = max_atr * setup_atr if max_atr and positive(setup_atr) else None
    reward = sign * (signal.tp1 - price) if number(price) else None
    required_noise = noise
    rr_if_noise_safe = (
        (reward - cost) / (required_noise + cost)
        if positive(required_noise) and number(cost) and number(reward)
        else None
    )
    too_wide = bool(
        any("STOP_TOO_WIDE" in r for r in plan.get("reasons", []))
        or positive(distance)
        and positive(limit)
        and distance > limit
    )
    invalid = not positive(distance) or not positive(reward) or not positive(signal.stop)
    incompatible_noise = (
        ratio is not None
        and ratio < 1
        and (
            rr_if_noise_safe is not None
            and rr_if_noise_safe < minimum_rr
            or positive(limit)
            and noise > limit
        )
    )
    state = (
        "UNTRADEABLE_GEOMETRY"
        if invalid or incompatible_noise
        else "STRUCTURALLY_TOO_WIDE"
        if too_wide
        else "UNKNOWN"
        if ratio is None
        else "NOISE_CONFLICT"
        if ratio < 1
        else "TIGHT_BUT_VALID"
        if ratio < 1.5
        else "HEALTHY_STOP"
    )
    # Smoothly reward economic room; no discontinuous historical loss-fitted cutoff.
    score = 100 * ratio / (ratio + 0.75) if ratio is not None and ratio > 0 else None
    if state in {"UNTRADEABLE_GEOMETRY", "STRUCTURALLY_TOO_WIDE"}:
        score = 0.0
    elif state == "NOISE_CONFLICT":
        score = min(score, 35.0)
    return envelope(
        signal,
        "stop-viability-v1",
        now,
        state=state,
        coverage_complete=ratio is not None,
        score=clamp(score) if score is not None else None,
        stop=signal.stop,
        structural_anchor=plan.get("anchor", e.get("structural_trigger", {}).get("extreme")),
        stop_distance=distance,
        stop_noise_ratio=ratio,
        stop_execution_atr=distance / execution_atr if number(distance) and positive(execution_atr) else None,
        stop_setup_atr=distance / setup_atr if number(distance) and positive(setup_atr) else None,
        **{
            f"stop_wick_q{q}": distance / wick[f"adverse_wick_q{q}"]
            if number(distance) and positive(wick.get(f"adverse_wick_q{q}"))
            else None
            for q in (75, 80, 90)
        },
        ordinary_noise=noise,
        maximum_structural_distance=limit,
        noise_safe_net_rr=rr_if_noise_safe,
        spread_bps=spread,
        residual_impact=impact,
        cost_risk_fraction=cost / distance if number(cost) and positive(distance) else None,
        reasons=["STOP_GEOMETRY_UNTRADEABLE"]
        if invalid or incompatible_noise
        else ["STRUCTURAL_STOP_TOO_WIDE"]
        if too_wide
        else [],
        assumption="Noise-conflicted stops earn less credit; infeasible noise-safe geometry is rejected, never widened",
    )
