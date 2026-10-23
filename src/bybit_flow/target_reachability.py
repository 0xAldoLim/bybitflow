"""Causal movement capacity and execution targets under fixed v2.2 assumptions."""

from copy import deepcopy
from decimal import ROUND_HALF_UP, Decimal
from math import ceil
from statistics import mean

from .quality_v21 import path_quality
from .risk import execution_cost
from .setup_common import causal, clamp, envelope, number, positive, quantile

POLICY = "target-reachability-v1"
MAX_BARS = 512
MIN_SAMPLES = 20
MAX_TARGETS = 24


def movement_capacity(signal, bars, now, horizon_minutes=None):
    """MFE of entirely completed prior windows, scaled by each start's own ATR.

    Overlapping windows are descriptive observations, not independent trades.
    A 14-bar true-range mean must be known at every pseudo-start. Gaps invalidate
    the affected window. Never use the new setup's subsequent path.
    """
    closed = sorted((b for b in bars if b.end <= now), key=lambda b: b.start)[-MAX_BARS:]
    remaining = horizon_minutes
    if remaining is None:
        deadline = signal.holding_deadline_ms or signal.created_ms + signal.expected_hold_max * 60000
        remaining = max(0, (deadline - now) / 60000)
    base = envelope(
        signal,
        "horizon-movement-capacity-v1",
        now,
        state="UNKNOWN",
        samples=0,
        coverage_complete=False,
        horizon_minutes=remaining,
        minimum_samples=MIN_SAMPLES,
        q50_atr=None,
        q75_atr=None,
        q90_atr=None,
        source_ms=closed[-1].end if closed else None,
        limitation="Overlapping historical MFE observations; not target-hit probabilities or account fills",
    )
    features = signal.evidence.get("setup_features", {})
    atr_now = features.get("atr")
    if (
        not closed
        or not positive(atr_now)
        or not positive(remaining)
        or features.get("asof", signal.created_ms) > now
    ):
        return base
    interval = closed[-1].interval
    fractional_steps = remaining * 60000 / interval
    lower_steps, steps = int(fractional_steps), ceil(fractional_steps)
    fraction = fractional_steps - lower_steps
    base.update(
        interval_ms=interval,
        measured_horizon_minutes=remaining,
        horizon_interpolation="Linear between adjacent fully historical bar-window MFEs; not an intrabar path reconstruction",
    )
    if not steps or steps + 15 > len(closed):
        return base | dict(reason="Insufficient completed comparable horizon history")
    sign = 1 if signal.direction == "LONG" else -1
    samples = []
    for i in range(14, len(closed) - steps):
        window = closed[i - 14 : i + steps + 1]
        if any(b.interval != interval or b.start != a.end for a, b in zip(window, window[1:])):
            continue
        if not all(positive(v) for b in window for v in (b.open, b.high, b.low, b.close)):
            continue
        history = closed[i - 14 : i + 1]
        atr = mean(
            max(b.high - b.low, abs(b.high - a.close), abs(b.low - a.close))
            for a, b in zip(history, history[1:])
        )
        if not positive(atr):
            continue
        forward = closed[i + 1 : i + steps + 1]

        def excursion(rows):
            if not rows:
                return 0.0
            extreme = max(b.high for b in rows) if sign > 0 else min(b.low for b in rows)
            return max(0, sign * (extreme - closed[i].close)) / atr

        upper = excursion(forward)
        lower = excursion(forward[:lower_steps])
        samples.append(lower + fraction * (upper - lower) if fraction else upper)
    base["samples"] = len(samples)
    if len(samples) < MIN_SAMPLES:
        return base | dict(reason="Insufficient causal pseudo-start samples")
    return base | dict(
        state="READY",
        coverage_complete=True,
        current_setup_atr=atr_now,
        **{f"q{q}_atr": quantile(samples, q / 100) for q in (50, 75, 90)},
        reference_end_ms=closed[-1].end,
        lookback_bars=len(closed),
    )


def reachability(signal, target, price, capacity, now):
    sign = 1 if signal.direction == "LONG" else -1
    distance = sign * (target - price) if number(price) and number(target) else None
    atr = signal.evidence.get("setup_features", {}).get("atr")
    execution_atr = signal.evidence.get("execution_features", {}).get("atr")
    noise = signal.evidence.get("execution", {}).get("short_horizon_noise")
    mature = causal(capacity, signal, now) and capacity.get("state") == "READY" and positive(atr)
    distance_atr = distance / atr if number(distance) and positive(atr) else None
    q75, q90 = capacity.get("q75_atr"), capacity.get("q90_atr")
    state = "UNKNOWN"
    if mature and number(q75) and number(q90) and number(distance_atr):
        state = (
            "REACHABLE" if distance_atr <= q75 else "STRETCH" if distance_atr <= q90 else "LOW_REACHABILITY"
        )
    return envelope(
        signal,
        POLICY,
        now,
        state=state,
        coverage_complete=bool(mature),
        target=target,
        executable_price=price,
        distance=distance,
        target_setup_atr=distance_atr,
        target_execution_atr=distance / execution_atr
        if number(distance) and positive(execution_atr)
        else None,
        target_noise=distance / noise if number(distance) and positive(noise) else None,
        capacity=capacity,
        score={"REACHABLE": 90.0, "STRETCH": 40.0, "LOW_REACHABILITY": 10.0}.get(state),
    )


def path(signal, target, price, now):
    """Preserve volume-weighted path density; merge barrier aliases by tick."""
    e = signal.evidence
    profile = e.get("volume_profile") or {}
    ready = bool(
        profile.get("available") and profile.get("coverage_complete") and causal(profile, signal, now, 120000)
    )
    tick = float(e.get("price_tick") or 1e-8)
    low, high = sorted((price, target))
    levels = []
    if ready:
        levels += [profile.get(k) for k in ("poc", "vah", "val")]
        levels += list(profile.get("hvn", []))[:128]
    anchored = e.get("anchored") or {}
    if anchored.get("production_ready") and causal(anchored, signal, now):
        levels += [r.get("avwap") for r in anchored.get("anchors", {}).values()]
    frozen = e.get("frozen_structure") or {}
    if frozen.get("available_ms", now + 1) <= now:
        levels += [frozen.get(k) for k in ("frozen_high", "frozen_low", "frozen_reference")]
    barriers = sorted({round(p / tick) * tick for p in levels if number(p) and low < p < high})
    copy = deepcopy(signal)
    copy.entry, copy.tp1 = price, target
    weighted = path_quality(copy, profile if ready else {}, barriers)
    weighted.pop("policy", None)
    difficulty = weighted.get("path_difficulty_score")
    state = (
        "UNKNOWN"
        if difficulty is None
        else "CLEAR"
        if difficulty == 0
        else "MODERATE_FRICTION"
        if difficulty <= 50
        else "HEAVY_FRICTION"
    )
    strongest = max(
        weighted.get("barrier_strength", []), key=lambda r: r.get("volume_fraction") or 0, default={}
    )
    return envelope(
        signal,
        "volume-weighted-path-v22",
        now,
        **weighted,
        state=state,
        barriers=barriers[:32],
        barrier_count=len(barriers),
        coverage_complete=ready,
        strongest_barrier=strongest,
        strongest_barrier_distance_r=abs(strongest["price"] - price) / abs(signal.entry - signal.stop)
        if strongest and signal.entry != signal.stop
        else None,
        score=clamp(100 - difficulty) if number(difficulty) else None,
    )


def candidate_targets(signal, capacity, now):
    """A bounded causal set; unknown capacity never invents a target."""
    e = signal.evidence
    raw = [(e.get("structural_tp1", signal.tp1), "STRUCTURAL_TP1")]
    policy = e.get("level_policy") or {}
    if policy.get("available_ms", now + 1) <= now:
        raw += [
            (r.get("price"), r.get("kind", "FROZEN_STRUCTURE"))
            for r in policy.get("candidates", [])[:64]
            if r.get("available_ms", now + 1) <= now
        ]
    frozen = e.get("frozen_structure") or {}
    if frozen.get("available_ms", now + 1) <= now:
        raw += [(frozen.get(k), k.upper()) for k in ("frozen_high", "frozen_low")]
    features = e.get("setup_features") or {}
    if features.get("asof", now + 1) <= now:
        raw += [
            (r.get("price"), "CONFIRMED_SWING")
            for r in features.get("swings", [])[-32:]
            if r.get("available_ms", now + 1) <= now
        ]
    profile = e.get("volume_profile") or {}
    if profile.get("available") and profile.get("coverage_complete") and causal(profile, signal, now, 120000):
        raw += [(profile.get(k), k.upper()) for k in ("poc", "vah", "val")]
        raw += [(v, "HVN") for v in profile.get("hvn", [])[:32]]
    if capacity.get("state") == "READY" and causal(capacity, signal, now):
        sign = 1 if signal.direction == "LONG" else -1
        atr = capacity["current_setup_atr"]
        raw += [(signal.entry + sign * capacity[f"q{q}_atr"] * atr, f"CAPACITY_Q{q}") for q in (75, 90)]
    tick = Decimal(str(e.get("price_tick") or "0.00000001"))
    unique = {}
    reserved = []
    sign = 1 if signal.direction == "LONG" else -1
    for value, kind in raw:
        if not positive(value):
            continue
        value = float((Decimal(str(value)) / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick)
        if sign * (value - signal.entry) < float(tick):
            continue
        unique.setdefault(value, kind)
        if kind == "STRUCTURAL_TP1" or kind.startswith("CAPACITY_Q"):
            reserved.append(value)
    ordered = list(dict.fromkeys(reserved)) + sorted(
        (v for v in unique if v not in reserved), key=lambda v: sign * (v - signal.entry)
    )
    return [(value, unique[value]) for value in ordered[:MAX_TARGETS]]


def select_target(signal, capacity, now, minimum_rr=1.5):
    e, risk = signal.evidence, signal.risk
    structural = e.get("structural_tp1", signal.tp1)
    choices = []
    sign = 1 if signal.direction == "LONG" else -1
    distance = sign * (signal.entry - signal.stop)
    assumptions = [
        risk.get(k) for k in ("fee_bps_assumption", "slippage_bps_assumption", "funding_reserve_bps")
    ]
    valid_cost = all(number(v) and v >= 0 for v in assumptions)
    tick = float(e.get("price_tick") or 1e-8)
    for target, origin in candidate_targets(signal, capacity, now):
        reward = sign * (target - signal.entry)
        cost = execution_cost(signal.entry, signal.stop, target, *assumptions) if valid_cost else {}
        amount = cost.get("cost_per_base")
        rr = (reward - amount) / (distance + amount) if positive(distance) and number(amount) else None
        reach = reachability(signal, target, signal.entry, capacity, now)
        friction = path(signal, target, signal.entry, now)
        viable = bool(
            reward >= tick
            and positive(target)
            and positive(distance)
            and number(rr)
            and rr >= minimum_rr
            and sign * (signal.tp2 - target) >= 0
            and reach["state"] != "LOW_REACHABILITY"
        )
        choices.append(
            dict(
                target=target,
                origin=origin,
                net_rr=rr,
                cost=cost,
                viable=viable,
                reachability=reach["state"],
                path=friction["state"],
                path_score=friction["score"],
            )
        )
    viable = [r for r in choices if r["viable"]]
    preferred = next(
        (r for r in viable if r["target"] == structural and r["reachability"] in {"REACHABLE", "UNKNOWN"}),
        None,
    )
    if preferred is None and viable:
        # Reachability first, then volume-weighted path, then economic RR.
        preferred = max(
            viable,
            key=lambda r: (
                {"REACHABLE": 2, "UNKNOWN": 1, "STRETCH": 0}[r["reachability"]],
                r["path_score"] if r["path_score"] is not None else 0,
                r["net_rr"],
            ),
        )
    return envelope(
        signal,
        "execution-target-v1",
        now,
        structural_tp1=structural,
        execution_tp1=preferred["target"] if preferred else None,
        state="VIABLE" if preferred else "UNKNOWN" if not valid_cost else "UNTRADEABLE",
        reason=None if preferred else "TARGET_GEOMETRY_UNTRADEABLE" if valid_cost else "TARGET_COST_UNKNOWN",
        choices=choices,
        assumptions="Fixed causal capacity q75/q90 and configured minimum RR; no fitted target or success probability",
    )
