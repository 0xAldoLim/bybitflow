"""Causal, versioned validity for new opportunities; never a lifecycle policy.

Windows count existing horizon bars. No parameters are fitted to local outcomes.
Missing optional observations remain unknown, rather than an invented veto.
"""

import math
from statistics import mean

POLICY = "trade-validity-v21"
SCORE_PROFILE = "bybitflow-v21-evidence-1"


def enabled(signal):
    return signal.evidence.get("production_policies", {}).get("trade_quality") == POLICY


def structural(bars, family, direction, context, features, asof):
    closed = [b for b in bars if b.end <= asof]
    last = closed[-1]
    sign = 1 if direction == "LONG" else -1
    # Two acceptance closes and one retest, all after the frozen reference window.
    reference = closed[-23:-3] if family == "breakout_retest" else closed[-21:-1]
    high, low = max(b.high for b in reference), min(b.low for b in reference)
    level = high if sign > 0 else low
    if family != "breakout_retest":
        level = low if sign > 0 else high
    if family in {"liquidity_sweep", "trend_pullback"}:
        level = features["low" if sign > 0 else "high"]
    known = reference[-1].end
    closes = [b.close for b in reference]
    travel = sum(abs(a - b) for a, b in zip(closes, closes[1:]))
    efficiency = abs(closes[-1] - closes[0]) / travel if travel else 0
    balanced = context["regime"] == "range" and efficiency < 0.25
    trend = "trending up" if sign > 0 else "trending down"
    accepted = all(sign * (b.close - level) > 0 for b in closed[-3:-1])
    reclaim = last.low < low < last.close if sign > 0 else last.high > high > last.close
    outside = (
        all(b.close < low for b in closed[-3:-1]) if sign > 0 else all(b.close > high for b in closed[-3:-1])
    )
    retest = (last.low <= high < last.close) if sign > 0 else (last.high >= low > last.close)
    typical = mean(b.high - b.low for b in reference)
    compression = reference[-1].high - reference[-1].low == min(b.high - b.low for b in reference[-4:])
    expansion = last.high - last.low > typical and (last.close > high or last.close < low)
    compact = (
        "EXPANSION_TRIGGERED"
        if compression and expansion
        else "COMPRESSION_READY"
        if compression
        else "FAILED_EXPANSION"
        if (last.high > high or last.low < low) and low <= last.close <= high
        else "NONE"
    )
    condition = {
        "range": "BALANCED_RANGE",
        "trending up": "TREND_UP",
        "trending down": "TREND_DOWN",
        "high-volatility disorder": "VOLATILITY_EXPANSION",
    }.get(context["regime"], "UNCERTAIN")
    if condition == "BALANCED_RANGE" and not balanced:
        condition = "UNCERTAIN"
    if (
        context.get("previous_regime") == "high-volatility disorder"
        and context["regime"] != "high-volatility disorder"
    ):
        condition = "POST_SHOCK_NORMALIZATION"
    if family == "breakout_retest" and accepted:
        condition = "BREAKOUT_DISCOVERY_UP" if sign > 0 else "BREAKOUT_DISCOVERY_DOWN"
    elif compact == "COMPRESSION_READY" and condition == "UNCERTAIN":
        condition = "VOLATILITY_COMPRESSION"
    reasons = []
    if known > last.start or high <= low:
        reasons.append("STRUCTURAL_LEVEL_NOT_FROZEN")
    if family == "range_rejection":
        if not balanced:
            reasons.append("REGIME_SETUP_MISMATCH")
        if not reclaim or outside:
            reasons.append("FAILED_RECLAIM")
    elif family == "breakout_retest" and (not accepted or not retest):
        reasons.append("FAILED_ACCEPTANCE")
    elif family == "trend_pullback":
        # Persistence belongs to the horizon's context, not the pullback candle.
        if context["regime"] != trend or context.get("previous_regime") != trend:
            reasons.append("REGIME_SETUP_MISMATCH")
    # Count separated MA touches in the bounded trend segment; never veto later attempts.
    attempts, touching = 0, False
    for i in range(max(20, len(closed) - 20), len(closed)):
        ma = mean(b.close for b in closed[i - 19 : i + 1])
        touch = closed[i].low <= ma if sign > 0 else closed[i].high >= ma
        attempts += bool(touch and not touching)
        touching = touch
    return dict(
        policy=POLICY,
        level_policy="prior-20-horizon-bars-v1",
        level_known_ms=known,
        frozen_high=high,
        frozen_low=low,
        frozen_reference=level,
        trigger_ms=closed[-3].start if family == "breakout_retest" else last.start,
        available_ms=last.end,
        market_condition=condition,
        compression_state=compact,
        balanced=balanced,
        accepted=accepted,
        reclaimed=reclaim,
        retest_held=retest,
        pullback_attempt_index=attempts if family == "trend_pullback" else None,
        attempt_scope="last 20 completed setup bars; separated MA touches",
        reasons=reasons,
    )


def entry_quality(signal, price, now, minimum_rr=1.5):
    e = signal.evidence
    atr = (
        (e.get("stop_plan") or {}).get("atr")
        or e.get("execution_features", {}).get("atr")
        or abs(signal.entry - signal.stop)
    )
    sign = 1 if signal.direction == "LONG" else -1
    cost = signal.risk.get("cost_per_base")
    valid_cost = isinstance(cost, (int, float)) and math.isfinite(cost) and cost >= 0
    loss = sign * (price - signal.stop) + cost if valid_cost else 0
    reward = sign * (signal.tp1 - price) - cost if valid_cost else 0
    rr = reward / loss if loss > 0 else None
    ref = e.get("frozen_structure", {}).get("frozen_reference", signal.entry)
    zone_distance = max(signal.zone[0] - price, 0, price - signal.zone[1])
    expired = now >= min(signal.expires_ms, signal.trigger_expires_ms or signal.expires_ms)
    acceptable = bool(
        valid_cost and loss > 0 and reward > 0 and rr >= minimum_rr and zone_distance == 0 and not expired
    )
    return dict(
        policy=POLICY,
        available_ms=now,
        executable_price=price,
        distance_from_trigger_atr=abs(price - ref) / max(atr, 1e-12),
        distance_from_zone_atr=zone_distance / max(atr, 1e-12),
        distance_from_retest_atr=abs(price - ref) / max(atr, 1e-12)
        if signal.family == "breakout_retest"
        else None,
        post_signal_impulse_atr=sign * (price - signal.entry) / max(atr, 1e-12),
        cost_adjusted_rr_now=rr,
        entry_not_chased=acceptable,
        expired=expired,
        cost_per_base=cost,
        cost_basis="original frozen fees/slippage/funding allowance",
    )


def liquidity_update(previous, sample, now):
    """One observation per minute; rank current against strictly earlier samples.

    Joint spread/depth deterioration, 3 bad minutes / 2 recovery minutes.
    Percentile cutoffs are a predeclared research risk policy, not fitted results.
    """
    rows = [r for r in previous.get("rows", []) if now - 21_600_000 <= r["at_ms"] < now]
    if rows and rows[-1]["at_ms"] // 60000 == now // 60000:
        return previous
    contiguous = bool(rows and 0 < now - rows[-1]["at_ms"] <= 120000)
    episode = previous.get("episode_ms") if contiguous else None
    prior = [
        r
        for r in rows
        if r.get("session") == sample.get("session") and (episode is None or r["at_ms"] < episode)
    ]
    ranks = {}
    for name in ("spread", "depth", "impact"):
        vals = [r[name] for r in prior if isinstance(r.get(name), (int, float))]
        value = sample.get(name)
        ranks[name] = (
            sum(v < value for v in vals) / len(vals)
            if len(vals) >= 20 and isinstance(value, (int, float))
            else None
        )
    mature = ranks["spread"] is not None and ranks["depth"] is not None
    bad = mature and ranks["spread"] >= 0.95 and ranks["depth"] <= 0.10
    bad_run = (previous.get("bad_run", 0) if contiguous else 0) + 1 if bad else 0
    good_run = (previous.get("good_run", 0) if contiguous else 0) + 1 if mature and not bad else 0
    stressed = bad_run >= 3 or (contiguous and previous.get("state") == "LIQUIDITY_STRESSED" and good_run < 2)
    state = (
        "LIQUIDITY_STRESSED"
        if stressed
        else "LIQUIDITY_THIN"
        if bad
        else "LIQUIDITY_NORMAL"
        if mature
        else "UNKNOWN"
    )
    return dict(
        policy="liquidity-relative-v21",
        at_ms=now,
        state=state,
        percentiles=ranks,
        samples=len(prior),
        bad_run=bad_run,
        good_run=good_run,
        reset_delta=max(0, sample.get("epoch", 0) - rows[-1].get("epoch", 0)) if rows else None,
        episode_ms=(episode or now) if bad or stressed else None,
        rows=(rows + [dict(sample, at_ms=now)])[-360:],
    )


def counterfactual(signal, reasons, now):
    reasons = sorted(set(reasons))
    if not reasons:
        return
    signal.evidence["v21_counterfactual"] = dict(
        policy=POLICY,
        reasons=reasons,
        available_ms=now,
        entry=signal.entry,
        zone=signal.zone,
        stop=signal.stop,
        tp1=signal.tp1,
        tp2=signal.tp2,
        label_policies=["prints-v1", "monitored-ohlc-v1"],
        hypothetical=True,
    )
    signal.gates = sorted(set(signal.gates + reasons))
    signal.final_tier = "REJECTED"


def apply(signal, price, now, liquidity=None, minimum_rr=1.5):
    if not enabled(signal):
        return
    e = signal.evidence
    reasons = list(e.get("frozen_structure", {}).get("reasons", []))
    q = entry_quality(signal, price, now, minimum_rr)
    e["entry_quality"] = q
    if not q["entry_not_chased"]:
        reasons.append("STALE_EXECUTION" if q["expired"] else "CHASED_ENTRY")
    flow = e.get("flow", {})
    sign = 1 if signal.direction == "LONG" else -1
    if flow.get("available") and signal.coverage.get("trade_window_complete"):
        trust = flow.get("quality", {}).get("flow_trust_score")
        aggressive = sign * flow.get("delta_pct", 0) >= 10
        if (
            trust is not None
            and trust >= 0.6
            and aggressive
            and (
                sign * flow.get("price_change", 0) < 0
                or flow.get("absorption_short" if sign > 0 else "absorption_long")
            )
        ):
            reasons.append("MECHANISM_CONTRADICTED")
    distance = abs(signal.entry - signal.stop)
    cost = signal.risk.get("cost_per_base")
    if distance and cost is not None and cost / distance >= 1:
        reasons.append("COST_DOMINATES_RISK")
    if liquidity:
        e["liquidity_stress"] = {k: v for k, v in liquidity.items() if k != "rows"}
        if liquidity.get("state") == "LIQUIDITY_STRESSED":
            reasons.append("LIQUIDITY_STRESS")
            e["market_condition_now"] = "LIQUIDITY_STRESS"
    counterfactual(signal, reasons, now)


def overlaps_existing(signal, rows):
    """Upgrading policy is not a new structural event, irrespective of old schema."""
    return any(
        all(
            row.get(k) == getattr(signal, k)
            for k in ("source", "symbol", "direction", "family", "horizon_profile")
        )
        and row.get("evidence", {}).get("trigger_bar_end") == signal.evidence.get("trigger_bar_end")
        for row in rows
    )


def path_quality(signal, profile, barriers):
    """Weight obstacles by observed volume, not the number of aliases/levels."""
    bins = profile.get("raw_volume_profile", [])
    volumes = {float(r["price"]): r.get("buy", 0) + r.get("sell", 0) for r in bins}
    total = sum(volumes.values())
    distance = abs(signal.entry - signal.stop)
    low, high = sorted((signal.entry, signal.tp1))
    density = sum(v for p, v in volumes.items() if low < p < high) / total if total else None
    strengths = []
    for barrier in barriers:
        nearest = min(volumes, key=lambda p: abs(p - barrier)) if volumes else None
        strengths.append(dict(price=barrier, volume_fraction=volumes[nearest] / total if total else None))
    strength = max(
        (r["volume_fraction"] for r in strengths if r["volume_fraction"] is not None), default=None
    )
    difficulty = min(1, max(density or 0, strength or 0)) if total else None
    return dict(
        policy="volume-weighted-path-v21",
        path_difficulty_score=100 * difficulty if difficulty is not None else None,
        nearest_barrier_distance_r=min((abs(p - signal.entry) / distance for p in barriers), default=None)
        if distance
        else None,
        barrier_strength=strengths,
        volume_density=density,
        visible_opposing_depth=signal.evidence.get("book", {})
        .get("depth", {})
        .get("10", {})
        .get("ask" if signal.direction == "LONG" else "bid"),
        limitation="Executed volume concentration, not a probability; visible depth is transient",
    )


def remember_attempt(store, signal):
    """Bounded causal attempt journal. A failure must be known before this decision."""
    key = f"attempt-v21:{signal.source}:{signal.symbol}:{signal.horizon_profile}:{signal.family}:{signal.direction}"
    rows = store.get(key, [])
    current_event = signal.evidence.get("trigger_bar_end")
    prior = [r for r in rows if r["created_ms"] < signal.created_ms and r["event"] != current_event]
    failures = []
    for row in prior:
        saved = store.db.execute("SELECT payload FROM signals WHERE id=?", (row["id"],)).fetchone()
        if saved:
            import json

            value = json.loads(saved[0])
            # Current terminal state is known now; no later outcome enters this snapshot.
            if value["state"] in {"INVALIDATED", "EXPIRED"}:
                failures.append(value)
    signal.evidence["attempt_memory"] = dict(
        available_ms=signal.created_ms,
        attempt_index=len(prior) + 1,
        prior_failed_attempts=len(failures),
        prior_false_breaks=sum("FAILED_ACCEPTANCE" in x.get("gates", []) for x in failures),
        prior_failed_reclaims=sum("FAILED_RECLAIM" in x.get("gates", []) for x in failures),
        time_since_last_attempt=signal.created_ms - prior[-1]["created_ms"] if prior else None,
        scope="last 20 distinct structural events, same venue/symbol/horizon/family/side; descriptive only",
    )
    if not any(r["event"] == current_event for r in rows):
        store.put(key, (rows + [dict(id=signal.id, event=current_event, created_ms=signal.created_ms)])[-20:])
