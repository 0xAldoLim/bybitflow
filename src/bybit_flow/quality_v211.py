"""Predeclared v21.1 hypotheses. No fitted weights, lifecycle changes or extra feeds."""

import hashlib
import math
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from statistics import mean

from .quality_v21 import liquidity_update as previous_liquidity
from .risk import execution_cost

POLICY = "trade-validity-v21.1"
ENTRY_SCHEMA = "entry-quality-v2"


def trend_qualification(bars, asof):
    """Existing classifier, last 20 causal prefixes; once per generation call."""
    from .features import candle_features

    closed = [b for b in bars if b.end <= asof]
    regimes = []
    for end in range(max(60, len(closed) - 19), len(closed) + 1):
        regimes.append((closed[end - 1].end, candle_features(closed[:end], closed[end - 1].end)["regime"]))
    result = {}
    for regime in ("trending up", "trending down"):
        qualified = None
        for i, (at, value) in enumerate(regimes):
            if value != regime:
                qualified = None
            elif i and regimes[i - 1][1] == regime and qualified is None:
                qualified = at
        result[regime] = qualified
    return result


def structural(
    bars, family, direction, context, features, asof, *, core_bars=None, _research_start_after=None
):
    """Bounded completed-bar sequences; anchors precede their first event bar.

    Recompute at most six event anchors from the same cached horizon bars. Stop at
    the first outcome of an event, so subsequent retests cannot mint a new event.
    """
    closed = [b for b in bars if b.end <= asof]
    sign = 1 if direction == "LONG" else -1
    if len(closed) < 24:
        return dict(
            policy=POLICY,
            reasons=["STRUCTURAL_HISTORY_INSUFFICIENT"],
            frozen_reference=features.get("low" if sign > 0 else "high"),
            candidate_event=False,
            required_bars=24,
            available_bars=len(closed),
            accepted=False,
            retest_held=False,
        )
    last = closed[-1]
    anchor = len(closed) - 1
    sequence = {}
    found = False
    for start in range(
        max(20, len(closed) - (6 if family == "breakout_retest" else 4), (_research_start_after or -1) + 1),
        len(closed),
    ):
        if family not in {"range_rejection", "breakout_retest"}:
            break
        ref = closed[start - 20 : start]
        hi, lo = max(b.high for b in ref), min(b.low for b in ref)
        boundary = (hi if sign > 0 else lo) if family == "breakout_retest" else (lo if sign > 0 else hi)
        event = closed[start:]
        if family == "range_rejection":
            excursion = event[0].low < lo if sign > 0 else event[0].high > hi
            if not excursion:
                continue

            def outside(b):
                return sign * (b.close - boundary) < 0

            accepted_outside = all(outside(b) for b in closed[start - 2 : start])
            reclaimed = next((i for i, b in enumerate(event) if sign * (b.close - boundary) > 0), None)
            # Consecutive outside closes before the reclaim are a separate thesis failure.
            before = event[:reclaimed] if reclaimed is not None else event
            accepted_outside |= any(outside(a) and outside(b) for a, b in zip(before, before[1:]))
            state = (
                "RANGE_ACCEPTANCE_OUTSIDE"
                if accepted_outside
                else "RECLAIMED"
                if reclaimed is not None
                else "RECLAIM_PENDING"
                if len(event) < 4
                else "FAILED_RECLAIM"
            )
            completion = reclaimed if reclaimed is not None else len(event) - 1
            sequence = dict(
                reclaimed=reclaimed is not None,
                reclaim_delay_bars=reclaimed,
                reclaim_subtype=("SAME_BAR_RECLAIM" if reclaimed == 0 else f"DELAYED_RECLAIM_{reclaimed}")
                if reclaimed is not None
                else None,
                range_state=state,
                candidate_event=completion == len(event) - 1,
                reasons=[]
                if state == "RECLAIMED"
                else [state if state != "RECLAIM_PENDING" else "FAILED_RECLAIM"],
            )
        else:

            def outside(b):
                return sign * (b.close - boundary) > 0

            if not outside(event[0]):
                continue
            accepted = len(event) >= 2 and outside(event[1])
            if not accepted:
                if len(event) > 2:
                    continue  # A failed older acceptance is not a veto on another event.
                sequence = dict(
                    accepted=False,
                    breakout_state="BREAKOUT_ACCEPTING" if len(event) == 1 else "FAILED_ACCEPTANCE",
                    acceptance_bar_1=event[0].end,
                    acceptance_bar_2=None,
                    candidate_event=False,
                    reasons=["FAILED_ACCEPTANCE"],
                )
                anchor, found = start, True
                break
            tail = event[2:]
            outcome = None
            for i, b in enumerate(tail, 1):
                touched = b.low <= boundary if sign > 0 else b.high >= boundary
                held = outside(b)
                if not held or touched:
                    outcome = (i, b, touched, held)
                    break
            delay = outcome[0] if outcome else len(tail)
            state = (
                "RETEST_HELD"
                if outcome and outcome[3]
                else "RETEST_FAILED"
                if outcome
                else "RETEST_EXPIRED"
                if delay >= 4
                else "RETEST_PENDING"
            )
            sequence = dict(
                accepted=True,
                acceptance_bar_1=event[0].end,
                acceptance_bar_2=event[1].end,
                retest_bar=outcome[1].end if outcome else None,
                retest_delay_bars=delay,
                retest_touched=outcome[2] if outcome else False,
                retest_held=outcome[3] if outcome else False,
                retest_closed_back_inside=not outcome[3] if outcome else False,
                breakout_state=state,
                research_state="ACCEPTED_NO_RETEST" if state == "RETEST_EXPIRED" else None,
                candidate_event=(outcome[1].end == last.end if outcome else delay == 4),
                reasons=[] if state == "RETEST_HELD" else [state],
            )
        anchor, found = start, True
        break
    reference = closed[anchor - 20 : anchor]
    high, low = max(b.high for b in reference), min(b.low for b in reference)
    level = (high if sign > 0 else low) if family == "breakout_retest" else (low if sign > 0 else high)
    if family in {"trend_pullback", "liquidity_sweep"}:
        level = features["low" if sign > 0 else "high"]
    closes = [b.close for b in reference]
    travel = sum(abs(b - a) for a, b in zip(closes, closes[1:]))
    efficiency = abs(closes[-1] - closes[0]) / travel if travel else 0
    atr = features.get("atr")
    balanced = context["regime"] == "range" and efficiency < 0.25
    reasons = sequence.pop("reasons", [])
    if high <= low or reference[-1].end > closed[anchor].start:
        reasons.append("STRUCTURAL_LEVEL_NOT_FROZEN")
    if family == "range_rejection":
        if not balanced:
            reasons.append("REGIME_SETUP_MISMATCH")
        if not found:
            reasons.append("FAILED_RECLAIM")
    if family == "breakout_retest" and not found:
        reasons.append("FAILED_ACCEPTANCE")
    trend = "trending up" if sign > 0 else "trending down"
    qualification = context.get("trend_qualification", {}).get(trend)
    if family == "trend_pullback" and (context["regime"] != trend or context.get("previous_regime") != trend):
        reasons.append("REGIME_SETUP_MISMATCH")
    attempts, touching = 0, False
    if qualification is not None:
        for i in range(max(20, len(closed) - 60), len(closed)):
            if closed[i].start < qualification:
                continue
            ma = mean(b.close for b in closed[i - 19 : i + 1])
            touch = closed[i].low <= ma if sign > 0 else closed[i].high >= ma
            attempts += bool(touch and not touching)
            touching = touch
    prev, preceding = closed[-2], closed[-3]
    inside = prev.high <= preceding.high and prev.low >= preceding.low
    nr4 = prev.high - prev.low <= min(b.high - b.low for b in closed[-5:-1])
    compression = "ID_NR4" if inside and nr4 else "INSIDE_ONLY" if inside else "NR4_ONLY" if nr4 else "NONE"
    expanded = last.close > prev.high or last.close < prev.low
    expansion = (
        "EXPANSION_TRIGGERED"
        if compression == "ID_NR4" and expanded
        else "FAILED_EXPANSION"
        if compression == "ID_NR4" and (last.high > prev.high or last.low < prev.low)
        else "NONE"
    )
    comparable = [b.volume for b in closed[-81:-1] if b.start % 86400000 == last.start % 86400000]
    relative_volume = (
        last.volume / mean(comparable) if len(comparable) >= 3 and mean(comparable) > 0 else None
    )
    event_start = closed[anchor].start
    event_id = hashlib.sha256(
        f"{family}:{direction}:{last.interval}:{event_start}:{high}:{low}".encode()
    ).hexdigest()[:20]
    condition = {
        "range": "BALANCED_RANGE" if balanced else "UNCERTAIN",
        "trending up": "TREND_UP",
        "trending down": "TREND_DOWN",
        "high-volatility disorder": "VOLATILITY_EXPANSION",
    }.get(context["regime"], "UNCERTAIN")
    result = dict(
        policy=POLICY,
        setup_interval_ms=last.interval,
        level_policy="prior-20-horizon-bars-v211",
        reference_policy="prior-20-horizon-bars-v211",
        reference_start_ms=reference[0].start,
        reference_end_ms=reference[-1].end,
        reference_bar_count=20,
        reference_high=high,
        reference_low=low,
        reference_width=high - low,
        reference_width_atr=(high - low) / atr if number(atr) and atr > 0 else None,
        reference_efficiency_ratio=efficiency,
        level_known_ms=reference[-1].end,
        frozen_high=high,
        frozen_low=low,
        frozen_reference=level,
        structure_event_id=event_id,
        event_start_ms=event_start,
        event_extreme=min(b.low for b in closed[anchor:])
        if sign > 0
        else max(b.high for b in closed[anchor:]),
        trigger_ms=event_start,
        available_ms=last.end,
        market_condition=condition,
        balanced=balanced,
        accepted=False,
        retest_held=False,
        candidate_event=True,
        compression_state=compression,
        expansion_state=expansion,
        trend_qualified_ms=qualification,
        time_since_trend_qualification=last.end - qualification if qualification is not None else None,
        pullback_attempt_index_since_qualification=attempts if qualification is not None else None,
        prior_pullback_attempts=max(0, attempts - 1) if qualification is not None else None,
        pullback_trigger_confirmed=last.close > closed[-2].high if sign > 0 else last.close < closed[-2].low,
        pullback_trigger_mode="research-only",
        trigger_reference_high=closed[-2].high,
        trigger_reference_low=closed[-2].low,
        trigger_break_ms=last.end
        if (last.close > closed[-2].high if sign > 0 else last.close < closed[-2].low)
        else None,
        attempt_scope="last 20 context prefixes; last 60 setup bars after current continuous qualification",
        relative_volume=relative_volume,
        relative_volume_samples=len(comparable),
        relative_volume_session_basis="Same source/symbol/setup interval and UTC time of day; last 80 completed bars, minimum three prior observations",
        reasons=sorted(set(reasons)),
    )
    result.update(sequence)
    if core_bars is not None:
        boundary = last.start // 21600000 * 21600000
        core_ref = [b for b in core_bars if b.interval == 3600000 and b.end <= boundary][-24:]
        complete = (
            len(core_ref) == 24
            and core_ref[-1].end == boundary
            and all(b.start == a.end for a, b in zip(core_ref, core_ref[1:]))
        )
        result["core_24h_6h_variant"] = dict(
            policy="core-24h-6h-research-v1",
            research_only=True,
            available=complete,
            frozen_until_ms=boundary + 21600000,
            reference_end_ms=boundary,
            high=max(b.high for b in core_ref) if complete else None,
            low=min(b.low for b in core_ref) if complete else None,
        )
    return result


def enabled(signal):
    return signal.evidence.get("production_policies", {}).get("trade_quality") == POLICY


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def quote_evidence(signal, book, now, settings):
    if book is None:
        return {}
    return dict(
        book_timestamp_ms=book.event_ms,
        book_receipt_ms=book.receipt_ms,
        book_epoch=book.ofi_epoch,
        fresh=book.fresh(now, settings.book_stale_ms),
        spread_bps=book.features(now).get("spread_bps"),
        max_age_ms=settings.book_stale_ms,
    )


def current_cost(signal, price, book=None):
    r = signal.risk
    assumptions = [r.get(k) for k in ("fee_bps_assumption", "slippage_bps_assumption", "funding_reserve_bps")]
    if not number(price) or any(x is None for x in assumptions):
        return dict(cost_per_base=None, state="UNKNOWN", impact_state="UNKNOWN")
    if not all(number(x) and x >= 0 for x in assumptions):
        return dict(cost_per_base=None, state="INVALID", impact_state="UNKNOWN")
    notional = r.get("notional")
    impact = (
        book.impact(signal.direction, notional)
        if book and book.valid and number(notional) and notional > 0
        else None
    )
    sign = 1 if signal.direction == "LONG" else -1
    residual = max(0, sign * (impact["vwap"] - price)) if impact else None
    return execution_cost(price, signal.stop, signal.tp1, *assumptions, residual) | dict(
        state="KNOWN",
        impact_state="KNOWN" if impact else "UNKNOWN",
        notional=notional,
    )


def entry_quality(signal, price, now, minimum_rr=1.5, *, quote=None, current=None):
    e, sign = signal.evidence, 1 if signal.direction == "LONG" else -1
    q = quote or {}
    cost = e.get("frozen_entry_cost", {}).get("cost_per_base", signal.risk.get("cost_per_base"))
    current_value = current.get("cost_per_base") if current is not None else cost
    invalid_cost = (
        any(x is not None and (not number(x) or x < 0) for x in (cost, current_value))
        or (current or {}).get("state") == "INVALID"
    )
    cost_state = (
        "COST_INVALID"
        if invalid_cost
        else "COST_UNKNOWN"
        if cost is None or current_value is None
        else "COST_KNOWN"
    )
    valid_price = all(
        number(x) and x > 0 for x in (price, signal.entry, signal.stop, signal.tp1, *signal.zone)
    )
    tick = e.get("price_tick")
    try:
        tick_value = Decimal(str(tick)) if tick is not None else None
    except InvalidOperation:
        tick_value = None
    if tick_value is not None and (not tick_value.is_finite() or tick_value <= 0):
        tick_value = None

    def normalized(value):
        return (
            (Decimal(str(value)) / tick_value).to_integral_value(rounding=ROUND_HALF_UP)
            if tick_value
            else Decimal(str(value))
        )

    relation = "UNKNOWN"
    if valid_price and signal.zone[0] <= signal.zone[1]:
        p, lo, hi = map(normalized, (price, *signal.zone))
        relation = (
            "INSIDE_ZONE"
            if lo <= p <= hi
            else "CHASE_SIDE"
            if (p > hi if sign > 0 else p < lo)
            else "ADVERSE_SIDE"
        )
    trigger_until = signal.trigger_expires_ms or signal.expires_ms
    time_state = (
        "PLAN_EXPIRED" if now >= signal.expires_ms else "TRIGGER_EXPIRED" if now >= trigger_until else "VALID"
    )
    geometry = []
    risk, reward, gross, rr = None, None, None, None
    if not valid_price:
        geometry.append("INVALID_PRICE")
    else:
        risk, reward = sign * (price - signal.stop), sign * (signal.tp1 - price)
        if sign * (signal.entry - signal.stop) <= 0:
            geometry.append("NON_POSITIVE_RISK")
        if sign * (signal.tp1 - signal.entry) <= 0:
            geometry.append("NON_POSITIVE_REWARD")
        if not geometry and risk <= 0:
            geometry.append("STOP_ALREADY_BREACHED")
        if not geometry and reward <= 0:
            geometry.append("TARGET_ALREADY_PASSED")
        if signal.zone[0] > signal.zone[1]:
            geometry.append("INVALID_ZONE")
        gross = reward / risk if risk > 0 else None
        if cost_state == "COST_KNOWN" and risk + current_value > 0:
            rr = (reward - current_value) / (risk + current_value)
    rr_state = "RR_UNKNOWN" if rr is None else "RR_ACCEPTABLE" if rr >= minimum_rr else "RR_DEGRADED"
    quote_age = now - q["book_timestamp_ms"] if number(q.get("book_timestamp_ms")) else None
    quote_state = (
        "QUOTE_STALE" if q.get("fresh") is False else "QUOTE_FRESH" if q.get("fresh") is True else "UNKNOWN"
    )
    reasons = []
    if geometry:
        reasons.extend(
            [
                g if g in {"STOP_ALREADY_BREACHED", "TARGET_ALREADY_PASSED"} else "INVALID_EXECUTION_GEOMETRY"
                for g in geometry
            ]
        )
    # A crossed stop/target is geometry, not price chasing.
    elif relation != "INSIDE_ZONE":
        reasons.append("ENTRY_PRICE_CHASED" if relation == "CHASE_SIDE" else "ENTRY_ZONE_BROKEN_ADVERSE")
    if time_state != "VALID":
        reasons.append("ENTRY_PLAN_EXPIRED" if time_state == "PLAN_EXPIRED" else "ENTRY_TRIGGER_EXPIRED")
    if cost_state != "COST_KNOWN":
        reasons.append("EXECUTION_COST_UNKNOWN" if cost_state == "COST_UNKNOWN" else "EXECUTION_COST_INVALID")
    if rr_state == "RR_DEGRADED" and not geometry:
        reasons.append("RR_DEGRADED")
    if quote_state == "QUOTE_STALE":
        reasons.append("EXECUTION_QUOTE_STALE")
    atr = (
        (e.get("stop_plan") or {}).get("setup_atr")
        or e.get("execution_features", {}).get("atr")
        or abs(signal.entry - signal.stop)
    )
    atr = atr if number(atr) and atr > 0 else None
    ref = e.get("frozen_structure", {}).get("frozen_reference", signal.entry)
    distance = max(signal.zone[0] - price, 0, price - signal.zone[1]) if valid_price else None
    original_risk = sign * (signal.entry - signal.stop)
    original_reward = sign * (signal.tp1 - signal.entry)
    return dict(
        policy=POLICY,
        schema=ENTRY_SCHEMA,
        available_ms=now,
        executable_price=price if number(price) else None,
        price_source="BEST_ASK" if sign > 0 else "BEST_BID",
        quote_age_ms=quote_age,
        quote_state=quote_state,
        quote_provenance=q,
        zone_relation=relation,
        time_state=time_state,
        cost_state=cost_state,
        rr_state=rr_state,
        geometry_state=geometry[0] if geometry else "VALID_GEOMETRY",
        geometry_states=geometry or ["VALID_GEOMETRY"],
        liquidity_state="UNKNOWN",
        gross_rr_now=gross,
        cost_adjusted_rr_now=rr,
        original_gross_rr=original_reward / original_risk if original_risk > 0 else None,
        original_net_rr=e.get("frozen_entry_cost", {}).get("net_rr", signal.risk.get("net_rr")),
        frozen_cost_per_base=cost if number(cost) else None,
        current_execution_cost_estimate=current
        or dict(cost_per_base=current_value, basis="frozen-only; current impact unknown"),
        distance_from_zone_atr=distance / atr if atr and distance is not None else None,
        distance_from_trigger_atr=abs(price - ref) / atr if valid_price and atr and number(ref) else None,
        post_signal_impulse_atr=sign * (price - signal.entry) / atr if valid_price and atr else None,
        signal_age_ms=now - signal.created_ms,
        trigger_remaining_ms=trigger_until - now,
        plan_remaining_ms=signal.expires_ms - now,
        effective_remaining_ms=min(trigger_until, signal.expires_ms) - now,
        entry_not_chased=relation != "CHASE_SIDE" if relation != "UNKNOWN" else None,
        expired=time_state != "VALID",
        acceptable=not reasons,
        reasons=sorted(set(reasons)),
        price_normalization="instrument ticks; ROUND_HALF_UP"
        if tick_value
        else "exact Decimal; tick unavailable",
    )


def counterfactual(signal, reasons, now):
    reasons = sorted(set(reasons))
    if not reasons:
        return
    signal.evidence["v21_counterfactual"] = dict(
        policy=POLICY,
        reasons=reasons,
        reason_set=reasons,
        reason_count=len(reasons),
        primary_reason=reasons[0] if len(reasons) == 1 else None,
        sole_reason=reasons[0] if len(reasons) == 1 else None,
        shared_rejection=len(reasons) > 1,
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


def apply(signal, price, now, liquidity=None, minimum_rr=1.5, *, book=None, settings=None):
    if not enabled(signal):
        return
    e = signal.evidence
    from .research_v212 import capture_initial, execution_change
    from .research_v212 import enabled as research_enabled

    if settings:
        capture_initial(signal, book, settings, now)
    if "cost_per_base" in signal.risk:
        e.setdefault(
            "frozen_entry_cost",
            dict(
                cost_per_base=signal.risk["cost_per_base"],
                gross_rr=signal.risk.get("gross_rr"),
                net_rr=signal.risk.get("net_rr"),
                components=signal.risk.get("cost_components"),
                available_ms=now,
            ),
        )
    q = entry_quality(
        signal,
        price,
        now,
        minimum_rr,
        quote=quote_evidence(signal, book, now, settings) if settings else None,
        current=current_cost(signal, price, book) if settings else None,
    )
    e["entry_quality"] = q
    if research_enabled(signal):
        e["execution_deterioration"] = execution_change(signal, q)
    reasons = list(signal.gates) + list(e.get("frozen_structure", {}).get("reasons", [])) + q["reasons"]
    flow = e.get("flow", {})
    sign = 1 if signal.direction == "LONG" else -1
    trust = flow.get("quality", {}).get("flow_trust_score")
    response = flow.get("quality", {})
    displacement = response.get("price_displacement_bps", flow.get("price_change"))
    e["aggression_response"] = dict(
        available_ms=now,
        aggressive_notional=response.get("gross_notional"),
        signed_aggressor_notional=response.get("net_signed_notional", flow.get("delta_notional")),
        delta_pct=flow.get("delta_pct"),
        price_displacement_bps=displacement,
        flow_trust=trust,
        response_efficiency=response.get("flow_efficiency"),
        absorption_against=flow.get("absorption_short" if sign > 0 else "absorption_long"),
        limitation="Observed response, not trader intent or future direction",
    )
    if (
        flow.get("available")
        and signal.coverage.get("trade_window_complete")
        and number(trust)
        and trust >= 0.6
        and sign * flow.get("delta_pct", 0) >= 10
        and (
            (number(displacement) and sign * displacement < 0)
            or flow.get("absorption_short" if sign > 0 else "absorption_long")
        )
    ):
        reasons.append("MECHANISM_CONTRADICTED")
    distance, cost = abs(signal.entry - signal.stop), signal.risk.get("cost_per_base")
    if distance and number(cost) and cost / distance >= 1:
        reasons.append("COST_DOMINATES_RISK")
    if liquidity:
        e["liquidity_stress"] = {k: v for k, v in liquidity.items() if k != "rows"}
        q["liquidity_state"] = liquidity.get("subtype", "UNKNOWN")
        if liquidity.get("state") == "LIQUIDITY_STRESSED":
            reasons.append("LIQUIDITY_STRESS")
    counterfactual(signal, reasons, now)


def liquidity_update(previous, sample, now):
    result = previous_liquidity(previous, sample, now)
    ranks = result["percentiles"]
    spread, depth = ranks.get("spread"), ranks.get("depth")
    mature = spread is not None and depth is not None
    bad_spread, bad_depth = mature and spread >= 0.95, mature and depth <= 0.10
    subtype = (
        "UNKNOWN"
        if not mature
        else "JOINT_STRESS"
        if bad_spread and bad_depth
        else "SPREAD_STRESS_ONLY"
        if bad_spread
        else "DEPTH_STRESS_ONLY"
        if bad_depth
        else "NORMAL"
    )
    return result | dict(
        research_policy="liquidity-subtypes-v211",
        subtype=subtype,
        variants=dict(
            joint=bool(bad_spread and bad_depth),
            spread_only=bool(bad_spread),
            depth_only=bool(bad_depth),
            either=bool(bad_spread or bad_depth),
        )
        if mature
        else dict.fromkeys(("joint", "spread_only", "depth_only", "either")),
        coverage_gap_reset=bool(previous.get("rows") and now - previous["rows"][-1]["at_ms"] > 120000),
    )


def overlaps_existing(signal, rows):
    from .quality_v21 import overlaps_existing as old

    if old(signal, rows):
        return True
    structure = signal.evidence.get("frozen_structure", {})
    return any(
        all(
            row.get(k) == getattr(signal, k)
            for k in ("source", "symbol", "direction", "family", "horizon_profile")
        )
        and (
            row.get("evidence", {}).get("frozen_structure", {}).get("structure_event_id")
            == structure.get("structure_event_id")
            if structure.get("structure_event_id")
            else False
        )
        for row in rows
    )
