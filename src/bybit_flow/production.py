"""Explicit deterministic production validity policies; no profitability claim."""

INTRADAY = {"SHORT_INTRADAY", "CORE_INTRADAY"}
REVERSALS = {"liquidity_sweep", "range_rejection"}


def wick_regime(bars, direction, atr, asof):
    import numpy as np

    prior = [b for b in bars if b.end <= asof][-96:]
    rows = []
    previous = prior[0].open if prior else 0
    for b in prior:
        width = b.high - b.low
        upper, lower = b.high - max(b.open, b.close), min(b.open, b.close) - b.low
        rows.append(
            dict(
                upper_wick=upper,
                lower_wick=lower,
                body=abs(b.close - b.open),
                range=width,
                true_range=max(width, abs(b.high - previous), abs(b.low - previous)),
                upper_wick_atr=upper / max(atr, 1e-12),
                lower_wick_atr=lower / max(atr, 1e-12),
                body_to_range=abs(b.close - b.open) / max(width, 1e-12),
                close_location=(b.close - b.low) / max(width, 1e-12),
                range_atr=width / max(atr, 1e-12),
            )
        )
        previous = b.close
    adverse = [r["lower_wick" if direction == "LONG" else "upper_wick"] for r in rows]
    ranges = [r["range"] for r in rows]

    def quantile(values, q):
        return float(np.quantile(values, q)) if values else 0.0

    frequency = sum(v > 0.5 * atr for v in adverse) / max(1, len(adverse))
    q80 = quantile(adverse, 0.8)
    state = (
        "LIQUIDITY_STRESS"
        if frequency >= 0.3 or quantile(ranges, 0.9) > 2 * atr
        else "WICKY"
        if q80 >= 0.30 * atr
        else "NORMAL"
    )
    return dict(
        policy="wick-regime-v1",
        state=state,
        samples=len(rows),
        available_ms=asof,
        adverse_wick_q50=quantile(adverse, 0.5),
        adverse_wick_q75=quantile(adverse, 0.75),
        adverse_wick_q80=q80,
        adverse_wick_q90=quantile(adverse, 0.9),
        range_q80=quantile(ranges, 0.8),
        range_q90=quantile(ranges, 0.9),
        abnormal_wick_frequency=frequency,
        latest=rows[-1] if rows else {},
        normalization="symbol ATR; bounded closed bars",
    )


def intraday_stop(entry, direction, anchor, setup, execution, atr, horizon, asof):
    short = horizon == "SHORT_INTRADAY"
    wick = wick_regime(setup[:-1] if short else execution[:-1], direction, atr, asof)
    noise = wick_regime(execution[:-1], direction, atr, asof)
    buffer = max(
        0.15 * atr, (1.10 if short else 1.0) * wick["adverse_wick_q80"], 0.75 * noise["adverse_wick_q80"]
    )
    sign = 1 if direction == "LONG" else -1
    stop = anchor - sign * buffer
    reasons = []
    if short and buffer > 0.75 * atr:
        reasons.append("SHORT_INTRADAY_NOISE_EXCEEDS_SAFE_STOP")
    if sign * (entry - stop) > (1.5 if short else 2.25) * atr:
        reasons.append(horizon + "_STRUCTURAL_STOP_TOO_WIDE")
    return dict(
        policy=horizon.lower() + "-stop-v2",
        stop=stop,
        anchor=anchor,
        buffer=buffer,
        atr=atr,
        execution_noise_q80=noise["adverse_wick_q80"],
        wick_regime=wick,
        reasons=reasons,
    )


def reclaim_hold(signal, bars, asof):
    sign = 1 if signal.direction == "LONG" else -1
    trigger = signal.evidence.get("structural_trigger", {})
    level, extreme = trigger.get("level"), trigger.get("extreme")
    closed = [b for b in bars if b.interval == 300000 and trigger.get("available_ms", 0) < b.end <= asof]
    if level is None or extreme is None or len(closed) < 2:
        return False
    if any(b.low < extreme if sign > 0 else b.high > extreme for b in closed):
        return False
    reclaim = None
    for b in closed:
        if b.low < extreme if sign > 0 else b.high > extreme:
            return False
        if (
            reclaim is not None
            and sign * (b.close - level) > 0
            and (b.low >= level if sign > 0 else b.high <= level)
        ):
            return True
        if (
            reclaim is not None
            and sign * (b.close - level) > 0
            and (b.low <= level if sign > 0 else b.high >= level)
        ):
            return True
        if sign * (b.close - level) > 0:
            reclaim = b
    return False


def entry_position(signal, price, spread_bps=0):
    sign = 1 if signal.direction == "LONG" else -1
    plan = signal.evidence.get("stop_plan") or {}
    atr = plan.get("atr") or abs(signal.entry - signal.stop)
    cost = abs(price) * (0.0012 + max(0, spread_bps) / 10000)
    risk = sign * (price - signal.stop) + cost
    reward = sign * (signal.tp1 - price) - cost
    remaining_rr = reward / risk if risk > 0 else 0
    distance = sign * (price - signal.entry)
    state = (
        "MISSED_ENTRY"
        if reward <= 0
        else "CHASED"
        if distance > 0.5 * atr or remaining_rr < 1.5
        else "IDEAL_ENTRY"
        if distance <= 0.15 * atr
        else "ACCEPTABLE_ENTRY"
    )
    return dict(
        state=state, remaining_net_rr=remaining_rr, distance_atr=distance / max(atr, 1e-12), cost=cost
    )


def participation(metrics, sign, threshold, trusted=False):
    """Count categories, not correlated volume/count/intensity aliases."""
    mature = metrics.get("effective_baseline_samples" if trusted else "baseline_samples", 0) >= 20
    groups = {
        "activity": ("effective_trade_intensity",) if trusted else ("trade_count", "trade_intensity"),
        "size": ("effective_volume", "effective_buy_notional" if sign > 0 else "effective_sell_notional")
        if trusted
        else ("volume", "aggressive_buy_notional" if sign > 0 else "aggressive_sell_notional"),
        "imbalance": ("effective_delta_magnitude",) if trusted else ("delta_magnitude",),
    }
    support = {
        name: any(
            metrics.get(k + "_percentile") is not None and metrics[k + "_percentile"] >= threshold
            for k in keys
        )
        for name, keys in groups.items()
    }
    return dict(
        mode="PERCENTILE" if mature else "RAW_FALLBACK",
        threshold=threshold,
        categories=support,
        passed=sum(support.values()) >= 2 if mature else None,
    )


def flow_support(flow, sign, response):
    side = "long" if sign > 0 else "short"
    delta = sign * flow.get("delta_pct", 0)
    slope = sign * flow.get("cvd_slope", 0)
    persistence = flow.get("delta_persistence", 0)
    initiative = bool(flow.get("initiative_" + side))
    absorption = bool(flow.get("absorption_" + side)) and response
    stack = flow.get("stacked_buy" if sign > 0 else "stacked_sell", 0) >= 3
    acceleration = sign * (flow.get("cvd_acceleration") or 0) > 0
    run = flow.get("same_side_buy_run" if sign > 0 else "same_side_sell_run", 0) >= 5
    quality = flow.get("quality", {})
    trusted = "quality" not in flow or (
        quality.get("flow_trust_score") is not None and quality["flow_trust_score"] >= 0.6
    )
    opposing = delta <= -10 and slope < 0 and persistence >= 0.6
    supportive = (
        flow.get("available", False)
        and trusted
        and not opposing
        and (
            (
                delta >= 10
                and slope > 0
                and persistence >= 0.5
                and (initiative or stack or run or acceleration)
            )
            or (absorption and delta > -10 and slope >= 0)
        )
    )
    return dict(
        state="FLOW_OPPOSING" if opposing else "FLOW_SUPPORTIVE" if supportive else "FLOW_NEUTRAL",
        directional_delta=delta,
        directional_cvd_slope=slope,
        delta_persistence=persistence,
        initiative=initiative,
        absorption_response=absorption,
        stacked_imbalance=stack,
        aggressor_run=run,
        cvd_acceleration=acceleration,
        raw_fallback=bool(
            flow.get("available")
            and trusted
            and delta >= 15
            and persistence >= 0.60
            and slope > 0
            and (initiative or absorption)
        ),
        strong=bool(
            flow.get("available")
            and trusted
            and delta >= 20
            and persistence >= 0.75
            and slope > 0
            and (initiative or absorption or stack or run or acceleration)
        ),
        flow_trust=quality.get("flow_trust_score"),
        flow_quality_state=quality.get("flow_quality_state"),
    )


def structure_response(signal, bars, asof):
    trigger = signal.evidence.get("structural_trigger", {})
    level = trigger.get("level")
    sign = 1 if signal.direction == "LONG" else -1
    closed = [b for b in bars if signal.evidence.get("trigger_bar_end", 0) <= b.end <= asof]
    if not trigger.get("valid") or level is None or not closed:
        return False
    last = closed[-1]
    # Original family event plus a closed response through its actual level.
    return sign * (last.close - level) > 0 and (
        sign * (last.close - last.open) > 0
        or (last.low < level < last.close if sign > 0 else last.high > level > last.close)
    )


def market_alignment(signal, btc_features, eth_features, asof, response=False):
    if ":flow-quality-v1" in signal.version:
        return market_alignment_v7(signal, asof, response)
    sign = 1 if signal.direction == "LONG" else -1
    result = dict(
        policy="market-alignment-v2",
        alignment="FACTOR_RELATIONSHIP_UNCERTAIN",
        blocked=False,
        available_ms=asof,
        horizon=signal.horizon_profile,
        reasons=[],
    )
    if signal.symbol == "BTCUSDT":
        return result | dict(alignment="MARKET_NEUTRAL")
    contexts = signal.evidence.get("factor_timeframes", {})
    regimes = signal.evidence.get("factor_regimes", {})
    # Short emphasizes execution/setup; core consults all three; Swing ignores 5m/15m noise.
    timeframes = (
        [signal.execution_timeframe, signal.setup_timeframe]
        if signal.horizon_profile == "SHORT_INTRADAY"
        else [signal.setup_timeframe, signal.context_timeframe, signal.execution_timeframe]
    )
    reliable = []
    for tf in dict.fromkeys(timeframes):
        factor = contexts.get(
            tf, signal.evidence.get("market_factor", {}) if tf == signal.setup_timeframe else {}
        )
        btc = regimes.get(tf, {}).get("btc", btc_features if tf == signal.setup_timeframe else {})
        eth = regimes.get(tf, {}).get("eth", eth_features if tf == signal.setup_timeframe else {})
        if not factor.get("available") or factor.get("available_ms", asof + 1) > asof:
            continue
        beta, corr, stability = (
            factor.get(k) for k in ("beta_to_btc", "correlation_to_btc", "beta_stability_btc")
        )
        if beta is None or corr is None or stability is None or beta <= 0 or corr < 0.5 or stability > 0.5:
            continue
        direction = {"trending up": 1, "trending down": -1}.get(btc.get("regime"), 0)
        eth_direction = {"trending up": 1, "trending down": -1}.get(eth.get("regime"), 0)
        if direction and eth_direction and direction != eth_direction:
            return result | dict(reason="BTC_ETH_DISAGREEMENT")
        if direction and btc.get("efficiency", 0) >= 0.35:
            reliable.append((tf, direction, factor))
    if not reliable:
        factor = signal.evidence.get("market_factor", {})
        if factor.get("available") and btc_features.get("regime") == "range":
            result["alignment"] = "MARKET_NEUTRAL"
        return result
    if len({d for _, d, _ in reliable}) > 1:
        return result | dict(reason="HORIZON_FACTOR_DISAGREEMENT")
    tf, direction, factor = reliable[0]
    result.update(
        alignment="MARKET_ALIGNED" if direction == sign else "MARKET_CONTRARIAN",
        timeframe=tf,
        factor=factor,
        factor_timeframes=[t for t, _, _ in reliable],
    )
    if direction == sign or signal.horizon_profile not in INTRADAY:
        return result
    residual = sign * factor.get("residual_btc", 0)
    threshold = max(0.001, 0.5 * abs(factor.get("expected_return_btc", 0)))
    flow = flow_support(signal.evidence.get("flow", {}), sign, response)
    part = participation(
        signal.evidence.get("session_metrics", {}), sign, 0.70, ":flow-quality-v1" in signal.version
    )
    passed = (
        residual > threshold and flow["strong"] and (part["passed"] if part["mode"] == "PERCENTILE" else True)
    )
    result.update(
        blocked=not passed,
        directional_residual=residual,
        residual_threshold=threshold,
        participation=part,
        flow=flow,
    )
    if passed:
        result["alignment"] = "IDIOSYNCRATIC_DIVERGENCE"
    else:
        result.update(reason="CONTRARIAN_EVIDENCE_INSUFFICIENT", reasons=["CONTRARIAN_EVIDENCE_INSUFFICIENT"])
    return result


def market_alignment_v7(signal, asof, response=False):
    """Higher-timeframe BTC prior with a causal, demanding intraday override."""
    sign = 1 if signal.direction == "LONG" else -1
    result = dict(
        policy="market-alignment-v3",
        alignment="FACTOR_RELATIONSHIP_UNCERTAIN",
        market_alignment_state="FACTOR_RELATIONSHIP_UNCERTAIN",
        blocked=False,
        available_ms=asof,
        horizon=signal.horizon_profile,
        reasons=[],
        contrarian_override_passed=False,
    )
    if signal.symbol == "BTCUSDT":
        return result | dict(alignment="MARKET_NEUTRAL", market_alignment_state="MARKET_NEUTRAL")
    frames = signal.evidence.get("factor_timeframes", {})
    regimes = signal.evidence.get("factor_regimes", {})
    context_tf = signal.context_timeframe
    setup_tf = signal.setup_timeframe
    execution_tf = signal.execution_timeframe
    factor = frames.get(context_tf, {})
    btc = regimes.get(context_tf, {}).get("btc", {})
    eth = regimes.get(context_tf, {}).get("eth", {})
    beta, corr, stability = (
        factor.get(k) for k in ("beta_to_btc", "correlation_to_btc", "beta_stability_btc")
    )
    direction = {"trending up": 1, "trending down": -1}.get(btc.get("regime"), 0)
    eth_direction = {"trending up": 1, "trending down": -1}.get(eth.get("regime"), 0)
    if (
        not factor.get("available")
        or factor.get("available_ms", asof + 1) > asof
        or beta is None
        or beta <= 0
        or corr is None
        or corr < 0.5
        or stability is None
        or stability > 0.5
        or not direction
        or btc.get("efficiency", 0) < 0.35
        or (eth_direction and eth_direction != direction)
    ):
        return result | dict(reason="FACTOR_RELATIONSHIP_UNCERTAIN")
    result.update(
        htf_factor_direction="BULLISH" if direction > 0 else "BEARISH",
        htf_factor_strength=btc.get("efficiency"),
        timeframe=context_tf,
        factor=factor,
        factor_timeframes=[context_tf],
    )
    setup_regime = regimes.get(setup_tf, {}).get("btc", {}).get("regime")
    execution_regime = regimes.get(execution_tf, {}).get("btc", {}).get("regime")
    setup_direction = {"trending up": 1, "trending down": -1}.get(setup_regime, 0)
    execution_direction = {"trending up": 1, "trending down": -1}.get(execution_regime, 0)
    pullback = setup_direction == direction and execution_direction == -direction
    if (
        setup_direction == -direction
        and regimes.get(setup_tf, {}).get("btc", {}).get("efficiency", 0) >= 0.35
    ):
        result["timeframe_context"] = "FACTOR_REGIME_TRANSITION"
    elif pullback:
        result["timeframe_context"] = "HTF_TREND_WITH_LTF_PULLBACK"
    elif execution_direction == -direction:
        result["timeframe_context"] = "HTF_TREND_WITH_LTF_REVERSAL_ATTEMPT"
    if sign == direction:
        state = (
            "FACTOR_REGIME_TRANSITION"
            if result.get("timeframe_context") == "FACTOR_REGIME_TRANSITION"
            else "HTF_TREND_WITH_LTF_PULLBACK"
            if pullback
            else "MARKET_ALIGNED"
        )
        return result | dict(alignment=state, market_alignment_state=state)
    if signal.horizon_profile not in INTRADAY:
        return result | dict(alignment="MARKET_NEUTRAL", market_alignment_state="MARKET_NEUTRAL")
    flow = signal.evidence.get("flow", {})
    quality = flow.get("quality", {})
    auction = signal.evidence.get("auction", {})
    profile = signal.evidence.get("volume_profile", {})
    residual = sign * factor.get("residual_btc", factor.get("residual_return", 0))
    expected = abs(factor.get("expected_return_btc", 0))
    threshold = max(0.0015, 0.75 * expected)
    support = flow_support(flow, sign, response)
    price_response = (
        sign * quality.get("price_displacement_bps", 0) >= 3
        or quality.get("flow_quality_state") == "GENUINE_ABSORPTION"
        and response
    )
    acceptance = bool(
        response
        and (
            auction.get("acceptance_above" if sign > 0 else "acceptance_below")
            or profile.get("acceptance") == ("ABOVE_VALUE" if sign > 0 else "BELOW_VALUE")
            or profile.get("value_migration_direction") == ("UP" if sign > 0 else "DOWN")
        )
    )
    part = participation(signal.evidence.get("session_metrics", {}), sign, 0.70, True)
    trusted_participation = part["passed"] if part["mode"] == "PERCENTILE" else True
    passed = bool(
        residual > threshold
        and support["strong"]
        and price_response
        and acceptance
        and trusted_participation
        and (quality.get("flow_trust_score") or 0) >= 0.6
        and quality.get("flow_quality_state") != "REPETITIVE_TWO_SIDED_CHURN"
    )
    state = "IDIOSYNCRATIC_DIVERGENCE" if passed else "MARKET_CONTRARIAN_WEAK"
    result.update(
        alignment=state,
        market_alignment_state=state,
        blocked=not passed,
        reason=None if passed else "HTF_MARKET_CONFLICT_WITHOUT_STRONG_DIVERGENCE",
        reasons=[] if passed else ["HTF_MARKET_CONFLICT_WITHOUT_STRONG_DIVERGENCE"],
        directional_residual=residual,
        residual_threshold=threshold,
        contrarian_override_passed=passed,
        flow=support,
        price_response=price_response,
        auction_acceptance=acceptance,
        participation=part,
        cross_venue_support=signal.evidence.get("cross_exchange", {}).get(
            "cross_venue_trusted_flow_agreement"
        ),
    )
    return result


def evaluate_intraday_confirmation(signal, flow, bars, asof, alignment=None):
    sign = 1 if signal.direction == "LONG" else -1
    response = structure_response(signal, bars, asof)
    support = flow_support(flow, sign, response)
    metrics = signal.evidence.get("session_metrics", {})
    part = participation(metrics, sign, 0.60, ":flow-quality-v1" in signal.version)
    part_ok = part["passed"] if part["mode"] == "PERCENTILE" else support["raw_fallback"]
    profile = signal.evidence.get("volume_profile", {})
    valid_profile = (
        profile.get("available")
        and profile.get("coverage_complete")
        and profile.get("available_ms", asof + 1) <= asof
        and profile.get("end_ms", asof + 1) <= asof
    )
    trusted_profile = ":flow-quality-v1" in signal.version
    profile_ok = bool(
        valid_profile
        and (profile.get("profile_confidence") == "HIGH" if trusted_profile else True)
        and (
            profile.get(
                ("effective_" if trusted_profile else "")
                + ("rejection_low" if sign > 0 else "rejection_high")
            )
            or profile.get(
                ("effective_" if trusted_profile else "") + ("excess_low" if sign > 0 else "excess_high")
            )
            or profile.get("value_migration_direction") == ("UP" if sign > 0 else "DOWN")
        )
    )
    if profile.get("acceptance") == ("BELOW_VALUE" if sign > 0 else "ABOVE_VALUE"):
        profile_ok = False
    factor_ok = not (alignment or {}).get("blocked", False)
    passed = bool(response and support["state"] == "FLOW_SUPPORTIVE" and part_ok and factor_ok)
    stressed = (
        signal.horizon_profile == "SHORT_INTRADAY"
        and signal.family in REVERSALS
        and signal.evidence.get("wick_regime", {}).get("state") in {"WICKY", "LIQUIDITY_STRESS"}
    )
    hold_ok = reclaim_hold(signal, bars, asof) if stressed else True
    if stressed:
        passed = passed and hold_ok and profile_ok
    reasons = [] if passed else ["UNCONFIRMED_LIQUIDITY_SWEEP"]
    if not factor_ok:
        reasons.append((alignment or {}).get("reason") or "CONTRARIAN_EVIDENCE_INSUFFICIENT")
    if trusted_profile and (flow.get("quality", {}).get("flow_trust_score") or 0) < 0.6:
        passed = False
        reasons.append("LOW_INFORMATION_EXECUTED_FLOW")
    return dict(
        policy="intraday-confirmation-v2",
        passed=passed,
        reason_codes=reasons,
        structure_support=response,
        flow_support=support["state"],
        participation_support=bool(part_ok),
        factor_support=factor_ok,
        profile_support=profile_ok,
        reclaim_hold_support=hold_ok,
        participation_mode=part["mode"],
        metrics=dict(flow=support, participation=part, baseline=metrics),
        available_ms=asof,
        window_start_ms=signal.coverage.get("window_start_ms"),
        window_end_ms=signal.coverage.get("window_end_ms"),
        sample_count=metrics.get("baseline_samples", 0),
    )
