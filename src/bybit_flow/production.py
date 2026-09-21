"""Explicit deterministic production validity policies; no profitability claim."""

INTRADAY = {"SHORT_INTRADAY", "CORE_INTRADAY"}
REVERSALS = {"liquidity_sweep", "range_rejection"}


def participation(metrics, sign, threshold):
    """Count categories, not correlated volume/count/intensity aliases."""
    mature = metrics.get("baseline_samples", 0) >= 20
    groups = {
        "activity": ("trade_count", "trade_intensity"),
        "size": ("volume", "aggressive_buy_notional" if sign > 0 else "aggressive_sell_notional"),
        "imbalance": ("delta_magnitude",),
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
    opposing = delta <= -10 and slope < 0 and persistence >= 0.6
    supportive = (
        flow.get("available", False)
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
            and delta >= 15
            and persistence >= 0.60
            and slope > 0
            and (initiative or absorption)
        ),
        strong=bool(
            flow.get("available")
            and delta >= 20
            and persistence >= 0.75
            and slope > 0
            and (initiative or absorption or stack or run or acceleration)
        ),
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
    part = participation(signal.evidence.get("session_metrics", {}), sign, 0.70)
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


def evaluate_intraday_confirmation(signal, flow, bars, asof, alignment=None):
    sign = 1 if signal.direction == "LONG" else -1
    response = structure_response(signal, bars, asof)
    support = flow_support(flow, sign, response)
    metrics = signal.evidence.get("session_metrics", {})
    part = participation(metrics, sign, 0.60)
    part_ok = part["passed"] if part["mode"] == "PERCENTILE" else support["raw_fallback"]
    profile = signal.evidence.get("volume_profile", {})
    valid_profile = (
        profile.get("available")
        and profile.get("coverage_complete")
        and profile.get("available_ms", asof + 1) <= asof
        and profile.get("end_ms", asof + 1) <= asof
    )
    profile_ok = bool(
        valid_profile
        and (
            profile.get("rejection_low" if sign > 0 else "rejection_high")
            or profile.get("value_migration_direction") == ("UP" if sign > 0 else "DOWN")
        )
    )
    factor_ok = not (alignment or {}).get("blocked", False)
    passed = bool(response and support["state"] == "FLOW_SUPPORTIVE" and part_ok and factor_ok)
    reasons = [] if passed else ["UNCONFIRMED_LIQUIDITY_SWEEP"]
    if not factor_ok:
        reasons.append("CONTRARIAN_EVIDENCE_INSUFFICIENT")
    return dict(
        policy="intraday-confirmation-v2",
        passed=passed,
        reason_codes=reasons,
        structure_support=response,
        flow_support=support["state"],
        participation_support=bool(part_ok),
        factor_support=factor_ok,
        profile_support=profile_ok,
        participation_mode=part["mode"],
        metrics=dict(flow=support, participation=part, baseline=metrics),
        available_ms=asof,
        window_start_ms=signal.coverage.get("window_start_ms"),
        window_end_ms=signal.coverage.get("window_end_ms"),
        sample_count=metrics.get("baseline_samples", 0),
    )
