"""Versioned causal monitoring; frozen plans and historical scores never change."""

POLICY = "thesis-health-v2"


def summary(store):
    result = dict(
        policy=POLICY,
        eligible=0,
        current=0,
        healthy=0,
        degraded=0,
        toxic=0,
        paused_for_coverage=0,
        unavailable=0,
    )
    for signal in store.active_signals():
        if signal.get("source") == "tradingview":
            continue
        result["eligible"] += 1
        health = store.get("thesis_health:" + signal["id"], {})
        if signal.get("coverage", {}).get("monitoring") == "paused":
            result["paused_for_coverage"] += 1
        elif not health or health.get("coverage_status") == "UNAVAILABLE":
            result["unavailable"] += 1
        elif health.get("state") != "HEALTHY":
            result["degraded"] += 1
            result["toxic"] += int(health.get("state") == "TOXIC")
        else:
            result["current"] += 1
            result["healthy"] += 1
    result["active_healthy"] = result["healthy"]
    result["active_degraded"] = result["degraded"]
    for key in ("eligible", "current", "degraded", "paused_for_coverage", "unavailable"):
        result["health_" + key] = result[key]
    return result


def evaluate(signal, observation, previous, now):
    sign = 1 if signal.direction == "LONG" else -1
    hold = (
        1_800_000
        if signal.horizon_profile in {"SWING", "EXTENDED_SWING"}
        else 120_000
        if signal.horizon_profile == "SHORT_INTRADAY"
        else 600_000
    )
    result = dict(
        policy=POLICY,
        checked_ms=now,
        state="HEALTHY",
        withdraw=False,
        reasons=[],
        production_enabled=True,
        observation=observation,
        **{
            name + "_health": "UNAVAILABLE"
            for name in (
                "flow",
                "liquidity",
                "structure",
                "auction",
                "factor",
                "derivatives",
                "cross_venue",
                "volatility_liquidity",
            )
        },
    )
    if not observation.get("coverage_complete"):
        return result | dict(state="DEGRADED", reasons=["Insufficient live evidence"], adverse_since_ms=None)
    flow = observation.get("flow", {})
    quality = flow.get("quality", {})
    trusted_flow = "quality" not in flow or (
        quality.get("flow_trust_score") is not None and quality["flow_trust_score"] >= 0.6
    )
    low_information_flow = quality.get("flow_quality_state") == "REPETITIVE_TWO_SIDED_CHURN"
    adverse_flow = (
        trusted_flow
        and sign * flow.get("delta_pct", 0) <= -20
        and sign * flow.get("cvd_slope", 0) < 0
        and flow.get("delta_persistence", 0) >= 0.75
    )
    adverse_book = sign * (observation.get("obi") or 0) <= -0.2
    structural = observation.get("structural_failure", False)
    if adverse_flow:
        result["reasons"].append("Persistent opposing executed flow")
    if adverse_book:
        result["reasons"].append("Supporting depth imbalance reversed")
    if structural:
        result["reasons"].append("Setup-timeframe structural reclaim failed")
    higher = signal.horizon_profile in {"SWING", "EXTENDED_SWING"}
    if higher and observation.get("structure_timeframe") not in {"240", "D"}:
        structural = False
    if higher and structural:
        return result | dict(
            state="HARD_FAILURE", withdraw=True, reason="STRUCTURAL_FAILURE", structure_health="HARD_FAILURE"
        )
    result.update(
        flow_health="DEGRADED" if adverse_flow or low_information_flow else "HEALTHY",
        liquidity_health="DEGRADED" if adverse_book else "HEALTHY",
        structure_health="DEGRADED" if structural else "HEALTHY",
        auction_health=observation.get("auction_health", "UNAVAILABLE"),
        factor_health=observation.get("factor_health", "UNAVAILABLE"),
        derivatives_health=observation.get("derivatives_health", "UNAVAILABLE"),
        cross_venue_health=observation.get("cross_venue_health", "UNAVAILABLE"),
        volatility_liquidity_health=observation.get("volatility_liquidity_health", "UNAVAILABLE"),
    )
    adverse_auction = result["auction_health"] == "DEGRADED"
    adverse_factor = result["factor_health"] == "DEGRADED"
    secondary = sum(
        result[key + "_health"] == "DEGRADED"
        for key in ("derivatives", "cross_venue", "volatility_liquidity")
    )
    if higher and not structural:
        # Short-term tape is contextual only. Higher horizons need sustained
        # value/acceptance deterioration with independent factor evidence.
        if not (adverse_auction and adverse_factor):
            return result | dict(
                state="DEGRADED" if adverse_auction or adverse_factor else "HEALTHY", adverse_since_ms=None
            )
        continuous = now - previous.get("checked_ms", 0) <= 90000
        since = previous.get("adverse_since_ms") if continuous else None
        since = now if since is None else since
        toxic = now - since >= (14400000 if signal.horizon_profile == "EXTENDED_SWING" else 1800000)
        return result | dict(
            state="TOXIC" if toxic else "DEGRADED",
            withdraw=toxic,
            adverse_since_ms=since,
            reason="AUCTION_FAILURE" if toxic else None,
        )
    if not adverse_flow:
        return result | dict(
            state="DEGRADED"
            if structural or adverse_book or adverse_auction or adverse_factor
            else "HEALTHY",
            adverse_since_ms=None,
        )
    # Gaps and delayed observations reset persistence; repeated evaluation of one
    # old window cannot accumulate evidence indefinitely.
    continuous = now - previous.get("checked_ms", 0) <= 90_000
    since = previous.get("adverse_since_ms") if continuous else None
    since = since if since is not None else now
    result.update(state="DEGRADED", adverse_since_ms=since)
    new_window = observation.get("window_end_ms", 0) > previous.get("observation", {}).get("window_end_ms", 0)
    if (
        (
            structural
            and (adverse_book or secondary >= 1)
            or adverse_auction
            and (adverse_factor or secondary >= 2)
        )
        and now - since >= hold
        and new_window
    ):
        result.update(
            state="TOXIC", withdraw=result["production_enabled"], reason="THESIS_WITHDRAWN_BEFORE_STOP"
        )
    return result


def market_gate(signal, btc_features, eth_features):
    """A common-factor conflict is a validity gate, never five cosmetic points."""
    factor, flow = signal.evidence.get("market_factor", {}), signal.evidence.get("flow", {})
    sign = 1 if signal.direction == "LONG" else -1
    result = dict(policy="market-alignment-v1", alignment="UNCERTAIN", blocked=False)
    if signal.symbol == "BTCUSDT" or not factor.get("available"):
        return result
    beta, corr, stability = (
        factor.get(k) for k in ("beta_to_btc", "correlation_to_btc", "beta_stability_btc")
    )
    if beta is None or corr is None or stability is None or beta <= 0 or corr < 0.5 or stability > 0.5:
        return result
    regime = btc_features.get("regime")
    direction = 1 if regime == "trending up" else -1 if regime == "trending down" else 0
    eth = eth_features.get("regime")
    disagreement = eth in {"trending up", "trending down"} and eth != regime
    if disagreement or not direction or btc_features.get("efficiency", 0) < 0.35:
        return result | dict(alignment="MARKET-NEUTRAL")
    result.update(
        alignment="MARKET-ALIGNED" if sign == direction else "MARKET-CONTRARIAN",
        btc_regime=regime,
        btc_impulse_strength=btc_features.get("slope"),
        btc_beta=beta,
        btc_correlation=corr,
    )
    if sign != direction and signal.horizon_profile in {"SHORT_INTRADAY", "CORE_INTRADAY"}:
        residual = sign * factor.get("residual_btc", factor.get("residual_return", 0))
        expected = abs(factor.get("expected_return_btc", 0))
        participation = (
            sign * flow.get("delta_pct", 0) >= 20
            and flow.get("delta_persistence", 0) >= 0.75
            and sign * flow.get("cvd_slope", 0) > 0
            and flow.get("initiative_long" if sign == 1 else "initiative_short", False)
        )
        result["blocked"] = not (residual > max(0.001, 0.5 * expected) and participation)
        if result["blocked"]:
            result["reason"] = "MARKET_CONFLICT_WITHOUT_IDIOSYNCRATIC_CONFIRMATION"
        else:
            result["alignment"] = "IDIOSYNCRATIC"
    return result
