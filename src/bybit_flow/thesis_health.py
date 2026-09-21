"""Versioned causal monitoring; frozen plans and historical scores never change."""

POLICY = "thesis-health-v1"


def summary(store):
    import json

    result = dict(
        policy=POLICY,
        active_healthy=0,
        active_degraded=0,
        withdrawn_toxic_flow=0,
        withdrawn_structure_failure=0,
    )
    for key, text in store.db.execute("SELECT key,payload FROM kv WHERE key LIKE 'thesis_health:%'"):
        health = json.loads(text)
        signal = store.db.execute(
            "SELECT state FROM signals WHERE id=?", (key.removeprefix("thesis_health:"),)
        ).fetchone()
        if not signal:
            continue
        if signal[0] not in {"INVALIDATED", "EXPIRED", "RESOLVED"}:
            result["active_healthy" if health["state"] == "HEALTHY" else "active_degraded"] += 1
        elif health.get("withdraw"):
            result["withdrawn_toxic_flow"] += 1
    return result


def evaluate(signal, observation, previous, now):
    sign = 1 if signal.direction == "LONG" else -1
    hold = (
        1_800_000
        if signal.horizon_profile in {"SWING", "EXTENDED_SWING"}
        else 120_000
        if signal.horizon_profile == "SHORT_INTRADAY"
        else 180_000
    )
    result = dict(
        policy=POLICY,
        checked_ms=now,
        state="HEALTHY",
        withdraw=False,
        reasons=[],
        production_enabled=":autonomy-v1" in signal.version,
        observation=observation,
    )
    if not observation.get("coverage_complete"):
        return result | dict(state="DEGRADED", reasons=["Insufficient live evidence"], adverse_since_ms=None)
    flow = observation.get("flow", {})
    adverse_flow = (
        sign * flow.get("delta_pct", 0) <= -20
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
    if not adverse_flow:
        return result | dict(
            state="DEGRADED" if structural or adverse_book else "HEALTHY", adverse_since_ms=None
        )
    # Gaps and delayed observations reset persistence; repeated evaluation of one
    # old window cannot accumulate evidence indefinitely.
    continuous = now - previous.get("checked_ms", 0) <= 90_000
    since = previous.get("adverse_since_ms") if continuous else None
    since = since if since is not None else now
    result.update(state="DEGRADED", adverse_since_ms=since)
    new_window = observation.get("window_end_ms", 0) > previous.get("observation", {}).get("window_end_ms", 0)
    if structural and adverse_book and now - since >= hold and new_window:
        result.update(
            state="TOXIC", withdraw=result["production_enabled"], reason="TOXIC_FLOW_WITH_STRUCTURAL_FAILURE"
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
