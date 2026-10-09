"""Explicit feature allowlist; no outcome, lifecycle, model prediction or rejection leakage."""

import math
from datetime import UTC, datetime

# (group, path, definition). Missing values are NOT zero-valued observations.
CATALOG = {
    **{
        f"{tf}_{key}": ("regime", f"evidence.{tf}.{key}", definition)
        for tf in ("h4", "h1", "m15")
        for key, definition in {
            "atr": "14 closed-bar mean true range in quote price units",
            "realized_volatility": "RMS of last 20 closed-bar log returns",
            "efficiency": "absolute 20-bar price change / sum of absolute changes",
            "slope": "5-bar change in MA20 divided by 5 ATR",
            "volume_expansion": "latest closed volume / preceding volume baseline",
            "premium_discount": "close location in confirmed dealing range",
        }.items()
    },
    **{
        key: ("smc", f"evidence.h1.{key}", definition)
        for key, definition in {
            "sweep_long": "known sell-side level breached and closed above",
            "sweep_short": "known buy-side level breached and closed below",
            "choch": "confirmed change of character: up +1, down -1; absent is missing",
            "bos": "confirmed structure break: up +1, down -1; absent is missing",
        }.items()
    },
    **{
        key: ("orderflow", f"evidence.flow.{key}", definition)
        for key, definition in {
            "buy_base": "executed aggressive buy volume in base units",
            "sell_base": "executed aggressive sell volume in base units",
            "delta_base": "aggressive buy minus sell base volume",
            "delta_notional": "signed executed quote notional",
            "delta_pct": "100*(buy-sell)/(buy+sell), execution window",
            "cvd": "execution-window cumulative signed volume, not session CVD",
            "stacked_buy": "maximum adjacent diagonal buy imbalance run",
            "stacked_sell": "maximum adjacent diagonal sell imbalance run",
            "absorption_long": "aggressive selling with defended passive bid evidence",
            "absorption_short": "aggressive buying with defended passive ask evidence",
            "initiative_long": "positive delta and positive displacement",
            "initiative_short": "negative delta and negative displacement",
            "delta_divergence": "signed delta opposes window displacement",
            "potential_trapped_buyers": "positive delta and upward excursion followed by rejection below start/VWAP; heuristic, not inventory",
            "potential_trapped_sellers": "negative delta and downward excursion followed by reclaim above start/VWAP; heuristic, not inventory",
            "average_trade_size": "mean base size of venue-reported execution records; aggregates differ by venue",
            "trades_per_second": "observed trade count / window duration seconds",
            "poc": "maximum executed volume bucket price",
            "vah": "upper contiguous 70-percent volume area boundary",
            "val": "lower contiguous 70-percent volume area boundary",
            "vwap": "executed quote notional / base volume",
        }.items()
    },
    **{
        f"book_{key}": ("microstructure", f"evidence.book.{key}", definition)
        for key, definition in {
            "spread_bps": "quoted spread / mid * 10000",
            "microprice": "opposite best-size weighted mid price",
            "visible_flow_imbalance": "bid additions minus ask additions; not cancellation attribution",
            "depth.10.imbalance": "(bid-ask)/(bid+ask) notional within 10 bps",
            "replenishment_60s.bid": "visible positive bid size changes times price, 60 seconds",
            "replenishment_60s.ask": "visible positive ask size changes times price, 60 seconds",
        }.items()
    },
    **{
        key: ("derivatives", f"evidence.derivatives.{key}", definition)
        for key, definition in {
            "oi_change_pct": "observed base open-interest percentage change",
            "oi_notional": "reported quote open-interest value",
            "funding_rate": "predicted funding rate, not a settled payment",
        }.items()
    },
    **{
        key: ("execution", f"risk.{key}", definition)
        for key, definition in {
            "net_rr": "planned reward/risk after configured costs",
            "cost_per_base": "fee, slippage and funding assumptions per base unit",
            "quantity": "hypothetical stop-risk-sized quantity, absent without equity",
            "estimated_margin": "illustrative notional/leverage margin",
        }.items()
    },
}
CATALOG.update(
    {
        "macro_minutes_to_high_impact_usd": (
            "macro",
            "evidence.macro.minutes_to_high_impact_usd",
            "Scheduled high USD event proximity known at decision time; no release result",
        ),
        "macro_pause_active": (
            "macro",
            "evidence.macro.macro_pause_active",
            "Scheduled New York session USD high-impact pause flag",
        ),
        "btc_regime_impulse": (
            "market_alignment",
            "evidence.market_alignment.btc_impulse_strength",
            "Causal BTC setup-timeframe slope in ATR units",
        ),
        "market_conflict_blocked": (
            "market_alignment",
            "evidence.market_alignment.blocked",
            "Versioned market-conflict validity gate at decision time",
        ),
    }
)
for _key in (
    "flow_trust_score",
    "gross_notional",
    "effective_gross_notional",
    "effective_volume_ratio",
    "net_to_gross_ratio",
    "aggressor_side_alternation_rate",
    "same_price_alternation_rate",
    "same_size_repeat_ratio",
    "mirror_pair_ratio",
    "size_entropy",
    "size_concentration",
    "impact_persistence",
    "book_response_consistency",
    "raw_vs_effective_profile_shift",
):
    CATALOG[_key] = (
        "orderflow",
        "evidence.flow.quality." + _key,
        "Causal flow-quality-v1 decision-window observation",
    )
CATALOG["cross_venue_trusted_flow_agreement"] = (
    "cross_market",
    "evidence.cross_exchange.cross_venue_trusted_flow_agreement",
    "Two causally aligned venues with trusted effective delta direction",
)
for _key in ("htf_factor_strength", "directional_residual", "contrarian_override_passed"):
    CATALOG[_key] = (
        "market_alignment",
        "evidence.market_alignment." + _key,
        "Causal higher-timeframe market prior at decision time",
    )

CONTEXT = (
    "family",
    "direction",
    "regime",
    "source",
    "liquidity_bucket",
    "score_profile",
    "horizon_profile",
    "entry_session",
    "flow_quality_state",
    "market_alignment_state",
    "htf_factor_direction",
)
for _group, _fields in {
    "range": ("location", "width_atr", "compression", "boundary_interactions", "breakout_distance_atr"),
    "auction": ("poc_shift", "value_overlap"),
    "flow": (
        "buy_efficiency",
        "sell_efficiency",
        "delta_persistence",
        "cvd_slope",
        "cvd_acceleration",
        "same_side_run",
        "signed_aggressor_notional",
        "directional_price_change",
        "impact_per_signed_notional",
        "flow_efficiency",
        "impact_persistence",
        "absorption_ratio",
        "reversal_after_flow",
    ),
    "book": ("obi_touch", "obi_5bps", "obi_10bps", "obi_25bps", "obi_persistence", "microprice_minus_mid"),
    "market_factor": (
        "beta_to_btc",
        "beta_to_eth",
        "correlation_to_btc",
        "correlation_to_eth",
        "residual_return",
    ),
    "execution": ("stop_noise_ratio", "depth_consumed", "volatility_ratio"),
    "derivatives": (
        "price_up_oi_up",
        "price_up_oi_down",
        "price_down_oi_up",
        "price_down_oi_down",
        "crowding_score",
        "deleveraging_score",
        "basis",
    ),
    "session_metrics": (
        "baseline_samples",
        "spread_relative",
        "depth_relative",
        "trade_intensity_relative",
        "turnover_relative",
        "delta_relative",
    ),
}.items():
    for _field in _fields:
        CATALOG[_group + "_" + _field] = (
            _group,
            "evidence." + _group + "." + _field,
            "Causal observed " + _group + " " + _field,
        )
for _metric in (
    "volume",
    "trade_count",
    "trade_intensity",
    "aggressive_buy_notional",
    "aggressive_sell_notional",
    "delta_magnitude",
    "cvd_slope",
    "replenishment",
    "obi",
):
    CATALOG["participation_" + _metric + "_percentile"] = (
        "participation",
        "evidence.session_metrics." + _metric + "_percentile",
        "Prior same-instrument venue horizon session empirical percentile; missing until 20 windows",
    )
CATALOG["quality_score"] = ("quality", "quality", "Frozen combined quality; never calibrated probability")
for _category in (
    "regime",
    "structure",
    "orderflow",
    "derivatives",
    "execution",
    "fundamentals",
    "cross_market",
):
    CATALOG["score_" + _category] = (
        "quality",
        "evidence.score_components." + _category + ".earned",
        "Frozen earned category points",
    )

# V8 observations are descriptive and remain missing until causally observed.
for _group, _fields in {
    "context_ev": {
        "context_ev_samples": "samples",
        "context_ev_shrunk_expectancy_r": "shrunk_expectancy_r",
        "context_ev_lower_bound_r": "lower_confidence_bound_r",
    },
    "liquidation": {
        key: key
        for key in (
            "liquidation_imbalance",
            "liquidation_intensity_percentile",
            "liquidation_acceleration",
            "liquidation_price_response_bps",
            "oi_change_1m_pct",
            "oi_change_5m_pct",
            "oi_change_since_first_liquidation_pct",
        )
    },
    "breadth": {
        **{
            "breadth_" + key: key
            for key in (
                "pct_positive_return_1h",
                "pct_negative_return_1h",
                "pct_positive_return_4h",
                "pct_negative_return_4h",
                "pct_uptrend_1h",
                "pct_downtrend_1h",
                "pct_uptrend_4h",
                "pct_downtrend_4h",
                "positive_btc_residual_fraction",
                "negative_btc_residual_fraction",
            )
        },
        "breadth_change_1h": "breadth_change_1h",
        "breadth_cross_sectional_dispersion": "cross_sectional_return_dispersion",
        "breadth_impulse_15m": "breadth_impulse_15m",
    },
    "ofi": {
        key: key
        for key in (
            "ofi_normalized_l1",
            "ofi_normalized_5bps",
            "ofi_normalized_10bps",
            "ofi_persistence",
            "ofi_acceleration",
            "ofi_l1_60s",
            "ofi_5bps_60s",
            "ofi_10bps_60s",
            "ofi_strength_percentile",
            "ofi_coverage_seconds",
            "mid_response_bps",
            "microprice_response_bps",
        )
    },
    "anchored": {
        key: key
        for key in (
            "distance_setup_avwap_atr",
            "setup_avwap_slope",
            "setup_avwap_acceptance_ratio",
            "time_above_vah_ratio",
            "time_below_val_ratio",
            "time_inside_value_ratio",
            "time_above_poc_ratio",
            "time_below_poc_ratio",
        )
    },
    "spot_perp": {
        key: key
        for key in (
            "spot_return_1m",
            "spot_return_5m",
            "perp_return_1m",
            "perp_return_5m",
            "spot_trade_window_complete",
            "price_coverage_complete",
            "spot_perp_return_spread",
            "spot_perp_basis_bps",
            "basis_change_bps",
            "spot_perp_delta_agreement",
            "spot_perp_price_agreement",
            "spot_lead_lag_ms",
        )
    },
    "volatility": {
        key: key
        for key in (
            "rv_ratio",
            "vol_of_vol",
            "parkinson_volatility",
            "jump_ratio",
            "range_expansion_percentile",
        )
    },
}.items():
    for _name, _field in _fields.items():
        if _name == "basis_change_bps":
            _name = "spot_perp_basis_change_bps"
        CATALOG[_name] = (_group, f"evidence.{_group}.{_field}", f"Causal V8 research observation: {_field}")

CATALOG["v8_gate_ready"] = (
    "v8_gate",
    "evidence.v8_gate.gate_ready",
    "Explicit readiness of frozen V8 production policy",
)
CATALOG["v8_gate_adverse"] = (
    "v8_gate",
    "evidence.v8_gate.hard_adverse",
    "Ready adverse V8 evidence; independent of global live switch",
)
for _family in ("spot_perp", "ofi", "breadth", "anchored", "liquidation", "volatility", "context_ev"):
    CATALOG["v8_" + _family + "_ready"] = (
        "v8_gate",
        f"evidence.v8_gate.feature_states.{_family}.gate_ready",
        "Causal feature-specific production readiness",
    )


def lookup(value, path):
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


V20_CATALOG = {
    **CATALOG,
    **{
        f"v2_{name}": ("v2", f"evidence.v2.{name}.score", f"v2-score-1 deterministic {name}; not probability")
        for name in ("location", "mechanism", "trade_offer")
    },
    "v2_evidence_confidence": ("v2", "evidence.v2.evidence_confidence", "v2-score-1 evidence confidence cap"),
    "v2_barrier_count": (
        "v2",
        "evidence.v2.trade_offer.barrier_count",
        "Observed distinct reference barriers from entry to original TP1",
    ),
    "v2_cost_risk_fraction": (
        "v2",
        "evidence.v2.trade_offer.cost_risk_fraction",
        "Estimated original offer costs / original stop distance",
    ),
    "v2_event_efficiency": (
        "event_rotation_state",
        "evidence.event_rotation_state.price_efficiency",
        "Closed causal activity rotation displacement / price path",
    ),
    "v2_size_divergence": (
        "trade_size_state",
        "evidence.trade_size_state.large_small_divergence",
        "Prior-quantile large versus small signed-print divergence",
    ),
}


def snapshot(signal, decision_ms, stage, membership=None):
    """Receipt-time snapshot. No backdating late market observations to candle timestamps."""
    if decision_ms < signal.created_ms:
        raise ValueError("Candidate cannot be available before its creation")
    schema = signal.feature_schema_version
    payload = signal.model_dump(mode="json")
    values, metadata = {}, {}
    for name, (group, path, definition) in (V20_CATALOG if schema == "candidate-v20" else CATALOG).items():
        actual_path = path
        if signal.horizon_profile != "LEGACY":
            for old, new in (
                ("h4", "context_features"),
                ("h1", "setup_features"),
                ("m15", "execution_features"),
            ):
                if new in signal.evidence:
                    actual_path = actual_path.replace("evidence." + old + ".", "evidence." + new + ".")
        value = lookup(payload, actual_path)
        if name in {"bos", "choch"}:
            value = {"up": 1.0, "down": -1.0}.get(value)
        source = signal.source + (
            ":classified-footprint"
            if signal.source == "tradingview" and group == "orderflow"
            else ":deterministic-observation"
        )
        prefix = actual_path.split(".")[1] if actual_path.startswith("evidence.") else "risk"
        section = signal.evidence.get(prefix, {})
        section = section if isinstance(section, dict) else {}
        if prefix in {"event_rotation_state", "trade_size_state"} and not section.get("available"):
            value = None
        if prefix == "context_ev" and section.get("status") != "AVAILABLE":
            value = None
        source = section.get("source", source)
        source_ms = section.get("source_ms", section.get("asof", section.get("event_ms", decision_ms)))
        available_ms = section.get(
            "available_ms", section.get("receipt_ms", section.get("collected_ms", decision_ms))
        )
        missing = (
            not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not isinstance(source_ms, (int, float))
            or not math.isfinite(source_ms)
            or not isinstance(available_ms, (int, float))
            or not math.isfinite(available_ms)
            or source_ms > decision_ms
            or available_ms > decision_ms
        )
        values[name] = None if missing else float(value)
        metadata[name] = dict(
            group=group,
            source=source,
            source_ms=source_ms,
            available_ms=available_ms,
            definition=definition,
            version=schema,
            timeframe={
                "context_features": signal.context_timeframe,
                "setup_features": signal.setup_timeframe,
                "execution_features": signal.execution_timeframe,
                "h4": "240",
                "h1": "60",
                "m15": "15",
            }.get(prefix),
            missing=missing,
        )
    # Derived features use only the frozen plan, not subsequent execution outcomes.
    atr = values.get("h1_atr")
    derived = {
        "stop_atr": abs(signal.entry - signal.stop) / atr if atr else None,
        "utc_hour": datetime.fromtimestamp(decision_ms / 1000, UTC).hour,
        "fundamental_coverage": sum(
            bool(f.get("source") and f.get("definition"))
            for f in signal.evidence.get("fundamentals", [])
            if f.get("known_ms", decision_ms + 1) <= decision_ms < f.get("expires_ms", 0)
        ),
    }
    for key, value in derived.items():
        values[key] = value
        metadata[key] = dict(
            group="execution" if key == "stop_atr" else "context",
            source=signal.source + ":frozen-plan",
            source_ms=decision_ms,
            available_ms=decision_ms,
            definition=key,
            version=schema,
            missing=value is None,
        )
    for key in CONTEXT:
        values[key] = (
            signal.evidence.get("flow", {}).get("quality", {}).get("flow_quality_state")
            if key == "flow_quality_state"
            else signal.evidence.get("market_alignment", {}).get("market_alignment_state")
            if key == "market_alignment_state"
            else signal.evidence.get("market_alignment", {}).get("htf_factor_direction")
            if key == "htf_factor_direction"
            else getattr(signal, key, None)
        )
    values["liquidity_bucket"] = "observed" if membership and membership["eligible"] else "unknown"
    values["score_profile"] = signal.evidence.get("score_profile", "unscored")
    for key in CONTEXT:
        metadata[key] = dict(
            group="context",
            source=signal.source + ":frozen-plan",
            source_ms=decision_ms,
            available_ms=decision_ms,
            definition="candidate context: " + key,
            version=schema,
            missing=values[key] is None,
        )
    return dict(
        signal_id=signal.id,
        decision_ms=decision_ms,
        stage=stage,
        schema_version=schema,
        source=signal.source,
        signal=payload,
        values=values,
        feature_metadata=metadata,
        membership=membership,
        universe_scope="recorded-membership" if membership else "restricted-universe",
        data_coverage=sum(v is not None for k, v in values.items() if k not in CONTEXT)
        / (len(metadata) - len(CONTEXT)),
    )
