"""Explicit feature allowlist; no outcome, lifecycle, model prediction or rejection leakage."""

import math
from datetime import UTC, datetime

from . import SCHEMA_VERSION

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
CATALOG.update({
    "macro_minutes_to_high_impact_usd": ("macro", "evidence.macro.minutes_to_high_impact_usd", "Scheduled high USD event proximity known at decision time; no release result"),
    "macro_pause_active": ("macro", "evidence.macro.macro_pause_active", "Scheduled New York session USD high-impact pause flag"),
    "btc_regime_impulse": ("market_alignment", "evidence.market_alignment.btc_impulse_strength", "Causal BTC setup-timeframe slope in ATR units"),
    "market_conflict_blocked": ("market_alignment", "evidence.market_alignment.blocked", "Versioned market-conflict validity gate at decision time"),
})

CONTEXT = (
    "family",
    "direction",
    "regime",
    "source",
    "liquidity_bucket",
    "score_profile",
    "horizon_profile",
    "entry_session",
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

# Source-specific fields; TV classified volumes never populate native buy_base/sell_base.
for _key in (
    "regime_slope_atr",
    "regime_efficiency",
    "atr",
    "buy_volume",
    "sell_volume",
    "level",
    "setup_low",
    "setup_high",
    "setup_close",
    "bos_level",
    "fvg_low",
    "fvg_high",
    "turnover_median_7d",
    "continuous_days",
):
    CATALOG["tv_" + _key] = (
        "orderflow"
        if _key in {"buy_volume", "sell_volume"}
        else "smc"
        if _key in {"level", "setup_low", "setup_high", "setup_close", "bos_level", "fvg_low", "fvg_high"}
        else "regime",
        "evidence.observations." + _key,
        "TradingView source-attested Observation." + _key + "; see Pine tv-1 definition",
    )


def lookup(value, path):
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def snapshot(signal, decision_ms, stage, membership=None):
    """Receipt-time snapshot. No backdating late market observations to candle timestamps."""
    if decision_ms < signal.created_ms:
        raise ValueError("Candidate cannot be available before its creation")
    payload = signal.model_dump(mode="json")
    values, metadata = {}, {}
    for name, (group, path, definition) in CATALOG.items():
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
        source_ms = section.get("asof", section.get("event_ms", decision_ms))
        available_ms = section.get("receipt_ms", section.get("collected_ms", decision_ms))
        missing = (
            not isinstance(value, (int, float))
            or not math.isfinite(value)
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
            version=SCHEMA_VERSION,
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
            version=SCHEMA_VERSION,
            missing=value is None,
        )
    for key in CONTEXT:
        values[key] = getattr(signal, key, None)
    values["liquidity_bucket"] = "observed" if membership and membership["eligible"] else "unknown"
    values["score_profile"] = signal.evidence.get("score_profile", "unscored")
    for key in CONTEXT:
        metadata[key] = dict(
            group="context",
            source=signal.source + ":frozen-plan",
            source_ms=decision_ms,
            available_ms=decision_ms,
            definition="candidate context: " + key,
            version=SCHEMA_VERSION,
            missing=values[key] is None,
        )
    return dict(
        signal_id=signal.id,
        decision_ms=decision_ms,
        stage=stage,
        schema_version=SCHEMA_VERSION,
        source=signal.source,
        signal=payload,
        values=values,
        feature_metadata=metadata,
        membership=membership,
        universe_scope="recorded-membership" if membership else "restricted-universe",
        data_coverage=sum(v is not None for k, v in values.items() if k not in CONTEXT)
        / (len(metadata) - len(CONTEXT)),
    )
