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
            "choch": "confirmed change of structure character",
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
CONTEXT = ("family", "direction", "regime", "source", "liquidity_bucket", "score_profile")


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
        value = lookup(payload, path)
        source = signal.source + (
            ":classified-footprint"
            if signal.source == "tradingview" and group == "orderflow"
            else ":deterministic-observation"
        )
        prefix = path.split(".")[1] if path.startswith("evidence.") else "risk"
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
        data_coverage=sum(v is not None for k, v in values.items() if k not in CONTEXT) / len(metadata),
    )
