"""Versioned projection of stable, causal frozen fields; originals never change.

The v4..v10 catalog/history retains these price/plan definitions. Horizon roles
changed in v5, so their actual timeframes are explicit model contexts. Effective
flow, scoring, alignment and V8 additions changed semantics and are excluded.
"""

import math

from .features import CATALOG

SCHEMA = "bootstrap-core-v1"
SUPPORTED = frozenset(f"candidate-v{i}" for i in range(4, 11))
CONTEXTS = ("family", "direction", "regime", "source", "horizon_profile", "entry_session")
NUMERIC = tuple(
    f"{tf}_{key}"
    for tf in ("h4", "h1", "m15")
    for key in ("atr", "realized_volatility", "efficiency", "slope", "volume_expansion")
) + ("book_spread_bps", "book_microprice", "funding_rate", "net_rr", "cost_per_base")


def project(row):
    if row["schema_version"] not in SUPPORTED or row["source"] not in {"binance", "bybit", "okx"}:
        raise ValueError("Unsupported bootstrap source/schema")
    decision = row["decision_ms"]
    values, metadata = {}, {}
    for key in CONTEXTS + NUMERIC:
        meta = row["feature_metadata"].get(key)
        value = row["values"].get(key)
        if not meta:
            value = None
        elif key in NUMERIC and meta.get("definition") != CATALOG[key][2]:
            raise ValueError("Changed bootstrap definition: " + key)
        if meta and not meta.get("missing"):
            times = (meta.get("source_ms"), meta.get("available_ms"))
            if any(not isinstance(t, (int, float)) or not math.isfinite(t) or t > decision for t in times):
                raise ValueError("Noncausal bootstrap feature: " + key)
        if key in NUMERIC and (not isinstance(value, (int, float)) or not math.isfinite(value)):
            value = None
        if meta and meta.get("missing"):
            value = None
        values[key] = value
        metadata[key] = dict(meta or {}, version=SCHEMA, missing=value is None)
    for tf, field, legacy in (
        ("h4", "context_timeframe", "240"),
        ("h1", "setup_timeframe", "60"),
        ("m15", "execution_timeframe", "15"),
    ):
        value = str(row["signal"].get(field) or legacy)
        values[tf + "_timeframe"] = value
        metadata[tf + "_timeframe"] = dict(
            version=SCHEMA,
            missing=False,
            source_ms=decision,
            available_ms=decision,
            definition="Frozen horizon-role timeframe",
        )
    identity = {
        key: row[key]
        for key in (
            "id",
            "signal_id",
            "decision_ms",
            "stage",
            "source",
            "universe_scope",
            "candidate_identity",
            "label",
            "label_available_ms",
        )
        if key in row
    }
    frozen = {
        key: row["signal"].get(key)
        for key in (
            "id",
            "symbol",
            "family",
            "direction",
            "regime",
            "version",
            "quality",
            "gates",
            "entry",
            "zone",
            "stop",
            "tp1",
            "tp2",
            "created_ms",
            "expires_ms",
            "trigger_expires_ms",
            "expected_hold_max",
            "horizon_profile",
        )
    }
    frozen["risk"] = {
        key: row["signal"].get("risk", {}).get(key) for key in ("accepted", "net_rr", "cost_per_base")
    }
    frozen["risk"]["accepted"] = bool(row["signal"].get("risk", {}).get("accepted", False))
    return identity | dict(
        signal=frozen,
        original_schema_version=row["schema_version"],
        schema_version=SCHEMA,
        projection_version=SCHEMA,
        values=values,
        feature_metadata=metadata,
        data_coverage=sum(values[k] is not None for k in NUMERIC) / len(NUMERIC),
        sequence=[],
    )
