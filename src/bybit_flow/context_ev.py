"""Bounded, causal descriptive expectancy from immutable primary print labels."""

import json
import math
from collections import defaultdict
from statistics import mean, median, pstdev

from .ml.store import FeatureStore

POLICY = "context-ev-v1"
SHRINKAGE = 50
MINIMUM = (30, 50, 70, 100, 150)


def _value(signal, name, default=None):
    return signal.get(name, default) if isinstance(signal, dict) else getattr(signal, name, default)


def _dimensions(signal):
    evidence = _value(signal, "evidence", {}) or {}
    flow = evidence.get("flow", {})
    alignment = evidence.get("market_alignment", {})
    risk = _value(signal, "risk", {}) or {}
    quality = _value(signal, "quality", 0) or 0
    rr = risk.get("net_rr")
    return [
        _value(signal, "source"),
        _value(signal, "horizon_profile"),
        _value(signal, "family"),
        _value(signal, "direction"),
        _value(signal, "regime"),
        alignment.get("market_alignment_state"),
        flow.get("quality", {}).get("flow_quality_state"),
        _value(signal, "entry_session"),
        evidence.get("breadth", {}).get("state"),
        evidence.get("volatility", {}).get("state"),
        evidence.get("spot_perp", {}).get("state"),
        int(quality // 10) * 10,
        int(rr) if isinstance(rr, (int, float)) and math.isfinite(rr) else None,
    ]


def _keys(signal):
    dims = _dimensions(signal)
    for width in (3, 5, 7, 10, 13):
        yield json.dumps(dims[:width], separators=(",", ":"))


def _summary(rows):
    values = [float(row["label"]["net_r"]) for row in rows]
    n = len(values)
    weeks = len({row["decision_ms"] // 604_800_000 for row in rows})
    effective = min(n, max(1, weeks) * 5)
    reasons = [str(row["label"].get("exit_reason", "")).lower() for row in rows]

    def time_for(term):
        observed = [
            row["label"]["exit_ms"] - (row["label"].get("actual_entry_ms") or row["decision_ms"])
            for row, reason in zip(rows, reasons)
            if term in reason and row["label"].get("exit_ms") is not None
        ]
        return median(observed) if observed else None

    return dict(
        samples=n,
        effective_samples=effective,
        mean_net_r=mean(values),
        median_net_r=median(values),
        win_rate=sum(value > 0 for value in values) / n,
        stop_rate=sum("stop" in reason for reason in reasons) / n,
        expiry_rate=sum("time" in reason or "expir" in reason for reason in reasons) / n,
        mean_mfe_r=mean(float(row["label"]["mfe_r"]) for row in rows if row["label"].get("mfe_r") is not None)
        if any(row["label"].get("mfe_r") is not None for row in rows)
        else None,
        mean_mae_r=mean(float(row["label"]["mae_r"]) for row in rows if row["label"].get("mae_r") is not None)
        if any(row["label"].get("mae_r") is not None for row in rows)
        else None,
        median_time_to_tp1_ms=time_for("target"),
        median_time_to_stop_ms=time_for("stop"),
        dispersion_r=pstdev(values) if n > 1 else 0,
        latest_label_available_ms=max(row["label_available_ms"] for row in rows),
    )


def refresh(store, asof_ms, max_rows=3000):
    """Run outside event callbacks; only labels available strictly before asof enter."""
    rows = FeatureStore(store).dataset(asof_ms - 1, limit=max_rows)
    groups = defaultdict(list)
    for row in rows:
        for key in _keys(row["signal"]):
            groups[key].append(row)
    summaries = {key: _summary(group) for key, group in groups.items() if len(group) >= 15}
    cache = dict(
        policy=POLICY,
        built_ms=asof_ms,
        available_ms=asof_ms,
        max_label_available_ms=max((row["label_available_ms"] for row in rows), default=0),
        primary_outcomes=len(rows),
        groups=summaries,
        source_methodology="recorded-public-prints only; late OHLC excluded",
    )
    store.put("v8_context_ev", cache)
    return cache


def assess(cache, signal, asof_ms):
    base = dict(
        policy=POLICY,
        status="INSUFFICIENT",
        confidence="INSUFFICIENT",
        matched_level=0,
        samples=0,
        effective_samples=0,
        mean_net_r=None,
        median_net_r=None,
        win_rate=None,
        stop_rate=None,
        expiry_rate=None,
        mean_mfe_r=None,
        mean_mae_r=None,
        median_time_to_tp1_ms=None,
        median_time_to_stop_ms=None,
        shrunk_expectancy_r=None,
        lower_confidence_bound_r=None,
        source=_value(signal, "source"),
        source_ms=cache.get("max_label_available_ms") if cache else None,
        available_ms=cache.get("available_ms") if cache else None,
        production_gate=False,
        score_effect=0,
    )
    if (
        not cache
        or cache.get("policy") != POLICY
        or cache.get("max_label_available_ms", 0) >= asof_ms
        or cache.get("available_ms", asof_ms + 1) > asof_ms
    ):
        return base
    keys = list(_keys(signal))
    groups = cache.get("groups", {})
    level = next(
        (i for i in range(len(keys) - 1, -1, -1) if groups.get(keys[i], {}).get("samples", 0) >= MINIMUM[i]),
        None,
    )
    if level is None:
        return base
    current = groups[keys[level]]
    parent = groups.get(keys[level - 1]) if level else current
    n = current["samples"]
    weight = n / (n + SHRINKAGE)
    shrunk = weight * current["mean_net_r"] + (1 - weight) * parent["mean_net_r"]
    effective = current["effective_samples"]
    lower = shrunk - 1.64 * current["dispersion_r"] / math.sqrt(max(1, effective))
    confidence = "MATURE" if effective >= 200 else "DEVELOPING" if effective >= 75 else "EARLY"
    return (
        base
        | {key: value for key, value in current.items() if key in base}
        | dict(
            status="AVAILABLE",
            confidence=confidence,
            matched_level=level + 1,
            shrunk_expectancy_r=shrunk,
            lower_confidence_bound_r=lower,
        )
    )
