"""Causal helpers for fixed, uncalibrated v2.2 research assumptions."""

import math

ASSUMPTION = "RESEARCH-GROUNDED ASSUMPTIONS; not validated alpha or win probabilities"


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def positive(value):
    return number(value) and value > 0


def clamp(value, low=0.0, high=100.0):
    return min(high, max(low, value))


def envelope(signal, policy, now, **values):
    return dict(policy=policy, source=signal.source, symbol=signal.symbol, available_ms=now, **values)


def causal(row, signal, now, ttl=None):
    """Old shared evidence inherits source/symbol from its enclosing signal."""
    at = row.get("available_ms") if isinstance(row, dict) else None
    return bool(
        isinstance(row, dict)
        and row.get("source", signal.source) == signal.source
        and row.get("symbol", signal.symbol) == signal.symbol
        and number(at)
        and 0 <= now - at
        and (ttl is None or now - at <= ttl)
        and all(not number(row.get(k)) or row[k] <= now for k in ("source_ms", "window_end_ms", "end_ms"))
    )


def quantile(values, q):
    ordered = sorted(values)
    if not ordered:
        return None
    index = (len(ordered) - 1) * q
    left = int(index)
    return ordered[left] + (ordered[min(left + 1, len(ordered) - 1)] - ordered[left]) * (index - left)
