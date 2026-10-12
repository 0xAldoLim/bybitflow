"""Exact policy dispatch: historical v21 semantics remain in their original module."""

from . import quality_v21 as old
from . import quality_v211 as new

SCORE_PROFILE = old.SCORE_PROFILE


def enabled(signal):
    return old.enabled(signal) or new.enabled(signal)


def entry_quality(signal, price, now, minimum_rr=1.5, **kwargs):
    if new.enabled(signal):
        return new.entry_quality(signal, price, now, minimum_rr, **kwargs)
    return old.entry_quality(signal, price, now, minimum_rr)


def apply(signal, price, now, liquidity=None, minimum_rr=1.5, **kwargs):
    if new.enabled(signal):
        return new.apply(signal, price, now, liquidity, minimum_rr, **kwargs)
    return old.apply(signal, price, now, liquidity, minimum_rr)


def remember_attempt(store, signal):
    old.remember_attempt(store, signal)
    if new.enabled(signal):
        structure = signal.evidence.get("frozen_structure", {})
        qualification = structure.get("trend_qualified_ms")
        failures = 0
        if qualification is not None and signal.family == "trend_pullback":
            import json

            key = f"attempt-v21:{signal.source}:{signal.symbol}:{signal.horizon_profile}:{signal.family}:{signal.direction}"
            for row in store.get(key, []):
                if not qualification <= row["event"] < signal.evidence["trigger_bar_end"]:
                    continue
                saved = store.db.execute("SELECT payload FROM signals WHERE id=?", (row["id"],)).fetchone()
                if saved and json.loads(saved[0])["state"] in {"EXPIRED", "INVALIDATED"}:
                    failures += 1
        structure["prior_failed_pullback_attempts"] = failures if qualification is not None else None


path_quality = old.path_quality
overlaps_existing = new.overlaps_existing
liquidity_update = new.liquidity_update
