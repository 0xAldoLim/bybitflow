"""Presentation-only readiness from existing bounded worker summaries."""

import time


def project(summary, now=None):
    now = int(time.time() * 1000) if now is None else now
    observed = summary.get("summary_at_ms")
    stale = (
        summary.get("status") in {"STALE", "DISABLED"}
        or not isinstance(observed, (int, float))
        or not 0 <= now - observed <= 900_000
    )
    stages = []

    def add(stage, state, detail):
        stages.append(dict(stage=stage, state=state, detail=detail))

    add("Worker", summary.get("status", "UNKNOWN"), summary.get("worker_state") or "Awaiting heartbeat")
    sources = summary.get("ml_trainability", {}).get("by_source", {})
    detail = "; ".join(
        f"{source}: {row.get('trainable_current_schema', 0)}/500 usable current-schema outcomes"
        for source, row in sources.items()
    )
    add(
        "Data", "STALE" if stale else "OBSERVED" if sources else "UNKNOWN", detail or "Awaiting worker counts"
    )
    add(
        "Training",
        "STALE" if stale else summary.get("cycle_status") or "UNKNOWN",
        summary.get("last_training_reason") or "Chronological partitions and resource admission still apply",
    )
    add(
        "Advisory compatibility",
        "STALE"
        if stale
        else "COMPATIBLE_CHALLENGER"
        if summary.get("latest_compatible_challenger")
        else "NOT_ESTABLISHED",
        "Compatibility is evaluated per source, feature schema and model context; a saved model is not sufficient",
    )
    reasons = sorted({reason for m in summary.get("models", []) for reason in m.get("promotion_reasons", [])})
    add(
        "Production validation",
        "STALE" if stale else "REQUIRES_REVIEW",
        "; ".join(reasons) or "Verified costs, held-out incremental value and explicit approval are required",
    )
    add(
        "Authority",
        "CHAMPION_REGISTERED" if summary.get("champion") else "DETERMINISTIC",
        "A registered champion still requires runtime compatibility and degradation checks; no automatic promotion",
    )
    return dict(
        stages=stages,
        asof_ms=summary.get("summary_at_ms"),
        stale=stale,
        note="Event totals are not training examples. Proxy recovery remains research-only; assumed costs do not satisfy verified-cost promotion.",
    )
