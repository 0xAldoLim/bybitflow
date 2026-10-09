"""Immutable deployment epoch and cached progress; never used for trade decisions."""

import json
import re

from ..storage import now_ms
from . import SCHEMA_VERSION
from .validation import PARTITION_POLICY

FROZEN_POLICIES = (
    "stored candidate-v10 and candidate-v20 feature meanings; schema-separated learning",
    "V8 production-gate thresholds",
    "strategy family rules",
    "quality-score weights",
    "label policies",
    "bootstrap feature projection",
    "partition policy",
    "ML minimums",
)


def start_epoch(store, code_commit):
    """Called by the CI-verified deployment script; atomic create-once, no reset."""
    if not re.fullmatch(r"[a-f0-9]{40}", code_commit):
        raise ValueError("Maturity epoch requires the full deployed commit SHA")
    epoch = dict(
        started_ms=now_ms(),
        code_commit=code_commit,
        candidate_schema=SCHEMA_VERSION,
        primary_label_policy="prints-v1",
        bootstrap_schema="bootstrap-core-v1",
        bootstrap_label_policy="ohlc-path-v1",
        partition_policy=PARTITION_POLICY,
        v8_gating_policy="v8-production-gating-v1",
        frozen_policies=list(FROZEN_POLICIES),
        note="Stable maturity period: accumulate causal outcomes; no automatic strategy optimization, threshold tuning or promotion.",
    )
    with store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO kv(key,payload) VALUES(?,?)", ("ml_maturity_epoch", json.dumps(epoch))
        )
    return store.get("ml_maturity_epoch")


def progress(store, primary_model_id=None, bootstrap_model_id=None, asof_ms=None):
    now = asof_ms or now_ms()
    epoch = store.get("ml_maturity_epoch", {})
    readiness = store.get("ml_trainability_cache", {})
    primary = readiness.get("by_source", {})
    bootstrap = store.get("ml_bootstrap_trainability", {})
    # Progress is source-specific. Pooling venues cannot make a model eligible.
    complete = {s: d.get("trainable_current_schema", 0) for s, d in primary.items()}
    sequences = {s: d.get("sequence_ready_16", 0) for s, d in primary.items()}
    source = readiness.get("active_source")
    storage = store.get("storage_status", {})
    used, budget = storage.get("used_bytes"), storage.get("budget_bytes")
    pct = used / budget * 100 if used is not None and budget else storage.get("usage_percent")
    return dict(
        epoch_started_ms=epoch.get("started_ms"),
        epoch_age_days=max(0, now - epoch["started_ms"]) / 86_400_000 if epoch else None,
        candidate_schema=SCHEMA_VERSION,
        current_schema_decisions=sum(d.get("current_schema_decisions", 0) for d in primary.values()),
        epoch_current_schema_decisions=sum(
            d.get("epoch_current_schema_decisions", 0) for d in primary.values()
        ),
        prints_v1_complete_unique_by_source=complete,
        bootstrap_complete_unique_by_source={s: d.get("complete_unique", 0) for s, d in bootstrap.items()},
        active_source=source,
        primary_progress_pct=min(100, complete.get(source, 0) / 500 * 100),
        sequence_progress_pct=min(100, sequences.get(source, 0) / 500 * 100),
        primary_progress_pct_by_source={s: min(100, n / 500 * 100) for s, n in complete.items()},
        sequence_progress_pct_by_source={s: min(100, n / 500 * 100) for s, n in sequences.items()},
        bootstrap_model_id=bootstrap_model_id,
        primary_model_id=primary_model_id,
        storage_pct=pct,
        storage_state="NOT_OBSERVED"
        if pct is None
        else "HEALTHY"
        if pct < 80
        else "WARNING"
        if pct < 90
        else "PRESSURE_CLEANUP"
        if pct < 95
        else "TRAINING_BACKPRESSURE",
        recorder_drop_count=store.get("recorder_health", {}).get("events_dropped"),
        note="Progress toward 500 source-specific usable outcomes, not statistical confidence.",
    )
