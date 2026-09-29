"""Small worker heartbeat and bounded advisory-inference diagnostics."""

import json
import os
import sqlite3
import threading
from contextlib import contextmanager

from ..storage import now_ms


@contextmanager
def heartbeat(root):
    stop = threading.Event()

    def run():
        with sqlite3.connect(root / "research.sqlite", timeout=5) as db:
            while not stop.is_set():
                try:
                    with db:
                        db.execute(
                            "INSERT OR REPLACE INTO kv VALUES(?,?)",
                            ("ml_worker_heartbeat", json.dumps(dict(at_ms=now_ms(), pid=os.getpid()))),
                        )
                except sqlite3.OperationalError:
                    pass  # A busy writer must not terminate training; staleness stays observable.
                stop.wait(20)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=6)


def abstain(store, now, reason):
    buckets = store.get("ml_abstentions", {})
    minute = str(now // 60000 * 60000)
    counts = buckets.setdefault(minute, {})
    counts[reason] = counts.get(reason, 0) + 1
    store.put("ml_abstentions", {k: v for k, v in buckets.items() if int(k) >= now - 3600000})


def trainability(store, asof_ms=None):
    """Aggregate causal, independent outcome readiness without loading feature rows."""
    from . import SCHEMA_VERSION

    asof_ms = asof_ms or now_ms()
    valid = (
        "l.available_ms<=:asof AND json_extract(l.payload,'$.complete')=1 "
        "AND json_extract(l.payload,'$.net_r') IS NOT NULL "
        "AND json_extract(l.payload,'$.exit_ms')<=:asof"
    )
    query = (
        "SELECT json_extract(s.payload,'$.source') source, "
        f"count(DISTINCT CASE WHEN {valid} THEN coalesce(c.candidate_identity,s.signal_id) END) unique_complete, "
        "count(CASE WHEN json_extract(l.payload,'$.complete')=1 THEN 1 END) complete, "
        f"count(DISTINCT CASE WHEN {valid} AND s.schema_version=:schema "
        "THEN coalesce(c.candidate_identity,s.signal_id) END) trainable, "
        f"count(DISTINCT CASE WHEN {valid} AND s.schema_version=:schema "
        "AND json_array_length(s.payload,'$.sequence')=16 "
        "THEN coalesce(c.candidate_identity,s.signal_id) END) sequence_ready "
        "FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' "
        "LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
        "WHERE s.stage='decision' GROUP BY source"
    )
    by_source = {}
    for source, unique, complete, trainable, sequence in store.db.execute(
        query, {"asof": asof_ms, "schema": SCHEMA_VERSION}
    ):
        if source not in {"binance", "bybit", "okx"}:
            continue
        by_source[source] = dict(
            complete_labels=complete,
            raw_complete_snapshots=complete,
            unique_complete_candidates=unique,
            trainable_current_schema=trainable,
            sequence_ready_16=sequence,
            baseline_minimum_required=500,
            baseline_trainable=trainable,
            baseline_ready=trainable >= 500,
            two_stage_minimum_sequences=500,
            two_stage_sequence_ready=sequence,
            two_stage_ready=sequence >= 500,
        )
    for source in ("binance", "bybit", "okx"):
        by_source.setdefault(
            source,
            dict(
                complete_labels=0,
                raw_complete_snapshots=0,
                unique_complete_candidates=0,
                trainable_current_schema=0,
                sequence_ready_16=0,
                baseline_minimum_required=500,
                baseline_trainable=0,
                baseline_ready=False,
                two_stage_minimum_sequences=500,
                two_stage_sequence_ready=0,
                two_stage_ready=False,
            ),
        )
    active_source = store.get("runtime_health", {}).get("source")
    excluded = store.db.execute(
        "SELECT "
        "sum(CASE WHEN l.snapshot_id IS NULL THEN 1 ELSE 0 END), "
        "sum(CASE WHEN json_extract(l.payload,'$.complete')=0 "
        "AND coalesce(json_extract(l.payload,'$.classification'),'')!='technical_duplicate' THEN 1 ELSE 0 END), "
        "sum(CASE WHEN json_extract(l.payload,'$.classification')='technical_duplicate' THEN 1 ELSE 0 END), "
        "sum(CASE WHEN json_extract(l.payload,'$.complete')=1 "
        "AND json_extract(l.payload,'$.net_r') IS NULL THEN 1 ELSE 0 END), "
        "sum(CASE WHEN s.schema_version!=:schema THEN 1 ELSE 0 END), "
        "sum(CASE WHEN json_extract(s.payload,'$.source')!=:source THEN 1 ELSE 0 END), "
        "sum(CASE WHEN l.available_ms>:asof OR json_extract(l.payload,'$.exit_ms')>:asof THEN 1 ELSE 0 END) "
        "FROM ml_snapshots s LEFT JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' "
        "WHERE s.stage='decision'",
        {"schema": SCHEMA_VERSION, "source": active_source, "asof": asof_ms},
    ).fetchone()
    names = (
        "unresolved",
        "incomplete",
        "technical_duplicate",
        "net_r_missing",
        "old_schema",
        "source_mismatch",
        "future_label",
    )
    return dict(
        at_ms=asof_ms,
        current_schema=SCHEMA_VERSION,
        policy="prints-v1",
        active_source=active_source,
        by_source=by_source,
        excluded=dict(zip(names, (n or 0 for n in excluded))),
        excluded_unit="raw decision snapshots; categories may overlap",
    )


def status(store, summary, enabled=True):
    now = now_ms()
    heartbeat_ms = store.get("ml_worker_heartbeat", {}).get("at_ms")
    monitor = store.get("ml_monitor", {})
    cycle = store.get("ml_cycle", {})
    advisory = store.get("ml_advisory_status", {})
    predictions = store.db.execute(
        "SELECT count(*),max(at_ms) FROM ml_predictions WHERE at_ms>=?", (now - 3600000,)
    ).fetchone()
    reasons = {}
    for minute, counts in store.get("ml_abstentions", {}).items():
        if int(minute) >= now - 3600000:
            for reason, count in counts.items():
                reasons[reason] = reasons.get(reason, 0) + count
    latest_model = store.db.execute("SELECT max(created_ms) FROM ml_models").fetchone()[0]
    latest_model_id = store.db.execute("SELECT id FROM ml_models ORDER BY created_ms DESC LIMIT 1").fetchone()
    mode = store.get("ml_training_mode", {})
    readiness = store.get("ml_trainability_cache", {})
    if not readiness:
        from . import SCHEMA_VERSION

        readiness = dict(
            at_ms=None,
            current_schema=SCHEMA_VERSION,
            policy="prints-v1",
            by_source={},
            excluded={},
            status="AWAITING_WORKER_SUMMARY",
        )
    return dict(
        status="DISABLED"
        if not enabled
        else "STALE"
        if not heartbeat_ms or now - heartbeat_ms > 90000
        else "WORKING"
        if predictions[0] or monitor.get("status") == "materializing"
        else "COLLECTING"
        if not latest_model
        else "ABSTAINED",
        enabled=enabled,
        champion=store.get("ml_champion"),
        latest_compatible_challenger=advisory.get("compatible_challenger"),
        latest_model_created_ms=latest_model,
        worker_last_seen_ms=heartbeat_ms,
        worker_state=store.get("ml_worker_state", {}).get("status"),
        monitor_status=monitor.get("status"),
        last_materialization_ms=monitor.get("last_success_ms"),
        cycle_status=cycle.get("status"),
        pipeline_mode=mode.get("mode", "COLLECTING"),
        training_mode=mode,
        snapshots=summary.get("snapshots", 0),
        labels=summary.get("labels", 0),
        complete_labels=summary.get("complete_labels", 0),
        sequence_outcomes=sum(v["sequence_ready_16"] for v in readiness["by_source"].values()),
        last_training_ms=cycle.get("at_ms"),
        last_training_result=cycle,
        last_training_reason=cycle.get("reason"),
        latest_model_id=latest_model_id[0] if latest_model_id else None,
        ml_trainability=readiness,
        trainable_current_schema=sum(v["trainable_current_schema"] for v in readiness["by_source"].values()),
        unique_trainable_candidates=sum(
            v["trainable_current_schema"] for v in readiness["by_source"].values()
        ),
        last_live_inference_ms=store.db.execute("SELECT max(at_ms) FROM ml_predictions").fetchone()[0],
        live_predictions_1h=predictions[0],
        abstentions_1h=sum(reasons.values()),
        top_abstention_reasons=dict(sorted(reasons.items(), key=lambda pair: -pair[1])[:5]),
    )
