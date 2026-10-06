"""Small worker heartbeat and bounded advisory-inference diagnostics."""

import json
import math
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


def cache_partition_feasibility(store, rows, track, source, schema, policy, asof_ms=None):
    """Plan already-loaded worker rows; operator views only read the small result."""
    from .validation import partition_feasibility

    scope = "|".join((track, source, schema, policy))
    scopes = (scope, "legacy-primary") if track == "primary" else (scope, scope)
    previous_end = store.db.execute(
        "SELECT MAX(end_ms) FROM ml_holdouts WHERE scope IN (?,?)", scopes
    ).fetchone()[0]
    report = partition_feasibility(rows, previous_holdout_end=previous_end) | dict(
        at_ms=asof_ms or now_ms(),
        track=track,
        source=source,
        schema=schema,
        policy=policy,
        previous_holdout_end=previous_end,
        training_window_limit=10_000,
    )
    store.put(f"ml_partition_feasibility:{track}:{source}:{schema}:{policy}", report)
    return report


def partition_status(store, primary, bootstrap):
    """Expose source/track-specific cached plans without reading feature payloads."""
    from . import SCHEMA_VERSION

    result = {"primary": {}, "bootstrap": {}}
    for track, sources in (("primary", primary), ("bootstrap", bootstrap)):
        schema, policy = (
            (SCHEMA_VERSION, "prints-v1") if track == "primary" else ("bootstrap-core-v1", "ohlc-path-v1")
        )
        for source, detail in sources.items():
            count = detail.get("baseline_trainable" if track == "primary" else "trainable", 0)
            if source not in {"binance", "bybit", "okx"} or count < 500:
                continue
            report = store.get(f"ml_partition_feasibility:{track}:{source}:{schema}:{policy}", {})
            result[track][source] = report or dict(
                status="AWAITING_WORKER_PLAN", total_rows=count, track=track, source=source
            )
    return result


def trainability(store, asof_ms=None):
    """Cache-ready counts from bounded scalar pages, never sorting feature payloads."""
    from . import SCHEMA_VERSION

    asof_ms = asof_ms or now_ms()
    active_source = store.get("runtime_health", {}).get("source")
    sources = {
        name: dict(
            decisions=0,
            identities=set(),
            complete=0,
            unique=set(),
            current=set(),
            sequence=set(),
            planning_rows={},
        )
        for name in ("binance", "bybit", "okx")
    }
    excluded = dict.fromkeys(
        (
            "unresolved",
            "incomplete",
            "technical_duplicate",
            "net_r_missing",
            "old_schema",
            "source_mismatch",
            "future_label",
        ),
        0,
    )
    cursor = None
    gaps = dict.fromkeys(
        (
            "initial_observation_delay",
            "trade_feed_stale",
            "subscription_gap",
            "recorder_chain_gap",
            "clock_damage",
            "retention_missing_interval",
        ),
        0,
    )
    while True:
        page = " AND (s.decision_ms,s.id)>(?,?)" if cursor else ""
        params = list(cursor) if cursor else []
        rows = store.db.execute(
            "SELECT s.decision_ms,s.id,s.schema_version,"
            "json_extract(s.payload,'$.source'),json_array_length(s.payload,'$.sequence'),"
            "coalesce(c.candidate_identity,s.signal_id),l.snapshot_id,l.available_ms,"
            "json_extract(l.payload,'$.complete'),json_extract(l.payload,'$.net_r'),"
            "json_extract(l.payload,'$.exit_ms'),json_extract(l.payload,'$.classification'),json_extract(l.payload,'$.data_gaps') "
            "FROM ml_snapshots s LEFT JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' "
            "LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
            "WHERE s.stage='decision'" + page + " ORDER BY s.decision_ms,s.id LIMIT 1000",
            params,
        ).fetchall()
        if not rows:
            break
        for (
            decision,
            ident,
            schema,
            source,
            sequence,
            identity,
            label_id,
            available,
            complete,
            net_r,
            exit_ms,
            kind,
            data_gaps,
        ) in rows:
            cursor = (decision, ident)
            reason = (data_gaps or "").lower()
            for category, patterns in {
                "initial_observation_delay": ("entry coverage", "before the live observation", "initial"),
                "trade_feed_stale": ("trade-feed stale", "latest trade stale"),
                "subscription_gap": ("subscription", "continuity lost"),
                "recorder_chain_gap": ("recording exclusion", "unchained", "recorded coverage gap"),
                "clock_damage": ("clock", "non-monotonic", "overlapping"),
                "retention_missing_interval": ("retention", "missing", "recording ended"),
            }.items():
                gaps[category] += any(pattern in reason for pattern in patterns)
            excluded["unresolved"] += label_id is None
            excluded["incomplete"] += complete == 0 and kind != "technical_duplicate"
            excluded["technical_duplicate"] += kind == "technical_duplicate"
            excluded["net_r_missing"] += complete == 1 and net_r is None
            excluded["old_schema"] += schema != SCHEMA_VERSION
            excluded["source_mismatch"] += active_source is not None and source != active_source
            excluded["future_label"] += available is not None and (
                available > asof_ms or (exit_ms is not None and exit_ms > asof_ms)
            )
            if source not in sources:
                continue
            bucket = sources[source]
            bucket["decisions"] += 1
            bucket["identities"].add(identity)
            bucket["complete"] += complete == 1
            if (
                complete != 1
                or net_r is None
                or not math.isfinite(net_r)
                or exit_ms is None
                or not decision < exit_ms <= available <= asof_ms
            ):
                continue
            bucket["unique"].add(identity)
            if schema == SCHEMA_VERSION:
                bucket["current"].add(identity)
                # The latest causal row represents each identity, matching the
                # source/schema-specific export's bounded 10,000-row window.
                planning = bucket["planning_rows"]
                planning.pop(identity, None)
                planning[identity] = dict(
                    id=ident,
                    candidate_identity=identity,
                    source=source,
                    decision_ms=decision,
                    label_available_ms=available,
                    label=dict(complete=True, net_r=net_r, exit_ms=exit_ms),
                )
                if len(planning) > 10_000:
                    del planning[next(iter(planning))]
                if sequence == 16:
                    bucket["sequence"].add(identity)
        if len(rows) < 1000:
            break
    by_source = {}
    for name, bucket in sources.items():
        trainable = len(bucket["current"])
        sequence_ready = len(bucket["sequence"])
        by_source[name] = dict(
            historical_decision_snapshots=bucket["decisions"],
            unique_candidate_identities=len(bucket["identities"]),
            complete_labels=bucket["complete"],
            raw_complete_snapshots=bucket["complete"],
            unique_complete_candidates=len(bucket["unique"]),
            trainable_current_schema=trainable,
            sequence_ready_16=sequence_ready,
            baseline_minimum_required=500,
            baseline_trainable=trainable,
            baseline_ready=trainable >= 500,
            two_stage_minimum_sequences=500,
            two_stage_sequence_ready=sequence_ready,
            two_stage_ready=sequence_ready >= 500,
        )
        if trainable >= 500:
            by_source[name]["partition_feasibility"] = cache_partition_feasibility(
                store,
                list(bucket["planning_rows"].values()),
                "primary",
                name,
                SCHEMA_VERSION,
                "prints-v1",
                asof_ms,
            )
    return dict(
        at_ms=asof_ms,
        current_schema=SCHEMA_VERSION,
        policy="prints-v1",
        active_source=active_source,
        by_source=by_source,
        excluded=excluded,
        coverage_loss_reasons=gaps,
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
    primary_id = store.db.execute(
        "SELECT id FROM ml_models WHERE coalesce(json_extract(manifest,'$.track'),'primary')='primary' ORDER BY created_ms DESC LIMIT 1"
    ).fetchone()
    bootstrap_id = store.db.execute(
        "SELECT id FROM ml_models WHERE json_extract(manifest,'$.track')='bootstrap' ORDER BY created_ms DESC LIMIT 1"
    ).fetchone()
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
        model_status="PRIMARY_CHAMPION"
        if store.get("ml_champion")
        else "PRIMARY_CHALLENGER"
        if primary_id
        else "BOOTSTRAP_CHALLENGER"
        if bootstrap_id
        else "NO_MODEL",
        primary_model_id=primary_id[0] if primary_id else None,
        bootstrap_model_id=bootstrap_id[0] if bootstrap_id else None,
        bootstrap_trainability=store.get("ml_bootstrap_trainability", {}),
        bootstrap_backfill={
            source: store.get("ml_bootstrap_backfill:" + source + ":ohlc-path-v1", {})
            for source in ("binance", "bybit", "okx")
        },
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
        last_training_reason=cycle.get("reason")
        or "; ".join(
            f"{track}: {result['reason']}"
            for track, result in cycle.get("tracks", {}).items()
            if result.get("status") == "abstained" and result.get("reason")
        )
        or None,
        latest_model_id=latest_model_id[0] if latest_model_id else None,
        ml_trainability=readiness,
        partition_feasibility=partition_status(
            store, readiness["by_source"], store.get("ml_bootstrap_trainability", {})
        ),
        trainable_current_schema=sum(v["trainable_current_schema"] for v in readiness["by_source"].values()),
        unique_trainable_candidates=sum(
            v["trainable_current_schema"] for v in readiness["by_source"].values()
        ),
        last_live_inference_ms=store.db.execute("SELECT max(at_ms) FROM ml_predictions").fetchone()[0],
        live_predictions_1h=predictions[0],
        abstentions_1h=sum(reasons.values()),
        top_abstention_reasons=dict(sorted(reasons.items(), key=lambda pair: -pair[1])[:5]),
    )
