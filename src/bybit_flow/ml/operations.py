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
    mode = store.get("ml_training_mode", {})
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
        monitor_status=monitor.get("status"),
        cycle_status=cycle.get("status"),
        pipeline_mode=mode.get("mode", "COLLECTING"),
        training_mode=mode,
        snapshots=summary.get("snapshots", 0),
        labels=summary.get("labels", 0),
        complete_labels=summary.get("complete_labels", 0),
        sequence_outcomes=summary.get("sequence_outcomes", 0),
        last_training_ms=cycle.get("at_ms"),
        last_training_result=cycle,
        last_live_inference_ms=store.db.execute("SELECT max(at_ms) FROM ml_predictions").fetchone()[0],
        live_predictions_1h=predictions[0],
        abstentions_1h=sum(reasons.values()),
        top_abstention_reasons=dict(sorted(reasons.items(), key=lambda pair: -pair[1])[:5]),
    )
