"""Incremental signal-funnel accounting; raw events are not strategy candidates."""

import os
import time

START_MS = time.time_ns() // 1_000_000
METRICS = (
    "recorded_events broad_markets_discovered preeligible_markets depth_verified_markets deep_selected_markets candidates_generated pending_confirmation confirmation_attempts flow_confirmed flow_rejected market_alignment_blocked unconfirmed_liquidity_sweep risk_rejected spread_rejected price_outside_zone derivatives_rejected macro_deferred coverage_incomplete execution_expired other_gate_rejected confirmed alerted alert_claim_attempts alert_claimed duplicate_suppressed discord_http_attempts discord_sent discord_uncertain discord_rejected lifecycle_updates_sent"
).split()


def migrate(store):
    store.db.executescript("""
    CREATE TABLE IF NOT EXISTS funnel_events(event_key TEXT PRIMARY KEY, at_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS funnel_totals(metric TEXT, dimension TEXT, value TEXT, n INTEGER NOT NULL,
        last_ms INTEGER NOT NULL, signal_id TEXT, PRIMARY KEY(metric,dimension,value));
    CREATE TABLE IF NOT EXISTS funnel_minutes(minute_ms INTEGER,metric TEXT,dimension TEXT,value TEXT,
        n INTEGER NOT NULL,last_ms INTEGER NOT NULL,signal_id TEXT,PRIMARY KEY(minute_ms,metric,dimension,value));
    CREATE INDEX IF NOT EXISTS funnel_event_time ON funnel_events(at_ms);
    CREATE INDEX IF NOT EXISTS funnel_minute_time ON funnel_minutes(minute_ms);
    """)
    if not store.get("funnel_started"):
        store.put(
            "funnel_started",
            dict(
                at_ms=time.time_ns() // 1_000_000,
                history="historical import pending; gates unavailable before instrumentation are not inferred",
            ),
        )


def emit(store, metric, at_ms, *, signal=None, key=None, reason="", amount=1):
    if signal is not None and signal.synthetic:
        return
    event_key = key or f"{metric}:{getattr(signal, 'id', '')}:{reason}:{at_ms // 60000}"
    from contextlib import nullcontext

    # Join the caller transaction so signal/segment and counters commit together.
    with nullcontext() if store.db.in_transaction else store.db:
        if not store.db.execute(
            "INSERT OR IGNORE INTO funnel_events VALUES(?,?)", (event_key, at_ms)
        ).rowcount:
            return
        dimensions = [("all", "all")]
        if signal is not None:
            dimensions += [
                ("horizon", signal.horizon_profile),
                ("family", signal.family),
                ("direction", signal.direction),
            ]
        if reason:
            dimensions.append(("reason", reason))
        for dimension, value in dimensions:
            row = (metric, dimension, value, amount, at_ms, getattr(signal, "id", None))
            store.db.execute(
                "INSERT INTO funnel_totals VALUES(?,?,?,?,?,?) ON CONFLICT(metric,dimension,value) DO UPDATE SET n=n+excluded.n,last_ms=max(last_ms,excluded.last_ms),signal_id=CASE WHEN excluded.last_ms>=last_ms THEN excluded.signal_id ELSE signal_id END",
                row,
            )
            store.db.execute(
                "INSERT INTO funnel_minutes VALUES(?,?,?,?,?,?,?) ON CONFLICT(minute_ms,metric,dimension,value) DO UPDATE SET n=n+excluded.n,last_ms=max(last_ms,excluded.last_ms),signal_id=excluded.signal_id",
                (at_ms // 60000 * 60000, *row),
            )


def reject(store, signal, reason, at_ms):
    upper = reason.upper()
    if "UNCONFIRMED_LIQUIDITY_SWEEP" in upper:
        metric, code = "unconfirmed_liquidity_sweep", "UNCONFIRMED_LIQUIDITY_SWEEP"
    elif "MARKET_CONFLICT" in upper or "CONTRARIAN" in upper:
        metric, code = "market_alignment_blocked", "CONTRARIAN_EVIDENCE_INSUFFICIENT"
    elif "FLOW" in upper and "CONFIRM" in upper:
        metric, code = "flow_rejected", "EXECUTED_FLOW_NOT_CONFIRMED"
    elif "OUTSIDE" in upper and "ZONE" in upper:
        metric, code = "price_outside_zone", "CURRENT_PRICE_OUTSIDE_ENTRY_ZONE"
    elif "SPREAD" in upper:
        metric, code = "spread_rejected", "SPREAD_EXCEEDS_GATE"
    elif "EXPIRED" in upper:
        metric, code = "execution_expired", "EXECUTION_TRIGGER_EXPIRED"
    elif "FUNDING" in upper:
        metric, code = "derivatives_rejected", "EXTREME_FUNDING"
    elif "DERIVATIVE" in upper:
        metric, code = "derivatives_rejected", "DERIVATIVES_STALE"
    elif "RATIO" in upper or "REWARD" in upper or "R:R" in upper:
        metric, code = "risk_rejected", "NET_RR_TOO_LOW"
    else:
        metric, code = "other_gate_rejected", upper.replace(" ", "_")[:100]
    emit(store, metric, at_ms, signal=signal, reason=code)


def import_history(store):
    """One explicit additive migration after backup; do not scan history in doctor."""
    if store.get("funnel_history_imported"):
        return
    from .models import Signal

    for (ident,) in store.db.execute("SELECT id FROM signals ORDER BY created_ms").fetchall():
        s = Signal.model_validate_json(
            store.db.execute("SELECT payload FROM signals WHERE id=?", (ident,)).fetchone()[0]
        )
        emit(store, "candidates_generated", s.created_ms, signal=s, key="generated:" + ident)
    for ident, at_ms, state in store.db.execute(
        "SELECT signal_id,min(at_ms),state FROM transitions GROUP BY signal_id,state"
    ).fetchall():
        metric = {
            "PENDING CONFIRMATION": "pending_confirmation",
            "CONFIRMED": "confirmed",
            "ALERTED": "alerted",
        }.get(state)
        if not metric:
            continue
        row = store.db.execute("SELECT payload FROM signals WHERE id=?", (ident,)).fetchone()
        if row:
            emit(store, metric, at_ms, signal=Signal.model_validate_json(row[0]), key=metric + ":" + ident)
    # Only retained segments are reconstructable; never imply deleted raw history
    # can be counted. Aggregate small scalar values, not full segment payloads.
    for minute, rows in store.db.execute(
        "SELECT at_ms/60000*60000,sum(json_extract(payload,'$.rows')) FROM segments WHERE NOT EXISTS (SELECT 1 FROM funnel_events WHERE event_key='segment:'||segments.id) GROUP BY 1"
    ).fetchall():
        emit(store, "recorded_events", minute, key="history-segment-minute:" + str(minute), amount=rows or 0)
    store.put(
        "funnel_history_imported",
        dict(
            at_ms=time.time_ns() // 1_000_000,
            unavailable="historical confirmation attempts and unsaved gates",
        ),
    )


def status(store, now=None, configured=None):
    now = now or time.time_ns() // 1_000_000
    process_start = store.get("scanner_process_start", {}).get("at_ms", START_MS)
    total = list(store.db.execute("SELECT * FROM funnel_totals"))
    cumulative = {metric: 0 for metric in METRICS}
    cumulative.update({r["metric"]: r["n"] for r in total if r["dimension"] == "all"})
    windows = {}
    for name, start in [
        ("15m", now - 900000),
        ("1h", now - 3600000),
        ("24h", now - 86400000),
    ]:
        windows[name] = dict(
            store.db.execute(
                "SELECT metric,sum(n) FROM funnel_minutes WHERE minute_ms>=? AND dimension='all' GROUP BY metric",
                (start // 60000 * 60000,),
            )
        )
    baseline = store.get("funnel_process_baseline", {})
    windows["process"] = {k: max(0, v - baseline.get(k, 0)) for k, v in cumulative.items()}
    recent = windows["1h"]
    gate_rows = store.db.execute(
        "SELECT value,sum(n),max(last_ms),signal_id FROM funnel_minutes WHERE minute_ms>=? AND dimension='reason' GROUP BY value ORDER BY sum(n) DESC LIMIT 12",
        ((now - 3600000) // 60000 * 60000,),
    ).fetchall()
    diagnosis = "NO_RECENT_CONFIRMED_SETUP"
    health = store.get("confirmation_health", {})
    if health.get("state") == "DEGRADED":
        diagnosis = "CONFIRMATION_RUNTIME_ERROR"
    elif recent.get("discord_rejected", 0) or recent.get("discord_uncertain", 0):
        diagnosis = "DISCORD_DELIVERY_BOTTLENECK"
    elif any(
        recent.get(k, 0)
        for k in ("coverage_incomplete", "flow_rejected", "risk_rejected", "price_outside_zone")
    ):
        biggest = max(
            ("coverage_incomplete", "flow_rejected", "risk_rejected", "price_outside_zone"),
            key=lambda k: recent.get(k, 0),
        )
        diagnosis = {
            "coverage_incomplete": "COVERAGE_BOTTLENECK",
            "flow_rejected": "CONFIRMATION_BOTTLENECK",
            "risk_rejected": "RISK_GATE_BOTTLENECK",
            "price_outside_zone": "ENTRY_ZONE_BOTTLENECK",
        }[biggest]
    elif not recent.get("candidates_generated", 0):
        diagnosis = "NO_RECENT_CANDIDATES"
    if recent.get("discord_sent", 0) and diagnosis not in {
        "CONFIRMATION_RUNTIME_ERROR",
        "DISCORD_DELIVERY_BOTTLENECK",
    }:
        diagnosis = "DELIVERING"
    from .identity import delivery_status

    delivery = delivery_status(store)
    discord = store.get("discord_transport", {})
    test = store.get("discord_connection_test", {})
    verified = test.get("status") == "sent" and 0 <= now - test.get("at_ms", 0) < 86400000
    message = "No genuine setup has passed all gates in the past hour."
    if diagnosis == "DELIVERING":
        message = "Genuine setup delivery confirmed in the past hour."
    elif diagnosis == "CONFIRMATION_RUNTIME_ERROR":
        message = "Confirmation encountered a runtime error. Review confirmation health before relying on new alerts."
    if verified and not recent.get("confirmed", 0) and diagnosis != "CONFIRMATION_RUNTIME_ERROR":
        message = "Discord connection test succeeded. No genuine setup passed all gates in the past hour."
    if recent.get("confirmed", 0) and diagnosis == "DISCORD_DELIVERY_BOTTLENECK":
        message = "Strategy produced confirmed setups; Discord delivery is failing."
    return dict(
        policy="signal-funnel-v1",
        at_ms=now,
        windows=windows,
        cumulative=cumulative,
        candidates_by_horizon={
            r["value"]: r["n"]
            for r in total
            if r["metric"] == "candidates_generated" and r["dimension"] == "horizon"
        },
        candidates_by_family={
            r["value"]: r["n"]
            for r in total
            if r["metric"] == "candidates_generated" and r["dimension"] == "family"
        },
        candidates_by_direction={
            r["value"]: r["n"]
            for r in total
            if r["metric"] == "candidates_generated" and r["dimension"] == "direction"
        },
        last_stage_ms={r["metric"]: r["last_ms"] for r in total if r["dimension"] == "all"},
        top_rejections=[
            dict(reason=r[0], count=r[1], last_seen_ms=r[2], representative_signal_id=r[3]) for r in gate_rows
        ],
        diagnosis=diagnosis,
        message=message,
        confirmation_health=health,
        delivery=delivery,
        discord=dict(configured=configured, connection_test=test, transport=discord),
        provenance=store.get("funnel_history_imported", store.get("funnel_started", {})),
        window_resolution="one minute; first partial minute included",
        process_start_ms=process_start,
        pid=os.getpid(),
    )


def maintain(store, now):
    """Bound transient telemetry; retain cumulative totals and stable stage keys."""
    cutoff = now - 7 * 86400000
    with store.db:
        store.db.execute(
            "DELETE FROM funnel_minutes WHERE rowid IN (SELECT rowid FROM funnel_minutes WHERE minute_ms<? LIMIT 5000)",
            (cutoff,),
        )
        store.db.execute(
            "DELETE FROM funnel_events WHERE rowid IN (SELECT rowid FROM funnel_events WHERE at_ms<? AND event_key NOT LIKE 'generated:%' AND event_key NOT LIKE 'pending_confirmation:%' AND event_key NOT LIKE 'confirmed:%' AND event_key NOT LIKE 'alerted:%' LIMIT 5000)",
            (cutoff,),
        )


def import_deliveries(store):
    if store.get("funnel_deliveries_imported"):
        return
    for key, ident, state, at_ms in store.db.execute(
        "SELECT key,signal_id,status,updated_ms FROM outbox WHERE signal_id IS NOT NULL AND key LIKE '%:initial'"
    ).fetchall():
        emit(store, "discord_http_attempts", at_ms, key="http:" + key)
        metric = {"sent": "discord_sent", "uncertain": "discord_uncertain"}.get(state, "discord_rejected")
        emit(store, metric, at_ms, key="http-result:" + key, reason="" if state == "sent" else state.upper())
    store.put("funnel_deliveries_imported", {"at_ms": time.time_ns() // 1000000})


def reconcile_stages(store):
    """Bridge events written by the previous process during additive deployment."""
    from .models import Signal

    rows = store.db.execute(
        "SELECT s.id FROM signals s WHERE NOT EXISTS (SELECT 1 FROM funnel_events f WHERE f.event_key='generated:'||s.id)"
    ).fetchall()
    with store.db:
        for (ident,) in rows:
            s = Signal.model_validate_json(
                store.db.execute("SELECT payload FROM signals WHERE id=?", (ident,)).fetchone()[0]
            )
            emit(store, "candidates_generated", s.created_ms, signal=s, key="generated:" + ident)
    for state, metric in [
        ("PENDING CONFIRMATION", "pending_confirmation"),
        ("CONFIRMED", "confirmed"),
        ("ALERTED", "alerted"),
    ]:
        rows = store.db.execute(
            "SELECT t.signal_id,min(t.at_ms) FROM transitions t WHERE t.state=? AND NOT EXISTS (SELECT 1 FROM funnel_events f WHERE f.event_key=?||':'||t.signal_id) GROUP BY t.signal_id",
            (state, metric),
        ).fetchall()
        with store.db:
            for ident, at_ms in rows:
                s = Signal.model_validate_json(
                    store.db.execute("SELECT payload FROM signals WHERE id=?", (ident,)).fetchone()[0]
                )
                emit(store, metric, at_ms, signal=s, key=metric + ":" + ident)
