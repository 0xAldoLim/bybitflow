"""Incremental forward v2.2 exit research. No production labels or ML authority."""

import json
from copy import deepcopy

from .quality_v22 import enabled
from .setup_common import number, positive


def migrate(store):
    store.db.executescript("""
    CREATE TABLE IF NOT EXISTS setup_path_jobs(
      signal_id TEXT PRIMARY KEY,source TEXT,symbol TEXT,next_ms INTEGER,cursor_ms INTEGER,
      deadline_ms INTEGER,payload TEXT);
    CREATE INDEX IF NOT EXISTS setup_path_due ON setup_path_jobs(next_ms);
    CREATE TABLE IF NOT EXISTS setup_path_labels(
      signal_id TEXT PRIMARY KEY,available_ms INTEGER,payload TEXT);
    """)


def register(store, signal, now, stage="decision"):
    if not enabled(signal) or signal.synthetic or not signal.evidence.get("v22"):
        return
    job_id = signal.id if stage == "decision" else signal.id + ":delivery"
    delivery = signal.evidence.get("delivery_execution_snapshot") or {}
    if stage == "delivery":
        sent = store.db.execute(
            "SELECT updated_ms FROM outbox WHERE signal_id=? AND key LIKE '%:initial' "
            "AND status='sent' AND message_id IS NOT NULL LIMIT 1",
            (signal.id,),
        ).fetchone()
        if not sent or not number(delivery.get("executable_price")):
            return
    if store.db.execute("SELECT 1 FROM setup_path_labels WHERE signal_id=?", (job_id,)).fetchone():
        return
    risk = abs(signal.entry - signal.stop)
    cost = (
        delivery.get("cost", {}).get("cost_per_base")
        if stage == "delivery"
        else signal.risk.get("cost_per_base")
    )
    if not positive(risk) or not number(cost):
        return
    e = signal.evidence
    sign = 1 if signal.direction == "LONG" else -1
    deadline = signal.holding_deadline_ms or signal.expires_ms
    decision = max(delivery["available_ms"], sent[0]) if stage == "delivery" else e["v22"]["available_ms"]
    entry = delivery["executable_price"] if stage == "delivery" else signal.entry
    entry_window = min(signal.expires_ms, signal.trigger_expires_ms or signal.expires_ms)
    if decision >= entry_window:
        return
    targets = {
        "execution_tp1": signal.tp1,
        "structural_tp1": e.get("structural_tp1", signal.tp1),
        "r050": entry + sign * 0.5 * risk,
        "r075": entry + sign * 0.75 * risk,
        "r100": entry + sign * risk,
        "partial075_structural": e.get("structural_tp1", signal.tp1),
        "time_decay": signal.tp1,
    }
    policies = {
        key: dict(
            target=target,
            partial_fraction=0.5 if key == "partial075_structural" else 0,
            partial_done=False,
            complete=False,
            ambiguous=False,
            outcome=None,
            net_r=None,
        )
        for key, target in targets.items()
    }
    payload = dict(
        policy="setup-exit-research-v22",
        source=signal.source,
        symbol=signal.symbol,
        signal_id=signal.id,
        stage=stage,
        visible_at_entry=stage == "delivery",
        delivered_ms=sent[0] if stage == "delivery" else None,
        observed_quote_ms=delivery.get("available_ms") if stage == "delivery" else None,
        direction=signal.direction,
        available_ms=decision,
        feature_schema=signal.feature_schema_version,
        decision_ms=decision,
        entry=entry,
        original_planned_entry=signal.entry,
        stop=signal.stop,
        risk=risk,
        cost_per_base=cost,
        structural_tp1=e.get("structural_tp1", signal.tp1),
        execution_tp1=signal.tp1,
        entry_deadline_ms=entry_window,
        deadline_ms=deadline,
        decay_after_ms=decision + signal.expected_hold_min * 60000,
        coverage_complete=True,
        entered_ms=None,
        first_stop_ms=None,
        entry_stop_order_unknown=False,
        entry_milestones_unknown=[],
        mfe_lower_bound_r=0.0,
        mae_lower_bound_r=0.0,
        milestones={},
        policies=policies,
        score=signal.quality,
        original_gates=list(signal.gates),
        v211_baseline=deepcopy(e.get("v211_baseline_assessment")),
        ablations=deepcopy(e.get("v22", {}).get("research_ablations", {})),
        entry_assumption="First complete 1m candle touching the frozen planned entry; touch is not a verified fill",
        costs_verified=False,
        account_fill_verified=False,
        research_only=True,
        production_ml=False,
        partial_assumption="50% at +0.75 original R; remainder at frozen structural target/stop/deadline",
        decay_assumption="At/after minimum planned hold, close if favorable excursion remains below +0.25R",
    )
    store.db.execute(
        "INSERT OR IGNORE INTO setup_path_jobs VALUES(?,?,?,?,?,?,?)",
        (
            job_id,
            signal.source,
            signal.symbol,
            now,
            decision // 60000 * 60000,
            deadline,
            json.dumps(payload),
        ),
    )


def finish_policy(p, row, reason, at, price):
    sign = 1 if p["direction"] == "LONG" else -1
    gross_r = sign * (price - p["entry"]) / p["risk"]
    if row["partial_done"]:
        gross_r = row["partial_fraction"] * 0.75 + (1 - row["partial_fraction"]) * gross_r
    valid = p["coverage_complete"] and not row["ambiguous"]
    row.update(
        outcome=reason,
        exit_ms=at,
        exit_price=price,
        net_r=gross_r - p["cost_per_base"] / p["risk"] if valid else None,
        complete=valid,
    )


def advance(store, ident, bars, now):
    row = store.db.execute(
        "SELECT cursor_ms,deadline_ms,payload FROM setup_path_jobs WHERE signal_id=?", (ident,)
    ).fetchone()
    if not row:
        return
    cursor, deadline, raw = row
    p = json.loads(raw)
    sign = 1 if p["direction"] == "LONG" else -1
    for b in sorted(bars, key=lambda b: b.start):
        if b.end <= cursor or b.end > min(now, deadline):
            continue
        if b.start != cursor or b.interval != 60000:
            p["coverage_complete"] = False
        cursor = b.end
        if (
            not all(positive(v) for v in (b.open, b.high, b.low, b.close))
            or not b.low <= min(b.open, b.close) <= max(b.open, b.close) <= b.high
        ):
            p["coverage_complete"] = False
            continue
        if b.start < p["decision_ms"]:
            continue  # Partial decision candle cannot order entry and exits.
        if p["entered_ms"] is None:
            if b.start >= p["entry_deadline_ms"]:
                break
            if not b.low <= p["entry"] <= b.high:
                continue
            p["entered_ms"] = b.end
            if b.low <= p["stop"] <= b.high:
                p["entry_stop_order_unknown"] = True
            entry_peak = sign * ((b.high if sign > 0 else b.low) - p["entry"]) / p["risk"]
            p["entry_milestones_unknown"] = [v for v in (0.25, 0.5, 0.75, 1.0) if entry_peak >= v]
            if entry_peak >= 0.25:
                # A lower-bound MFE below .25R cannot prove that the entry
                # candle never reached it after entry. Keep decay ambiguous.
                p["policies"]["time_decay"]["ambiguous"] = True
            # Entry-candle path order is unknown. Do not guess MFE or a fill.
            for key, policy in p["policies"].items():
                partial = p["entry"] + sign * 0.75 * p["risk"]
                levels = [p["stop"], policy["target"]] + ([partial] if policy["partial_fraction"] else [])
                if any(b.low <= price <= b.high for price in levels):
                    policy["ambiguous"] = True
            continue
        adverse, favorable = (b.low, b.high) if sign > 0 else (b.high, b.low)
        stop_hit = sign * (adverse - p["stop"]) <= 0
        peak = sign * (favorable - p["entry"]) / p["risk"]
        trough = sign * (adverse - p["entry"]) / p["risk"]
        if p["first_stop_ms"] is None and not p.get("entry_stop_order_unknown"):
            if not stop_hit:
                p["mfe_lower_bound_r"] = max(p["mfe_lower_bound_r"], peak)
                p["mae_lower_bound_r"] = min(p["mae_lower_bound_r"], trough)
            else:
                p["mae_lower_bound_r"] = min(
                    p["mae_lower_bound_r"], sign * (p["stop"] - p["entry"]) / p["risk"]
                )
            for milestone in (0.25, 0.5, 0.75, 1.0):
                if peak >= milestone and str(milestone) not in p["milestones"]:
                    p["milestones"][str(milestone)] = dict(
                        at_ms=b.end, elapsed_ms=b.end - p["entered_ms"], ordering_ambiguous=stop_hit
                    )
            if stop_hit:
                p["first_stop_ms"] = b.end
        for key, policy in p["policies"].items():
            if policy["outcome"] is not None:
                continue
            target_hit = sign * (favorable - policy["target"]) >= 0
            partial_hit = bool(policy["partial_fraction"] and not policy["partial_done"] and peak >= 0.75)
            if stop_hit and (target_hit or partial_hit):
                policy["ambiguous"] = True
            if partial_hit and not stop_hit:
                policy["partial_done"] = True
                policy["partial_ms"] = b.end
            if stop_hit:
                finish_policy(p, policy, "STOP", b.end, p["stop"])
            elif target_hit:
                finish_policy(p, policy, "TARGET", b.end, policy["target"])
            elif key == "time_decay" and b.end >= p["decay_after_ms"] and p["mfe_lower_bound_r"] < 0.25:
                finish_policy(p, policy, "EDGE_DECAY", b.end, b.close)
                p.setdefault("edge_decay_ms", b.end)
            elif b.end >= deadline:
                finish_policy(p, policy, "TIME_EXIT", b.end, b.close)
        if all(r["outcome"] is not None for r in p["policies"].values()):
            break
    done = (
        p["entered_ms"] is None
        and cursor >= p["entry_deadline_ms"]
        or all(r["outcome"] is not None for r in p["policies"].values())
        or cursor >= deadline
    )
    with store.db:
        if done:
            for milestone in (0.5, 1.0):
                reached = p["milestones"].get(str(milestone))
                p[f"stop_before_{milestone:g}r"] = (
                    (
                        None
                        if (reached and reached["ordering_ambiguous"])
                        or (not reached and milestone in p.get("entry_milestones_unknown", []))
                        else bool(
                            p["first_stop_ms"] and (not reached or p["first_stop_ms"] < reached["at_ms"])
                        )
                    )
                    if p["entered_ms"] is not None
                    and p["coverage_complete"]
                    and not p.get("entry_stop_order_unknown")
                    else None
                )
            for name in ("execution_tp1", "structural_tp1"):
                row = p["policies"][name]
                p["time_to_" + name + "_ms"] = (
                    row["exit_ms"] - p["entered_ms"]
                    if row["complete"] and row["outcome"] == "TARGET"
                    else None
                )
            p.update(
                available_ms=now,
                complete=p["coverage_complete"]
                and p["entered_ms"] is not None
                and all(r["complete"] for r in p["policies"].values()),
                state="NO_ENTRY"
                if p["entered_ms"] is None
                else "OBSERVED"
                if p["coverage_complete"]
                else "INCOMPLETE",
                extrema_scope="Pre-stop horizon price opportunity; excludes entry candle and stop-bar favorable extrema; not exact tick MFE/MAE or post-exit position returns",
                time_exit_r=p["policies"]["execution_tp1"]["net_r"]
                if p["policies"]["execution_tp1"]["outcome"] == "TIME_EXIT"
                else None,
            )
            store.db.execute(
                "INSERT OR IGNORE INTO setup_path_labels VALUES(?,?,?)", (ident, now, json.dumps(p))
            )
            store.db.execute("DELETE FROM setup_path_jobs WHERE signal_id=?", (ident,))
        else:
            store.db.execute(
                "UPDATE setup_path_jobs SET next_ms=?,cursor_ms=?,payload=? WHERE signal_id=?",
                (now + 60000, cursor, json.dumps(p), ident),
            )
    return p


def advance_batch(root, identifiers, bars, now):
    from .storage import Store

    store = Store(root)
    try:
        for ident in identifiers:
            advance(store, ident, bars, now)
    finally:
        store.close()


def advance_mixed_batch(root, identifiers, bars, now):
    """Share one candle response and metadata connection per original market."""
    from .path_research import advance as advance_swing
    from .storage import Store

    store = Store(root)
    try:
        for ident, kind in identifiers:
            (advance if kind == "v22" else advance_swing)(store, ident, bars, now)
    finally:
        store.close()
