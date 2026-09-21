"""Bounded forward Swing path research, separate from production lifecycle/ML labels.

Records OHLC bounds and a four-policy stop grid fixed at the decision. No stop is
changed in a live signal. OHLC entry-order ambiguity excludes executable returns.
"""

import json
import math
from statistics import mean


def migrate(store):
    store.db.executescript("""
    CREATE TABLE IF NOT EXISTS swing_path_jobs(signal_id TEXT PRIMARY KEY,source TEXT,symbol TEXT,
        next_ms INTEGER,cursor_ms INTEGER,deadline_ms INTEGER,payload TEXT);
    CREATE INDEX IF NOT EXISTS swing_path_due ON swing_path_jobs(next_ms);
    CREATE TABLE IF NOT EXISTS swing_path_labels(signal_id TEXT PRIMARY KEY,available_ms INTEGER,payload TEXT);
    CREATE INDEX IF NOT EXISTS swing_path_label_time ON swing_path_labels(available_ms);
    """)


def register(store, signal, decision_ms):
    if signal.synthetic or signal.horizon_profile != "SWING" or not signal.evidence.get("score_components"):
        return
    identity = store.db.execute(
        "SELECT canonical_signal_id FROM candidate_identities WHERE signal_id=?", (signal.id,)
    ).fetchone()
    if identity and identity[0] != signal.id:
        return
    if store.db.execute("SELECT 1 FROM swing_path_labels WHERE signal_id=?", (signal.id,)).fetchone():
        return
    atr = signal.evidence.get("setup_features", {}).get("atr")
    noise = signal.evidence.get("execution", {}).get("short_horizon_noise")
    risk = abs(signal.entry - signal.stop)
    if not atr or noise is None or not risk:
        return
    sign = 1 if signal.direction == "LONG" else -1
    structural = (signal.evidence.get("stop_plan") or {}).get("anchor", signal.stop + sign * 0.15 * atr)
    distances = {
        "incumbent": risk,
        "noise_buffer": max(risk, noise),
        "sweep_volatility": max(risk, abs(signal.entry - structural) + 0.3 * atr),
        "atr_floor": max(risk, atr),
    }
    costs = signal.risk.get("cost_per_base")
    payload = dict(
        policy="swing-path-v1",
        signal_id=signal.id,
        source=signal.source,
        symbol=signal.symbol,
        direction=signal.direction,
        family=signal.family,
        decision_ms=decision_ms,
        entry=signal.entry,
        stop=signal.stop,
        tp1=signal.tp1,
        tp2=signal.tp2,
        trigger_deadline=signal.trigger_expires_ms or signal.expires_ms,
        primary_deadline=signal.holding_deadline_ms or signal.expires_ms,
        decision_atr=atr,
        decision_noise=noise,
        structural_level=structural,
        risk=risk,
        cost_per_base=costs,
        coverage_complete=True,
        ambiguous=False,
        entered_ms=None,
        first_stop_ms=None,
        first_tp1_ms=None,
        first_tp2_ms=None,
        reclaim_ms=None,
        max_stop_overshoot=0.0,
        time_beyond_stop_ms=0,
        duration_method="sum of bar durations whose close is beyond stop; not continuous tick duration",
        post_stop_mfe_r=0.0,
        post_stop_mae_r=0.0,
        higher_timeframe_failure=None,
        policies={
            name: dict(
                stop=signal.entry - sign * d,
                risk=d,
                net_rr=(sign * (signal.tp1 - signal.entry) - costs) / d if costs is not None else None,
                size_per_risk_unit=1 / d,
                outcome=None,
                net_r=None,
            )
            for name, d in distances.items()
        },
    )
    cursor = decision_ms // 60000 * 60000
    deadline = max(payload["primary_deadline"], decision_ms + 7 * 86400000)
    # Joins Store.signal's transaction; a repeated evaluation cannot rewrite the grid.
    store.db.execute(
        "INSERT OR IGNORE INTO swing_path_jobs VALUES(?,?,?,?,?,?,?)",
        (signal.id, signal.source, signal.symbol, decision_ms, cursor, deadline, json.dumps(payload)),
    )


def classify_path(p):
    """Descriptive predeclared research classes, never edits a primary outcome."""
    if not p.get("complete"):
        return "PATH_AMBIGUOUS"
    original = p["policies"]["incumbent"]["outcome"]
    if original == "TARGET":
        return "THESIS_AND_TIMING_CORRECT"
    if original == "EXPIRED" and p.get("first_tp1_ms") and not p.get("first_stop_ms"):
        return "THESIS_CORRECT_HORIZON_TOO_SHORT"
    stopped = p.get("first_stop_ms")
    reclaimed = p.get("reclaim_ms")
    recovered = p.get("first_tp1_ms")
    wider_success = any(
        name != "incumbent" and row["outcome"] == "TARGET" and (row.get("net_rr") or 0) >= 2
        for name, row in p["policies"].items()
    )
    if (
        original == "STOP"
        and stopped
        and reclaimed
        and recovered
        and recovered <= p["primary_deadline"]
        and 0 < reclaimed - stopped <= 14400000
        and p["max_stop_overshoot"] <= 0.25 * p["decision_atr"]
        and p.get("higher_timeframe_failure") is None
        and wider_success
    ):
        return "THESIS_CORRECT_STOP_TOO_TIGHT"
    if p.get("higher_timeframe_failure") and p["post_stop_mfe_r"] < 0.25:
        return "THESIS_WRONG"
    return "UNRESOLVED_THESIS_VS_STOP"


def advance(store, ident, bars, now):
    row = store.db.execute(
        "SELECT cursor_ms,deadline_ms,payload FROM swing_path_jobs WHERE signal_id=?", (ident,)
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
        prices = (b.open, b.high, b.low, b.close)
        if (
            not all(math.isfinite(x) and x > 0 for x in prices)
            or not b.low <= min(b.open, b.close) <= max(b.open, b.close) <= b.high
        ):
            p["coverage_complete"] = False
            continue
        if b.start < p["decision_ms"] < b.end:
            # Whole-bar bounds cannot locate a crossing before/after the decision.
            if any(b.low <= level <= b.high for level in (p["entry"], p["stop"], p["tp1"], p["tp2"])):
                p["ambiguous"] = True
            continue
        if p["entered_ms"] is None:
            if b.start > p["trigger_deadline"]:
                continue
            if not b.low <= p["entry"] <= b.high:
                continue
            p["entered_ms"] = b.end
            # The entry candle does not establish intrabar ordering.
            if any(b.low <= x <= b.high for x in [p["stop"], p["tp1"], p["tp2"]]):
                p["ambiguous"] = True
        adverse = b.low if sign > 0 else b.high
        favorable = b.high if sign > 0 else b.low
        stop = sign * (adverse - p["stop"]) <= 0
        tp1 = sign * (favorable - p["tp1"]) >= 0
        tp2 = sign * (favorable - p["tp2"]) >= 0
        for hit, key in [(stop, "first_stop_ms"), (tp1, "first_tp1_ms"), (tp2, "first_tp2_ms")]:
            if hit and p[key] is None:
                p[key] = b.end
        if stop and tp1:
            p["ambiguous"] = True
        if p["first_stop_ms"] is not None:
            p["max_stop_overshoot"] = max(p["max_stop_overshoot"], -sign * (adverse - p["stop"]))
            if sign * (b.close - p["stop"]) < 0:
                p["time_beyond_stop_ms"] += b.interval
            elif p["reclaim_ms"] is None and b.end > p["first_stop_ms"]:
                p["reclaim_ms"] = b.end
            p["post_stop_mfe_r"] = max(p["post_stop_mfe_r"], sign * (favorable - p["entry"]) / p["risk"])
            p["post_stop_mae_r"] = min(p["post_stop_mae_r"], sign * (adverse - p["entry"]) / p["risk"])
        if b.end % 14400000 == 0 and sign * (b.close - p["structural_level"]) < 0:
            p["higher_timeframe_failure"] = b.end
        for policy in p["policies"].values():
            if policy["outcome"] is not None:
                continue
            crossed = sign * (adverse - policy["stop"]) <= 0
            if crossed or tp1 or b.end >= p["primary_deadline"]:
                outcome = "STOP" if crossed else "TARGET" if tp1 else "EXPIRED"
                price = policy["stop"] if crossed else p["tp1"] if tp1 else b.close
                policy.update(
                    outcome=outcome,
                    exit_ms=b.end,
                    ambiguous=bool(crossed and tp1) or b.end == p["entered_ms"],
                    net_r=(sign * (price - p["entry"]) - p["cost_per_base"]) / policy["risk"]
                    if p["cost_per_base"] is not None
                    else None,
                )
    with store.db:
        if cursor >= deadline:
            complete = (
                p["coverage_complete"]
                and not p["ambiguous"]
                and p["entered_ms"] is not None
                and not any(x.get("ambiguous") for x in p["policies"].values())
            )
            p.update(
                complete=complete,
                available_ms=now,
                method="closed 1m OHLC bounds; stop first; no account fills; 4H close through decision structure is a failure proxy, not a validated thesis label",
            )
            p["classification"] = classify_path(p)
            p["classification_scope"] = (
                "predeclared descriptive hypothesis; not a validated training target for production"
            )
            store.db.execute(
                "INSERT OR IGNORE INTO swing_path_labels VALUES(?,?,?)", (ident, now, json.dumps(p))
            )
            store.db.execute("DELETE FROM swing_path_jobs WHERE signal_id=?", (ident,))
        else:
            store.db.execute(
                "UPDATE swing_path_jobs SET next_ms=?,cursor_ms=?,payload=? WHERE signal_id=?",
                (now + 60000, cursor, json.dumps(p), ident),
            )
    return p


def advance_batch(root, ids, bars, now):
    from .storage import Store

    store = Store(root)
    try:
        for ident in ids:
            advance(store, ident, bars, now)
    finally:
        store.close()


def summary(store, now):
    rows = [
        json.loads(r[0])
        for r in store.db.execute(
            "SELECT payload FROM swing_path_labels WHERE available_ms<=? ORDER BY available_ms DESC LIMIT 10000",
            (now,),
        )
    ]
    eligible = [r for r in rows if r.get("complete") and r.get("cost_per_base") is not None]
    report = dict(
        policy="swing-path-v1",
        status="INSUFFICIENT_EVIDENCE",
        production_enabled=False,
        samples=len(rows),
        complete=len(eligible),
        minimum=500,
        pending=store.db.execute("SELECT count(*) FROM swing_path_jobs").fetchone()[0],
        stop_policies=["incumbent", "noise_buffer", "sweep_volatility", "atr_floor"],
        descriptive_net_r={
            name: mean(
                p["policies"][name]["net_r"] for p in eligible if p["policies"][name]["net_r"] is not None
            )
            for name in ("incumbent", "noise_buffer", "sweep_volatility", "atr_floor")
            if any(p["policies"][name]["net_r"] is not None for p in eligible)
        },
        decision="Original live stops retained; independent purged chronological validation required",
        limitation="OHLC bounds cannot establish all intrabar entry ordering or higher-timeframe thesis failure",
    )

    from .ml.confirmation_research import assess, partitions

    boundary = store.get("swing_stop_holdout_end", -1)
    prepared = [
        dict(
            id=p["signal_id"],
            decision_ms=p["decision_ms"],
            available_ms=p["available_ms"],
            net_r=p["policies"]["incumbent"]["net_r"],
            policies=p["policies"],
        )
        for p in eligible
        if p["decision_ms"] > boundary and all(x.get("net_r") is not None for x in p["policies"].values())
    ]
    parts = partitions(prepared)
    if parts:
        store.put("swing_stop_holdout_end", max(r["decision_ms"] for r in prepared))
        report.update(
            status="SHADOW_EVALUATED",
            partition_sizes=list(map(len, parts)),
            embargo_ms=86400000,
            validation={},
            holdout={},
        )
        for phase, part in [("validation", parts[2]), ("holdout", parts[3])]:
            for policy in report["stop_policies"]:
                rows = [r | dict(net_r=r["policies"][policy]["net_r"]) for r in part]
                report[phase][policy] = assess(rows, lambda r: r["policies"][policy]["net_rr"] >= 2)
                report[phase][policy]["paired_incremental_mean_r"] = mean(
                    r["policies"][policy]["net_r"] - r["policies"]["incumbent"]["net_r"] for r in part
                )
        report["limitation"] += "; fixed 2R net-RR inclusion; costs fixed at decision; no promotion"
    return report
