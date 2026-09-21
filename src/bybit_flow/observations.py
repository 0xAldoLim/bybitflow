"""Separate immutable primary outcomes and compact, path-dependent late research.

Candle bounds are observations, never account fills. Ambiguous stop/target candles
resolve conservatively to stop. Missing candles preclude successful policy labels.
"""

import hashlib
import json
from collections import defaultdict
from statistics import mean, median

from .horizons import PROFILES, session_context
from .models import Signal

TERMINAL = {"EXPIRED", "INVALIDATED", "RESOLVED"}


def migrate(store):
    db = store.db
    if store.get("horizons_migration_complete"):
        return
    # SQLite backup, not a copy of a live WAL database. Never remove previous backups.
    target = store.root / "before-horizons-v1.sqlite"
    if not target.exists():
        store.backup(target)
    db.executescript("""
    CREATE TABLE IF NOT EXISTS signal_origins(signal_id TEXT PRIMARY KEY, payload TEXT NOT NULL, sha256 TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS observations(signal_id TEXT PRIMARY KEY, status TEXT NOT NULL, next_ms INTEGER NOT NULL,
        payload TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS observations_due ON observations(status,next_ms);
    CREATE TABLE IF NOT EXISTS research_checkpoints(signal_id TEXT, at_ms INTEGER, payload TEXT NOT NULL,
        PRIMARY KEY(signal_id,at_ms));
    CREATE TABLE IF NOT EXISTS research_labels(signal_id TEXT PRIMARY KEY, available_ms INTEGER, payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS storage_leases(id TEXT PRIMARY KEY, start_ms INTEGER, end_ms INTEGER, expires_ms INTEGER,
        reason TEXT NOT NULL);
    """)
    # Original serialized payload remains byte-identical; defaults are applied only on read.
    with db:
        for ident, payload in db.execute("SELECT id,payload FROM signals").fetchall():
            db.execute(
                "INSERT OR IGNORE INTO signal_origins VALUES(?,?,?)",
                (ident, payload, hashlib.sha256(payload.encode()).hexdigest()),
            )
        db.execute("INSERT OR REPLACE INTO kv VALUES('horizons_migration_complete','true')")


def preserve_origin(store, signal):
    payload = signal.model_dump_json()
    store.db.execute(
        "INSERT OR IGNORE INTO signal_origins VALUES(?,?,?)",
        (signal.id, payload, hashlib.sha256(payload.encode()).hexdigest()),
    )


def start(store, signal, at_ms, checkpoints=None):
    if signal.synthetic or signal.state not in TERMINAL:
        return
    if store.db.execute("SELECT 1 FROM observations WHERE signal_id=?", (signal.id,)).fetchone():
        return
    # Unscored warmup attempts have no frozen trading decision and are not ML outcomes.
    if not signal.evidence.get("score_components"):
        return
    schedule = (
        checkpoints
        or store.get("post_terminal_checkpoints", {}).get(signal.horizon_profile)
        or PROFILES[signal.horizon_profile].checkpoints
    )
    ends = [signal.created_ms + m * 60_000 for m in schedule if signal.created_ms + m * 60_000 > at_ms]
    if not ends:
        return
    primary = signal.evidence.get("primary_outcome", signal.state)
    exit_price = signal.evidence.get("latest_observed_price")
    net_r = None
    if (
        exit_price is not None
        and signal.evidence.get("observed_entry_ms")
        and signal.evidence.get("primary_coverage_complete", True)
        and signal.coverage.get("monitoring") == "active"
        and signal.entry != signal.stop
    ):
        sign = 1 if signal.direction == "LONG" else -1
        net_r = (sign * (exit_price - signal.entry) - signal.risk.get("cost_per_base", 0)) / abs(
            signal.entry - signal.stop
        )
    payload = dict(
        signal=signal.model_dump(mode="json"),
        primary_outcome=primary,
        primary_net_r=net_r,
        primary_return_method="observed executable quote versus planned entry, configured costs; not an account return",
        primary_terminal_reason=signal.invalidation,
        terminal_ms=at_ms,
        terminal_session=session_context(at_ms)["primary"],
        entry_session=signal.entry_session,
        sessions_traversed=[signal.entry_session] if signal.entry_session else [],
        research_observation_status="FOLLOWING_LATE_OUTCOME",
        operational_status=signal.state,
        checkpoints=ends,
        deadline_ms=max(ends),
        cursor_ms=at_ms,
        coverage_complete=signal.evidence.get("primary_coverage_complete", True)
        and signal.coverage.get("monitoring") != "paused",
        post_terminal_mfe=0.0,
        post_terminal_mae=0.0,
        late_target_hit=False,
        late_stop_hit=False,
        time_to_late_target=None,
        time_to_late_stop=None,
        extended_same_rules_outcome=(
            "STOP"
            if primary == "STOP" or "stop" in signal.invalidation.lower() and signal.state == "INVALIDATED"
            else "TARGET"
            if primary == "TARGET"
            else "PENDING"
        ),
        best_observed_horizon=None,
        method="closed 1-minute OHLC bounds, stop-first ambiguity policy",
    )
    with store.db:
        store.db.execute(
            "INSERT INTO observations VALUES(?,?,?,?)",
            (signal.id, "FOLLOWING_LATE_OUTCOME", at_ms, json.dumps(payload)),
        )


def classify(p):
    if not p["coverage_complete"]:
        return "UNCLEAR"
    if p["primary_outcome"] == "TARGET":
        return "THESIS_AND_TIMING_CORRECT"
    if p["extended_same_rules_outcome"] == "TARGET" and p["primary_outcome"] == "EXPIRED":
        return "THESIS_CORRECT_HORIZON_TOO_SHORT"
    if p["late_target_hit"] and p["late_stop_hit"]:
        # Cannot distinguish poor entry from overly tight stop without another validated policy.
        return "THESIS_CORRECT_TIMING_TOO_EARLY"
    if p["post_terminal_mfe"] < 0.25 and p["post_terminal_mae"] <= -1:
        return "THESIS_WRONG"
    return "UNCLEAR"


def advance(store, ident, bars, now):
    row = store.db.execute("SELECT payload FROM observations WHERE signal_id=?", (ident,)).fetchone()
    if not row:
        return
    p = json.loads(row[0])
    s = Signal.model_validate(p["signal"])
    sign = 1 if s.direction == "LONG" else -1
    distance = abs(s.entry - s.stop)
    if not distance:
        return
    for b in sorted(bars, key=lambda b: b.start):
        if b.start < p["terminal_ms"] < b.end <= now and b.end > p["cursor_ms"]:
            # Whole-bar bounds can rule out a crossing, but cannot locate one before/after terminal time.
            crossed = (
                (b.low <= s.stop or b.high >= s.tp1) if sign > 0 else (b.high >= s.stop or b.low <= s.tp1)
            )
            if crossed:
                p["coverage_complete"] = False
            p["cursor_ms"] = b.end
            continue
        if b.end > now or b.end <= p["cursor_ms"] or b.start < p["terminal_ms"] or b.end > p["deadline_ms"]:
            continue
        # Ignore the partial terminal candle, record the initial coverage boundary explicitly.
        expected = ((p["cursor_ms"] + 59_999) // 60_000) * 60_000
        if b.start > expected:
            p["coverage_complete"] = False
        favorable = (b.high - s.entry) / distance if sign > 0 else (s.entry - b.low) / distance
        adverse = (b.low - s.entry) / distance if sign > 0 else (s.entry - b.high) / distance
        p["post_terminal_mfe"] = max(p["post_terminal_mfe"], favorable)
        p["post_terminal_mae"] = min(p["post_terminal_mae"], adverse)
        stop = b.low <= s.stop if sign > 0 else b.high >= s.stop
        target = b.high >= s.tp1 if sign > 0 else b.low <= s.tp1
        if stop and not p["late_stop_hit"]:
            p.update(late_stop_hit=True, time_to_late_stop=b.end - p["terminal_ms"])
        if target and not p["late_target_hit"]:
            p.update(late_target_hit=True, time_to_late_target=b.end - p["terminal_ms"])
            elapsed = (b.end - s.created_ms) / 60_000
            p["best_observed_horizon"] = next(
                (name for name, h in PROFILES.items() if name != "LEGACY" and elapsed <= h.hold_max), None
            )
        if p["extended_same_rules_outcome"] == "PENDING" and (stop or target):
            p["extended_same_rules_outcome"] = (
                "STOP" if stop else "TARGET" if p["coverage_complete"] else "UNCLEAR"
            )
            p["extended_policy_duration"] = b.end - s.created_ms
        session = session_context(b.end)["primary"]
        if not p["sessions_traversed"] or p["sessions_traversed"][-1] != session:
            p["sessions_traversed"].append(session)
        p["cursor_ms"] = b.end
        due = [t for t in p["checkpoints"] if t <= b.end]
        with store.db:
            for t in due:
                checkpoint = {k: v for k, v in p.items() if k not in {"signal", "checkpoints"}}
                store.db.execute(
                    "INSERT OR IGNORE INTO research_checkpoints VALUES(?,?,?)",
                    (ident, t, json.dumps(checkpoint)),
                )
        p["checkpoints"] = [t for t in p["checkpoints"] if t > b.end]
    if now >= p["deadline_ms"] and p["cursor_ms"] >= p["deadline_ms"] - 60_000:
        p["research_observation_status"] = "COMPLETE"
        if p["extended_same_rules_outcome"] == "PENDING":
            p["extended_same_rules_outcome"] = "EXPIRED"
        p["timing_classification"] = classify(p)
        p["directional_recovery"] = p["post_terminal_mfe"] >= 1
        p["recovery_magnitude_r"] = p["post_terminal_mfe"]
        p["extended_policy_outcome"] = p["extended_same_rules_outcome"]
        if s.evidence.get("withdrawal"):
            stop_time, target_time = p["time_to_late_stop"], p["time_to_late_target"]
            p["withdrawal_research"] = dict(
                policy=s.evidence["withdrawal"]["policy"],
                classification="WITHDRAWAL_AMBIGUOUS"
                if not p["coverage_complete"]
                else "WITHDRAWAL_TOO_EARLY"
                if target_time is not None and (stop_time is None or target_time < stop_time)
                else "WITHDRAWAL_SAVED_STOP"
                if stop_time is not None
                else "INCONCLUSIVE",
                post_withdrawal_mfe=p["post_terminal_mfe"],
                post_withdrawal_mae=p["post_terminal_mae"],
                original_stop_later_hit=p["late_stop_hit"],
                original_tp1_later_hit=p["late_target_hit"],
            )

        with store.db:
            store.db.execute(
                "INSERT OR IGNORE INTO research_labels VALUES(?,?,?)", (ident, now, json.dumps(p))
            )
    with store.db:
        store.db.execute(
            "UPDATE observations SET status=?,next_ms=?,payload=? WHERE signal_id=?",
            (p["research_observation_status"], now + 300_000, json.dumps(p), ident),
        )
    return p


def advance_batch(root, identities, bars, at_ms):
    """Late research uses its own connection away from live socket processing."""
    from .storage import Store

    store = Store(root)
    try:
        for ident in identities:
            advance(store, ident, bars, at_ms)
    finally:
        store.close()


def metrics(store):
    groups = defaultdict(list)
    for (ident,) in store.db.execute(
        "SELECT signal_id FROM research_labels ORDER BY available_ms DESC LIMIT 10000"
    ).fetchall():
        p = json.loads(
            store.db.execute("SELECT payload FROM research_labels WHERE signal_id=?", (ident,)).fetchone()[0]
        )
        s = p["signal"]
        for dimension, value in (
            ("tier", s["raw_tier"]),
            ("family", s["family"]),
            ("session", s.get("entry_session")),
            ("regime", s["regime"]),
            ("direction", s["direction"]),
            ("volatility", s.get("evidence", {}).get("execution", {}).get("volatility_regime")),
        ):
            groups[(s.get("horizon_profile", "LEGACY"), dimension, str(value))].append(p)
    report = []
    for (h, d, v), rows in groups.items():
        known = [p for p in rows if p["coverage_complete"]]
        report.append(
            dict(
                horizon=h,
                dimension=d,
                value=v,
                count=len(rows),
                complete=len(known),
                primary_win_rate=sum(p["primary_outcome"] == "TARGET" for p in known) / len(known)
                if known
                else None,
                late_followthrough_rate=mean(p["late_target_hit"] for p in known) if known else None,
                extended_same_rules_success=mean(p["extended_same_rules_outcome"] == "TARGET" for p in known)
                if known
                else None,
                mfe=mean(p["post_terminal_mfe"] for p in rows),
                mae=mean(p["post_terminal_mae"] for p in rows),
                median_time_to_target=median(
                    [p["time_to_late_target"] for p in rows if p["time_to_late_target"] is not None]
                )
                if any(p["time_to_late_target"] is not None for p in rows)
                else None,
                net_expectancy=mean([p["primary_net_r"] for p in rows if p.get("primary_net_r") is not None])
                if any(p.get("primary_net_r") is not None for p in rows)
                else None,
                average_win=mean([p["primary_net_r"] for p in rows if (p.get("primary_net_r") or 0) > 0])
                if any((p.get("primary_net_r") or 0) > 0 for p in rows)
                else None,
                average_loss=mean([p["primary_net_r"] for p in rows if (p.get("primary_net_r") or 0) < 0])
                if any((p.get("primary_net_r") or 0) < 0 for p in rows)
                else None,
                median_time_to_stop=median(
                    [p["time_to_late_stop"] for p in rows if p.get("time_to_late_stop") is not None]
                )
                if any(p.get("time_to_late_stop") is not None for p in rows)
                else None,
                limitation="late OHLC observations are not executable primary returns",
            )
        )
    return report


def refresh_recommendations(root, asof):
    """Materialize small causal summaries outside the live confirmation loop."""
    from .storage import Store

    store = Store(root)
    try:
        families = {}
        query = "SELECT json_extract(payload,'$.coverage_complete'), json_extract(payload,'$.signal.family'), json_extract(payload,'$.extended_same_rules_outcome'), json_extract(payload,'$.best_observed_horizon') FROM research_labels WHERE available_ms<? ORDER BY available_ms DESC LIMIT 10000"
        for complete, family, outcome, horizon in store.db.execute(query, (asof,)).fetchall():
            if not complete:
                continue
            group = families.setdefault(family, dict(samples=0, successful_horizon_counts={}))
            group["samples"] += 1
            if outcome == "TARGET" and horizon:
                group["successful_horizon_counts"][horizon] = (
                    group["successful_horizon_counts"].get(horizon, 0) + 1
                )
        store.put("horizon_recommendations", dict(available_ms=asof, families=families))
    finally:
        store.close()


def recommend(store, signal, asof, minimum=100):
    # Never sort multi-megabyte labels on the live event loop. A missing/stale
    # optional research summary cannot prevent confirmation or Discord delivery.
    cache = store.get("horizon_recommendations", {})
    valid = 0 <= asof - cache.get("available_ms", -86_400_000) <= 86_400_000
    group = cache.get("families", {}).get(signal.family, {}) if valid else {}
    counts = group.get("successful_horizon_counts", {})
    samples = group.get("samples", 0)
    return dict(
        status="research-only" if samples >= minimum else "insufficient evidence",
        samples=samples,
        recommended_horizon_profile=max(counts, key=counts.get) if counts and samples >= minimum else None,
        successful_horizon_counts=counts,
        available_ms=cache.get("available_ms") if valid else None,
        production_enabled=False,
    )
