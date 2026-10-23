"""Versioned economic identities and presentation-only notification claims."""

import hashlib
import json
from decimal import ROUND_HALF_UP, Decimal

TERMINAL = {"EXPIRED", "INVALIDATED", "RESOLVED"}


def fingerprint(signal):
    e = signal.evidence
    tick = Decimal(str(e.get("price_tick") or "0.00000001"))

    def price(v):
        rounded = (Decimal(str(v)) / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick
        return format(rounded.normalize(), "f")

    plan = dict(
        version="alert-fingerprint-v1",
        source=signal.source,
        symbol=signal.symbol,
        direction=signal.direction,
        horizon=signal.horizon_profile,
        family=signal.family,
        trigger=e.get("trigger_bar_end"),
        thesis=signal.setup_thesis_id,
        prices=[price(x) for x in (signal.entry, *signal.zone, signal.stop, signal.tp1, signal.tp2)],
    )
    if e.get("production_policies", {}).get("trade_quality") == "trade-validity-v22":
        # Multiple recognizers of identical executable geometry are one visible
        # opportunity. Every setup ID still retains its own lifecycle.
        plan = dict(
            version="alert-fingerprint-v22",
            source=signal.source,
            symbol=signal.symbol,
            direction=signal.direction,
            horizon=signal.horizon_profile,
            trigger=e.get("frozen_structure", {}).get("event_start_ms", e.get("trigger_bar_end")),
            prices=[price(x) for x in (signal.entry, signal.stop, signal.tp1)],
        )
    return (
        plan["version"]
        + ":"
        + hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )


def migrate(store, backup_directory=None):
    if store.get("hardening_migration_complete"):
        seed_delivered(store)
        return
    target = (backup_directory or store.root) / "before-hardening-v1.sqlite"
    if not target.exists() and store.db.execute("SELECT 1 FROM signals LIMIT 1").fetchone():
        store.backup(target)
    store.db.executescript("""
    CREATE TABLE IF NOT EXISTS initial_alert_claims(
      fingerprint TEXT PRIMARY KEY, signal_id TEXT NOT NULL, cluster_id TEXT,
      claimed_ms INTEGER NOT NULL, delivery_status TEXT NOT NULL, message_id TEXT);
    CREATE TABLE IF NOT EXISTS alert_members(
      signal_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, cluster_id TEXT NOT NULL,
      relationship TEXT NOT NULL, created_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS real_delivery_attempts(
      id INTEGER PRIMARY KEY, at_ms INTEGER NOT NULL, signal_id TEXT NOT NULL,
      symbol TEXT NOT NULL, horizon TEXT NOT NULL, fingerprint TEXT NOT NULL,
      status TEXT NOT NULL, relationship TEXT, message_id TEXT, origin_key TEXT UNIQUE);
    CREATE TABLE IF NOT EXISTS candidate_identities(
      signal_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, candidate_identity TEXT NOT NULL,
      canonical_signal_id TEXT NOT NULL, evaluation_ms INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS candidate_fingerprint ON candidate_identities(fingerprint,evaluation_ms);
    """)
    # Add provenance; never update signal payloads, outbox records, or existing labels.
    from .models import Signal

    rows = store.db.execute("SELECT payload FROM signals ORDER BY created_ms,id").fetchall()
    with store.db:
        for row in rows:
            s = Signal.model_validate_json(row[0])
            register_candidate(store, s)
        store.db.execute("INSERT OR REPLACE INTO kv VALUES('hardening_migration_complete','true')")
    seed_delivered(store)


def seed_delivered(store):
    """Catch older-runtime sends during additive deployment without reclustering them."""
    from .models import Signal

    with store.db:
        for row in store.db.execute(
            "SELECT o.signal_id,o.status,o.message_id,o.updated_ms,s.payload,o.key FROM outbox o JOIN signals s ON s.id=o.signal_id LEFT JOIN alert_members a ON a.signal_id=s.id WHERE o.key LIKE '%:initial' AND a.signal_id IS NULL ORDER BY o.updated_ms"
        ).fetchall():
            s = Signal.model_validate_json(row[4])
            fp = fingerprint(s)
            store.db.execute(
                "INSERT OR IGNORE INTO real_delivery_attempts(at_ms,signal_id,symbol,horizon,fingerprint,status,relationship,message_id,origin_key) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    row[3],
                    s.id,
                    s.symbol,
                    s.horizon_profile,
                    fp,
                    row[1],
                    "MATERIALLY_DIFFERENT_PLAN",
                    row[2],
                    row[5],
                ),
            )
            store.db.execute(
                "INSERT OR IGNORE INTO initial_alert_claims VALUES(?,?,?,?,?,?)",
                (fp, s.id, "cluster:" + s.id, row[3], row[1], row[2]),
            )
            store.db.execute(
                "INSERT OR IGNORE INTO alert_members VALUES(?,?,?,?,?)",
                (s.id, fp, "cluster:" + s.id, "MATERIALLY_DIFFERENT_PLAN", row[3]),
            )


def register_candidate(store, signal):
    fp = fingerprint(signal)
    old = store.db.execute(
        "SELECT candidate_identity FROM candidate_identities WHERE signal_id=?", (signal.id,)
    ).fetchone()
    if old:
        return old[0]
    prior = store.db.execute(
        """SELECT c.candidate_identity,c.canonical_signal_id,s.payload FROM candidate_identities c
      JOIN signals s ON s.id=c.canonical_signal_id WHERE c.fingerprint=? ORDER BY c.evaluation_ms DESC LIMIT 1""",
        (fp,),
    ).fetchone()
    identity, canonical = fp + ":" + signal.id, signal.id
    if prior:
        p = json.loads(prior[2])
        # Adjacent execution slots within the original opportunity are reevaluations.
        deadline = p.get("holding_deadline_ms") or p["expires_ms"]
        if signal.created_ms <= deadline:
            identity, canonical = prior[0], prior[1]
    store.db.execute(
        "INSERT OR IGNORE INTO candidate_identities VALUES(?,?,?,?,?)",
        (signal.id, fp, identity, canonical, signal.created_ms),
    )
    return identity


def relationship(a, b):
    if a.source != b.source or a.symbol != b.symbol:
        return "MATERIALLY_DIFFERENT_PLAN"
    if min(a.holding_deadline_ms or a.expires_ms, b.holding_deadline_ms or b.expires_ms) < max(
        a.created_ms, b.created_ms
    ):
        return "MATERIALLY_DIFFERENT_PLAN"
    if a.direction != b.direction:
        return "CONFLICTING_HORIZON"
    risk = max(abs(a.entry - a.stop), abs(b.entry - b.stop), 1e-12)
    overlap = max(a.zone[0], b.zone[0]) <= min(a.zone[1], b.zone[1])
    if not (overlap or abs(a.entry - b.entry) <= 0.25 * risk) or abs(a.stop - b.stop) > 0.25 * risk:
        return "MATERIALLY_DIFFERENT_PLAN"
    new_rubric = any(
        s.evidence.get("production_policies", {}).get("trade_quality") == "trade-validity-v22" for s in (a, b)
    )
    same_rubric = not new_rubric or a.evidence.get("score_profile") == b.evidence.get("score_profile")
    return "STRONGER_REPLACEMENT" if same_rubric and a.quality >= b.quality + 5 else "CONFIRMING_HORIZON"


def same_v22_execution_event(a, b):
    """Cross-policy comparison without rewriting historical fingerprints."""
    if a.evidence.get("production_policies", {}).get("trade_quality") != "trade-validity-v22":
        return False
    if any(getattr(a, k) != getattr(b, k) for k in ("source", "symbol", "direction", "horizon_profile")):
        return False
    event_a = a.evidence.get("frozen_structure", {}).get("event_start_ms")
    event_b = b.evidence.get("frozen_structure", {}).get("event_start_ms")
    same_event = (
        event_a is not None
        and event_a == event_b
        or a.evidence.get("trigger_bar_end") is not None
        and a.evidence.get("trigger_bar_end") == b.evidence.get("trigger_bar_end")
    )
    tick = Decimal(str(a.evidence.get("price_tick") or "0.00000001"))
    return bool(
        same_event
        and all(
            (Decimal(str(getattr(a, k))) / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            == (Decimal(str(getattr(b, k))) / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            for k in ("entry", "stop", "tp1")
        )
    )


def claim(store, signal, at_ms, *, retry_unattempted=False):
    from .models import Signal

    fp = fingerprint(signal)
    # Serializes independent processes, not just async tasks in one event loop.
    store.db.execute("BEGIN IMMEDIATE")
    try:
        # Match the existing partial-index predicate exactly. SQLite does not
        # normalize reordered NOT IN lists when choosing a partial index.
        # Scanning historical payloads here blocks feeds and holds the writer lock.
        members = store.db.execute(
            """SELECT a.cluster_id,s.payload FROM alert_members a JOIN signals s ON s.id=a.signal_id
          WHERE s.symbol=? AND s.state NOT IN ('INVALIDATED','EXPIRED','RESOLVED') ORDER BY json_extract(s.payload,'$.quality') DESC,a.created_ms DESC""",
            (signal.symbol,),
        ).fetchall()
        cluster, relation, primary = "cluster:" + signal.id, "MATERIALLY_DIFFERENT_PLAN", None
        economic_duplicate = False
        for row in members:
            other = Signal.model_validate_json(row[1])
            if other.id == signal.id:
                continue
            if same_v22_execution_event(signal, other):
                prior = store.db.execute(
                    "SELECT 1 FROM initial_alert_claims WHERE signal_id=? AND "
                    "delivery_status IN ('sent','uncertain','sending') LIMIT 1",
                    (other.id,),
                ).fetchone()
                if prior:
                    economic_duplicate = True
            rel = relationship(signal, other)
            if rel != "MATERIALLY_DIFFERENT_PLAN" and primary is None:
                cluster, relation, primary = row[0], rel, other
                if (
                    signal.evidence.get("production_policies", {}).get("trade_quality")
                    != "trade-validity-v22"
                ):
                    break
        inserted = store.db.execute(
            "INSERT OR IGNORE INTO initial_alert_claims SELECT ?,?,?,?,?,? WHERE ?",
            (fp, signal.id, cluster, at_ms, "sending", None, not economic_duplicate),
        ).rowcount
        if not inserted and retry_unattempted and store.get("v22_http_dispatched:" + signal.id) is False:
            inserted = store.db.execute(
                "UPDATE initial_alert_claims SET delivery_status='sending' WHERE fingerprint=? "
                "AND signal_id=? AND delivery_status LIKE 'blocked:%'",
                (fp, signal.id),
            ).rowcount
        status = "sending" if inserted else "duplicate-plan-suppressed"
        cur = store.db.execute(
            "INSERT INTO real_delivery_attempts(at_ms,signal_id,symbol,horizon,fingerprint,status,relationship) VALUES(?,?,?,?,?,?,?)",
            (at_ms, signal.id, signal.symbol, signal.horizon_profile, fp, status, relation),
        )
        if inserted:
            store.db.execute(
                "INSERT OR IGNORE INTO alert_members VALUES(?,?,?,?,?)",
                (signal.id, fp, cluster, relation, at_ms),
            )
        store.db.commit()
        return dict(
            claimed=bool(inserted),
            fingerprint=fp,
            cluster_id=cluster,
            relationship=relation,
            primary=primary,
            attempt=cur.lastrowid,
        )
    except BaseException:
        store.db.rollback()
        raise


def delivery_status(store):
    rows = dict(store.db.execute("SELECT status,count(*) FROM real_delivery_attempts GROUP BY status"))
    last = store.db.execute(
        "SELECT signal_id,symbol,horizon,fingerprint,status,message_id FROM real_delivery_attempts ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return dict(
        real_initial_attempts=sum(rows.values()),
        sent=rows.get("sent", 0),
        duplicate_suppressed=rows.get("duplicate-plan-suppressed", 0),
        uncertain=rows.get("uncertain", 0) + rows.get("sending", 0),
        rejected=rows.get("rejected", 0) + rows.get("rate-limited", 0),
        thesis_cluster_updates=store.db.execute(
            "SELECT count(*) FROM real_delivery_attempts WHERE status='sent' AND relationship!='MATERIALLY_DIFFERENT_PLAN'"
        ).fetchone()[0],
        **dict(
            zip(
                (
                    "last_real_signal_id",
                    "last_symbol",
                    "last_horizon",
                    "last_fingerprint",
                    "last_delivery_status",
                    "last_message_id",
                ),
                last or [None] * 6,
            )
        ),
    )
