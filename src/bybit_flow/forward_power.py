"""Paired shadow measurements that give forward evidence statistical power (2.2.3).

Everything here is research_only=True and production_ml=False. Nothing changes a
production score, gate, plan, lifecycle, ML feature or Discord payload, and no
row is ever sent anywhere. Rows are append-only; plans are frozen from data
available at their decision timestamp. Unknown remains UNKNOWN.
"""

import hashlib
import json
from copy import deepcopy

from .setup_common import number, positive

EPOCH = "forward_evidence_power_epoch_v1"
CONFIG = "evidence_power_config_v1"
GAPS = "evidence_power_attempt_gaps_v1"
HOUR = 3_600_000
DEFAULTS = dict(
    delays=[60, 180, 600],
    max_per_hour=60,
    control_max_per_hour=20,
    max_rows=200_000,
    attempt_window_ms=72 * HOUR,
    bootstrap=2000,
    seed=20221103,
    cluster_window_ms=60 * 60_000,
)
# A structure-only control failed nothing except executed-flow confirmation.
FLOW_ONLY_GATES = frozenset(
    {
        "executed order flow did not confirm family trigger",
        "UNCONFIRMED_LIQUIDITY_SWEEP",
        "LOW_INFORMATION_EXECUTED_FLOW",
    }
)
VARIANTS = (
    "execution_tp1",
    "structural_tp1",
    "r050",
    "r075",
    "r100",
    "partial075_structural",
    "time_decay",
)
TERMINAL = ("INVALIDATED", "EXPIRED", "RESOLVED")
IMMUTABLE = ("evidence_power_paths", "structure_controls", "candidate_attempts", "policy_preregistrations")
POWER_STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS evidence_power_paths(
 path_id TEXT PRIMARY KEY,trigger_id TEXT NOT NULL,kind TEXT NOT NULL,source TEXT NOT NULL,
 created_ms INTEGER NOT NULL,payload TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS evidence_power_paths_trigger ON evidence_power_paths(trigger_id,kind)",
    "CREATE INDEX IF NOT EXISTS evidence_power_paths_created ON evidence_power_paths(kind,created_ms)",
    """CREATE TABLE IF NOT EXISTS structure_controls(
 id TEXT PRIMARY KEY,candidate_identity TEXT NOT NULL,source TEXT NOT NULL,symbol TEXT NOT NULL,
 decision_ms INTEGER NOT NULL,created_ms INTEGER NOT NULL,payload TEXT NOT NULL,
 UNIQUE(source,candidate_identity))""",
    "CREATE INDEX IF NOT EXISTS structure_controls_created ON structure_controls(created_ms)",
    """CREATE TABLE IF NOT EXISTS candidate_attempts(
 signal_id TEXT PRIMARY KEY,attempt_key TEXT,pullback_key TEXT,source TEXT NOT NULL,
 decision_ms INTEGER NOT NULL,created_ms INTEGER NOT NULL,payload TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS candidate_attempt_level ON candidate_attempts(attempt_key,decision_ms)",
    "CREATE INDEX IF NOT EXISTS candidate_attempt_pullback ON candidate_attempts(pullback_key,decision_ms)",
    """CREATE TABLE IF NOT EXISTS policy_preregistrations(
 id TEXT PRIMARY KEY,created_ms INTEGER NOT NULL,digest TEXT NOT NULL,payload TEXT NOT NULL)""",
    *(
        f"CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()} BEFORE {action} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'immutable research evidence'); END"
        for table in IMMUTABLE
        for action in ("UPDATE", "DELETE")
    ),
)
POWER_SQL = ";\n".join(POWER_STATEMENTS)


def migrate(store):
    # Executed with the registry receipt in one transaction; additive only.
    for statement in POWER_STATEMENTS:
        store.db.execute(statement)


def initialize(store, now, settings=None):
    if store.get(EPOCH) is None:
        store.put(EPOCH, dict(start_ms=now, definition="evidence-power-v1", historical_backfill=False))
    if settings is not None:
        store.put(
            CONFIG,
            dict(
                delays=sorted(set(settings.forward_delay_seconds)),
                max_per_hour=settings.forward_power_max_per_hour,
                control_max_per_hour=settings.forward_control_max_per_hour,
                max_rows=settings.forward_power_max_rows,
                attempt_window_ms=settings.forward_attempt_window_hours * HOUR,
                bootstrap=settings.forward_stats_bootstrap,
                seed=settings.forward_stats_seed,
                cluster_window_ms=settings.forward_cluster_window_minutes * 60_000,
            ),
        )
    return store.get(EPOCH)


def config(store):
    return DEFAULTS | (store.get(CONFIG) or {})


def bounded(store, table, cfg):
    # Rows are never deleted, so the largest rowid is the row count.
    return (store.db.execute(f"SELECT max(rowid) FROM {table}").fetchone()[0] or 0) >= cfg["max_rows"]


def identity(store, signal):
    row = store.db.execute(
        "SELECT candidate_identity FROM candidate_identities WHERE signal_id=?", (signal.id,)
    ).fetchone()
    return row[0] if row else signal.id


def bucket(value):
    return "UNKNOWN" if not number(value) else str(int(value)) if value < 3 else "3+"


# ---------------------------------------------------------------- save hooks


def on_save(store, signal, now, created):
    """Called after the production save, in its own transaction; failures are counted only."""
    epoch = store.get(EPOCH)
    if (
        not epoch
        or signal.synthetic
        or signal.source == "tradingview"
        or signal.created_ms < epoch["start_ms"]
    ):
        return
    from .operational import Priority, allowed

    cfg = config(store)
    admitted = allowed(store, Priority.FORWARD_RESEARCH, now)
    if created:
        record_attempt(store, signal, now, cfg, epoch, admitted)
    if admitted:
        enroll_control(store, signal, now, cfg, epoch)


def attempt_keys(signal):
    frozen = signal.evidence.get("frozen_structure") or {}
    base = f"{signal.source}|{signal.symbol}|{signal.family}|{signal.direction}"
    level, tick = frozen.get("frozen_reference"), signal.evidence.get("price_tick")
    try:
        tick = float(tick)
    except (TypeError, ValueError):
        tick = None
    level_key = base + "|" + str(round(level / tick)) if positive(level) and positive(tick) else None
    return level_key, base if signal.family == "trend_pullback" else None, frozen


def gaps(store):
    row = store.db.execute("SELECT payload FROM kv WHERE key=?", (GAPS,)).fetchone()
    return json.loads(row[0]) if row else []


def covered(store, start, end, epoch):
    """Prior-attempt history is complete only after the epoch and without deferral gaps."""
    return start >= epoch["start_ms"] and not any(a <= end and b >= start for a, b in gaps(store))


def record_attempt(store, signal, now, cfg, epoch, admitted):
    decision = signal.created_ms
    if not admitted or bounded(store, "candidate_attempts", cfg):
        # Later ordinals whose window spans this moment become lower bounds.
        merged = gaps(store)
        if merged and decision - merged[-1][1] <= 60_000:
            merged[-1][1] = max(merged[-1][1], decision)
        else:
            merged.append([decision, decision])
        store.db.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (GAPS, json.dumps(merged[-500:])))
        return
    level_key, pullback_key, frozen = attempt_keys(signal)
    window = cfg["attempt_window_ms"]
    payload = dict(
        signal_id=signal.id,
        source=signal.source,
        symbol=signal.symbol,
        family=signal.family,
        direction=signal.direction,
        decision_ms=decision,
        attempt_key=level_key,
        window_ms=window,
        attempt_ordinal=None,
        attempt_ordinal_status="UNKNOWN",
        prior_attempt_ids=[],
        prior_attempt_outcomes_known=[],
        pullback_ordinal=None,
        pullback_ordinal_status="UNKNOWN",
        trend_qualified_ms=frozen.get("trend_qualified_ms"),
        generation_pullback_index=frozen.get("pullback_attempt_index_since_qualification"),
        research_only=True,
        production_ml=False,
        ml_feature=False,
    )
    if level_key:
        prior = store.db.execute(
            "SELECT signal_id,decision_ms FROM candidate_attempts WHERE attempt_key=? AND decision_ms<? "
            "AND decision_ms>=? ORDER BY decision_ms",
            (level_key, decision, decision - window),
        ).fetchall()
        known = []
        for ident, _ in prior[-50:]:
            label = store.db.execute(
                "SELECT available_ms,payload FROM setup_path_labels WHERE signal_id=? AND available_ms<?",
                (ident, decision),
            ).fetchone()
            if label:
                p = json.loads(label[1])
                row = p.get("policies", {}).get("execution_tp1", {})
                known.append(
                    dict(
                        signal_id=ident,
                        resolved_ms=label[0],
                        state=p.get("state"),
                        execution_tp1=row.get("outcome"),
                        net_r=row.get("net_r") if row.get("complete") else None,
                    )
                )
        payload.update(
            attempt_ordinal=len(prior) + 1,
            attempt_ordinal_status="EXACT"
            if covered(store, decision - window, decision, epoch)
            else "LOWER_BOUND",
            prior_attempt_ids=[r[0] for r in prior[-50:]],
            prior_attempt_outcomes_known=known,
        )
    qualified = frozen.get("trend_qualified_ms")
    if pullback_key and number(qualified) and qualified <= decision:
        count = store.db.execute(
            "SELECT count(*) FROM candidate_attempts WHERE pullback_key=? AND decision_ms<? AND decision_ms>=?",
            (pullback_key, decision, qualified),
        ).fetchone()[0]
        payload.update(
            pullback_ordinal=count + 1,
            pullback_ordinal_status="EXACT" if covered(store, qualified, decision, epoch) else "LOWER_BOUND",
        )
    store.db.execute(
        "INSERT OR IGNORE INTO candidate_attempts VALUES(?,?,?,?,?,?,?)",
        (signal.id, level_key, pullback_key, signal.source, decision, now, json.dumps(payload)),
    )


def attempt(store, signal_id):
    row = store.db.execute(
        "SELECT payload FROM candidate_attempts WHERE signal_id=?", (signal_id,)
    ).fetchone()
    return json.loads(row[0]) if row else None


def enroll_control(store, signal, now, cfg, epoch):
    """Once per identity: structure/validity/risk passed and only flow confirmation failed."""
    from .quality_v22 import enabled

    e = signal.evidence
    decision = (e.get("v22") or {}).get("available_ms")
    if (
        not enabled(signal)
        or not number(decision)
        or decision > now
        or decision < epoch["start_ms"]
        or signal.state in TERMINAL
        or e.get("flow_confirmed", e.get("confirmation", {}).get("passed", False))
        or not signal.gates
        or not set(signal.gates) <= FLOW_ONLY_GATES
        or not e.get("v22_counterfactual")
    ):
        return
    candidate = identity(store, signal)
    ident = hashlib.sha256(("control:" + signal.source + ":" + candidate).encode()).hexdigest()[:32]
    if store.db.execute("SELECT 1 FROM structure_controls WHERE id=?", (ident,)).fetchone():
        return
    recent = store.db.execute(
        "SELECT count(*) FROM structure_controls WHERE created_ms>=?", (now - HOUR,)
    ).fetchone()[0]
    if recent >= cfg["control_max_per_hour"] or bounded(store, "structure_controls", cfg):
        store.db.health.increment("evidence_power_control_deferred")
        return  # A later structural pass inside the window may enroll at its own time.
    path_id = signal.id + ":control"
    if store.db.execute("SELECT 1 FROM setup_path_jobs WHERE signal_id=?", (path_id,)).fetchone():
        return
    from .setup_research import register as register_path

    path_signal = deepcopy(signal)
    path_signal.id = path_id
    register_path(store, path_signal, now)
    job = store.db.execute("SELECT payload FROM setup_path_jobs WHERE signal_id=?", (path_id,)).fetchone()
    if not job:
        return
    plan = json.loads(job[0])
    tried = attempt(store, signal.id) or {}
    payload = dict(
        policy="structure_only_control",
        signal_id=signal.id,
        path_id=path_id,
        candidate_identity=candidate,
        source=signal.source,
        symbol=signal.symbol,
        horizon=signal.horizon_profile,
        family=signal.family,
        direction=signal.direction,
        regime=signal.regime,
        decision_ms=decision,
        structural_decision_ms=e.get("frozen_structure", {}).get("available_ms", e.get("trigger_bar_end")),
        score=signal.quality,
        flow_confirmed=False,
        flow_gates=sorted(signal.gates),
        entry=signal.entry,
        stop=signal.stop,
        execution_target=signal.tp1,
        structural_target=e.get("structural_tp1"),
        exposure_cluster=e.get("opportunity_priority", {}).get("cluster_id", "UNKNOWN"),
        attempt_ordinal=tried.get("attempt_ordinal"),
        frozen_path=plan,
        research_only=True,
        production_ml=False,
        production_authority=False,
        discord=False,
    )
    store.db.execute(
        "INSERT INTO structure_controls VALUES(?,?,?,?,?,?,?)",
        (ident, candidate, signal.source, signal.symbol, decision, now, json.dumps(payload)),
    )


# ------------------------------------------------------- research-loop sweep


def mark(store, path_id, trigger, kind, source, now, payload):
    store.db.execute(
        "INSERT OR IGNORE INTO evidence_power_paths VALUES(?,?,?,?,?,?)",
        (path_id, trigger, kind, source, now, json.dumps(payload)),
    )


def sweep(store, now, limit=50):
    """Enroll delayed and trade-through stages before any research advance can finish their parents.

    Runs inside the forward-research admission class. Each parent is decided once,
    when first seen; a cap leaves an explicit NOT_ENROLLED marker, never a later retry.
    """
    epoch = store.get(EPOCH)
    if not epoch:
        return dict(delay=0, trade_through=0, not_enrolled=0)
    cfg = config(store)
    counts = dict(delay=0, trade_through=0, not_enrolled=0)
    from .setup_research import delayed_plan, trade_through_plan

    def room(size):
        used = store.db.execute(
            "SELECT count(*) FROM evidence_power_paths WHERE kind IN ('delay','trade_through') AND created_ms>=?",
            (now - HOUR,),
        ).fetchone()[0]
        if bounded(store, "evidence_power_paths", cfg):
            return "STORAGE_BOUND"
        return None if used + size <= cfg["max_per_hour"] else "HOURLY_CAP"

    delivery_rows = store.db.execute(
        "SELECT j.signal_id,j.payload FROM setup_path_jobs j WHERE j.signal_id LIKE '%:delivery' AND NOT EXISTS("
        "SELECT 1 FROM evidence_power_paths p WHERE p.trigger_id=j.signal_id) LIMIT ?",
        (limit,),
    ).fetchall()
    with store.db:
        for trigger, raw in delivery_rows:
            delivery = json.loads(raw)
            base = delivery["signal_id"]
            delays = [d * 1000 for d in cfg["delays"]]
            meta = dict(
                signal_id=base,
                source=delivery["source"],
                symbol=delivery["symbol"],
                direction=delivery["direction"],
                decision_ms=delivery["delivered_ms"],
                research_only=True,
                production_ml=False,
            )
            reason = "PRE_EPOCH" if delivery["delivered_ms"] < epoch["start_ms"] else room(len(delays))
            saved = store.db.execute("SELECT payload FROM signals WHERE id=?", (base,)).fetchone()
            evidence = json.loads(saved[0]).get("evidence", {}) if saved else {}
            if not reason and not saved:
                reason = "SIGNAL_MISSING"
            if reason:
                mark(
                    store,
                    trigger + ":not_enrolled",
                    trigger,
                    "not_enrolled",
                    delivery["source"],
                    now,
                    meta | dict(reason=reason),
                )
                counts["not_enrolled"] += 1
                continue
            meta["exposure_cluster"] = evidence.get("opportunity_priority", {}).get("cluster_id", "UNKNOWN")
            snapshot = evidence.get("delivery_execution_snapshot") or {}
            for delay in delays:
                plan = delayed_plan(delivery, snapshot, delay)
                path_id = base + ":" + plan["stage"]
                store.db.execute(
                    "INSERT OR IGNORE INTO setup_path_jobs VALUES(?,?,?,?,?,?,?)",
                    (
                        path_id,
                        delivery["source"],
                        delivery["symbol"],
                        now,
                        delivery["delivered_ms"] // 60000 * 60000,
                        delivery["deadline_ms"],
                        json.dumps(plan),
                    ),
                )
                mark(
                    store,
                    path_id,
                    trigger,
                    "delay",
                    delivery["source"],
                    now,
                    meta | dict(stage=plan["stage"]),
                )
                counts["delay"] += 1
    opportunities = store.db.execute(
        "SELECT o.id,o.payload FROM policy_opportunities o WHERE o.decision_ms>=? AND NOT EXISTS("
        "SELECT 1 FROM evidence_power_paths p WHERE p.trigger_id=json_extract(o.payload,'$.path_id')) "
        "ORDER BY o.decision_ms LIMIT ?",
        (epoch["start_ms"], limit),
    ).fetchall()
    with store.db:
        for _, raw in opportunities:
            o = json.loads(raw)
            trigger = o["path_id"]
            meta = dict(
                signal_id=o["signal_id"],
                paired_path_id=trigger,
                source=o["source"],
                direction=o["direction"],
                decision_ms=o["decision_ms"],
                exposure_cluster=o.get("exposure_cluster", "UNKNOWN"),
                research_only=True,
                production_ml=False,
            )
            try:
                tick = float(o.get("price_tick"))
            except (TypeError, ValueError):
                tick = None
            reason = room(1) if positive(tick) else "TICK_UNKNOWN"
            if reason:
                mark(
                    store,
                    trigger + ":not_enrolled",
                    trigger,
                    "not_enrolled",
                    o["source"],
                    now,
                    meta | dict(reason=reason),
                )
                counts["not_enrolled"] += 1
                continue
            plan = trade_through_plan(o["frozen_path"], tick)
            path_id = trigger + ":tt"
            store.db.execute(
                "INSERT OR IGNORE INTO setup_path_jobs VALUES(?,?,?,?,?,?,?)",
                (
                    path_id,
                    o["source"],
                    o["symbol"],
                    now,
                    plan["decision_ms"] // 60000 * 60000,
                    plan["deadline_ms"],
                    json.dumps(plan),
                ),
            )
            mark(store, path_id, trigger, "trade_through", o["source"], now, meta)
            counts["trade_through"] += 1
    return counts


# ------------------------------------------------------------ preregistration

PREREG_REQUIRED = (
    "hypothesis",
    "metric",
    "comparison",
    "acceptance_threshold",
    "evaluation_window",
    "attempted_variants",
)


def preregister(store, record, now):
    """Immutable, hashed research intent. Grants no production authority."""
    window = record.get("evaluation_window") if isinstance(record, dict) else None
    if (
        not isinstance(record, dict)
        or any(k not in record for k in PREREG_REQUIRED)
        or not isinstance(window, dict)
        or not number(window.get("start_ms"))
        or not number(window.get("end_ms"))
        or window["end_ms"] <= window["start_ms"]
        or not isinstance(record["attempted_variants"], list)
        or not record["attempted_variants"]
    ):
        raise ValueError("Pre-registration requires " + ", ".join(PREREG_REQUIRED) + " with a valid window")
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    ident = digest[:32]
    with store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO policy_preregistrations VALUES(?,?,?,?)",
            (
                ident,
                now,
                digest,
                json.dumps(
                    dict(record=record, digest=digest, research_only=True, production_authority=False)
                ),
            ),
        )
    created = store.db.execute(
        "SELECT created_ms FROM policy_preregistrations WHERE id=?", (ident,)
    ).fetchone()[0]
    return dict(prereg_id=ident, digest=digest, created_ms=created, production_changed=False)


def preregistration_status(store, review):
    ident = review.get("prereg_id") if isinstance(review, dict) else None
    row = (
        store.db.execute(
            "SELECT created_ms,digest FROM policy_preregistrations WHERE id=?", (ident,)
        ).fetchone()
        if isinstance(ident, str)
        else None
    )
    if not row:
        return dict(
            status="NOT_PREREGISTERED", reason="MISSING_OR_UNKNOWN_PREREG_ID", production_authority=False
        )
    start = review.get("forward_start")
    if not number(start) or start < row[0]:
        return dict(
            status="NOT_PREREGISTERED",
            reason="REVIEW_WINDOW_STARTED_BEFORE_PREREGISTRATION",
            prereg_id=ident,
            prereg_created_ms=row[0],
            production_authority=False,
        )
    return dict(
        status="PREREGISTERED",
        prereg_id=ident,
        prereg_created_ms=row[0],
        digest=row[1],
        production_authority=False,
    )


# --------------------------------------------------------------------- report


def labels(store, ids, now):
    found, ids = {}, list(dict.fromkeys(i for i in ids if i))
    for i in range(0, len(ids), 500):
        chunk = ids[i : i + 500]
        for ident, raw in store.db.execute(
            "SELECT signal_id,payload FROM setup_path_labels WHERE available_ms<=? AND signal_id IN ("
            + ",".join("?" * len(chunk))
            + ")",
            (now, *chunk),
        ):
            found[ident] = json.loads(raw)
    return found


def per_opportunity(label, variant):
    """Known net R per opportunity: an observed non-fill is explicit zero exposure."""
    if not label:
        return None
    if label.get("zero_exposure") or (label.get("state") == "NO_ENTRY" and label.get("coverage_complete")):
        return 0.0
    row = label.get("policies", {}).get(variant, {})
    return row["net_r"] if row.get("complete") and number(row.get("net_r")) else None


def entered_net(label, variant):
    if not label or label.get("entered_ms") is None:
        return None
    row = label.get("policies", {}).get(variant, {})
    return row["net_r"] if row.get("complete") and number(row.get("net_r")) else None


def bounds(label, variant):
    """(conservative, optimistic) net R; ambiguous ordering is bounded, never merged."""
    if not label or label.get("entered_ms") is None or not label.get("coverage_complete"):
        return None
    row = label.get("policies", {}).get(variant) or {}
    if row.get("complete") and number(row.get("net_r")):
        return row["net_r"], row["net_r"]
    if not row.get("ambiguous") or not positive(label.get("risk")):
        return None
    sign = 1 if label["direction"] == "LONG" else -1
    stop_r = sign * (label["stop"] - label["entry"]) / label["risk"]
    target_r = sign * (row["target"] - label["entry"]) / label["risk"]
    fraction = row.get("partial_fraction") or 0
    low = fraction * 0.75 + (1 - fraction) * stop_r if row.get("partial_done") else stop_r
    high = fraction * 0.75 + (1 - fraction) * target_r if fraction else target_r
    options = [low, high] + ([row["gross_r"]] if number(row.get("gross_r")) else [])
    cost = label["cost_per_base"] / label["risk"]
    return min(options) - cost, max(options) - cost


def obs(meta, value):
    return dict(
        t=meta["decision_ms"],
        key=(meta["source"], meta["direction"], meta.get("exposure_cluster") or "UNKNOWN"),
        value=value,
    )


def describe(values, minimum):
    return dict(
        n=len(values),
        mean=sum(values) / len(values) if values else None,
        status="DESCRIPTIVE" if len(values) >= minimum else "INSUFFICIENT SAMPLE",
    )


def report_section(store, now, limit=5000, minimum=30):
    from . import forward_stats as stats

    cfg = config(store)
    settings = dict(bootstrap=cfg["bootstrap"], seed=cfg["seed"], cluster_window_ms=cfg["cluster_window_ms"])
    comparisons = []

    def compared(family, ident, row):
        row["comparison_id"] = family + ":" + ident
        comparisons.append(row)
        return row

    # Delay ladder, paired with the delivery stage of the same opportunity.
    delay_rows = [
        (path_id, trigger, json.loads(raw))
        for path_id, trigger, raw in store.db.execute(
            "SELECT path_id,trigger_id,payload FROM evidence_power_paths WHERE kind='delay' "
            "ORDER BY created_ms DESC LIMIT ?",
            (limit,),
        )
    ]
    found = labels(store, [r[0] for r in delay_rows] + [r[1] for r in delay_rows], now)
    outcomes, stages, paired = {}, {}, {}
    for path_id, trigger, meta in delay_rows:
        label, delivery = found.get(path_id), found.get(trigger)
        group = meta["stage"] + ":research_only:" + meta["source"]
        state = (
            "PENDING"
            if not label
            else label["state"]
            if label["state"].startswith(("MISSED", "UNKNOWN", "INCOMPLETE", "NO_ENTRY"))
            else "ENTERED"
        )
        counts = outcomes.setdefault(group, {})
        counts[state] = counts.get(state, 0) + 1
        for variant in VARIANTS:
            later, base = per_opportunity(label, variant), per_opportunity(delivery, variant)
            if later is not None:
                stages.setdefault(
                    meta["stage"] + ":" + variant + ":research_only:" + meta["source"], []
                ).append(obs(meta, later))
            if later is not None and base is not None:
                paired.setdefault(
                    meta["stage"] + ":" + variant + ":research_only:" + meta["source"], []
                ).append(obs(meta, later - base))
                stages.setdefault("delivery_paired:" + variant + ":research_only:" + meta["source"], {})[
                    meta["signal_id"]
                ] = obs(meta, base)
    delay = dict(
        outcomes=outcomes,
        per_opportunity_net_r={
            k: stats.summarize(list(v.values()) if isinstance(v, dict) else v, settings, minimum)
            for k, v in stages.items()
        },
        minus_delivery={
            k: compared("delay", k, stats.summarize(v, settings, minimum)) for k, v in paired.items()
        },
        scope="Delayed entries at the first complete 1m open at/after durable send + delay, half delivery "
        "spread against the trader, delivery cost, frozen stop/targets/deadline and original R. "
        "MISSED_* rows are explicit zero exposure, never losses; UNKNOWN/INCOMPLETE rows are excluded.",
    )

    # Fill bound: trade-through (conservative) versus touch (optimistic) on one frozen plan.
    tt_rows = [
        (path_id, trigger, json.loads(raw))
        for path_id, trigger, raw in store.db.execute(
            "SELECT path_id,trigger_id,payload FROM evidence_power_paths WHERE kind='trade_through' "
            "ORDER BY created_ms DESC LIMIT ?",
            (limit,),
        )
    ]
    found_tt = labels(store, [r[0] for r in tt_rows] + [r[1] for r in tt_rows], now)
    fills, touch, through, diff = {}, {}, {}, {}
    for path_id, trigger, meta in tt_rows:
        a, b = found_tt.get(trigger), found_tt.get(path_id)
        if not a or not b:
            continue
        rates = fills.setdefault(
            "research_only:" + meta["source"], dict(paired=0, touch_entered=0, trade_through_entered=0)
        )
        rates["paired"] += 1
        rates["touch_entered"] += a.get("entered_ms") is not None
        rates["trade_through_entered"] += b.get("entered_ms") is not None
        for variant in VARIANTS:
            x, y = per_opportunity(a, variant), per_opportunity(b, variant)
            if x is None or y is None:
                continue
            key = variant + ":research_only:" + meta["source"]
            touch.setdefault(key, []).append(obs(meta, x))
            through.setdefault(key, []).append(obs(meta, y))
            diff.setdefault(key, []).append(obs(meta, y - x))
    fill = dict(
        fill_counts=fills,
        touch_optimistic={k: stats.summarize(v, settings, minimum) for k, v in touch.items()},
        trade_through_conservative={k: stats.summarize(v, settings, minimum) for k, v in through.items()},
        trade_through_minus_touch={
            k: compared("fill", k, stats.summarize(v, settings, minimum)) for k, v in diff.items()
        },
        stop_exit_slippage="FIXED ASSUMPTION: stop exits use the stored cost_per_base (configured fees, "
        "slippage and funding reserve); no depth-dependent stop slippage enters any outcome.",
        stop_exit_depth_diagnostic=depth_diagnostic(store, now, limit),
    )

    # Structure-only control versus flow-confirmed opportunities.
    epoch = store.get(EPOCH) or dict(start_ms=now)
    controls = [
        json.loads(r[0])
        for r in store.db.execute(
            "SELECT payload FROM structure_controls WHERE decision_ms<=? ORDER BY decision_ms DESC LIMIT ?",
            (now, limit),
        )
    ]
    confirmed = [
        json.loads(r[0])
        for r in store.db.execute(
            "SELECT payload FROM policy_opportunities WHERE decision_ms>=? AND decision_ms<=? "
            "ORDER BY decision_ms DESC LIMIT ?",
            (epoch["start_ms"], now, limit),
        )
    ]
    found_c = labels(store, [c["path_id"] for c in controls] + [o["path_id"] for o in confirmed], now)
    groups = {}
    for c in controls:
        later = store.db.execute(
            "SELECT 1 FROM policy_opportunities WHERE source=? AND candidate_identity=?",
            (c["source"], c["candidate_identity"]),
        ).fetchone()
        state = store.db.execute("SELECT state FROM signals WHERE id=?", (c["signal_id"],)).fetchone()
        status = (
            "LATER_FLOW_CONFIRMED"
            if later
            else "NOT_FLOW_CONFIRMED"
            if state and state[0] in TERMINAL
            else "UNRESOLVED"
        )
        g = groups.setdefault(
            "research_only:" + c["source"], dict(all=[], never=[], confirmed=[], cost=[], status={})
        )
        g["status"][status] = g["status"].get(status, 0) + 1
        value = entered_net(found_c.get(c["path_id"]), "execution_tp1")
        if value is not None:
            g["all"].append(obs(c, value))
            if status == "NOT_FLOW_CONFIRMED":
                g["never"].append(obs(c, value))
    for o in confirmed:
        g = groups.setdefault(
            "research_only:" + o["source"], dict(all=[], never=[], confirmed=[], cost=[], status={})
        )
        value = entered_net(found_c.get(o["path_id"]), "execution_tp1")
        if value is not None:
            g["confirmed"].append(obs(o, value))
        if number(o.get("confirmation_cost_r")):
            g["cost"].append(obs(o, o["confirmation_cost_r"]))
    control = {
        key: dict(
            eventual_flow_status=g["status"],
            structure_only_control=stats.summarize(g["all"], settings, minimum),
            control_not_flow_confirmed=stats.summarize(g["never"], settings, minimum),
            flow_confirmed=stats.summarize(g["confirmed"], settings, minimum),
            flow_confirmed_minus_not_confirmed=compared(
                "control", key, stats.compare(g["never"], g["confirmed"], settings, minimum)
            ),
            confirmation_entry_cost_r=compared(
                "confirmation_cost", key, stats.summarize(g["cost"], settings, minimum)
            ),
        )
        for key, g in groups.items()
    }

    # Production-policy differences and score correlation from the existing ledger.
    rows = store.db.execute(
        "SELECT o.payload,r.policy_id,r.provenance,r.payload FROM policy_results r "
        "JOIN policy_opportunities o ON o.id=r.opportunity_id WHERE r.available_ms<=? "
        "ORDER BY r.available_ms DESC LIMIT ?",
        (now, limit),
    ).fetchall()
    pairs, scores = {}, {}
    for raw, policy, provenance, metrics in rows:
        o, m = json.loads(raw), json.loads(metrics)
        pairs.setdefault((o["signal_id"], provenance, o["source"]), [o, {}])[1][policy] = m
        if (
            policy == "v22_actual"
            and m.get("accepted") is True
            and m.get("complete")
            and number(m.get("net_r"))
            and number(o.get("score"))
        ):
            scores.setdefault(provenance + ":" + o["source"], []).append(
                obs(o, m["net_r"]) | dict(score=o["score"])
            )
    differences = {}
    for (_, provenance, source), (o, policies) in pairs.items():
        a = policies.get("v22_actual")
        if (
            not a
            or a.get("comparison_status") != "OBSERVED"
            or not a.get("complete")
            or not number(a.get("net_r"))
        ):
            continue
        for policy, b in policies.items():
            if policy == "v22_actual" or b.get("comparison_status") != "OBSERVED" or not b.get("complete"):
                continue
            if not number(b.get("net_r")):
                continue
            d = (b["net_r"] if b.get("accepted") is True else 0) - (
                a["net_r"] if a.get("accepted") is True else 0
            )
            differences.setdefault(policy + ":" + provenance + ":" + source, []).append(obs(o, d))
    policy = {k: compared("policy", k, stats.summarize(v, settings, minimum)) for k, v in differences.items()}
    correlation = {
        k: compared("score_spearman", k, stats.correlation(v, settings, minimum)) for k, v in scores.items()
    }

    # Ambiguous-ordering bounds and cost stress on every research-only stage.
    stage_labels = {}
    for o in confirmed:
        stage_labels[("decision", o["source"], o["path_id"])] = found_c.get(o["path_id"])
    for c in controls:
        stage_labels[("structure_control", c["source"], c["path_id"])] = found_c.get(c["path_id"])
    for path_id, trigger, meta in delay_rows:
        stage_labels[(meta["stage"], meta["source"], path_id)] = found.get(path_id)
        stage_labels[("delivery", meta["source"], trigger)] = found.get(trigger)
    for path_id, _, meta in tt_rows:
        stage_labels[("decision_trade_through", meta["source"], path_id)] = found_tt.get(path_id)
    ambiguity, stress = {}, {}
    for (stage, source, _), label in stage_labels.items():
        if not label:
            continue
        for variant in VARIANTS:
            key = stage + ":" + variant + ":research_only:" + source
            pair = bounds(label, variant)
            a = ambiguity.setdefault(key, dict(conservative=[], optimistic=[], ambiguous=0))
            if pair:
                a["conservative"].append(pair[0])
                a["optimistic"].append(pair[1])
                row = label["policies"].get(variant, {})
                a["ambiguous"] += bool(row.get("ambiguous"))
            row = label.get("policies", {}).get(variant, {})
            if not row.get("complete"):
                continue
            s = stress.setdefault(key, {"1.0x": [], "1.5x": [], "2.0x": [], "unknown_gross": 0})
            if not number(row.get("gross_r")):
                s["unknown_gross"] += 1
                continue
            cost = label["cost_per_base"] / label["risk"]
            for k, factor in (("1.0x", 1), ("1.5x", 1.5), ("2.0x", 2)):
                s[k].append(row["gross_r"] - factor * cost)
    ambiguity_bounds = {
        k: dict(
            ambiguous=v["ambiguous"],
            conservative_stop_first=describe(v["conservative"], minimum),
            optimistic_target_first=describe(v["optimistic"], minimum),
        )
        for k, v in ambiguity.items()
        if v["conservative"]
    }
    cost_stress = {
        k: dict(
            net_r_cost_1_0x=describe(v["1.0x"], minimum),
            net_r_cost_1_5x=describe(v["1.5x"], minimum),
            net_r_cost_2_0x=describe(v["2.0x"], minimum),
            unknown_gross=v["unknown_gross"],
            status="UNKNOWN (gross R not stored)" if not v["1.0x"] else "RECOMPUTED FROM STORED GROSS R",
        )
        for k, v in stress.items()
    }

    adjusted = stats.holm([c.get("p_value") for c in comparisons])
    for row, p in zip(comparisons, adjusted):
        row["holm_adjusted_p"] = p
    return dict(
        label="RESEARCH ONLY / NO PROFITABILITY CLAIM / NO PRODUCTION CHANGE",
        epoch=store.get(EPOCH),
        statistics=dict(
            resampling="Whole UTC days with replacement",
            bootstrap_draws=settings["bootstrap"],
            seed=settings["seed"],
            cluster_window_ms=settings["cluster_window_ms"],
            cluster_key="source, direction, shared-exposure group, window from first member",
            alpha=stats.ALPHA,
            power=stats.POWER,
            minimum_cohort=minimum,
            p_value="Two-sided percentile-bootstrap sign test on cluster means; Holm across every printed comparison",
        ),
        delay_ladder=delay,
        fill_bound=fill,
        structure_control=control,
        policy_differences=policy,
        score_correlation=correlation,
        ambiguity_bounds=ambiguity_bounds,
        cost_stress=cost_stress,
        comparisons=dict(
            total=len(comparisons),
            holm_family="All comparisons printed in this section; unknown p-values count as 1",
            ids=[c["comparison_id"] for c in comparisons],
        ),
        limit=limit,
        production_authority=False,
    )


def depth_diagnostic(store, now, limit):
    """Exit-side visible depth near mid at decision; NOT depth at the stop time."""
    groups = {}
    for (raw,) in store.db.execute(
        "SELECT payload FROM policy_opportunities WHERE decision_ms<=? ORDER BY decision_ms DESC LIMIT ?",
        (now, limit),
    ):
        o = json.loads(raw)
        for band, ratio in (o.get("stop_exit_depth") or {}).get("notional_to_depth", {}).items():
            if number(ratio):
                groups.setdefault(o["source"] + ":" + band + "bps", []).append(ratio)
    result = {}
    for key, values in groups.items():
        ordered = sorted(values)
        result[key] = dict(n=len(values), median_notional_to_exit_side_depth=ordered[len(ordered) // 2])
    return dict(
        status="RESEARCH_DIAGNOSTIC" if result else "UNKNOWN",
        bands=result,
        scope="Hypothetical notional divided by exit-side (bid for LONG, ask for SHORT) visible depth within "
        "each band of mid, frozen at decision. Not depth at stop time; not used in any outcome.",
    )
