"""Bounded local operational telemetry and background-only workload admission."""

import asyncio
import json
import sqlite3
import time
from dataclasses import asdict, dataclass
from enum import IntEnum
from pathlib import Path

from .storage import now_ms

MINUTE = 60_000
HOUR = 60 * MINUTE


class Priority(IntEnum):
    RECORDING = 1
    ACTIVE_LIFECYCLE = 2
    REQUIRED_FEEDS = 3
    CONFIRMATION_DELIVERY = 4
    SCANNING = 5
    FORWARD_RESEARCH = 6
    HISTORICAL_RECOVERY = 7
    TRAINING_BOOTSTRAP = 8


@dataclass(frozen=True)
class PressureState:
    state: str
    defer_from: int
    reasons: tuple[str, ...]


def classify(metrics):
    reasons = []
    floor = 9

    def pressure(code, level):
        nonlocal floor
        floor = min(floor, level)
        reasons.append(code)

    if metrics.get("storage_fraction", 0) >= 0.85:
        pressure("STORAGE_PRESSURE", 8)
    if metrics.get("storage_fraction", 0) >= 0.90:
        pressure("STORAGE_PRESSURE", 7)
    if metrics.get("rest_backlog", 0) >= 8 or metrics.get("memory_fraction", 0) >= 0.85:
        pressure("RESOURCE_PRESSURE", 7)
    if metrics.get("event_loop_lag_ms", 0) >= 250 or metrics.get("sqlite_busy_delta", 0) > 0:
        pressure("LOOP_OR_SQLITE_PRESSURE", 6)
    if metrics.get("required_feeds_stale") is True:
        pressure("REQUIRED_FEED_RECOVERY", 6)
    if metrics.get("recorder_pressure", 0) >= 0.5 or metrics.get("recorder_healthy") is False:
        pressure("RECORDER_PRESSURE", 6)
        return PressureState("CRITICAL_COLLECTION_PROTECTION", 6, tuple(sorted(set(reasons))))
    return PressureState(
        {9: "NORMAL", 8: "TRAINING_DEFERRED", 7: "RECOVERY_DEFERRED", 6: "RESEARCH_DEFERRED"}[floor],
        floor,
        tuple(sorted(set(reasons))),
    )


def allowed(store, priority, now=None):
    if priority <= Priority.SCANNING:
        return True  # These controls cannot change production qualification/scheduling.
    now = now_ms() if now is None else now
    row = store.get("operational_health", {})
    if not row:
        return True  # Compatibility with stores that have never run the new sampler.
    if not 0 <= now - row.get("at_ms", 0) <= 90_000:
        return False  # Stale observed admission state cannot authorize optional load.
    return int(priority) < row.get("defer_from", 9)


def category_bytes(root):
    result = dict(
        database=0,
        wal=0,
        raw_segments=0,
        packed_archives=0,
        datasets=0,
        models=0,
        maintenance_audit=0,
        research_exports=0,
        other=0,
    )
    roots = {
        "segments": "raw_segments",
        "packs": "packed_archives",
        "datasets": "datasets",
        "models": "models",
        "maintenance": "maintenance_audit",
        "audit": "maintenance_audit",
        "exports": "research_exports",
        "research": "research_exports",
    }
    for path in root.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root)
        key = (
            "database"
            if relative.as_posix() == "research.sqlite"
            else "wal"
            if relative.as_posix() == "research.sqlite-wal"
            else roots.get(relative.parts[0], "other")
        )
        parts = relative.parts
        if parts[0] == "ml" and len(parts) > 1:
            key = {
                "datasets": "datasets",
                "models": "models",
                "maintenance": "maintenance_audit",
                "horizon": "research_exports",
                "score-profile-research": "research_exports",
            }.get(parts[1], "other")
        if parts[0] in {"deployment-audits", "audit", "audits"}:
            key = "maintenance_audit"
        try:
            result[key] += path.stat().st_size
        except FileNotFoundError:
            pass  # Recorder may atomically rename its current segment.
    result["total"] = sum(result.values())
    return result


def forecast(history, budget):
    if not history:
        return dict(status="UNKNOWN", windows={}, days_to_budget="UNKNOWN")
    end, current = history[-1]
    windows = {}
    for title, duration in (("24h", 24 * HOUR), ("7d", 7 * 24 * HOUR), ("30d", 30 * 24 * HOUR)):
        old = next(((at, p) for at, p in reversed(history) if at <= end - duration), None)
        windows[title] = (
            "UNKNOWN"
            if old is None
            else {key: (current[key] - old[1].get(key, 0)) * 24 * HOUR / (end - old[0]) for key in current}
        )
    rate = next((windows[w]["total"] for w in ("7d", "24h", "30d") if isinstance(windows[w], dict)), None)
    days = max(0, budget - current["total"]) / rate if rate is not None and rate > 0 else "UNKNOWN"
    return dict(
        status="OBSERVED" if rate is not None else "UNKNOWN",
        windows=windows,
        window_units="bytes per day, including eligible retention reductions",
        days_to_budget=days,
        budget_bytes=budget,
        categories=current,
        at_ms=end,
        limitation="Linear observed-growth forecast, not a retention guarantee; database contains both metadata and immutable derived evidence",
    )


def persist(root, snapshot, storage_budget, collect_storage=False, checkpoint=False):
    """Private worker connection; fail/skip a diagnostic rather than delay collection."""
    db = sqlite3.connect(root / "research.sqlite", timeout=0)
    try:
        from .recovery_status import project
        from .storage import Store

        reader = Store.__new__(Store)
        reader.root, reader.db = root, db
        recovery = project(reader, snapshot["at_ms"], limit=100)
        snapshot["recovery"] = {k: v for k, v in recovery.items() if k not in {"jobs", "scope"}}
        with db:
            db.execute(
                "INSERT OR REPLACE INTO operational_samples VALUES(?,?)",
                (snapshot["at_ms"] // MINUTE * MINUTE, json.dumps(snapshot)),
            )
            db.execute(
                "DELETE FROM operational_samples WHERE bucket_ms<?", (snapshot["at_ms"] - 2 * 24 * HOUR,)
            )
        if checkpoint:
            start = time.monotonic()
            row = db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            snapshot["checkpoint"] = dict(
                mode="PASSIVE",
                busy=row[0],
                log_frames=row[1],
                checkpointed_frames=row[2],
                duration_ms=(time.monotonic() - start) * 1000,
                at_ms=snapshot["at_ms"],
            )
        growth = None
        if collect_storage:
            sizes = category_bytes(root)
            with db:
                db.execute(
                    "INSERT OR REPLACE INTO storage_growth VALUES(?,?)",
                    (snapshot["at_ms"] // HOUR * HOUR, json.dumps(sizes)),
                )
                db.execute(
                    "DELETE FROM storage_growth WHERE bucket_ms<?", (snapshot["at_ms"] - 45 * 24 * HOUR,)
                )
            history = [
                (r[0], json.loads(r[1]))
                for r in db.execute("SELECT bucket_ms,payload FROM storage_growth ORDER BY bucket_ms")
            ]
            growth = forecast(history, storage_budget)
            with db:
                db.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", ("storage_forecast", json.dumps(growth)))
        snapshot["discord_outbox"] = dict(db.execute("SELECT status,count(*) FROM outbox GROUP BY status"))
        with db:
            db.execute(
                "UPDATE operational_samples SET payload=? WHERE bucket_ms=?",
                (json.dumps(snapshot), snapshot["at_ms"] // MINUTE * MINUTE),
            )
        return growth
    finally:
        db.close()


def memory_pressure():
    try:
        used = int(Path("/sys/fs/cgroup/memory.current").read_text())
        limit = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        return used / int(limit) if limit.isdigit() and int(limit) > 0 else None
    except (OSError, ValueError):
        return None


async def monitor(runtime):
    loop = asyncio.get_running_loop()
    previous_busy = 0
    stale_since = None
    last_sample = last_storage = last_checkpoint = 0
    lag = 0.0
    while True:
        expected = loop.time() + 1
        await asyncio.sleep(1)
        lag = max(lag, max(0, (loop.time() - expected) * 1000))
        now = now_ms()
        if now - last_sample < MINUTE:
            continue
        rec = runtime.recorder.metrics()
        metrics = runtime.store.db.health.snapshot()
        busy = metrics.get("sqlite_busy_events", 0)
        feed = runtime.store.get("runtime_health", {})
        storage = runtime.store.get("storage_status", {})
        fraction = storage.get("used_bytes", storage.get("total_bytes", 0)) / max(
            1, storage.get("budget_bytes", 1)
        )
        sample = dict(
            event_loop_lag_ms=lag,
            recorder_pressure=runtime.recorder.pressure(),
            recorder_healthy=runtime.recorder.healthy,
            sqlite_busy_delta=max(0, busy - previous_busy),
            rest_backlog=runtime.rest_queue_depth,
            memory_fraction=memory_pressure(),
            storage_fraction=fraction,
            required_feeds_stale=bool(
                feed.get("active_required_streams_total", 0) > feed.get("active_required_streams_fresh", 0)
            ),
        )
        if sample["memory_fraction"] is None:
            sample.pop("memory_fraction")
        observed = 0 <= now - feed.get("at_ms", 0) <= 90_000
        stale = sample["required_feeds_stale"] if observed else None
        stale_since = (stale_since or now) if stale is True else None
        state = classify(sample)
        wal = runtime.store.root / "research.sqlite-wal"
        sample.update(
            at_ms=now,
            **asdict(state),
            sqlite=metrics,
            sqlite_wal_bytes=wal.stat().st_size if wal.exists() else 0,
            recorder_queue_depth=rec.get("queue_depth"),
            recorder_queue_high_water=rec.get("queue_high_watermark"),
            recorder_drops=rec.get("events_dropped"),
            recorder_overflow=rec.get("overflow_count"),
            recorder_pending_bytes=runtime.recorder.pending_bytes,
            recorder_metrics=rec,
            rest_inflight=runtime.rest_inflight,
            websocket=runtime.store.get("stream_health", {}),
            websocket_stale_duration_ms=now - stale_since
            if stale_since is not None
            else 0
            if stale is False
            else None,
            websocket_reconnect_count=sum(getattr(runtime.streams, "reconnects", {}).values())
            + metrics.get("websocket_reconnect_count", 0),
            scan=runtime.store.get("scanner_performance", {}),
            training=runtime.store.get("training_telemetry", {}),
            trainer_cycle=runtime.store.get("trainer_cycle_telemetry", {}),
            trainer_state=runtime.store.get("ml_worker_state", {}),
        )
        collect_growth = now - last_storage >= HOUR and state.defer_from > Priority.FORWARD_RESEARCH
        try:
            await asyncio.to_thread(
                persist,
                runtime.store.root,
                sample,
                runtime.settings.max_storage_gb * 1_000_000_000,
                collect_growth,
                state.state == "NORMAL" and now - last_checkpoint >= 5 * MINUTE,
            )
            if collect_growth:
                last_storage = now
            if sample.get("checkpoint"):
                last_checkpoint = now
        except (sqlite3.OperationalError, OSError) as exc:
            sample["diagnostic_persistence_error"] = type(exc).__name__
        runtime.store.put("operational_health", sample)
        previous_busy, last_sample, lag = busy, now, 0.0


def warnings(store, now):
    row = store.get("operational_health", {})
    if not 0 <= now - row.get("at_ms", 0) <= 90_000:
        return []
    result = []

    def add(code, value, expected, action):
        result.append(
            dict(
                code=code,
                severity="warning",
                title=code.replace("_", " ").title(),
                message=f"Measured {value}; expected {expected}.",
                measured=value,
                expected=expected,
                action=action,
                at_ms=row["at_ms"],
            )
        )

    if row.get("event_loop_lag_ms", 0) >= 250:
        add(
            "EVENT_LOOP_LAG",
            round(row["event_loop_lag_ms"], 1),
            "less than 250 ms per sampled minute",
            "Defer background recovery/training; inspect CPU and synchronous work.",
        )
    if row.get("sqlite_busy_delta", 0) > 0:
        add(
            "SQLITE_CONTENTION",
            row["sqlite_busy_delta"],
            "0 lock conflicts per sampled minute",
            "Inspect concurrent writers; background work defers automatically. Critical writes remain acknowledged or fail closed.",
        )
    if row.get("sqlite_wal_bytes", 0) >= 512 * 1024 * 1024:
        add(
            "WAL_GROWTH",
            row["sqlite_wal_bytes"],
            "WAL below 512 MiB",
            "Inspect long readers and passive checkpoint progress. Do not truncate a live WAL.",
        )
    if row.get("recorder_pressure", 0) >= 0.5:
        add(
            "RECORDER_PRESSURE",
            row["recorder_pressure"],
            "queue and byte pressure below 50%",
            "Background work defers; inspect disk latency and available storage.",
        )
    recovery = row.get("recovery", {})
    if (
        recovery.get("pending_sample", 0) >= 100
        or recovery.get("oldest_pending_age_ms", 0)
        and recovery["oldest_pending_age_ms"] > 7 * 24 * HOUR
    ):
        add(
            "RECOVERY_BACKLOG",
            recovery.get("pending_sample", 0),
            "fewer than 100 sampled pending jobs and oldest age below 7 days",
            "Inspect source history, retry reasons and coverage. Do not fabricate completion or overwrite labels.",
        )
    growth = store.get("storage_forecast", {})
    days = growth.get("days_to_budget")
    if isinstance(days, (int, float)) and days < 7:
        add(
            "STORAGE_EXHAUSTION_FORECAST",
            round(days, 2),
            "more than 7 observed-growth days of headroom",
            "Review category growth and existing safe retention; no new deletion policy is applied.",
        )
    return result


def samples(store, limit=120):
    limit = max(1, min(2880, int(limit)))
    return [
        dict(at_ms=at, metrics=json.loads(raw))
        for at, raw in store.db.execute(
            "SELECT bucket_ms,payload FROM operational_samples ORDER BY bucket_ms DESC LIMIT ?", (limit,)
        )
    ]
