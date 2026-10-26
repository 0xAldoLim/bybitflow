"""Read-only common projection over existing independent recovery engines."""

import json
from dataclasses import asdict, dataclass
from typing import Literal

State = Literal[
    "PENDING",
    "WAITING_SOURCE",
    "READY",
    "REPLAYING",
    "RETRY_AT",
    "COMPLETE",
    "UNVERIFIABLE",
    "FAILED_SOFTWARE",
]


@dataclass(frozen=True)
class RecoveryJob:
    job_id: str
    source: str | None
    symbol: str | None
    cursor: int | None
    start: int | None
    end: int | None
    last_attempt: int | None
    next_attempt: int | None
    attempt_count: int
    state: State
    reason_code: str | None
    progress: float | None
    engine: str


def failure_code(reason):
    reason = str(reason or "").lower()
    for tokens, code in (
        (("timeout",), "NETWORK_TIMEOUT"),
        (("rate", "429"), "RATE_LIMIT"),
        (("history unavailable", "unsupported source"), "SOURCE_HISTORY_UNAVAILABLE"),
        (("clock", "future"), "CLOCK_DAMAGE"),
        (("retention", "pruned", "missing segment"), "RETENTION_MISSING"),
        (("frozen plan", "invalid plan"), "INVALID_FROZEN_PLAN"),
        (("ambiguous", "ordering"), "AMBIGUOUS_ORDERING"),
        (("gap", "coverage", "missing candle"), "DATA_GAP"),
    ):
        if any(t in reason for t in tokens):
            return code
    return "SOFTWARE_ERROR" if reason else None


def state(status, retry_at, now):
    if status in {"VERIFIED", "COMPLETE", "OBSERVED", "RECOVERED"}:
        return "COMPLETE"
    if status in {"EXCLUDED", "INELIGIBLE", "UNVERIFIABLE"}:
        return "UNVERIFIABLE"
    if retry_at and retry_at > now:
        return "RETRY_AT"
    if status in {"FAILED", "FAILED_SOFTWARE"}:
        return "FAILED_SOFTWARE"
    if status in {"WAITING_SOURCE", "DEFERRED", "STORAGE_BACKPRESSURE"}:
        return "WAITING_SOURCE"
    return "REPLAYING" if status == "REPLAYING" else "READY" if status == "READY" else "PENDING"


def project(store, now, limit=500):
    jobs = []
    # These are projections, never rewritten checkpoints or labels.
    for prefix, engine in (
        ("monitored_recovery_attempt:", "monitored_ohlc"),
        ("primary_print_recovery_attempt:", "recovered_prints"),
    ):
        rows = store.db.execute(
            "SELECT key,payload FROM kv WHERE key LIKE ? ORDER BY key LIMIT ?", (prefix + "%", limit + 1)
        ).fetchall()
        for key, raw in rows[:limit]:
            p = json.loads(raw)
            ident = key[len(prefix) :]
            snapshot = store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()
            snap = json.loads(snapshot[0]) if snapshot else {}
            plan = snap.get("signal", {})
            checkpoint = (
                store.get("primary_print_recovery:" + ident, {}) if engine == "recovered_prints" else {}
            )
            cursor = checkpoint.get("last_event_ms")
            start = snap.get("decision_ms")
            end = plan.get("holding_deadline_ms") or plan.get("expires_ms")
            progress = (
                max(0, min(1, (cursor - start) / (end - start)))
                if all(isinstance(v, (int, float)) for v in (cursor, start, end)) and end > start
                else None
            )
            retry = p.get("retry_after_ms")
            jobs.append(
                asdict(
                    RecoveryJob(
                        key,
                        p.get("source", snap.get("source")),
                        plan.get("symbol"),
                        cursor,
                        start,
                        end,
                        p.get("last_attempt_ms"),
                        retry,
                        p.get("attempts", 0),
                        state(p.get("status"), retry, now),
                        failure_code(p.get("reason", p.get("last_error_type"))),
                        progress,
                        engine,
                    )
                )
            )
    for table, engine in (("setup_path_jobs", "forward_path"), ("swing_path_jobs", "swing_path")):
        for ident, source, symbol, cursor, retry, raw in store.db.execute(
            f"SELECT signal_id,source,symbol,cursor_ms,next_ms,payload FROM {table} ORDER BY next_ms LIMIT ?",
            (limit,),
        ):
            p = json.loads(raw)
            start = p.get("decision_ms")
            end = p.get("deadline_ms")
            jobs.append(
                asdict(
                    RecoveryJob(
                        engine + ":" + ident,
                        source,
                        symbol,
                        cursor,
                        start,
                        end,
                        None,
                        retry,
                        0,
                        state("PENDING", retry, now),
                        None,
                        max(0, min(1, (cursor - start) / (end - start)))
                        if start is not None and end and end > start
                        else None,
                        engine,
                    )
                )
            )
    pending = [j for j in jobs if j["state"] not in {"COMPLETE", "UNVERIFIABLE"}]
    return dict(
        at_ms=now,
        jobs=jobs[:limit],
        sampled_jobs=len(jobs),
        truncated=len(jobs) >= limit,
        pending_sample=len(pending),
        oldest_pending_age_ms=max(
            [max(0, now - j["start"]) for j in pending if j["start"] is not None], default=None
        ),
        scope="Bounded projection of existing durable attempts/path jobs; unattempted primary snapshots remain in their engine's eligibility report. Cursor/label authority stays with each engine.",
    )
