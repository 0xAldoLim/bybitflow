"""Remove permanent rows that nothing reads, then rebuild a compact database copy.

Measured on the production database (2026-11-27):

- `ml_snapshots` rows with stage `generation`: 121,818 rows, 9.3 GB. No label,
  prediction or query uses them; only a count was ever reported.
- `signal_origins`: 2.7 GB. Written for every candidate and never read.

Decision-stage snapshots, every label, every model, every signal and all Discord
receipts are left untouched. Origins are kept for delivered and still-active
signals. Deleting rows does not shrink an SQLite file, so `rebuild` writes a
compact, verified copy with `VACUUM INTO`; swapping the files is an explicit
operator step with the application stopped.
"""

import sqlite3
import time
from contextlib import closing
from pathlib import Path

from .storage import now_ms

AUDIT = "storage_slim_v1"
TERMINAL = "('INVALIDATED','EXPIRED','RESOLVED')"
UNUSED_GENERATION = (
    "stage='generation' "
    "AND NOT EXISTS (SELECT 1 FROM ml_labels l WHERE l.snapshot_id=ml_snapshots.id) "
    "AND NOT EXISTS (SELECT 1 FROM ml_predictions p WHERE p.snapshot_id=ml_snapshots.id)"
)
UNUSED_ORIGIN = (
    "NOT EXISTS (SELECT 1 FROM outbox o WHERE o.signal_id=signal_origins.signal_id "
    "AND o.key LIKE '%:initial' AND o.status IN ('sent','sending','uncertain')) "
    f"AND NOT EXISTS (SELECT 1 FROM signals s WHERE s.id=signal_origins.signal_id AND s.state NOT IN {TERMINAL})"
)


def busy(store):
    """Row deletion must not overlap a run that fingerprints the same tables."""
    retirement = store.get("raw_retirement_state_v1", {}) or {}
    claim = store.get("cleanup_run_claim_v212", {}) or {}
    if retirement.get("status") == "RUNNING":
        return "raw retirement is running"
    if claim.get("expires_ms", 0) > now_ms():
        return "storage cleanup is running"
    return None


def plan(store):
    db = store.db
    generation = db.execute(
        f"SELECT count(*),coalesce(sum(length(CAST(payload AS BLOB))),0) FROM ml_snapshots WHERE {UNUSED_GENERATION}"
    ).fetchone()
    origins = db.execute(
        f"SELECT count(*),coalesce(sum(length(CAST(payload AS BLOB))),0) FROM signal_origins WHERE {UNUSED_ORIGIN}"
    ).fetchone()
    return dict(
        generation_snapshots=generation[0],
        generation_bytes=generation[1],
        signal_origins=origins[0],
        signal_origin_bytes=origins[1],
        kept_decision_snapshots=db.execute(
            "SELECT count(*) FROM ml_snapshots WHERE stage='decision'"
        ).fetchone()[0],
        kept_labels=db.execute("SELECT count(*) FROM ml_labels").fetchone()[0],
        kept_signals=db.execute("SELECT count(*) FROM signals").fetchone()[0],
    )


def _delete(db, table, where, batch):
    removed = 0
    while True:
        with db:
            done = db.execute(
                f"DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE {where} LIMIT ?)",
                (batch,),
            ).rowcount
        removed += done
        if done < batch:
            return removed


def slim(store, dry_run=True, batch=500):
    before = plan(store)
    if dry_run:
        return before | dict(status="DRY_RUN")
    reason = busy(store)
    if reason:
        return before | dict(status="DEFERRED", reason=reason)
    started = now_ms()
    guarded = {
        table: store.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        for table in ("ml_labels", "ml_predictions", "ml_models", "signals", "outbox", "transitions")
    }
    decisions = before["kept_decision_snapshots"]
    snapshots = _delete(store.db, "ml_snapshots", UNUSED_GENERATION, batch)
    origins = _delete(store.db, "signal_origins", UNUSED_ORIGIN, batch)
    after = {table: store.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in guarded}
    kept = store.db.execute("SELECT count(*) FROM ml_snapshots WHERE stage='decision'").fetchone()[0]
    if after != guarded or kept != decisions:
        raise RuntimeError("Slimming changed a protected table; restore the backup")
    result = dict(
        status="SLIMMED",
        at_ms=now_ms(),
        duration_ms=now_ms() - started,
        deleted_generation_snapshots=snapshots,
        deleted_signal_origins=origins,
        estimated_payload_bytes=before["generation_bytes"] + before["signal_origin_bytes"],
        protected_counts=after | dict(decision_snapshots=kept),
        note="Run 'storage rebuild' and swap the file to return the space to the disk",
    )
    store.put(AUDIT, result)
    return result


def rebuild(source: Path, output: Path):
    """Write a compact copy next to nothing it could overwrite, then verify it."""
    source, output = Path(source), Path(output)
    if output.exists():
        raise ValueError("Rebuild output already exists; choose a new path")
    started = time.monotonic()
    with closing(sqlite3.connect(source, timeout=60)) as db:
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        tables = [
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        ]
        expected = {t: db.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in tables}
        objects = db.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]
        db.execute("VACUUM INTO ?", (str(output),))
    with closing(sqlite3.connect(output, timeout=60)) as copy:
        if copy.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise ValueError("Compact copy failed its integrity check; the original is unchanged")
        observed = {t: copy.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in tables}
        if (
            observed != expected
            or copy.execute("SELECT count(*) FROM sqlite_master").fetchone()[0] != objects
        ):
            raise ValueError("Compact copy differs from the original; the original is unchanged")
        copy.execute("PRAGMA journal_mode=WAL")
    return dict(
        status="REBUILT",
        output=str(output),
        original_bytes=source.stat().st_size,
        compact_bytes=output.stat().st_size,
        tables=len(tables),
        rows_verified=sum(expected.values()),
        seconds=round(time.monotonic() - started, 1),
        next_step="Stop the application, keep the original elsewhere, then replace it with the compact copy",
    )
