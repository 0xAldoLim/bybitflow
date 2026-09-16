"""Single-writer metadata and bounded, immutable market-data segments."""

import asyncio
import gzip
import hashlib
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def now_ms():
    return time.time_ns() // 1_000_000


def directory_bytes(root):
    if not root.exists():
        return 0
    total = 0
    pending = [root]
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
    return total


class StorageBudgetExceeded(OSError):
    """Configured recording budget reached; physical disk may still have space."""


class Store:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(root / "research.sqlite")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY);
        INSERT OR IGNORE INTO schema_version VALUES(1);
        CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS signals(id TEXT PRIMARY KEY, symbol TEXT, created_ms INTEGER,
            state TEXT, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS transitions(id INTEGER PRIMARY KEY, signal_id TEXT,
            at_ms INTEGER, state TEXT, reason TEXT);
        CREATE TABLE IF NOT EXISTS universe(at_ms INTEGER, symbol TEXT, eligible INTEGER,
            payload TEXT, PRIMARY KEY(at_ms,symbol));
        CREATE TABLE IF NOT EXISTS facts(id INTEGER PRIMARY KEY, asset TEXT, known_ms INTEGER,
            effective_ms INTEGER, expires_ms INTEGER, payload TEXT);
        CREATE TABLE IF NOT EXISTS journal(id INTEGER PRIMARY KEY, at_ms INTEGER, payload TEXT);
        CREATE TABLE IF NOT EXISTS experiments(id TEXT PRIMARY KEY, at_ms INTEGER, payload TEXT);
        CREATE TABLE IF NOT EXISTS segments(id TEXT PRIMARY KEY, at_ms INTEGER, payload TEXT);
        CREATE TABLE IF NOT EXISTS outbox(key TEXT PRIMARY KEY, signal_id TEXT, status TEXT,
            payload TEXT, message_id TEXT, updated_ms INTEGER);
        CREATE TABLE IF NOT EXISTS tv_inbox(event_id TEXT PRIMARY KEY, digest TEXT NOT NULL,
            received_ms INTEGER NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
            result TEXT);
        CREATE INDEX IF NOT EXISTS tv_pending ON tv_inbox(status,received_ms);
        CREATE INDEX IF NOT EXISTS signals_recent ON signals(created_ms DESC);
        INSERT OR IGNORE INTO schema_version VALUES(2);
        """)
        self.db.commit()
        from .ml.store import migrate

        migrate(self.db)
        from .observations import migrate as migrate_observations

        migrate_observations(self)
        from .packing import migrate as migrate_packing

        migrate_packing(self)
        if self.get("horizon_counts") is None:
            counts = dict(
                self.db.execute(
                    "SELECT coalesce(json_extract(payload,'$.horizon_profile'),'LEGACY'),count(*) FROM signals GROUP BY 1"
                ).fetchall()
            )
            self.put("horizon_counts", counts)

    def put(self, key, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (key, json.dumps(value)))

    def get(self, key, default=None):
        r = self.db.execute("SELECT payload FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(r[0]) if r else default

    def signal(self, signal, reason="evaluation"):
        from .ml.store import FeatureStore
        from .observations import preserve_origin, start

        if signal.synthetic:
            raise ValueError("Synthetic signals must never enter research storage")

        # Capture before lifecycle overwrites; both accepted and rejected decisions survive.
        features = FeatureStore(self)
        features.capture(signal, max(now_ms(), signal.created_ms), "generation")
        if signal.evidence.get("score_components"):
            features.capture(signal, max(now_ms(), signal.created_ms), "decision")
        old = self.db.execute("SELECT state FROM signals WHERE id=?", (signal.id,)).fetchone()
        with self.db:
            preserve_origin(self, signal)
            if not old:
                counts = self.get("horizon_counts", {})
                counts[signal.horizon_profile] = counts.get(signal.horizon_profile, 0) + 1
                self.db.execute("INSERT OR REPLACE INTO kv VALUES('horizon_counts',?)", (json.dumps(counts),))
            self.db.execute(
                "INSERT OR REPLACE INTO signals VALUES(?,?,?,?,?)",
                (signal.id, signal.symbol, signal.created_ms, signal.state, signal.model_dump_json()),
            )
            if not old or old[0] != signal.state:
                self.db.execute(
                    "INSERT INTO transitions(signal_id,at_ms,state,reason) VALUES(?,?,?,?)",
                    (signal.id, now_ms(), signal.state, reason),
                )
        start(self, signal, now_ms())

    def signals(self, limit=200):
        return [
            json.loads(r[0])
            for r in self.db.execute("SELECT payload FROM signals ORDER BY created_ms DESC LIMIT ?", (limit,))
        ]

    def active_signals(self, limit=2000):
        return [
            json.loads(r[0])
            for r in self.db.execute(
                "SELECT payload FROM signals WHERE state NOT IN ('INVALIDATED','EXPIRED','RESOLVED') "
                "ORDER BY CASE WHEN state='ALERTED' THEN 0 ELSE 1 END, created_ms DESC LIMIT ?",
                (limit,),
            )
        ]

    def rows(self, table, limit=200):
        if table not in {"journal", "experiments", "segments", "facts"}:
            raise ValueError("Unknown table")
        return [
            json.loads(r[0])
            for r in self.db.execute(f"SELECT payload FROM {table} ORDER BY rowid DESC LIMIT ?", (limit,))
        ]

    def membership(self, at_ms, symbol, eligible, payload):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO universe VALUES(?,?,?,?)",
                (at_ms, symbol, int(eligible), json.dumps(payload)),
            )

    def membership_asof(self, symbol, at_ms):
        r = self.db.execute(
            "SELECT eligible,payload,at_ms FROM universe WHERE symbol=? AND at_ms<=? "
            "ORDER BY at_ms DESC LIMIT 1",
            (symbol, at_ms),
        ).fetchone()
        return dict(r) if r else None

    def backup(self, target: Path):
        if target.exists():
            raise ValueError("Backup destination must be new")
        with sqlite3.connect(target) as dest:
            self.db.backup(dest)

    def close(self):
        self.db.close()


class Recorder:
    def metadata_size(self):
        return sum(p.stat().st_size for p in self.store.root.iterdir() if p.is_file()) + sum(
            directory_bytes(self.store.root / name) for name in ("ml", "quarantine")
        )

    def __init__(self, store, settings):
        self.store, self.settings = store, settings
        self.queue = asyncio.Queue(maxsize=settings.queue_size)
        self.healthy = True
        self.reason = "ready"
        self.written = 0
        self.pending_bytes = 0
        self.running = True
        self.disk_bytes = directory_bytes(store.root)
        self.metadata_bytes = self.metadata_size()
        self.retention_freed = store.get("recording_retention", {}).get("total_freed_bytes", 0)

    def offer(self, source, symbol, event_ms, payload, receipt_ms=None, complete=True):
        row = dict(
            source=source,
            symbol=symbol,
            event_ms=int(event_ms),
            receipt_ms=receipt_ms or now_ms(),
            schema_version=1,
            complete=complete,
            payload=json.dumps(payload, separators=(",", ":")),
        )
        if not self.healthy:
            raise RuntimeError("Recorder circuit open: " + self.reason)
        try:
            size = len(row["payload"].encode()) + 256
            if self.pending_bytes + size > self.settings.queue_byte_limit:
                raise asyncio.QueueFull
            self.queue.put_nowait(row)
            self.pending_bytes += size
        except asyncio.QueueFull:
            self.healthy, self.reason = False, "recording queue overflow; collection continuity lost"
            self.store.put("recorder_gap", {"at_ms": now_ms(), "reason": self.reason})
            raise RuntimeError(self.reason) from None

    def flush(self, batch):
        metadata = self.metadata_size()
        self.disk_bytes += metadata - self.metadata_bytes
        self.metadata_bytes = metadata
        freed = self.store.get("recording_retention", {}).get("total_freed_bytes", 0)
        self.disk_bytes = max(0, self.disk_bytes - max(0, freed - self.retention_freed))
        self.retention_freed = freed
        reserve = 2 * sum(len(json.dumps(row).encode()) for row in batch) + 65536
        if self.disk_bytes + reserve >= self.settings.max_storage_gb * 1e9:
            raise StorageBudgetExceeded("Configured recording storage limit reached")
        ident = f"{now_ms()}-{uuid.uuid4().hex[:8]}"
        directory = self.store.root / "segments"
        directory.mkdir(exist_ok=True)
        raw = directory / f"{ident}.jsonl.gz"
        normalized = directory / f"{ident}.parquet"
        # Live collection prioritizes keeping up with bursts over maximum compression.
        with gzip.open(raw, "wt", compresslevel=1) as f:
            for row in batch:
                f.write(json.dumps(row) + "\n")
        from .normalization import NORMALIZATION_VERSION, SCHEMA, normalize

        observations = [r for envelope in batch for r in normalize(envelope)]
        pq.write_table(pa.Table.from_pylist(observations, schema=SCHEMA), normalized, compression="zstd")
        digest = hashlib.sha256(raw.read_bytes()).hexdigest()
        manifest = dict(
            id=ident,
            source="mixed-public",
            schema_version=1,
            normalization_version=NORMALIZATION_VERSION,
            rows=len(batch),
            min_event_ms=min(r["event_ms"] for r in batch),
            max_event_ms=max(r["event_ms"] for r in batch),
            min_receipt_ms=min(r["receipt_ms"] for r in batch),
            max_receipt_ms=max(r["receipt_ms"] for r in batch),
            collected_ms=now_ms(),
            raw=str(raw),
            parquet=str(normalized),
            sha256=digest,
            completeness="observed segment only; connection coverage tracked separately",
        )
        manifest["previous_segment"] = self.store.get("last_segment")
        (directory / f"{ident}.manifest.json").write_text(json.dumps(manifest, indent=2))
        with self.store.db:
            self.store.db.execute(
                "INSERT INTO segments VALUES(?,?,?)", (ident, now_ms(), json.dumps(manifest))
            )
        self.store.put("last_segment", {"id": ident, "sha256": digest})
        self.disk_bytes += (
            raw.stat().st_size
            + normalized.stat().st_size
            + (directory / f"{ident}.manifest.json").stat().st_size
        )
        self.written += len(batch)

    async def run(self):
        batch = []
        last_flush = time.monotonic()
        try:
            while self.running or not self.queue.empty():
                try:
                    batch.append(await asyncio.wait_for(self.queue.get(), timeout=1))
                except TimeoutError:
                    pass
                # Drain buffered events before yielding to producers again. Awaiting
                # every queued row lets websocket bursts outrun the single writer.
                while len(batch) < self.settings.recorder_segment_rows:
                    try:
                        batch.append(self.queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                if batch and (
                    len(batch) >= self.settings.recorder_segment_rows
                    or self.pending_bytes >= self.settings.queue_byte_limit * 0.5
                    or time.monotonic() - last_flush >= self.settings.recorder_segment_seconds
                    or (not self.running and self.queue.empty())
                ):
                    self.flush(batch)
                    self.pending_bytes -= sum(len(r["payload"].encode()) + 256 for r in batch)
                    batch.clear()
                    last_flush = time.monotonic()
        except Exception as exc:
            self.healthy, self.reason = False, type(exc).__name__ + ": recorder write failed"
            if isinstance(exc, StorageBudgetExceeded):
                self.reason = (
                    f"Recording storage limit reached ({self.settings.max_storage_gb:g} GB); "
                    "safe cleanup could not free sufficient evidence-independent space; inspect storage status"
                )
            self.store.put("recorder_gap", {"at_ms": now_ms(), "reason": self.reason})
