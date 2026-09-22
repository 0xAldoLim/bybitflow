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
    def __init__(self, root: Path, migration_backup_dir: Path | None = None):
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
        CREATE TABLE IF NOT EXISTS terminal_events(signal_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
            notification_status TEXT NOT NULL, attempts INTEGER DEFAULT 0, next_ms INTEGER DEFAULT 0);
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
        from .identity import migrate as migrate_identity

        migrate_identity(self, migration_backup_dir)
        from .funnel import migrate as migrate_funnel

        migrate_funnel(self)
        from .path_research import migrate as migrate_paths

        migrate_paths(self)
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
        old = self.db.execute("SELECT state,payload FROM signals WHERE id=?", (signal.id,)).fetchone()
        if old:
            if old[0] in {"INVALIDATED", "EXPIRED", "RESOLVED"}:
                return  # A stale asynchronous evaluator must never resurrect a terminal setup.
            saved = json.loads(old[1])
            for key in ("observed_entry_ms", "tp1_touch_ms", "tp2_touch_ms"):
                if key in saved.get("evidence", {}):
                    signal.evidence.setdefault(key, saved["evidence"][key])
            for key, value in saved.get("coverage", {}).items():
                if key.startswith("monitor_") or key in {"last_checked_trade_id", "last_monitor_ms"}:
                    if saved["coverage"].get("last_monitor_ms", 0) > signal.coverage.get(
                        "last_monitor_ms", 0
                    ):
                        signal.coverage[key] = value
        with self.db:
            if signal.state in {"INVALIDATED", "EXPIRED", "RESOLVED"}:
                event = dict(
                    terminal_event_id="terminal:" + signal.id,
                    signal_id=signal.id,
                    terminal_state=signal.state,
                    terminal_reason=signal.coverage.get("terminal_reason", reason),
                    effective_ms=now_ms(),
                    detected_ms=now_ms(),
                    event_price=None,
                    reference_price=signal.stop,
                    source=signal.source,
                    method="LIFECYCLE_RULE",
                )
                event.update(signal.evidence.get("terminal_event", {}))
                delivered = self.db.execute(
                    "SELECT 1 FROM outbox WHERE signal_id=? AND key LIKE '%:initial' AND status='sent'",
                    (signal.id,),
                ).fetchone()
                self.db.execute(
                    "INSERT OR IGNORE INTO terminal_events(signal_id,payload,notification_status) VALUES(?,?,?)",
                    (signal.id, json.dumps(event), "pending" if delivered else "unseen"),
                )
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
            from .funnel import emit

            if not old:
                emit(
                    self,
                    "candidates_generated",
                    signal.created_ms,
                    signal=signal,
                    key="generated:" + signal.id,
                )
            stage = {
                "PENDING CONFIRMATION": "pending_confirmation",
                "CONFIRMED": "confirmed",
                "ALERTED": "alerted",
            }.get(signal.state)
            if stage and (not old or old[0] != signal.state):
                emit(self, stage, now_ms(), signal=signal, key=stage + ":" + signal.id)
            from .path_research import register

            register(self, signal, now_ms())
        start(self, signal, now_ms())

    def monitor(self, signal):
        """Persist only lightweight lifecycle progress, without scoring/ML jobs."""
        with self.db:
            self.db.execute(
                "UPDATE signals SET payload=? WHERE id=? AND state=?",
                (signal.model_dump_json(), signal.id, signal.state),
            )

    def signals(self, limit=200):
        return [
            json.loads(r[0])
            for r in self.db.execute("SELECT payload FROM signals ORDER BY created_ms DESC LIMIT ?", (limit,))
        ]

    def active_signals(self, limit=-1):
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
        if self.db.in_transaction:
            raise ValueError("Commit pending writes before taking a migration backup")
        # Pin a WAL read snapshot. Otherwise frequent health writes can restart
        # every incremental copy step and prevent a large live backup finishing.
        with sqlite3.connect(self.root / "research.sqlite") as source, sqlite3.connect(target) as dest:
            source.execute("BEGIN")
            source.execute("SELECT count(*) FROM sqlite_master").fetchone()
            source.backup(dest, pages=1000)

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
        self.critical_symbols = {"BTCUSDT", "ETHUSDT"}
        self.queue_high_watermark = self.events_dropped = self.overflow_count = 0
        self.last_overflow_ms = self.last_error_ms = self.last_success_ms = 0
        self.last_error_type = None
        self.writer_batch_size = self.writer_latency_ms = self.segment_flush_latency_ms = 0
        self.normalization_ms = self.serialization_ms = self.sqlite_write_ms = self.raw_write_ms = 0
        self.parquet_write_ms = 0
        self.state = "NORMAL"
        self.retries = 0
        self.gaps = {}
        from collections import deque

        self.enqueued = deque(maxlen=120)
        self.dequeued = deque(maxlen=120)

    @staticmethod
    def _count_rate(samples, count):
        second = now_ms() // 1000
        if samples and samples[-1][0] == second:
            samples[-1] = (second, samples[-1][1] + count)
        else:
            samples.append((second, count))

    def metrics(self):
        second = now_ms() // 1000
        return dict(
            state=self.state,
            healthy=self.healthy,
            reason=self.reason,
            queue_capacity=self.queue.maxsize,
            queue_depth=self.queue.qsize(),
            queue_usage_percent=100 * self.queue.qsize() / self.queue.maxsize,
            queue_high_watermark=self.queue_high_watermark,
            enqueue_rate_1m=sum(n for t, n in self.enqueued if t > second - 60) / 60,
            dequeue_rate_1m=sum(n for t, n in self.dequeued if t > second - 60) / 60,
            events_dropped=self.events_dropped,
            overflow_count=self.overflow_count,
            last_overflow_ms=self.last_overflow_ms,
            writer_batch_size=self.writer_batch_size,
            writer_latency_ms=self.writer_latency_ms,
            segment_flush_latency_ms=self.segment_flush_latency_ms,
            normalization_ms=self.normalization_ms,
            serialization_ms=self.serialization_ms,
            sqlite_write_ms=self.sqlite_write_ms,
            raw_write_ms=self.raw_write_ms,
            parquet_write_ms=self.parquet_write_ms,
            last_success_ms=self.last_success_ms,
            last_error_type=self.last_error_type,
            last_error_ms=self.last_error_ms,
            retries=self.retries,
        )

    def _gap(self, symbol, at, reason):
        self.events_dropped += 1
        gap = self.gaps.setdefault(symbol, dict(coverage_gap=True, gap_start_ms=at, reason=reason))
        gap["gap_end_ms"] = at

    def offer(self, source, symbol, event_ms, payload, receipt_ms=None, complete=True):
        # Producer performs no filesystem or SQLite operations.
        receipt = receipt_ms or now_ms()
        optional = symbol not in self.critical_symbols and "control/" not in source
        pressure = max(
            self.queue.qsize() / self.queue.maxsize, self.pending_bytes / self.settings.queue_byte_limit
        )
        if pressure >= 0.7 and optional:
            self.state = "RECORDER_BACKPRESSURE"
            self._gap(symbol, receipt, "RECORDER_BACKPRESSURE")
            return False
        before = time.monotonic()
        row = dict(
            source=source,
            symbol=symbol,
            event_ms=int(event_ms),
            receipt_ms=receipt,
            schema_version=1,
            complete=complete,
            payload=json.dumps(payload, separators=(",", ":")),
        )
        self.serialization_ms = (time.monotonic() - before) * 1000
        size = len(row["payload"].encode()) + 256
        try:
            if self.pending_bytes + size > self.settings.queue_byte_limit:
                raise asyncio.QueueFull
            self.queue.put_nowait(row)
            self.pending_bytes += size
            self.queue_high_watermark = max(self.queue_high_watermark, self.queue.qsize())
            self._count_rate(self.enqueued, 1)
            return True
        except asyncio.QueueFull:
            self.overflow_count += 1
            self.last_overflow_ms = self.last_error_ms = receipt
            self.last_error_type = "QueueFull"
            self.healthy, self.reason = False, "recording queue overflow; collection continuity lost"
            self.state = "RECORDER_BACKPRESSURE"
            self._gap(symbol, receipt, "RECORDER_BACKPRESSURE")
            raise RuntimeError(self.reason) from None

    def flush(self, batch, store=None, ident=None):
        store = store or self.store
        if ident and store.db.execute("SELECT 1 FROM segments WHERE id=?", (ident,)).fetchone():
            return
        flush_started = time.monotonic()
        metadata = self.metadata_size()
        self.disk_bytes += metadata - self.metadata_bytes
        self.metadata_bytes = metadata
        freed = store.get("recording_retention", {}).get("total_freed_bytes", 0)
        self.disk_bytes = max(0, self.disk_bytes - max(0, freed - self.retention_freed))
        self.retention_freed = freed
        reserve = 2 * sum(len(json.dumps(row).encode()) for row in batch) + 65536
        if self.disk_bytes + reserve >= self.settings.max_storage_gb * 1e9:
            from .retention import prune_recordings

            prune_recordings(store, self.settings)
            self.disk_bytes = directory_bytes(self.store.root)
            self.retention_freed = store.get("recording_retention", {}).get("total_freed_bytes", 0)
            if self.disk_bytes + reserve >= self.settings.max_storage_gb * 1e9:
                raise StorageBudgetExceeded("Configured recording storage limit reached")
        ident = ident or f"{now_ms()}-{uuid.uuid4().hex[:8]}"
        directory = self.store.root / "segments"
        directory.mkdir(exist_ok=True)
        raw = directory / f"{ident}.jsonl.gz"
        normalized = directory / f"{ident}.parquet"
        from .normalization import NORMALIZATION_VERSION, SCHEMA, prepare

        started = time.monotonic()
        batch, observations = prepare(batch)
        self.normalization_ms = (time.monotonic() - started) * 1000
        # Live collection prioritizes keeping up with bursts over maximum compression.
        started = time.monotonic()
        with gzip.open(raw, "wt", compresslevel=1) as f:
            for row in batch:
                f.write(json.dumps(row) + "\n")
        self.raw_write_ms = (time.monotonic() - started) * 1000
        started = time.monotonic()
        pq.write_table(pa.Table.from_pylist(observations, schema=SCHEMA), normalized, compression="zstd")
        self.parquet_write_ms = (time.monotonic() - started) * 1000
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
        manifest["previous_segment"] = store.get("last_segment")
        (directory / f"{ident}.manifest.json").write_text(json.dumps(manifest, indent=2))
        started = time.monotonic()
        with store.db:
            store.db.execute("INSERT INTO segments VALUES(?,?,?)", (ident, now_ms(), json.dumps(manifest)))
            store.db.execute(
                "INSERT OR REPLACE INTO kv VALUES('last_segment',?)",
                (json.dumps({"id": ident, "sha256": digest}),),
            )
            from .funnel import emit

            emit(store, "recorded_events", now_ms(), key="segment:" + ident, amount=len(batch))
        self.disk_bytes += (
            raw.stat().st_size
            + normalized.stat().st_size
            + (directory / f"{ident}.manifest.json").stat().st_size
        )
        self.written += len(batch)
        self.sqlite_write_ms = (time.monotonic() - started) * 1000
        self.segment_flush_latency_ms = (time.monotonic() - flush_started) * 1000

    async def run(self):
        from .recorder_worker import ProcessWriter

        batch, batch_bytes = [], 0
        last_flush = last_health = time.monotonic()
        writer = ProcessWriter(self)
        batch_id = None
        try:
            while self.running or not self.queue.empty() or batch:
                try:
                    if len(batch) < self.settings.recorder_segment_rows:
                        try:
                            row = await asyncio.wait_for(self.queue.get(), timeout=1)
                            batch.append(row)
                            batch_bytes += len(row["payload"].encode()) + 256
                        except TimeoutError:
                            pass
                    while len(batch) < self.settings.recorder_segment_rows:
                        try:
                            row = self.queue.get_nowait()
                            batch.append(row)
                            batch_bytes += len(row["payload"].encode()) + 256
                        except asyncio.QueueEmpty:
                            break
                    due = (
                        len(batch) >= self.settings.recorder_segment_rows
                        or self.pending_bytes >= self.settings.queue_byte_limit * 0.5
                        or time.monotonic() - last_flush >= self.settings.recorder_segment_seconds
                        or not self.running
                    )
                    if batch and due:
                        gaps = {}
                        if batch_id is None:
                            gaps, self.gaps = self.gaps, {}
                        for symbol, gap in gaps.items():
                            # Appended controls mark the whole affected interval as incomplete.
                            batch.append(
                                dict(
                                    source="control/gap",
                                    symbol=symbol,
                                    event_ms=batch[-1]["event_ms"],
                                    receipt_ms=batch[-1]["receipt_ms"],
                                    schema_version=1,
                                    complete=False,
                                    payload=json.dumps(gap),
                                )
                            )
                        started = time.monotonic()
                        self.writer_batch_size = len(batch)
                        batch_id = batch_id or f"{now_ms()}-{uuid.uuid4().hex[:8]}"
                        await writer.write(batch_id, batch)
                        batch_id = None
                        self.writer_latency_ms = (time.monotonic() - started) * 1000
                        self._count_rate(self.dequeued, len(batch))
                        self.pending_bytes = max(0, self.pending_bytes - batch_bytes)
                        batch, batch_bytes = [], 0
                        last_flush = time.monotonic()
                        self.last_success_ms = now_ms()
                        self.healthy, self.reason = True, "recording"
                        self.state = (
                            "NORMAL" if self.queue.qsize() < self.queue.maxsize * 0.3 else "RECORDER_PRESSURE"
                        )
                    if time.monotonic() - last_health >= 5 or not self.running:
                        self.store.put("recorder_health", self.metrics())
                        if self.last_error_ms:
                            self.store.put(
                                "recorder_gap",
                                dict(at_ms=self.last_error_ms, reason=self.reason, historical=self.healthy),
                            )
                        last_health = time.monotonic()
                    if not due:
                        await asyncio.sleep(0)
                except Exception as exc:
                    self.healthy = False
                    self.last_error_ms, self.last_error_type = now_ms(), type(exc).__name__
                    self.retries += 1
                    self.state = (
                        "STORAGE_BACKPRESSURE"
                        if isinstance(exc, StorageBudgetExceeded)
                        else "RECORDER_RECOVERY"
                    )
                    self.reason = (
                        "Recording storage limit reached; inspect storage status"
                        if isinstance(exc, StorageBudgetExceeded)
                        else type(exc).__name__ + ": recorder write failed; retry scheduled"
                    )
                    self.store.put("recorder_gap", dict(at_ms=self.last_error_ms, reason=self.reason))
                    self.store.put("recorder_health", self.metrics())
                    if not self.running:
                        break
                    # Retain the bounded failed batch and retry; never silently kill the writer.
                    await asyncio.sleep(min(30, 2 ** min(self.retries, 5)))
        finally:
            writer.close()
