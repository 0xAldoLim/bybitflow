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

RECEIPT_QUANTIZATION_MS = 1


def now_ms():
    return time.time_ns() // 1_000_000


def directory_bytes(root):
    if not root.exists():
        return 0
    total = 0
    pending = [root]
    while pending:
        try:
            with os.scandir(pending.pop()) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                    except FileNotFoundError:
                        # Retention/compaction may remove a listed file before stat.
                        continue
        except FileNotFoundError:
            # A queued directory may also disappear during the traversal.
            continue
    return total


class StorageBudgetExceeded(OSError):
    """Configured recording budget reached; physical disk may still have space."""


class Store:
    def __init__(self, root: Path, migration_backup_dir: Path | None = None):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        from .db_runtime import MeasuredConnection

        self._metadata = None
        self._runtime = False
        self.db = sqlite3.connect(root / "research.sqlite", timeout=5, factory=MeasuredConnection)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        from .migrations import apply

        apply(self, migration_backup_dir)
        if self.get("horizon_counts") is None:
            counts = dict(
                self.db.execute(
                    "SELECT coalesce(json_extract(payload,'$.horizon_profile'),'LEGACY'),count(*) FROM signals GROUP BY 1"
                ).fetchall()
            )
            self.put("horizon_counts", counts)

    def put(self, key, value):
        if key == "bybitflow_v2_cutover" and self.get(key) is not None:
            if self.get(key) != value:
                raise ValueError("Product cutover epoch is immutable")
            return
        payload = json.dumps(value)
        if getattr(self, "_metadata", None):
            from .db_runtime import diagnostic_key

            if diagnostic_key(key):
                self._metadata.offer(key, payload)
                return
        for attempt in range(4):
            try:
                with self.db:
                    self.db.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (key, payload))
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                if attempt == 3 or getattr(self, "_runtime", False):
                    raise  # A critical write is acknowledged or fails closed; no loop-blocking retries.
                self.db.health.increment("sqlite_busy_retries")
                time.sleep(0.05 * 2**attempt)

    def get(self, key, default=None):
        writer = getattr(self, "_metadata", None)
        if writer:
            found, value = writer.read(key)
            if found:
                return value
        r = self.db.execute("SELECT payload FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(r[0]) if r else default

    def signal(self, signal, reason="evaluation"):
        from .ml.store import FeatureStore
        from .observations import preserve_origin, start

        if signal.synthetic:
            raise ValueError("Synthetic signals must never enter research storage")
        old = self.db.execute("SELECT state,payload FROM signals WHERE id=?", (signal.id,)).fetchone()
        if signal.source == "tradingview" and not old:
            raise ValueError("TRADINGVIEW_SUBSYSTEM_RETIRED")
        if old:
            from .v2 import preserve

            preserve(signal, json.loads(old[1]), self.get("bybitflow_v2_cutover"))
            from .setup_freeze import preserve as preserve_published

            preserve_published(self, signal, json.loads(old[1]))

        # Capture before lifecycle overwrites; both accepted and rejected decisions survive.
        features = FeatureStore(self)
        features.capture(signal, max(now_ms(), signal.created_ms), "generation")
        if signal.evidence.get("score_components") or (
            (signal.evidence.get("v21_counterfactual") or signal.evidence.get("v22_counterfactual"))
            and "cost_per_base" in signal.risk
        ):
            features.capture(signal, max(now_ms(), signal.created_ms), "decision")
        if old:
            if old[0] in {"INVALIDATED", "EXPIRED", "RESOLVED"}:
                return  # A stale asynchronous evaluator must never resurrect a terminal setup.
            saved = json.loads(old[1])
            for key in (
                "observed_entry_ms",
                "observed_entry_price",
                "observed_entry_method",
                "observed_entry_source",
                "observed_entry_trade_id",
                "tp1_touch_ms",
                "tp2_touch_ms",
            ):
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
            from .setup_research import register as register_setup_paths

            register_setup_paths(self, signal, now_ms())
            register_setup_paths(self, signal, now_ms(), stage="delivery")
        start(self, signal, now_ms())
        # Shadow research cannot roll back an acknowledged production decision.
        from .forward_evidence import register as register_forward

        try:
            with self.db:
                register_forward(self, signal, now_ms())
        except Exception:
            self.db.health.increment("forward_enrollment_errors")

    def monitor(self, signal):
        """Persist only lightweight lifecycle progress, without scoring/ML jobs."""
        from .v2 import preserve

        saved = self.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()
        if saved:
            preserve(signal, json.loads(saved[0]), self.get("bybitflow_v2_cutover"))
            from .setup_freeze import preserve as preserve_published

            preserve_published(self, signal, json.loads(saved[0]))
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

    def signal_summaries(self, limit=200):
        """Dashboard list fields; complete evidence remains in signal detail."""
        fields = (
            "id",
            "symbol",
            "direction",
            "family",
            "horizon_profile",
            "quality",
            "final_tier",
            "state",
            "created_ms",
        )
        columns = ",".join(f"json_extract(payload,'$.{field}')" for field in fields)
        rows = self.db.execute(
            "SELECT " + columns + ",json_extract(payload,'$.coverage'),"
            "json_extract(payload,'$.evidence.expiry_mark') FROM signals "
            "ORDER BY created_ms DESC LIMIT ?",
            (limit,),
        )
        result = []
        for row in rows:
            summary = dict(zip(fields, row[: len(fields)]))
            summary["coverage"] = json.loads(row[-2]) if row[-2] else {}
            summary["evidence"] = {"expiry_mark": json.loads(row[-1])} if row[-1] else {}
            result.append(summary)
        return result

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
        from contextlib import closing

        with (
            closing(sqlite3.connect(self.root / "research.sqlite")) as source,
            closing(sqlite3.connect(target)) as dest,
        ):
            source.execute("BEGIN")
            source.execute("SELECT count(*) FROM sqlite_master").fetchone()
            source.backup(dest, pages=1000)

    def enable_runtime(self):
        from .db_runtime import MetadataWriter

        if not self._runtime:
            # Permit a short independent writer commit, with a hard 25ms wait
            # instead of the default five-second event-loop stall.
            self.db.execute("PRAGMA busy_timeout=25")
            self._runtime = True
            self._metadata = MetadataWriter(self.db, self.db.health)

    async def stop_runtime(self):
        if getattr(self, "_metadata", None):
            await self._metadata.close()
            self._metadata = None

    def close(self):
        if getattr(self, "_metadata", None):
            self._metadata.abort()
            self._metadata = None
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
        self.last_offered_receipt_ms = None
        self.receipt_clock_adjustments = 0
        self.backpressure_waits = self.backpressure_wait_ms = self.last_backpressure_ms = 0
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
        now = now_ms()
        second = now // 1000
        return dict(
            at_ms=now,
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
            receipt_clock_adjustments=self.receipt_clock_adjustments,
            backpressure_waits=self.backpressure_waits,
            backpressure_wait_ms=self.backpressure_wait_ms,
            last_backpressure_ms=self.last_backpressure_ms,
        )

    def pressure(self):
        return max(
            self.queue.qsize() / self.queue.maxsize, self.pending_bytes / self.settings.queue_byte_limit
        )

    async def wait_for_capacity(self, timeout=2):
        """Bound socket backpressure before consuming another market message."""
        if not self.healthy:
            raise RuntimeError("Recording unavailable")
        if self.pressure() < 0.5:
            return
        started = asyncio.get_running_loop().time()
        self.backpressure_waits += 1
        self.last_backpressure_ms = now_ms()
        try:
            while self.pressure() >= 0.5:
                if not self.healthy:
                    raise RuntimeError("Recording unavailable")
                if asyncio.get_running_loop().time() - started >= timeout:
                    raise RuntimeError("Recorder capacity did not recover within the feed buffer budget")
                await asyncio.sleep(0.01)
        finally:
            self.backpressure_wait_ms += (asyncio.get_running_loop().time() - started) * 1000
            self.last_backpressure_ms = now_ms()

    def _gap(self, source, symbol, at, reason):
        from .ml.coverage import market_source

        self.events_dropped += 1
        venue, _ = market_source(source)
        gap = self.gaps.setdefault(
            (venue, symbol),
            dict(coverage_gap=True, gap_start_ms=at, reason=reason, source=venue, dropped_events=0),
        )
        gap["gap_end_ms"] = at
        gap["dropped_events"] += 1

    def offer(self, source, symbol, event_ms, payload, receipt_ms=None, complete=True):
        # Producer performs no filesystem or SQLite operations.
        receipt = receipt_ms if receipt_ms is not None else now_ms()
        observed_receipt = receipt
        # Admission time is ordered separately from the observed wall clock.
        # Recorded 1 ms clock quantization reversals must not reorder new envelopes.
        # Keep the original clock reading; larger reversals still fail strict replay.
        if (
            self.last_offered_receipt_ms is not None
            and 0 < self.last_offered_receipt_ms - receipt <= RECEIPT_QUANTIZATION_MS
        ):
            receipt = self.last_offered_receipt_ms
        optional = symbol not in self.critical_symbols and "control/" not in source
        pressure = self.pressure()
        if pressure >= 0.7 and optional:
            self.state = "RECORDER_BACKPRESSURE"
            self._gap(source, symbol, receipt, "RECORDER_BACKPRESSURE")
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
        if receipt != observed_receipt:
            row["observed_receipt_ms"] = observed_receipt
            row["receipt_clock_adjustment_ms"] = receipt - observed_receipt
        self.serialization_ms = (time.monotonic() - before) * 1000
        size = len(row["payload"].encode()) + 256
        try:
            if self.pending_bytes + size > self.settings.queue_byte_limit:
                raise asyncio.QueueFull
            self.queue.put_nowait(row)
            self.last_offered_receipt_ms = receipt
            self.receipt_clock_adjustments += receipt != observed_receipt
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
            self._gap(source, symbol, receipt, "RECORDER_BACKPRESSURE")
            raise RuntimeError(self.reason) from None

    def flush(self, batch, store=None, ident=None):
        store = store or self.store
        ident = ident or f"{now_ms()}-{uuid.uuid4().hex[:8]}"
        key = "writer_batch_v212:recorder"
        store.put(
            key,
            dict(
                at_ms=now_ms(),
                id=ident,
                paths=[
                    str(store.root / "segments" / (ident + suffix))
                    for suffix in (".jsonl.gz", ".parquet", ".manifest.json")
                ],
            ),
        )
        try:
            self._flush(batch, store, ident)
        finally:
            with store.db:
                store.db.execute("DELETE FROM kv WHERE key=?", (key,))

    def _flush(self, batch, store=None, ident=None):
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
            # The independent storage loop owns cleanup. Running its full
            # archive/ML audit here exceeds the bounded writer's timeout and
            # repeatedly kills cleanup before it can restore recording.
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
            from .ml.coverage import update_coverage

            update_coverage(store, batch, self.settings.trade_stale_ms)
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
                        for (venue, symbol), gap in gaps.items():
                            # Appended controls mark the whole affected interval as incomplete.
                            batch.append(
                                dict(
                                    source=f"native/{venue}/control/gap",
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
                        try:
                            self.store.put("recorder_health", self.metrics())
                            if self.last_error_ms:
                                self.store.put(
                                    "recorder_gap",
                                    dict(
                                        at_ms=self.last_error_ms, reason=self.reason, historical=self.healthy
                                    ),
                                )
                            last_health = time.monotonic()
                        except sqlite3.OperationalError as exc:
                            from .db_runtime import busy

                            if not busy(exc):
                                raise
                            # The batch was already acknowledged. Retry cached
                            # status next pass; do not invent a raw recording gap
                            # or open a circuit that prevents the next batch.
                            self.store.db.health.increment("recorder_status_busy_retries")
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
                    # A real writer failure remains fail-closed until its gap
                    # is durable. Contended diagnostics cannot kill this retry
                    # loop or discard the bounded, idempotent pending batch.
                    while True:
                        try:
                            self.store.put("recorder_gap", dict(at_ms=self.last_error_ms, reason=self.reason))
                            self.store.put("recorder_health", self.metrics())
                            break
                        except sqlite3.OperationalError as metadata_exc:
                            from .db_runtime import busy

                            if not busy(metadata_exc):
                                raise
                            self.store.db.health.increment("recorder_status_busy_retries")
                            await asyncio.sleep(0.05)
                    if not self.running:
                        break
                    # Retain the bounded failed batch and retry; never silently kill the writer.
                    await asyncio.sleep(min(30, 2 ** min(self.retries, 5)))
        finally:
            writer.close()
