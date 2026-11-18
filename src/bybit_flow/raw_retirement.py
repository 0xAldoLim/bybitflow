"""Independent, audited retirement of raw evidence with no current consumer."""

import bisect
import hashlib
import json
import os
import sqlite3
import time
import uuid
import zipfile
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

from .cleanup_reconciliation import LATCH
from .db_runtime import busy
from .leases import RangeLease
from .recording_maintenance import (
    FINALIZED_QUERY,
    archive_health,
    classify,
    cleanup_claim,
    managed,
    ml_inventory,
    originals,
    replacement,
    tombstone,
    verify_ml,
)
from .retention import protection_ranges
from .storage import Store, directory_bytes, now_ms

POLICY = "raw-retirement-v1"
STATE = "raw_retirement_state_v1"
CLAIM = "cleanup_run_claim_v212"
ELIGIBLE = ("DELETABLE_FINALIZED_INCOMPLETE", "DELETABLE_NO_ML_CONSUMER")
REASONS = dict(zip(ELIGIBLE, ("FINALIZED_INCOMPLETE_NO_TRAINING_VALUE", "NO_CURRENT_ML_CONSUMER")))
TABLES = ("ml_snapshots", "ml_labels", "candidate_identities", "ml_models")
MAX_SEGMENTS = 640
MAX_BYTES = 800_000_000
MAX_PACK_BYTES = 64_000_000
MAX_SEGMENT_BYTES = 24_000_000
PLAN_FIELDS = (
    "id",
    "symbol",
    "source",
    "family",
    "direction",
    "created_ms",
    "entry",
    "zone",
    "stop",
    "tp1",
    "tp2",
    "expires_ms",
    "holding_deadline_ms",
    "horizon_profile",
)
GUARD_SQL = tuple(
    f"""
    CREATE TRIGGER IF NOT EXISTS raw_retirement_{table}_{verb.lower()}
    AFTER {verb} ON {table} BEGIN
      UPDATE kv SET payload=json_set(payload,'$.mutated_table','{table}')
      WHERE key='{STATE}' AND json_extract(payload,'$.status')='RUNNING'
        AND {record}.rowid<=json_extract(payload,'$.limits.{table}');
    END
    """
    for table in TABLES
    for verb, record in (("UPDATE", "OLD"), ("DELETE", "OLD"), ("INSERT", "NEW"))
)


def migrate(store):
    for statement in GUARD_SQL:
        store.db.execute(statement)


def overlapping(ranges, manifest):
    start, end = manifest["min_receipt_ms"], manifest["max_receipt_ms"]
    return any(lo <= end and hi >= start for lo, hi, *_ in ranges)


class Reachability:
    """Reuse a calculation only as long as no other connection has committed."""

    def __init__(self, store, settings, own_lease=None):
        self.store = store
        self.settings = settings
        self.own_lease = own_lease
        self.version = None

    def refresh(self, now):
        version = self.store.db.execute("PRAGMA data_version").fetchone()[0]
        if version != self.version:
            self.version = version
            self.ranges = [
                r for r in protection_ranges(self.store, now) if r[2] != "replay/maintenance lease"
            ]
            # An unresolved decision remains a committed consumer even after a
            # materializer checkpoint has moved the broad-cleanup frontier.
            self.ranges.extend(
                (at - 900_000, at + max(0, hold) * 60_000 + 60_000, "unresolved primary outcome")
                for at, hold in self.store.db.execute(
                    "SELECT s.decision_ms,coalesce(json_extract(s.payload,'$.signal.expected_hold_max'),240) "
                    "FROM ml_snapshots s WHERE s.stage='decision' AND NOT EXISTS "
                    "(SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy='prints-v1')"
                )
            )
            self.finalized = [
                (at - 900_000, max(at, end), complete, classification)
                for at, end, complete, classification in self.store.db.execute(FINALIZED_QUERY)
            ]
            # An alternative complete label cannot make a raw segment a
            # no-consumer or incomplete target, even if prints-v1 was incomplete.
            self.complete_alternative = [
                (at - 900_000, max(at, end))
                for at, end in self.store.db.execute(
                    "SELECT s.decision_ms,coalesce(l.exit_ms,l.available_ms) FROM ml_snapshots s "
                    "JOIN ml_label_retention l ON l.snapshot_id=s.id WHERE s.stage='decision' "
                    "AND l.policy<>'prints-v1' AND l.complete=1"
                )
            ]
        self.leases = [
            (lo, hi, "replay/maintenance lease")
            for lo, hi in self.store.db.execute(
                "SELECT start_ms,end_ms FROM storage_leases WHERE expires_ms>? AND id<>?",
                (now, self.own_lease or ""),
            )
        ]
        self.writer_paths = {
            path
            for (raw,) in self.store.db.execute("SELECT payload FROM kv WHERE key LIKE 'writer_batch_v212:%'")
            for path in json.loads(raw).get("paths", [])
        }

    def classes(self, manifest, now, paths):
        if any(str(p) in self.writer_paths for p in paths):
            return ["PROTECTED_WRITER_BATCH"]
        # Always honor the configured floor, including under storage pressure.
        floor = self.settings.ml_raw_retention_minutes * 60_000
        classes = classify(
            self.store, manifest, now, now - floor, self.ranges + self.leases, finalized=self.finalized
        )
        if classes[0] in ELIGIBLE and overlapping(self.complete_alternative, manifest):
            return ["DELETABLE_COMPLETE_LABEL_DURABLE"]
        return classes


def loose_paths(store, manifest):
    raw = managed(store, manifest["raw"], "segments")
    if raw.name != manifest["id"] + ".jsonl.gz":
        raise ValueError("Unexpected raw name")
    sidecar = managed(store, raw.with_name(manifest["id"] + ".manifest.json"), "segments")
    paths = [raw, sidecar]
    if manifest.get("parquet"):
        parquet = managed(store, manifest["parquet"], "segments")
        if parquet.name != manifest["id"] + ".parquet":
            raise ValueError("Unexpected parquet name")
        paths.append(parquet)
    return paths


def pack_members(store, path):
    return [
        json.loads(row[0])
        for row in store.db.execute(
            "SELECT s.payload FROM segment_packs p JOIN segments s ON s.id=p.segment_id "
            "WHERE p.archive=? ORDER BY s.id LIMIT ?",
            (str(path), MAX_SEGMENTS + 1),
        )
    ]


def pack_inventory(store, path, members):
    """Reject unmapped files, duplicate members and oversized rebuilds."""
    if not members or len(members) > MAX_SEGMENTS or path.stat().st_size > MAX_PACK_BYTES:
        raise ValueError("Archive exceeds retirement limits")
    expected = {p.name: m["id"] for m in members for p in loose_paths(store, m)}
    totals = Counter()
    with zipfile.ZipFile(path) as archive:
        names = set()
        decoded = 0
        for member in archive.infolist():
            name = member.filename
            decoded += member.file_size
            if name in names or name not in expected or decoded > MAX_PACK_BYTES:
                raise ValueError("Archive mapping or decoded size is unverified")
            names.add(name)
            totals[expected[name]] += member.compress_size
        if any(not {Path(m["raw"]).name, m["id"] + ".manifest.json"} <= names for m in members):
            raise ValueError("Archive is missing mapped evidence")
    return totals


def plan(store, settings, now):
    """Read-only selection. Exact content verification happens under a lease."""
    current = Reachability(store, settings)
    current.refresh(now)
    counts, sizes = Counter(), Counter()
    units = []
    unit_count = mixed_packs = 0

    def admit(path, members, targets, size):
        nonlocal unit_count, mixed_packs
        unit_count += 1
        mixed_packs += len(targets) < len(members)
        item = dict(path=path, members=members, targets=targets, bytes=size)
        bisect.insort(units, item, key=unit_priority)
        if len(units) > MAX_SEGMENTS:
            units.pop()

    def unit_priority(unit):
        return (
            min(ELIGIBLE.index(c) for _, c, _ in unit["targets"]),
            min(m["min_receipt_ms"] for m, _, _ in unit["targets"]),
            unit["path"],
        )

    def summarize(classes, size):
        category = classes[0] if len(classes) == 1 else "PROTECTED_OVERLAPPING"
        counts[category] += 1
        sizes[category] += size

    for raw in (store.root / "segments").glob("*.jsonl.gz"):
        try:
            row = store.db.execute(
                "SELECT s.payload,p.segment_id FROM segments s LEFT JOIN segment_packs p "
                "ON p.segment_id=s.id WHERE s.id=?",
                (raw.name.removesuffix(".jsonl.gz"),),
            ).fetchone()
            if not row:
                summarize(["UNKNOWN_FAIL_CLOSED"], raw.stat().st_size)
                continue
            if row[1] is not None:
                continue
            m = json.loads(row[0])
            paths = loose_paths(store, m)
            size = sum(p.stat().st_size for p in paths if p.is_file())
            classes = current.classes(m, now, paths)
            if paths[0] != raw or not paths[1].is_file() or size > MAX_SEGMENT_BYTES:
                classes = ["UNKNOWN_FAIL_CLOSED"]
            summarize(classes, size)
            if classes[0] in ELIGIBLE:
                admit(raw, [m], [(m, classes[0], size)], size)
        except (OSError, ValueError, KeyError, TypeError):
            if raw.is_file():
                summarize(["UNKNOWN_FAIL_CLOSED"], raw.stat().st_size)
    for path in (store.root / "packs").glob("*.zip"):
        try:
            path = managed(store, path, "packs")
            members = pack_members(store, path)
            sizes_by_id = pack_inventory(store, path, members)
            targets = []
            for m in members:
                size = sizes_by_id[m["id"]]
                classes = current.classes(m, now, [path, *loose_paths(store, m)])
                summarize(classes, size)
                if classes[0] in ELIGIBLE:
                    targets.append((m, classes[0], size))
            if targets:
                admit(path, members, targets, path.stat().st_size)
        except (OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile):
            if path.is_file():
                summarize(["UNKNOWN_FAIL_CLOSED"], path.stat().st_size)

    chosen, count, used = [], 0, 0
    for unit in units:
        target_count = len(unit["targets"])
        if count + target_count <= MAX_SEGMENTS and used + unit["bytes"] <= MAX_BYTES:
            chosen.append(unit)
            count += target_count
            used += unit["bytes"]
    report = dict(
        policy=POLICY,
        at_ms=now,
        counts=dict(counts),
        bytes_by_class=dict(sizes),
        protected_bytes=sum(v for k, v in sizes.items() if k.startswith("PROTECTED_")),
        unknown_bytes=sizes["UNKNOWN_FAIL_CLOSED"],
        complete_label_bytes=sizes["DELETABLE_COMPLETE_LABEL_DURABLE"],
        packs_requiring_repack=mixed_packs,
        batch_segments=count,
        batch_input_bytes=used,
        remaining_units=unit_count - len(chosen),
        historical_cleanup=store.get(LATCH, {}).get("status"),
        historical_reconciliation=store.get("cleanup_reconciliation", {}).get("state"),
        independent_retirement=store.get(STATE, {}).get("status"),
        eligibility="Reachability estimate. Checksums, current consumers and leases are checked at retirement.",
    )
    return report, chosen


def dry_run(root, settings):
    db = sqlite3.connect((root / "research.sqlite").resolve().as_uri() + "?mode=ro", uri=True, timeout=3)
    try:
        db.execute("PRAGMA query_only=ON")
        store = Store.__new__(Store)
        store.root, store.db = root.resolve(), db
        return plan(store, settings, now_ms())[0] | dict(
            status="DRY_RUN",
            total_bytes=directory_bytes(store.root),
            raw_bytes=sum(directory_bytes(store.root / folder) for folder in ("segments", "packs")),
        )
    finally:
        db.close()


def proof_rows(store, limits, *, yield_to_recorder=False):
    """Page over rowids and hash the stored payload bytes without JSON re-encoding."""
    result = {}
    deadline = time.monotonic() + 270
    next_pressure_check = 0
    for table in TABLES:
        digest = hashlib.sha256()
        cursor = count = 0
        while cursor < limits[table]:
            clock = time.monotonic()
            if clock >= deadline:
                raise TimeoutError("Frozen row verification exceeded its time limit")
            if yield_to_recorder and clock >= next_pressure_check:
                if under_pressure(store, now_ms()):
                    raise Deferred("Recorder or SQLite pressure during pre-verification")
                next_pressure_check = clock + 1
            rows = store.db.execute(
                f"SELECT rowid,* FROM {table} WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT 32",
                (cursor, limits[table]),
            ).fetchall()
            if not rows:
                break
            for row in rows:
                for value in row:
                    data = value.encode() if isinstance(value, str) else json.dumps(value).encode()
                    digest.update(type(value).__name__.encode() + len(data).to_bytes(8, "big"))
                    digest.update(data)
                cursor = row[0]
                count += 1
        result[table] = dict(count=count, sha256=digest.hexdigest())
    return result


def plan_fingerprints(store, ids=None):
    if ids is None:
        rows = store.db.execute(
            "SELECT id,payload FROM signals WHERE state NOT IN ('INVALIDATED','EXPIRED','RESOLVED')"
        )
    else:
        rows = (
            row
            for ident in ids
            for row in store.db.execute("SELECT id,payload FROM signals WHERE id=?", (ident,))
        )
    result = {}
    for ident, raw in rows:
        plan_row = json.loads(raw)
        value = {key: plan_row.get(key) for key in PLAN_FIELDS}
        result[ident] = hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
    return result


def receipts(store):
    return {
        row[0]: hashlib.sha256(json.dumps(tuple(row)).encode()).hexdigest()
        for row in store.db.execute(
            "SELECT key,signal_id,message_id FROM outbox WHERE status='sent' AND key LIKE '%:initial'"
        )
    }


def write_audit(directory, name, value):
    directory.mkdir(parents=True, exist_ok=True)
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    with (directory / name).open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    if os.name != "nt":
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return hashlib.sha256(data).hexdigest()


def under_pressure(store, now):
    rec = store.get("recorder_health", {})
    ops = store.get("operational_health", {})
    return (
        rec.get("queue_depth", 0) >= 0.4 * max(1, rec.get("queue_capacity", 10_000))
        or rec.get("state") in {"RECORDER_PRESSURE", "RECORDER_RECOVERY"}
        or (
            0 <= now - ops.get("at_ms", 0) <= 90_000
            and ((ops.get("recorder_pressure") or 0) >= 0.4 or (ops.get("sqlite_busy_delta") or 0) > 0)
        )
    )


class Deferred(Exception):
    """Retirement yielded before the next destructive step."""


@contextmanager
def immediate(store):
    store.db.execute("BEGIN IMMEDIATE")
    try:
        yield
        store.db.commit()
    except BaseException:
        store.db.rollback()
        raise


def verify_guard(store, owner):
    now = now_ms()
    state = store.get(STATE, {})
    if state.get("status") != "RUNNING" or state.get("mutated_table"):
        raise RuntimeError("Frozen retirement evidence changed: " + str(state.get("mutated_table")))
    claim = store.get(CLAIM, {})
    if claim.get("owner") != owner or claim.get("expires_ms", 0) <= now:
        raise RuntimeError("Retirement claim expired or changed")
    return now


def renew(store, owner):
    with immediate(store):
        now = verify_guard(store, owner)
        claim = store.get(CLAIM)
        store.db.execute(
            "UPDATE kv SET payload=? WHERE key=?", (json.dumps(claim | {"expires_ms": now + 600_000}), CLAIM)
        )


def file_signature(paths):
    return {
        p: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
        for p in paths
        if p.is_file() and not p.is_symlink()
        for st in (p.stat(),)
    }


def retire_unit(store, settings, unit, owner, run_id, result, current):
    path = unit["path"]
    is_pack = path.parent.name == "packs"
    members = unit["members"]
    targets = {m["id"]: (c, n) for m, c, n in unit["targets"]}
    start = min(m["min_receipt_ms"] for m in members)
    end = max(m["max_receipt_ms"] for m in members)
    target = None
    committed = False
    with RangeLease(store, start, end, POLICY, exclusive=True) as lease:
        current.own_lease = lease.id

        def still_eligible(refresh=True):
            now = verify_guard(store, owner)
            if under_pressure(store, now):
                raise Deferred("Recorder or SQLite pressure")
            if refresh:
                current.refresh(now)
            if store.db.execute("PRAGMA data_version").fetchone()[0] != current.version:
                return []
            if not store.db.execute(
                "SELECT 1 FROM storage_leases WHERE id=? AND expires_ms>?", (lease.id, now)
            ).fetchone():
                raise Deferred("Retirement lease expired")
            selected = []
            for m in members:
                paths = loose_paths(store, m)
                row = store.db.execute(
                    "SELECT s.payload,p.archive FROM segments s LEFT JOIN segment_packs p ON p.segment_id=s.id "
                    "WHERE s.id=?",
                    (m["id"],),
                ).fetchone()
                if not row or json.loads(row[0]) != m or row[1] != (str(path) if is_pack else None):
                    return []
                if m["id"] not in targets:
                    continue
                classes = current.classes(m, now, [path, *paths])
                if classes == [targets[m["id"]][0]]:
                    selected.append(m)
            return selected

        chosen = still_eligible()
        if not chosen:
            return
        if is_pack:
            if pack_members(store, path) != members or any(
                p.exists() for m in members for p in loose_paths(store, m)
            ):
                return
            originals_to_remove = [path]
            try:
                signature = file_signature(originals_to_remove)
                weights = pack_inventory(store, path, members)
                health = archive_health(store, path, members, run_id, persist=False)
            except (ValueError, OSError, KeyError, zipfile.BadZipFile):
                return
            if set(health["valid_segment_ids"]) != {m["id"] for m in members}:
                return
            retained = [m for m in members if m not in chosen]
            if retained:
                used = directory_bytes(store.root)
                physical = (
                    os.statvfs(store.root).f_bavail * os.statvfs(store.root).f_frsize
                    if os.name != "nt"
                    else None
                )
                if used + MAX_PACK_BYTES >= settings.max_storage_gb * 1e9 or (
                    physical is not None and physical < MAX_PACK_BYTES
                ):
                    return
                try:
                    target = replacement(store, path, retained, health, [lease])
                except (ValueError, OSError, KeyError, zipfile.BadZipFile):
                    return
            old_size = path.stat().st_size
            released = old_size - (target.stat().st_size if target else 0)
            if released <= 0:
                if target:
                    target.unlink()
                return
            total = sum(weights[m["id"]] for m in chosen)
            sizes = {m["id"]: released * weights[m["id"]] // max(1, total) for m in chosen}
            sizes[chosen[-1]["id"]] += released - sum(sizes.values())
        else:
            m = chosen[0]
            paths = loose_paths(store, m)
            try:
                originals_to_remove = [p for p in paths if p.exists()]
                signature = file_signature(originals_to_remove)
                valid = sum(s[2] for s in signature.values()) <= MAX_SEGMENT_BYTES and originals(store, m)
            except (ValueError, OSError, KeyError):
                valid = False
            if not valid:
                return
            sizes = {m["id"]: sum(p.stat().st_size for p in paths if p.exists())}
            retained, health = [], None

        try:
            lease.heartbeat(force=True)
            if still_eligible() != chosen or file_signature(originals_to_remove) != signature:
                return
            with immediate(store):
                # This check and the tombstone/mapping publication share one
                # transaction. Physical deletion happens under the same lease.
                if still_eligible(refresh=False) != chosen or len(signature) != len(originals_to_remove):
                    return
                for m in retained:
                    store.db.execute(
                        "UPDATE segment_packs SET archive=? WHERE segment_id=? AND archive=?",
                        (str(target), m["id"], str(path)),
                    )
                for m in chosen:
                    category, _ = targets[m["id"]]
                    tombstone(
                        store,
                        m,
                        [category],
                        run_id,
                        str(path) if is_pack else None,
                        health,
                        retirement=dict(
                            policy=POLICY,
                            classification=category,
                            reason_code=REASONS[category],
                            retired_ms=now_ms(),
                            run_id=run_id,
                            raw_bytes_freed=sizes[m["id"]],
                        ),
                    )
                    if is_pack:
                        store.db.execute(
                            "DELETE FROM segment_packs WHERE segment_id=? AND archive=?", (m["id"], str(path))
                        )
            committed = True
            for original in originals_to_remove:
                original.unlink()
            for m in chosen:
                category = targets[m["id"]][0]
                result["freed_by_class"][category] += sizes[m["id"]]
                result["freed_bytes"] += sizes[m["id"]]
                result["retired_segments"] += 1
            result["packs_rebuilt"] += bool(target)
        finally:
            if target and not committed:
                target.unlink(missing_ok=True)


def run(store, settings):
    now = now_ms()
    previous = store.get(STATE, {})
    if previous.get("status") in {"FAILED", "RUNNING"}:
        return dict(
            policy=POLICY, status="BLOCKED", reason="A prior retirement requires verification", prior=previous
        )
    if (
        not settings.recording_retention_enabled
        or store.get("cleanup_hold_v212")
        or under_pressure(store, now)
    ):
        return dict(policy=POLICY, status="DEFERRED", reason="Retention disabled, held or under pressure")
    report, units = plan(store, settings, now)
    if not units:
        return report | {"status": "NO_ELIGIBLE_RAW"}
    run_id = POLICY + "-" + uuid.uuid4().hex
    with cleanup_claim(store, run_id) as owner:
        if owner is None:
            return report | {"status": "DEFERRED", "reason": "Another cleanup is running"}
        with immediate(store):
            if store.get(STATE, {}).get("status") in {"FAILED", "RUNNING"}:
                return report | {"status": "BLOCKED"}
            limits = {
                table: store.db.execute(f"SELECT coalesce(max(rowid),0) FROM {table}").fetchone()[0]
                for table in TABLES
            }
            asof = now_ms()
            state = dict(run_id=run_id, policy=POLICY, status="RUNNING", limits=limits, at_ms=asof)
            store.db.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (STATE, json.dumps(state)))
        directory = store.root / "ml" / "raw-retirement"
        result = dict(
            policy=POLICY,
            run_id=run_id,
            at_ms=asof,
            freed_bytes=0,
            retired_segments=0,
            packs_rebuilt=0,
            skipped=0,
            freed_by_class=Counter(),
            historical_cleanup=report["historical_cleanup"],
            historical_reconciliation=report["historical_reconciliation"],
        )
        try:
            try:
                before = proof_rows(store, limits, yield_to_recorder=True)
            except sqlite3.OperationalError as exc:
                if not busy(exc):
                    raise
                raise Deferred("SQLite writer busy during pre-verification") from exc
            ml_before = ml_inventory(store, asof, artifacts=True, row_limits=limits)
            active_before, receipts_before = plan_fingerprints(store), receipts(store)
            proof = dict(
                state,
                frozen_rows=before,
                ml=ml_before,
                active_plans=active_before,
                initial_receipts=receipts_before,
                active_leases=[
                    tuple(r)
                    for r in store.db.execute(
                        "SELECT id,start_ms,end_ms,expires_ms FROM storage_leases WHERE expires_ms>?", (asof,)
                    )
                ],
                unresolved_primary=[
                    r[0]
                    for r in store.db.execute(
                        "SELECT s.id FROM ml_snapshots s WHERE s.stage='decision' AND NOT EXISTS "
                        "(SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy='prints-v1')"
                    )
                ],
                targets=[
                    dict(id=m["id"], sha256=m["sha256"], classification=c, bytes=n)
                    for u in units
                    for m, c, n in u["targets"]
                ],
            )
            result["pre_audit_sha256"] = write_audit(directory, run_id + "-pre.json", proof)
            current = Reachability(store, settings)
            deadline = time.monotonic() + 250
            for unit in units:
                try:
                    if time.monotonic() >= deadline:
                        raise Deferred("Per-batch time limit")
                    if store.get(CLAIM, {}).get("expires_ms", 0) - now_ms() < 60_000:
                        renew(store, owner)
                    start_count = result["retired_segments"]
                    retire_unit(store, settings, unit, owner, run_id, result, current)
                    result["skipped"] += len(unit["targets"]) - (result["retired_segments"] - start_count)
                except Deferred as exc:
                    result["yielded"] = str(exc)
                    break
                except BlockingIOError:
                    result["skipped"] += len(unit["targets"])
                except sqlite3.OperationalError as exc:
                    if not busy(exc):
                        raise
                    result["yielded"] = "SQLite writer busy"
                    break
            renew(store, owner)
            after = proof_rows(store, limits)
            ml_after = ml_inventory(store, asof, artifacts=True, row_limits=limits)
            verify_ml(ml_before, ml_after)
            active_after, receipts_after = plan_fingerprints(store, active_before), receipts(store)
            if (
                before != after
                or active_before != active_after
                or any(receipts_after.get(k) != v for k, v in receipts_before.items())
            ):
                raise RuntimeError("Frozen retirement rows, plans or receipts changed")
            result["post_audit_sha256"] = write_audit(
                directory,
                run_id + "-post.json",
                dict(
                    frozen_rows=after,
                    ml=ml_after,
                    active_plans=active_after,
                    initial_receipts={k: receipts_after[k] for k in receipts_before},
                ),
            )
            result["status"] = "PASSED"
            with immediate(store):
                verify_guard(store, owner)
                store.db.execute("UPDATE kv SET payload=? WHERE key=?", (json.dumps(result), STATE))
            write_audit(directory, run_id + "-result.json", result)
            return result
        except Deferred as exc:
            # Only pre-verification propagates a deferral here. After deletion,
            # the loop yields and still verifies the entire frozen proof.
            if result["retired_segments"]:
                raise
            deferred = result | {"status": "DEFERRED", "reason": str(exc)}
            store.put(STATE, deferred)
            return deferred
        except BaseException as exc:
            # RUNNING also fails closed after hard process/container loss.
            failed = result | {"status": "FAILED", "error_type": type(exc).__name__, "reason": str(exc)[:320]}
            try:
                write_audit(directory, run_id + "-failure.json", failed)
            finally:
                store.put(STATE, failed)
            if not isinstance(exc, Exception):
                raise
            return failed


def automatic(store, settings, used, now):
    """Sample at most once per six hours once ordinary storage pressure begins."""
    rec = store.get("recorder_health", {})
    if (
        used < 0.85 * settings.max_storage_gb * 1e9
        or not rec.get("healthy")
        or not 0 <= now - rec.get("at_ms", 0) <= 90_000
        or now - store.get("raw_retirement_attempt", {}).get("at_ms", 0) < 6 * 3_600_000
    ):
        return None
    store.put("raw_retirement_attempt", {"at_ms": now})
    return run(store, settings)
