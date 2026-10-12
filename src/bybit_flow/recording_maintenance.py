"""Reachability-based raw cleanup. Derived ML records are never cleanup targets."""

import hashlib
import json
import os
import uuid
import zipfile
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

from .diagnostics import cause_code, record_failure
from .storage import directory_bytes, now_ms

CLASSES = (
    "PROTECTED_ACTIVE_SETUP",
    "PROTECTED_UNRESOLVED_PRIMARY_ML",
    "PROTECTED_REPLAY_LEASE",
    "PROTECTED_RECENT_FLOOR",
    "DELETABLE_COMPLETE_LABEL_DURABLE",
    "DELETABLE_FINALIZED_INCOMPLETE",
    "DELETABLE_NO_ML_CONSUMER",
    "DELETABLE_CORRUPT_UNRECOVERABLE",
    "DELETABLE_ALREADY_TOMBSTONED_RESIDUE",
    "UNKNOWN_FAIL_CLOSED",
)
PROVEN_CORRUPTION = {
    "ARCHIVE_BAD_ZIP",
    "ARCHIVE_MEMBER_MISSING",
    "RAW_HASH_MISMATCH",
    "MANIFEST_MISMATCH",
    "RAW_GZIP_TRUNCATED",
    "JSON_DECODE_ERROR",
}


def migrate(store):
    with store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS archive_health(archive TEXT PRIMARY KEY,payload TEXT NOT NULL)"
        )


def file_hash(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1_048_576):
            h.update(chunk)
    return h.hexdigest()


def managed(store, path, directory):
    path = Path(path)
    if path.is_symlink() or path.resolve().parent != (store.root / directory).resolve():
        raise ValueError("Retention path outside managed " + directory + " directory")
    return path


def verify(data, manifest, sidecar):
    if hashlib.sha256(data).hexdigest() != manifest["sha256"]:
        raise ValueError("Raw hash mismatch")
    if sidecar != manifest:
        raise ValueError("Archive manifest mismatch")


def originals(store, m):
    raw = managed(store, m["raw"], "segments")
    sidecar = raw.with_name(m["id"] + ".manifest.json")
    if not raw.is_file() or not sidecar.is_file():
        return None
    data = raw.read_bytes()
    manifest = json.loads(sidecar.read_text())
    verify(data, m, manifest)
    files = {raw.name: data, sidecar.name: sidecar.read_bytes()}
    parquet = managed(store, m["parquet"], "segments") if m.get("parquet") else None
    if parquet and parquet.is_file():
        files[parquet.name] = parquet.read_bytes()
    return files


def archive_health(store, path, members, run_id):
    migrate(store)
    path = managed(store, path, "packs")
    stat = path.stat()
    previous = store.db.execute("SELECT payload FROM archive_health WHERE archive=?", (str(path),)).fetchone()
    old = json.loads(previous[0]) if previous else {}
    fingerprint = hashlib.sha256(
        json.dumps([(m["id"], m.get("sha256")) for m in members], sort_keys=True).encode()
    ).hexdigest()
    if old.get("signature") == [stat.st_size, stat.st_mtime_ns, fingerprint] and old.get(
        "verification_finished"
    ):
        return old
    now = now_ms()
    errors = []
    repairable = []
    valid = []
    member_bytes = {}
    health = dict(
        archive=str(path),
        archive_sha256=file_hash(path),
        archive_size=stat.st_size,
        first_seen_ms=old.get("first_seen_ms", now),
        last_checked_ms=now,
        member_segment_ids=[m["id"] for m in members],
        range_start_ms=min((m.get("min_receipt_ms", 0) for m in members), default=None),
        range_end_ms=max((m.get("max_receipt_ms", 0) for m in members), default=None),
        repair_attempted=True,
        repair_result="NO_REPAIR_NEEDED",
        protected_reason=[],
        disposition="PENDING_REACHABILITY",
        deleted_ms=None,
        signature=[stat.st_size, stat.st_mtime_ns, fingerprint],
    )

    def failure(exc, ident):
        record_failure(
            store,
            exc,
            scan_id=run_id,
            phase="archive verification",
            operation="mapped raw/hash/manifest verification",
            archive=str(path),
            segment_id=ident,
            range_start_ms=health["range_start_ms"],
            range_end_ms=health["range_end_ms"],
        )

    try:
        with zipfile.ZipFile(path) as archive:
            for m in members:
                try:
                    member_bytes[m["id"]] = sum(
                        i.compress_size
                        for i in archive.infolist()
                        if i.filename
                        in (Path(m["raw"]).name, m["id"] + ".manifest.json", Path(m.get("parquet", "")).name)
                    )
                    data = archive.read(Path(m["raw"]).name)
                    sidecar = json.loads(archive.read(m["id"] + ".manifest.json"))
                    verify(data, m, sidecar)
                    valid.append(m["id"])
                except Exception as exc:
                    failure(exc, m["id"])
                    errors.append(
                        dict(
                            segment_id=m["id"],
                            cause=cause_code(exc),
                            type=type(exc).__name__,
                            message=str(exc)[:300],
                        )
                    )
    except Exception as exc:
        failure(exc, None)
        errors.append(
            dict(segment_id=None, cause=cause_code(exc), type=type(exc).__name__, message=str(exc)[:300])
        )
    bad = {m["id"] for m in members if m["id"] not in valid}
    proven = {
        ident
        for ident in bad
        if any(e["cause"] in PROVEN_CORRUPTION and e["segment_id"] in (None, ident) for e in errors)
    }
    for m in members:
        if m["id"] in bad:
            try:
                if originals(store, m):
                    repairable.append(m["id"])
            except (OSError, ValueError, KeyError):
                pass
    health.update(
        status="HEALTHY"
        if not errors
        else "PARTIAL"
        if valid
        else "UNREADABLE"
        if errors[0]["cause"] == "ARCHIVE_BAD_ZIP"
        else "CORRUPT",
        errors=errors,
        error_type=errors[0]["type"] if errors else None,
        error_message=errors[0]["message"] if errors else None,
        valid_segment_ids=valid,
        member_bytes=member_bytes,
        repairable_segment_ids=repairable,
        corrupt_segment_ids=sorted(proven),
        unknown_segment_ids=sorted(bad - proven),
        verification_finished=True,
        repair_result="ORIGINALS_AVAILABLE"
        if repairable
        else "UNRECOVERABLE_MEMBERS"
        if errors
        else "NO_REPAIR_NEEDED",
    )
    save_health(store, health)
    return health


def save_health(store, health):
    with store.db:
        store.db.execute(
            "INSERT OR REPLACE INTO archive_health VALUES(?,?)", (health["archive"], json.dumps(health))
        )


def classify(store, m, now, cutoff, ranges, corrupt=False, finalized=()):
    if (
        not all(type(m.get(k)) is int for k in ("min_receipt_ms", "max_receipt_ms"))
        or m["min_receipt_ms"] > m["max_receipt_ms"]
    ):
        return ["UNKNOWN_FAIL_CLOSED"]
    start, end = m["min_receipt_ms"], m["max_receipt_ms"]
    reasons = []
    mapping = {
        "active setup": "PROTECTED_ACTIVE_SETUP",
        "unresolved primary outcome": "PROTECTED_UNRESOLVED_PRIMARY_ML",
        "replay/maintenance lease": "PROTECTED_REPLAY_LEASE",
    }
    for a, b, reason in ranges:
        if a <= end and b >= start:
            reasons.append(mapping.get(reason, "UNKNOWN_FAIL_CLOSED"))
    if end > cutoff:
        reasons.append("PROTECTED_RECENT_FLOOR")
    if reasons:
        return sorted(set(reasons))
    if store.get("pruned_segment:" + m["id"]):
        return ["DELETABLE_ALREADY_TOMBSTONED_RESIDUE"]
    if corrupt:
        return ["DELETABLE_CORRUPT_UNRECOVERABLE"]
    labels = [(r[2], r[3]) for r in finalized if r[0] <= end and r[1] >= start]
    if any(row[0] == 1 for row in labels):
        return ["DELETABLE_COMPLETE_LABEL_DURABLE"]
    if labels and all(row[1] in ("incomplete", "technical_duplicate", "no_entry") for row in labels):
        return ["DELETABLE_FINALIZED_INCOMPLETE"]
    if labels:
        return ["UNKNOWN_FAIL_CLOSED"]
    return ["DELETABLE_NO_ML_CONSUMER"]


def deletable(classes):
    return bool(classes) and all(c.startswith("DELETABLE_") for c in classes)


def cleanup_priority(classes):
    order = (
        "DELETABLE_ALREADY_TOMBSTONED_RESIDUE",
        "DELETABLE_CORRUPT_UNRECOVERABLE",
        "DELETABLE_FINALIZED_INCOMPLETE",
        "DELETABLE_COMPLETE_LABEL_DURABLE",
        "DELETABLE_NO_ML_CONSUMER",
    )
    return min((order.index(c) for c in classes if c in order), default=6)


def ml_inventory(store, asof, artifacts=False):
    from .ml import SCHEMA_VERSION
    from .ml.operations import trainability
    from .ml.store import FeatureStore

    ready = trainability(store, asof)["by_source"]
    identities = {}
    for source in ready:
        rows = FeatureStore(store).dataset(asof, source=source, schema_version=SCHEMA_VERSION)
        ids = sorted(r["candidate_identity"] for r in rows)
        identities[source] = dict(
            count=len(ids),
            digest=hashlib.sha256(json.dumps(ids, separators=(",", ":")).encode()).hexdigest(),
            identities=ids,
        )
    result = dict(
        at_ms=asof,
        by_source=ready,
        eligible=identities,
        models={r[0]: r[1] for r in store.db.execute("SELECT id,sha256 FROM ml_models")},
    )
    if artifacts:
        result["artifacts"] = {
            str(p.relative_to(store.root)): file_hash(p)
            for folder in ("ml/datasets", "ml/models")
            for p in (store.root / folder).rglob("*")
            if p.is_file() and not p.is_symlink()
        }
    return result


def verify_ml(before, after):
    for source in before["by_source"]:
        for field in ("trainable_current_schema", "sequence_ready_16", "unique_complete_candidates"):
            if before["by_source"][source][field] != after["by_source"][source][field]:
                raise RuntimeError(f"Cleanup ML verification FAILED: {source} {field}")
        if before["eligible"][source] != after["eligible"][source]:
            raise RuntimeError(f"Cleanup eligible identities FAILED: {source}")
    for field in ("models", "artifacts"):
        if field not in after:
            continue
        for key, value in before.get(field, {}).items():
            if after.get(field, {}).get(key) != value:
                raise RuntimeError("Cleanup immutable artifact verification FAILED: " + key)


def planning(store, settings, now=None, force=False, max_reclaim_bytes=None):
    from .retention import protection_ranges

    migrate(store)
    now = now or now_ms()
    usage = directory_bytes(store.root)
    budget = settings.max_storage_gb * 1e9
    floor = 15 if usage >= 0.85 * budget else getattr(settings, "ml_raw_retention_minutes", 60)
    finalized = [
        (r[0] - 900000, max(r[0], r[1]), r[2], r[3])
        for r in store.db.execute(
            "SELECT s.decision_ms,coalesce(json_extract(l.payload,'$.exit_ms'),l.available_ms),json_extract(l.payload,'$.complete'),json_extract(l.payload,'$.classification') FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' WHERE s.stage='decision'"
        )
    ]
    cutoff = now - floor * 60000
    ranges = protection_ranges(store, now)
    items = []
    packs = []
    counts = Counter()
    sizes = Counter()
    run_id = "cleanup:" + uuid.uuid4().hex
    unsafe = store.db.execute(
        "SELECT payload FROM segments WHERE json_extract(payload,'$.raw') NOT LIKE ? LIMIT 1",
        (str((store.root / "segments").resolve()) + os.sep + "%",),
    ).fetchone()
    if unsafe:
        managed(store, json.loads(unsafe[0])["raw"], "segments")
    referenced = set()
    active_batches = {
        p
        for _, raw in store.db.execute("SELECT key,payload FROM kv WHERE key LIKE 'writer_batch_v212:%'")
        for p in json.loads(raw).get("paths", [])
    }
    for path in sorted((store.root / "segments").glob("*")):
        managed(store, path, "segments")
        if not path.name.endswith(".jsonl.gz"):
            continue
        ident = path.name.removesuffix(".jsonl.gz")
        row = store.db.execute("SELECT payload FROM segments WHERE id=?", (ident,)).fetchone()
        if not row:
            continue
        m = json.loads(row[0])
        files = [
            managed(store, m["raw"], "segments"),
            managed(store, m["parquet"], "segments") if m.get("parquet") else None,
            path.with_name(ident + ".manifest.json"),
        ]
        files = [p for p in files if p is not None]
        referenced.update(str(p) for p in files)
        raw_error = None
        try:
            if not originals(store, m):
                raise FileNotFoundError("Original raw or sidecar missing")
        except Exception as exc:
            raw_error = record_failure(
                store,
                exc,
                scan_id=run_id,
                phase="raw verification",
                operation="original checksum/manifest",
                segment_id=m["id"],
                range_start_ms=m.get("min_receipt_ms"),
                range_end_ms=m.get("max_receipt_ms"),
            )
        corrupt = bool(raw_error and raw_error["cause_code"] in PROVEN_CORRUPTION)
        classes = classify(store, m, now, cutoff, ranges, corrupt, finalized=finalized)
        if raw_error and not corrupt:
            classes = ["UNKNOWN_FAIL_CLOSED"]
        size = sum(p.stat().st_size for p in files if p.is_file())
        items.append(
            dict(manifest=m, paths=[str(p) for p in files], classes=classes, bytes=size, raw_error=raw_error)
        )
        for c in classes:
            counts[c] += 1
            sizes[c] += size
    for path in sorted((store.root / "packs").glob("*.zip")):
        managed(store, path, "packs")
        members = [
            json.loads(r[0])
            for r in store.db.execute(
                "SELECT s.payload FROM segment_packs p JOIN segments s ON s.id=p.segment_id WHERE p.archive=? ORDER BY s.id",
                (str(path),),
            )
        ]
        if not members:
            continue  # Uncommitted archive handled by conservative orphan grace below.
        health = archive_health(store, path, members, run_id)
        member_rows = []
        for m in members:
            classes = (
                ["UNKNOWN_FAIL_CLOSED"]
                if m["id"] in health.get("unknown_segment_ids", [])
                else classify(
                    store,
                    m,
                    now,
                    cutoff,
                    ranges,
                    m["id"] in health["corrupt_segment_ids"]
                    and m["id"] not in health["repairable_segment_ids"],
                    finalized=finalized,
                )
            )
            member_rows.append(dict(manifest=m, classes=classes))
            counts.update(classes)
            for c in classes:
                sizes[c] += health.get("member_bytes", {}).get(m["id"], 0)
        dead = [m for m in member_rows if deletable(m["classes"])]
        retain = [m for m in member_rows if not deletable(m["classes"])]
        protected = [c for m in retain for c in m["classes"]]
        health.update(
            protected_reason=sorted(set(protected)),
            disposition="CORRUPT_BUT_PROTECTED"
            if health["corrupt_segment_ids"] and retain
            else "ELIGIBLE_FOR_RETIREMENT"
            if not retain
            else "RETAIN_OR_REPACK",
        )
        save_health(store, health)
        packs.append(
            dict(
                path=str(path),
                bytes=path.stat().st_size,
                members=member_rows,
                health=health,
                dead=len(dead),
                retained=len(retain),
                repackable=bool(
                    dead
                    and retain
                    and all(
                        m["manifest"]["id"] in health["valid_segment_ids"] + health["repairable_segment_ids"]
                        for m in retain
                    )
                ),
            )
        )
    # Any registered original (including a packed/tombstoned one) is referenced.
    for path in (store.root / "segments").glob("*"):
        if not path.is_file():
            continue
        ident = path.name.removesuffix(".jsonl.gz").removesuffix(".manifest.json").removesuffix(".parquet")
        if store.db.execute("SELECT 1 FROM segments WHERE id=?", (ident,)).fetchone():
            referenced.add(str(path))
    orphans = []
    grace = 3_600_000
    for directory in ("segments", "packs"):
        for path in (store.root / directory).glob("*"):
            if path.is_symlink() or not path.is_file():
                continue
            if path.resolve().parent != (store.root / directory).resolve():
                continue
            if str(path) in active_batches:
                continue
            if directory == "segments" and str(path) in referenced:
                continue
            if (
                directory == "packs"
                and store.db.execute(
                    "SELECT 1 FROM segment_packs WHERE archive=? LIMIT 1", (str(path),)
                ).fetchone()
            ):
                continue
            if now - int(path.stat().st_mtime * 1000) <= grace:
                continue
            # Exclusive global writer/maintenance lease at execution prevents publishing this file mid-cleanup.
            orphans.append(dict(path=str(path), directory=directory, bytes=path.stat().st_size))
    planned = (
        sum(x["bytes"] for x in items if deletable(x["classes"]))
        + sum(x["bytes"] for x in packs if not x["retained"])
        + sum(x["bytes"] for x in orphans)
    )
    return dict(
        run_id=run_id,
        at_ms=now,
        bytes_before=usage,
        budget_bytes=budget,
        cutoff_ms=cutoff,
        items=items,
        packs=packs,
        orphans=orphans,
        class_counts=dict(counts),
        class_bytes=dict(sizes),
        planned_reclaimed_bytes=planned,
        protected_bytes=sum(x["bytes"] for x in items if not deletable(x["classes"]))
        + sum(x["bytes"] for x in packs if x["retained"]),
        unknown_bytes=sum(x["bytes"] for x in items if "UNKNOWN_FAIL_CLOSED" in x["classes"])
        + sum(x["bytes"] for x in packs if any("UNKNOWN_FAIL_CLOSED" in m["classes"] for m in x["members"])),
        mixed_packs_repackable=sum(x["repackable"] for x in packs),
        mixed_pack_dead_bytes=sum(
            x["health"].get("member_bytes", {}).get(m["manifest"]["id"], 0)
            for x in packs
            if x["repackable"]
            for m in x["members"]
            if deletable(m["classes"])
        ),
        finalized_intervals=finalized,
        force=force,
        max_reclaim_bytes=max_reclaim_bytes,
    )


def inventory_status(store, plan):
    """Physical recording inventory; ML readiness is a separate cached unit."""
    counts = plan["class_counts"]
    names = {
        "protected_active_segments": "PROTECTED_ACTIVE_SETUP",
        "protected_unresolved_ml_segments": "PROTECTED_UNRESOLVED_PRIMARY_ML",
        "protected_lease_segments": "PROTECTED_REPLAY_LEASE",
        "protected_recent_segments": "PROTECTED_RECENT_FLOOR",
        "deletable_complete_segments": "DELETABLE_COMPLETE_LABEL_DURABLE",
        "deletable_incomplete_segments": "DELETABLE_FINALIZED_INCOMPLETE",
        "deletable_no_consumer_segments": "DELETABLE_NO_ML_CONSUMER",
        "deletable_corrupt_segments": "DELETABLE_CORRUPT_UNRECOVERABLE",
        "unknown_fail_closed_segments": "UNKNOWN_FAIL_CLOSED",
    }
    packs = plan["packs"]
    readiness = store.get("ml_trainability_cache", {})
    sources = readiness.get("by_source", {})
    health_rows = [json.loads(r[0]) for r in store.db.execute("SELECT payload FROM archive_health")]
    corrupt = {ident for p in packs for ident in p["health"]["corrupt_segment_ids"]}
    return {
        **{key: counts.get(c, 0) for key, c in names.items()},
        "class_bytes": plan["class_bytes"],
        "healthy_raw_segments": sum(not x.get("raw_error") for x in plan["items"])
        + sum(len(p["health"]["valid_segment_ids"]) for p in packs),
        "corrupt_raw_segments": len(corrupt),
        "corrupt_segments": len(corrupt),
        "unreadable_packs": sum(p["health"]["status"] == "UNREADABLE" for p in packs),
        "unreadable_pack_count": sum(bool(p["health"]["errors"]) for p in packs),
        "unreadable_pack_bytes": sum(p["bytes"] for p in packs if p["health"]["errors"]),
        "corrupt_protected": sum(
            m["manifest"]["id"] in corrupt and not deletable(m["classes"])
            for p in packs
            for m in p["members"]
        ),
        "corrupt_deletable": counts.get("DELETABLE_CORRUPT_UNRECOVERABLE", 0),
        "corrupt_retired": sum(h["status"] == "RETIRED_CORRUPT" for h in health_rows),
        "unresolved_corruption_diagnostics": sum(bool(p["health"]["errors"]) for p in packs),
        "deletable_orphan_bytes": sum(x["bytes"] for x in plan["orphans"]),
        "mixed_packs_repackable": plan["mixed_packs_repackable"],
        "mixed_pack_dead_bytes": plan["mixed_pack_dead_bytes"],
        "ml_primary_trainable_by_source": {s: r["trainable_current_schema"] for s, r in sources.items()},
        "ml_primary_trainable_total_information_only": sum(
            r["trainable_current_schema"] for r in sources.values()
        ),
        "ml_sequence_ready_by_source": {s: r["sequence_ready_16"] for s, r in sources.items()},
        "ml_sequence_ready_total_information_only": sum(r["sequence_ready_16"] for r in sources.values()),
        "ml_readiness_asof_ms": readiness.get("at_ms"),
        "ml_counts_state": "CACHED_CANONICAL" if sources else "AWAITING_WORKER_SUMMARY",
        "primary_threshold_policy": "Source-specific; raw files are not training rows; totals cannot be pooled",
    }


def tombstone(store, m, classes, run_id, archive=None, health=None):
    row = dict(
        at_ms=now_ms(),
        segment_id=m["id"],
        sha256=m["sha256"],
        original_start_ms=m["min_receipt_ms"],
        original_end_ms=m["max_receipt_ms"],
        disposition="RAW_RELEASED",
        reason_code=classes[0],
        reason="durable outcomes; no unresolved range",
        archive=archive,
        archive_sha256=(health or {}).get("archive_sha256"),
        original_manifest=m,
        training_dependency_at_deletion=False,
        unresolved_dependency_at_deletion=False,
        durable_label_state=classes[0],
        cleanup_run_id=run_id,
    )
    if classes[0] == "DELETABLE_CORRUPT_UNRECOVERABLE":
        row["reason"] = "Verified unrecoverable corruption; no active/unresolved/leased consumer"
    store.db.execute("INSERT OR IGNORE INTO kv VALUES(?,?)", ("pruned_segment:" + m["id"], json.dumps(row)))


def replacement(store, path, retained, health, leases):
    target = store.root / "packs" / (uuid.uuid4().hex + ".zip")
    batch = "writer_batch_v212:replacement:" + target.stem
    store.put(batch, dict(paths=[str(target)], at_ms=now_ms()))
    source = None
    try:
        source = zipfile.ZipFile(path)
    except zipfile.BadZipFile:
        pass
    try:
        expected = {}
        with zipfile.ZipFile(target, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as out:
            for m in retained:
                for lease in leases:
                    lease.heartbeat()
                if m["id"] in health["valid_segment_ids"]:
                    names = [Path(m["raw"]).name, m["id"] + ".manifest.json"]
                    if m.get("parquet") and Path(m["parquet"]).name in source.namelist():
                        names.append(Path(m["parquet"]).name)
                    files = {name: source.read(name) for name in names}
                else:
                    files = originals(store, m)
                if not files:
                    raise ValueError("Replacement originals unavailable")
                verify(files[Path(m["raw"]).name], m, json.loads(files[m["id"] + ".manifest.json"]))
                for name, data in files.items():
                    out.writestr(name, data)
                    expected[name] = hashlib.sha256(data).hexdigest()
        with target.open("r+b") as stream:
            os.fsync(stream.fileno())
        with zipfile.ZipFile(target) as out:
            for name, sha in expected.items():
                if hashlib.sha256(out.read(name)).hexdigest() != sha:
                    raise ValueError("Replacement checksum mismatch")
        for lease in leases:
            lease.heartbeat(force=True)
        return target
    finally:
        if source:
            source.close()
        # The exclusive member claims remain held until the mapping transaction ends.
        with store.db:
            store.db.execute("DELETE FROM kv WHERE key=?", (batch,))


def execute(store, settings, plan, dry_run=False):
    from .leases import RangeLease
    from .retention import protection_ranges

    if dry_run:
        return dict(
            status="dry-run",
            bytes_before=plan["bytes_before"],
            bytes_freed=plan["planned_reclaimed_bytes"],
            segments=[x["manifest"]["id"] for x in plan["items"] if deletable(x["classes"])],
            archives=[x["path"] for x in plan["packs"] if not x["retained"]],
            plan=plan,
        )
    if store.get("cleanup_hold_v212"):
        return dict(status="maintenance hold", freed_bytes=0)
    if store.get("cleanup_verification_v212", {}).get("status") == "FAILED":
        raise RuntimeError("Previous cleanup verification FAILED; cleanup remains stopped")
    asof = now_ms()
    before = ml_inventory(store, asof, artifacts=True)
    directory = store.root / "ml" / "maintenance"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / (plan["run_id"].replace(":", "-") + "-pre.json")).write_text(
        json.dumps(before, sort_keys=True)
    )
    state = dict(
        status="pruning",
        at_ms=asof,
        run_id=plan["run_id"],
        freed_bytes=0,
        freed_by_class={},
        deleted_segments=0,
        mixed_packs_repacked=0,
        corrupt_packs_retired=0,
        skipped_unverifiable=0,
    )
    old = store.get("recording_retention", {})
    store.put("recording_retention", old | state)

    def release(size, category):
        state["freed_bytes"] += size
        state["freed_by_class"][category] = state["freed_by_class"].get(category, 0) + size

    def row_digest():
        h = hashlib.sha256()
        for table, query in (
            ("snapshots", "SELECT id FROM ml_snapshots WHERE decision_ms<=? ORDER BY id"),
            (
                "labels",
                "SELECT snapshot_id,policy FROM ml_labels WHERE available_ms<=? ORDER BY snapshot_id,policy",
            ),
        ):
            h.update(table.encode())
            for row in store.db.execute(query, (asof,)):
                h.update(json.dumps(tuple(row)).encode())
        return h.hexdigest()

    rows_before = row_digest()

    def done():
        if rows_before != row_digest():
            raise RuntimeError("Cleanup durable ML row identities changed; stopped")
        maximum = plan.get("max_reclaim_bytes")
        return maximum is not None and state["freed_bytes"] >= maximum

    try:
        # Orphans require a global lease: no active recorder publication or replay may overlap.
        if plan["orphans"]:
            try:
                with RangeLease(store, 0, now_ms() + 300000, "Uncommitted recording residue", exclusive=True):
                    for item in plan["orphans"]:
                        path = managed(store, item["path"], item["directory"])
                        if path.is_file() and now_ms() - int(path.stat().st_mtime * 1000) > 3600000:
                            if any(
                                str(path) in json.loads(r[0]).get("paths", [])
                                for r in store.db.execute(
                                    "SELECT payload FROM kv WHERE key LIKE 'writer_batch_v212:%'"
                                )
                            ):
                                continue
                            if item["directory"] == "segments":
                                ident = (
                                    path.name.removesuffix(".jsonl.gz")
                                    .removesuffix(".manifest.json")
                                    .removesuffix(".parquet")
                                )
                                if store.db.execute("SELECT 1 FROM segments WHERE id=?", (ident,)).fetchone():
                                    continue
                            if (
                                item["directory"] == "packs"
                                and store.db.execute(
                                    "SELECT 1 FROM segment_packs WHERE archive=? LIMIT 1", (str(path),)
                                ).fetchone()
                            ):
                                continue
                            path.unlink()
                            release(item["bytes"], "orphan/temp")
                        if done():
                            break
            except BlockingIOError:
                pass
        for priority in range(6):
            for pack in plan["packs"]:
                pack_priority = (
                    5
                    if pack["retained"]
                    else min((cleanup_priority(m["classes"]) for m in pack["members"]), default=6)
                )
                if pack_priority != priority:
                    continue
                if done():
                    break
                path = managed(store, pack["path"], "packs")
                health = pack["health"]
                if pack["retained"] and not pack["repackable"] and not health.get("repairable_segment_ids"):
                    continue
                if not pack["dead"] and not health.get("repairable_segment_ids"):
                    continue
                try:
                    with ExitStack() as stack:
                        leases = [
                            stack.enter_context(
                                RangeLease(
                                    store,
                                    m["manifest"]["min_receipt_ms"],
                                    m["manifest"]["max_receipt_ms"],
                                    "Verified pack replacement/retirement",
                                    exclusive=True,
                                )
                            )
                            for m in pack["members"]
                        ]
                        # Claims exclude competing readers. Recheck active/unresolved consumers excluding our claims.
                        own = {lease.id for lease in leases}
                        rows = store.db.execute(
                            "SELECT id,start_ms,end_ms FROM storage_leases WHERE expires_ms>?", (now_ms(),)
                        ).fetchall()
                        ranges = [
                            r
                            for r in protection_ranges(store, now_ms())
                            if r[2] != "replay/maintenance lease"
                        ] + [(r[1], r[2], "replay/maintenance lease") for r in rows if r[0] not in own]
                        current = [
                            (
                                m["manifest"],
                                ["UNKNOWN_FAIL_CLOSED"]
                                if m["manifest"]["id"] in health.get("unknown_segment_ids", [])
                                else classify(
                                    store,
                                    m["manifest"],
                                    now_ms(),
                                    plan["cutoff_ms"],
                                    ranges,
                                    m["manifest"]["id"] in health["corrupt_segment_ids"]
                                    and m["manifest"]["id"] not in health["repairable_segment_ids"],
                                    finalized=plan["finalized_intervals"],
                                ),
                            )
                            for m in pack["members"]
                        ]
                        dead = [(m, c) for m, c in current if deletable(c)]
                        retain = [m for m, c in current if not deletable(c)]
                        if not dead and not health.get("repairable_segment_ids"):
                            continue
                        if any(
                            m["id"] not in health["valid_segment_ids"] + health["repairable_segment_ids"]
                            for m in retain
                        ):
                            continue
                        # All originals are unmodified until replacement verification and mapping commit.
                        target = replacement(store, path, retain, health, leases) if retain else None
                        with store.db:
                            if target:
                                for m in retain:
                                    store.db.execute(
                                        "UPDATE segment_packs SET archive=? WHERE segment_id=? AND archive=?",
                                        (str(target), m["id"], str(path)),
                                    )
                            for m, c in dead:
                                tombstone(store, m, c, plan["run_id"], str(path), health)
                                store.db.execute(
                                    "DELETE FROM segment_packs WHERE segment_id=? AND archive=?",
                                    (m["id"], str(path)),
                                )
                            # Durable intent and mappings commit together before filesystem removal.
                            health.update(
                                disposition="RETIREMENT_COMMITTED",
                                replacement_archive=str(target) if target else None,
                            )
                            store.db.execute(
                                "INSERT OR REPLACE INTO archive_health VALUES(?,?)",
                                (str(path), json.dumps(health)),
                            )
                        old_size = path.stat().st_size
                        new_size = target.stat().st_size if target else 0
                        path.unlink()
                        remaining = max(0, old_size - new_size)
                        if target:
                            release(remaining, "mixed-pack reclamation")
                        else:
                            for m, c in dead:
                                size = min(remaining, health.get("member_bytes", {}).get(m["id"], 0))
                                release(size, c[0])
                                remaining -= size
                            # ZIP overhead/unmapped dead members have no remaining consumer.
                            release(
                                remaining,
                                "corrupt/unreadable"
                                if not health["valid_segment_ids"] and health["errors"]
                                else "no-consumer raw",
                            )
                        state["deleted_segments"] += len(dead)
                        state["mixed_packs_repacked"] += bool(target and dead)
                        state["corrupt_packs_retired"] += bool(health["errors"])
                        health.update(
                            disposition="REBUILT"
                            if target
                            else "RETIRED_CORRUPT"
                            if health["errors"]
                            else "RETIRED_MATERIALIZED",
                            status="RETIRED_CORRUPT" if health["errors"] else health["status"],
                            deleted_ms=now_ms(),
                            replacement_archive=str(target) if target else None,
                            bytes_reclaimed=max(0, old_size - new_size),
                        )
                        save_health(store, health)
                except BlockingIOError:
                    continue
                except Exception as exc:
                    record_failure(
                        store,
                        exc,
                        scan_id=plan["run_id"],
                        phase="cleanup",
                        operation="pack transaction",
                        archive=str(path),
                    )
                    state["skipped_unverifiable"] += 1
                    if isinstance(exc, RuntimeError):
                        raise
            for item in sorted(plan["items"], key=lambda x: x["manifest"]["max_receipt_ms"]):
                if cleanup_priority(item["classes"]) != priority:
                    continue
                if done():
                    break
                m = item["manifest"]
                if not deletable(item["classes"]):
                    continue
                try:
                    with RangeLease(
                        store,
                        m["min_receipt_ms"],
                        m["max_receipt_ms"],
                        "Materialized raw release",
                        exclusive=True,
                    ) as lease:
                        ranges = [
                            r
                            for r in protection_ranges(store, now_ms())
                            if r[2] != "replay/maintenance lease"
                        ]
                        classes = classify(
                            store,
                            m,
                            now_ms(),
                            plan["cutoff_ms"],
                            ranges,
                            "DELETABLE_CORRUPT_UNRECOVERABLE" in item["classes"],
                            finalized=plan["finalized_intervals"],
                        )
                        if not deletable(classes):
                            continue
                        if not store.get("pruned_segment:" + m["id"]):
                            try:
                                if not originals(store, m):
                                    raise ValueError("Originals unverified")
                            except Exception as exc:
                                if (
                                    classes != ["DELETABLE_CORRUPT_UNRECOVERABLE"]
                                    or cause_code(exc) not in PROVEN_CORRUPTION
                                ):
                                    raise
                            with store.db:
                                tombstone(store, m, classes, plan["run_id"])
                            state["deleted_segments"] += 1
                        lease.heartbeat(force=True)
                        for value in item["paths"]:
                            path = managed(store, value, "segments")
                            if path.is_file():
                                size = path.stat().st_size
                                path.unlink()
                                release(size, classes[0])
                except BlockingIOError:
                    continue
                except Exception as exc:
                    record_failure(
                        store,
                        exc,
                        scan_id=plan["run_id"],
                        phase="cleanup",
                        operation="raw release",
                        segment_id=m["id"],
                    )
                    state["skipped_unverifiable"] += 1
                    if isinstance(exc, RuntimeError):
                        raise
        after = ml_inventory(store, asof, artifacts=True)
        verify_ml(before, after)
        (directory / (plan["run_id"].replace(":", "-") + "-post.json")).write_text(
            json.dumps(after, sort_keys=True)
        )
        store.put(
            "cleanup_verification_v212",
            dict(status="PASSED", run_id=plan["run_id"], before=before, after=after),
        )
        state.update(
            status="pruned",
            bytes_before=plan["bytes_before"],
            bytes_after=directory_bytes(store.root),
            total_freed_bytes=old.get("total_freed_bytes", 0) + state["freed_bytes"],
            through_ms=old.get("through_ms", 0),
            pruned_complete_outcomes=old.get("pruned_complete_outcomes", 0),
            pruned_incomplete_outcomes=old.get("pruned_incomplete_outcomes", 0),
        )
        store.put("recording_retention", old | state)
        store.put(
            "recording_retention_errors",
            dict(
                at_ms=now_ms(),
                items=[],
                total=state["skipped_unverifiable"],
                details="failure_causes_v212 and archive_health",
            ),
        )
        return old | state
    except Exception as exc:
        store.put(
            "cleanup_verification_v212", dict(status="FAILED", run_id=plan["run_id"], reason=str(exc)[:500])
        )
        raise
