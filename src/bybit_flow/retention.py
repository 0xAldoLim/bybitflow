"""Bound raw recordings while retaining immutable ML and segment audit records."""

import json
from pathlib import Path

from .storage import directory_bytes, now_ms


def protection_cutoff(store, now, cutoff):
    starts = []
    for (payload,) in store.db.execute(
        "SELECT payload FROM signals WHERE state NOT IN ('INVALIDATED','EXPIRED','RESOLVED')"
    ):
        s = json.loads(payload)
        starts.append(s["created_ms"] - 900_000)
    through = store.get("recording_retention", {}).get("through_ms", 0)
    row = store.db.execute(
        "SELECT min(s.decision_ms) FROM ml_snapshots s WHERE s.stage='decision' AND s.decision_ms>? AND NOT EXISTS (SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id)",
        (through,),
    ).fetchone()
    if row and row[0] is not None:
        starts.append(row[0] - 900_000)
    for row in store.db.execute("SELECT start_ms FROM storage_leases WHERE expires_ms>?", (now,)):
        starts.append(row[0])
    return min([cutoff] + starts)


def storage_status(store, settings):
    now = now_ms()
    used = directory_bytes(store.root)
    monitor = store.get("ml_monitor", {})
    cutoff = protection_cutoff(store, now, min(now - 6 * 3_600_000, monitor.get("at_ms", 0) - 4 * 3_600_000))
    budget = settings.max_storage_gb * 1e9
    row = store.db.execute(
        "SELECT count(*),coalesce(sum(json_extract(payload,'$.rows')),0) FROM segments WHERE at_ms>?",
        (now - 3_600_000,),
    ).fetchone()
    raw_bytes = sum(directory_bytes(store.root / directory) for directory in ("segments", "packs"))
    result = dict(
        at_ms=now,
        used_bytes=used,
        budget_bytes=budget,
        usage_fraction=used / budget,
        pressure="aggressive safe cleanup"
        if used / budget >= 0.95
        else "safe cleanup"
        if used / budget >= 0.85
        else "compact"
        if used / budget >= 0.7
        else "normal",
        protected_before_ms=cutoff,
        raw_bytes=raw_bytes,
        permanent_bytes=used - raw_bytes,
        segments_last_hour=row[0],
        rows_last_hour=row[1],
        rows_per_segment=row[1] / row[0] if row[0] else None,
        retention=store.get("recording_retention", {}),
        leases=store.db.execute("SELECT count(*) FROM storage_leases WHERE expires_ms>?", (now,)).fetchone()[
            0
        ],
        policy="Preserve primary evidence, active plans, unresolved labels, replay leases and compact afterlife records",
    )
    store.put("storage_status", result)
    return result


def prune_recordings(store, settings, at_ms=None, dry_run=False):
    if not settings.recording_retention_enabled:
        return {"status": "disabled"}
    now = at_ms or now_ms()
    monitor = store.get("ml_monitor", {})
    if monitor.get("status") != "observed":
        return {"status": "waiting for successful outcome processing"}
    # Keep active four-hour outcomes plus two hours of collection/worker margin.
    cutoff = protection_cutoff(store, now, min(now - 6 * 3_600_000, monitor["at_ms"] - 4 * 3_600_000))
    root = (store.root / "segments").resolve()
    state = store.get("recording_retention", {})
    through = state.get("through_ms", 0)
    usage = directory_bytes(store.root)
    budget = settings.max_storage_gb * 1e9
    manifests = [json.loads(r[0]) for r in store.db.execute("SELECT payload FROM segments")]
    manifests.sort(key=lambda m: m["max_receipt_ms"])
    selected, planned = [], 0
    pressure = usage >= budget * 0.85
    for m in manifests:
        end = m["max_receipt_ms"]
        resume = end <= through
        if not resume and (not pressure or end > cutoff or usage - planned <= budget * 0.6):
            continue
        paths = [Path(m["raw"]), Path(m["parquet"])]
        paths.append(paths[0].with_name(paths[0].name.removesuffix(".jsonl.gz") + ".manifest.json"))
        # Never remove files outside the exact managed segment directory, including symlinks.
        for path in paths:
            if path.is_symlink() or path.resolve().parent != root:
                raise ValueError("Retention path outside managed segment directory")
        size = sum(p.stat().st_size for p in paths if p.exists())
        if not size:
            continue
        selected.append((m, paths))
        planned += size
    packs = []
    for archive, end in store.db.execute(
        "SELECT p.archive,max(json_extract(s.payload,'$.max_receipt_ms')) FROM segment_packs p JOIN segments s ON s.id=p.segment_id GROUP BY p.archive"
    ):
        p = Path(archive)
        if p.is_symlink() or p.resolve().parent != (store.root / "packs").resolve():
            raise ValueError("Retention path outside managed packs")
        if pressure and end <= cutoff and p.exists() and usage - planned > budget * 0.6:
            packs.append((p, end))
            planned += p.stat().st_size
    if dry_run:
        return dict(
            status="dry-run",
            bytes_before=usage,
            bytes_freed=planned,
            segments=[m["id"] for m, _ in selected],
            archives=[str(p) for p, _ in packs],
            cutoff_ms=cutoff,
            reason="Old redundant raw evidence beyond active, unresolved-label and replay protection; permanent records remain",
        )
    if not selected and not packs:
        return {
            "status": "within budget" if not pressure else "active history protected",
            "bytes": usage,
            "freed_bytes": 0,
        }
    through = max([through] + [m["max_receipt_ms"] for m, _ in selected] + [end for _, end in packs])
    # Publish the replay boundary before deletion. A crash resumes the same prefix;
    # replay cannot mistake partially removed history for complete evidence.
    state.update(through_ms=through, at_ms=now, status="pruning", bytes_before=usage)
    store.put("recording_retention", state)
    freed = 0
    for path, _ in packs:
        size = path.stat().st_size
        path.unlink()
        freed += size
    for _, paths in selected:
        for path in paths:
            if path.exists():
                size = path.stat().st_size
                path.unlink()
                freed += size
    state.update(
        status="pruned",
        bytes_after=usage - freed,
        freed_bytes=freed,
        total_freed_bytes=state.get("total_freed_bytes", 0) + freed,
        preserved="database, feature snapshots, outcome labels, datasets, models and segment hashes",
    )
    store.put("recording_retention", state)
    return state
