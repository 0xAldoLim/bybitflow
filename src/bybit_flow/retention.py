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
    for row in store.db.execute("SELECT payload FROM observations WHERE status='FOLLOWING_LATE_OUTCOME'"):
        observation = json.loads(row[0])
        # Protect unprocessed observation evidence; durable checkpoints move this forward.
        starts.append(observation.get("cursor_ms", observation.get("terminal_ms", now)) - 900_000)
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
    ranges = protection_ranges(store, now)
    cutoff = min((a for a, _, _ in ranges), default=now - 6 * 3_600_000)
    budget = settings.max_storage_gb * 1e9
    row = store.db.execute(
        "SELECT count(*),coalesce(sum(json_extract(payload,'$.rows')),0) FROM segments WHERE at_ms>?",
        (now - 3_600_000,),
    ).fetchone()
    raw_bytes = sum(directory_bytes(store.root / directory) for directory in ("segments", "packs"))
    result = dict(
        at_ms=now,
        retention_enabled=settings.recording_retention_enabled,
        usage_percent=round(100 * used / budget, 2),
        total_freed_bytes=store.get("recording_retention", {}).get("total_freed_bytes", 0),
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
        raw_bytes=directory_bytes(store.root / "segments"),
        packed_bytes=directory_bytes(store.root / "packs"),
        maintenance=store.get("storage_maintenance", {}),
        last_compaction=store.get("last_compaction", {}),
        stale_leases=store.db.execute(
            "SELECT count(*) FROM storage_leases WHERE expires_ms<=?", (now,)
        ).fetchone()[0],
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
    deletable = prune_recordings(store, settings, dry_run=True).get("bytes_freed", 0)
    result.update(deletable_bytes=deletable, scheduled_deletable_bytes=deletable,
                  protected_bytes=max(0, used - deletable) if used >= budget * .85 else None,
                  eligibility_measurement="scheduled cleanup under current pressure; null protection means not measured")
    store.put("storage_status", result)
    return result


def prune_recordings(store, settings, at_ms=None, dry_run=False):
    if not settings.recording_retention_enabled:
        return {"status": "disabled"}
    now = at_ms or now_ms()
    usage = directory_bytes(store.root)
    budget = settings.max_storage_gb * 1e9
    if usage < budget * .85 and store.get("recording_retention", {}).get("status") != "pruning":
        return dict(status="dry-run" if dry_run else "within budget", bytes_before=usage,
                    bytes_freed=0, freed_bytes=0, segments=[], archives=[],
                    reason="No deletion scheduled below the pressure threshold")
    # Eligibility is per evidence interval, never gated by global ML health.
    cutoff = now - 6 * 3_600_000
    ranges = protection_ranges(store, now)
    from bisect import bisect_right

    merged = []
    for a, b, _ in sorted(ranges):
        if merged and a <= merged[-1][1]:
            merged[-1][1] = max(b, merged[-1][1])
        else:
            merged.append([a, b])
    starts = [r[0] for r in merged]

    def protected(start, end):
        index = bisect_right(starts, end) - 1
        return index >= 0 and merged[index][1] >= start

    pruned = {
        r[0].removeprefix("pruned_segment:")
        for r in store.db.execute("SELECT key FROM kv WHERE key LIKE 'pruned_segment:%'")
    }
    root = (store.root / "segments").resolve()
    state = store.get("recording_retention", {})
    through = state.get("through_ms", 0)
    manifests = [json.loads(r[0]) for r in store.db.execute("SELECT payload FROM segments")]
    manifests.sort(key=lambda m: m["max_receipt_ms"])
    selected, planned = [], 0
    pressure = usage >= budget * 0.85
    for m in manifests:
        end = m["max_receipt_ms"]
        resume = m.get("id") in pruned or end <= through
        if not resume and (not pressure or end > cutoff or usage - planned <= budget * 0.6):
            continue
        if protected(m.get("min_receipt_ms", end), end):
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
    for archive, end, start in store.db.execute(
        "SELECT p.archive,max(json_extract(s.payload,'$.max_receipt_ms')),"
        "min(json_extract(s.payload,'$.min_receipt_ms')) FROM segment_packs p "
        "JOIN segments s ON s.id=p.segment_id GROUP BY p.archive"
    ):
        p = Path(archive)
        if p.is_symlink() or p.resolve().parent != (store.root / "packs").resolve():
            raise ValueError("Retention path outside managed packs")
        if (
            pressure
            and end <= cutoff
            and p.exists()
            and usage - planned > budget * 0.6
            and not protected(start, end)
        ):
            packs.append((p, end, start))
            planned += p.stat().st_size
    if dry_run:
        return dict(
            status="dry-run",
            bytes_before=usage,
            bytes_freed=planned,
            segments=[m["id"] for m, _ in selected],
            archives=[str(p) for p, _, _ in packs],
            cutoff_ms=cutoff,
            reason="Old redundant raw evidence beyond active, unresolved-label and replay protection; permanent records remain",
        )
    if not selected and not packs:
        return {
            "status": "within budget" if not pressure else "active history protected",
            "bytes": usage,
            "freed_bytes": 0,
        }
    import hashlib

    from .leases import RangeLease
    from .packing import manifest_for, raw_bytes

    state.update(at_ms=now, status="pruning", bytes_before=usage)
    store.put("recording_retention", state)
    freed = 0

    def verify(m):
        if hashlib.sha256(raw_bytes(m["raw"])).hexdigest() != m["sha256"] or manifest_for(m["raw"]) != m:
            raise ValueError("Retention evidence integrity mismatch")

    for path, end, start in packs:
        try:
            with RangeLease(store, start, end, "Prune materialized evidence", exclusive=True) as lease:
                members = [
                    json.loads(r[0])
                    for r in store.db.execute(
                        "SELECT s.payload FROM segments s JOIN segment_packs p ON p.segment_id=s.id WHERE p.archive=?",
                        (str(path),),
                    )
                ]
                for m in members:
                    lease.heartbeat()
                    verify(m)
                lease.heartbeat(force=True)
                for m in members:
                    store.put(
                        "pruned_segment:" + m["id"],
                        dict(at_ms=now, sha256=m["sha256"], reason="durable outcomes; no unresolved range"),
                    )
                size = path.stat().st_size
                path.unlink()
                freed += size
        except BlockingIOError:
            continue
    for m, paths in selected:
        try:
            with RangeLease(
                store,
                m.get("min_receipt_ms", m["max_receipt_ms"]),
                m["max_receipt_ms"],
                "Prune materialized evidence",
                exclusive=True,
            ):
                # A published tombstone makes crash recovery and non-prefix pruning explicit.
                if not store.get("pruned_segment:" + m["id"]):
                    verify(m)
                    store.put(
                        "pruned_segment:" + m["id"],
                        dict(at_ms=now, sha256=m["sha256"], reason="durable outcomes; no unresolved range"),
                    )
                for path in paths:
                    if path.exists():
                        size = path.stat().st_size
                        path.unlink()
                        freed += size
        except BlockingIOError:
            continue
    state.update(
        status="pruned",
        bytes_after=usage - freed,
        freed_bytes=freed,
        total_freed_bytes=state.get("total_freed_bytes", 0) + freed,
        preserved="database, feature snapshots, outcome labels, datasets, models and segment hashes",
    )
    store.put("recording_retention", state)
    return state


def protection_ranges(store, now):
    """Protect only primary evidence and current readers, not unrelated history.

    Late observations consume public closed 1m candles (observations.advance),
    not recorder DOM/prints. Their durable checkpoints do not pin raw tape.
    """
    ranges = []
    for row in store.db.execute(
        "SELECT payload FROM signals WHERE state NOT IN ('INVALIDATED','EXPIRED','RESOLVED')"
    ):
        s = json.loads(row[0])
        ranges.append(
            (
                s["created_ms"] - 900_000,
                max(now, s.get("holding_deadline_ms") or s["expires_ms"]),
                "active setup",
            )
        )
    for row in store.db.execute(
        "SELECT s.decision_ms,s.payload FROM ml_snapshots s WHERE s.stage='decision' AND NOT EXISTS "
        "(SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy='prints-v1')"
    ):
        snapshot = json.loads(row[1])
        horizon = snapshot.get("signal", {}).get("expected_hold_max", 240) * 60_000
        ranges.append((row[0] - 900_000, row[0] + horizon + 60_000, "unresolved primary outcome"))
    ranges.extend(
        (r[0], r[1], "replay/maintenance lease")
        for r in store.db.execute("SELECT start_ms,end_ms FROM storage_leases WHERE expires_ms>?", (now,))
    )
    return ranges


def maintain(store, settings):
    """Pressure policy is budget-relative, including controlled 10 GB deployments."""
    from .leases import reap
    from .packing import compact

    reap(store)
    usage = directory_bytes(store.root) / (settings.max_storage_gb * 1e9)
    state = (
        "EMERGENCY_SAFE_MAINTENANCE"
        if usage >= 0.95
        else "PRUNING"
        if usage >= 0.85
        else "COMPACTING"
        if usage >= 0.7
        else "NORMAL"
    )
    result = dict(at_ms=now_ms(), state=state, usage_before=usage)
    if settings.recording_retention_enabled and usage >= 0.7:
        result["compaction"] = compact(store, limit=600 if usage >= 0.85 else 100)
    if settings.recording_retention_enabled and usage >= 0.85:
        result["pruning"] = prune_recordings(store, settings)
    after = directory_bytes(store.root) / (settings.max_storage_gb * 1e9)
    result["usage_after"] = after
    if after >= 0.95:
        result["state"] = "STORAGE_BACKPRESSURE"
    store.put("storage_maintenance", result)
    return result
