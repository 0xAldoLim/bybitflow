"""Bound raw recordings while retaining immutable ML and segment audit records."""

import hashlib
import json
import zipfile
from pathlib import Path

from .storage import directory_bytes, now_ms


def replayable_boundary(store, oldest=True):
    """Find a verified retained segment, skipping tombstones and damaged evidence."""
    from .packing import manifest_for, raw_bytes

    field = "min_event_ms" if oldest else "max_event_ms"
    manifests = {}
    for path in (store.root / "segments").glob("*.jsonl.gz"):
        ident = path.name.removesuffix(".jsonl.gz")
        row = store.db.execute(
            "SELECT payload FROM segments WHERE id=? AND NOT EXISTS "
            "(SELECT 1 FROM kv WHERE key='pruned_segment:'||segments.id)",
            (ident,),
        ).fetchone()
        if row:
            manifests[ident] = json.loads(row[0])
    for archive in (store.root / "packs").glob("*.zip"):
        for ident, payload in store.db.execute(
            "SELECT s.id,s.payload FROM segment_packs p JOIN segments s ON s.id=p.segment_id "
            "WHERE p.archive=? AND NOT EXISTS "
            "(SELECT 1 FROM kv WHERE key='pruned_segment:'||s.id)",
            (str(archive),),
        ):
            manifests[ident] = json.loads(payload)
    for manifest in sorted(manifests.values(), key=lambda m: m[field], reverse=not oldest):
        try:
            raw = raw_bytes(manifest["raw"])
            if (
                hashlib.sha256(raw).hexdigest() == manifest["sha256"]
                and manifest_for(manifest["raw"]) == manifest
            ):
                return manifest[field]
        except (OSError, ValueError, KeyError, zipfile.BadZipFile):
            continue
    return None


def byte_attribution_sample(store, now, cutoff, limit=200):
    """Bounded lower-bound byte estimates; categories can overlap."""
    ranges = protection_ranges(store, now)
    totals = dict(
        protected_active_bytes=0,
        protected_unresolved_ml_bytes=0,
        protected_lease_bytes=0,
        finalized_complete_deletable_bytes=0,
        finalized_incomplete_deletable_bytes=0,
    )
    rows = store.db.execute(
        "SELECT s.payload,p.archive,p.bytes FROM segments s "
        "LEFT JOIN segment_packs p ON p.segment_id=s.id WHERE NOT EXISTS "
        "(SELECT 1 FROM kv k WHERE k.key='pruned_segment:'||s.id) "
        "ORDER BY json_extract(s.payload,'$.max_receipt_ms') DESC LIMIT ?",
        (limit,),
    ).fetchall()
    for payload, archive, packed_bytes in rows:
        m = json.loads(payload)
        raw = Path(m["raw"])
        paths = [raw, raw.with_name(m["id"] + ".manifest.json")]
        if m.get("parquet"):
            paths.append(Path(m["parquet"]))
        size = sum(path.stat().st_size for path in paths if path.is_file())
        if not size and archive and Path(archive).is_file():
            size = packed_bytes  # Original member sizes approximate the packed share.
        if not size:
            continue
        start, end = m["min_receipt_ms"], m["max_receipt_ms"]
        reasons = {reason for a, b, reason in ranges if a <= end and b >= start}
        for reason, key in (
            ("active setup", "protected_active_bytes"),
            ("unresolved primary outcome", "protected_unresolved_ml_bytes"),
            ("replay/maintenance lease", "protected_lease_bytes"),
        ):
            if reason in reasons:
                totals[key] += size
        if reasons or end > cutoff:
            continue
        classifications = {
            r[0]
            for r in store.db.execute(
                "SELECT json_extract(l.payload,'$.classification') FROM ml_snapshots s "
                "JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' "
                "WHERE s.stage='decision' AND s.decision_ms BETWEEN ? AND ?",
                (start, end),
            )
        }
        if "incomplete" in classifications:
            totals["finalized_incomplete_deletable_bytes"] += size
        elif classifications:
            totals["finalized_complete_deletable_bytes"] += size
    return totals


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
    oldest = replayable_boundary(store)
    newest = replayable_boundary(store, oldest=False)
    retention = store.get("recording_retention", {})
    floor = 15 if used / budget >= 0.85 else getattr(settings, "ml_raw_retention_minutes", 60)
    estimated = byte_attribution_sample(store, now, now - floor * 60_000)
    result = dict(
        at_ms=now,
        retention_enabled=settings.recording_retention_enabled,
        usage_percent=round(100 * used / budget, 2),
        total_freed_bytes=retention.get("total_freed_bytes", 0),
        raw_retention_floor_minutes=getattr(settings, "ml_raw_retention_minutes", 60),
        pressure_raw_retention_floor_minutes=15,
        oldest_replayable_event_ms=oldest,
        newest_replayable_event_ms=newest,
        **estimated,
        byte_attribution="bounded latest-200-segment sample; packed sizes estimated; categories can overlap",
        pruned_complete_outcomes=retention.get("pruned_complete_outcomes", 0),
        pruned_incomplete_outcomes=retention.get("pruned_incomplete_outcomes", 0),
        skipped_unverifiable_count=retention.get("skipped_unverifiable", 0),
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
        retention=retention,
        leases=store.db.execute("SELECT count(*) FROM storage_leases WHERE expires_ms>?", (now,)).fetchone()[
            0
        ],
        policy="Preserve primary evidence, active plans, unresolved labels, replay leases and compact afterlife records",
    )
    deletable = prune_recordings(store, settings, dry_run=True).get("bytes_freed", 0)
    result.update(
        deletable_bytes=deletable,
        scheduled_deletable_bytes=deletable,
        protected_bytes=max(0, used - deletable) if used >= budget * 0.85 else None,
        eligibility_measurement="scheduled cleanup under current pressure; null protection means not measured",
    )
    store.put("storage_status", result)
    return result


def prune_recordings(store, settings, at_ms=None, dry_run=False, force=False, max_reclaim_bytes=None):
    if not settings.recording_retention_enabled:
        return {"status": "disabled"}
    now = at_ms or now_ms()
    usage = directory_bytes(store.root)
    budget = settings.max_storage_gb * 1e9
    if usage >= budget * 0.85:
        max_reclaim_bytes = None  # Emergency cleanup must recover enough headroom.
    if (
        not force
        and usage < budget * 0.85
        and store.get("recording_retention", {}).get("status") != "pruning"
    ):
        return dict(
            status="dry-run" if dry_run else "within budget",
            bytes_before=usage,
            bytes_freed=0,
            freed_bytes=0,
            segments=[],
            archives=[],
            reason="No deletion scheduled below the pressure threshold",
        )
    # Eligibility is per evidence interval, never gated by global ML health.
    # Routine ML cleanup retains a configurable recent floor. Active setups,
    # unresolved outcomes and readers still protect their exact ranges below.
    cutoff = now - (
        15 * 60_000
        if usage >= budget * 0.85
        else getattr(settings, "ml_raw_retention_minutes", 60) * 60_000
        if force
        else 6 * 3_600_000
    )
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
    pressure = usage >= budget * 0.85 or force
    for m in manifests:
        if max_reclaim_bytes is not None and planned >= max_reclaim_bytes:
            break
        end = m["max_receipt_ms"]
        resume = m.get("id") in pruned or end <= through
        if not resume and (not pressure or end > cutoff or (not force and usage - planned <= budget * 0.6)):
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
        if max_reclaim_bytes is not None and planned >= max_reclaim_bytes:
            break
        p = Path(archive)
        if p.is_symlink() or p.resolve().parent != (store.root / "packs").resolve():
            raise ValueError("Retention path outside managed packs")
        if (
            pressure
            and end <= cutoff
            and p.exists()
            and (force or usage - planned > budget * 0.6)
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
    skipped = []
    newly_pruned = {}

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
                    if not store.get("pruned_segment:" + m["id"]):
                        newly_pruned[m["id"]] = m
                    store.put(
                        "pruned_segment:" + m["id"],
                        dict(at_ms=now, sha256=m["sha256"], reason="durable outcomes; no unresolved range"),
                    )
                size = path.stat().st_size
                path.unlink()
                freed += size
        except BlockingIOError:
            continue
        except (zipfile.BadZipFile, KeyError, FileNotFoundError, ValueError) as exc:
            # A damaged archive is evidence of a recording gap. Keep its bytes and
            # manifest for diagnosis; it must not block unrelated safe cleanup.
            skipped.append(dict(path=str(path), error_type=type(exc).__name__))
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
                    newly_pruned[m["id"]] = m
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
        except (zipfile.BadZipFile, KeyError, FileNotFoundError, ValueError) as exc:
            skipped.append(dict(path=str(paths[0]), error_type=type(exc).__name__))
    outcomes = {}
    for m in newly_pruned.values():
        for snapshot_id, classification, complete in store.db.execute(
            "SELECT s.id,json_extract(l.payload,'$.classification'),"
            "json_extract(l.payload,'$.complete') FROM ml_snapshots s "
            "JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' "
            "WHERE s.stage='decision' AND s.decision_ms BETWEEN ? AND ?",
            (m["min_receipt_ms"], m["max_receipt_ms"]),
        ):
            outcomes[snapshot_id] = (classification, complete)
    state.update(
        status="pruned",
        bytes_after=usage - freed,
        freed_bytes=freed,
        total_freed_bytes=state.get("total_freed_bytes", 0) + freed,
        preserved="database, feature snapshots, outcome labels, datasets, models and segment hashes",
        skipped_unverifiable=len(skipped),
        pruned_complete_outcomes=state.get("pruned_complete_outcomes", 0)
        + sum(bool(complete) for _, complete in outcomes.values()),
        pruned_incomplete_outcomes=state.get("pruned_incomplete_outcomes", 0)
        + sum(kind == "incomplete" for kind, _ in outcomes.values()),
        pruned_outcome_count_note="decision-range estimate since counters were introduced",
    )
    store.put("recording_retention", state)
    if skipped:
        store.put("recording_retention_errors", dict(at_ms=now, items=skipped[:100], total=len(skipped)))
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
