"""Bound raw recordings while retaining immutable ML and segment audit records."""

import json
from pathlib import Path

from .storage import now_ms


def prune_recordings(store, settings, at_ms=None):
    if not settings.recording_retention_enabled:
        return {"status": "disabled"}
    now = at_ms or now_ms()
    monitor = store.get("ml_monitor", {})
    if monitor.get("status") != "observed":
        return {"status": "waiting for successful outcome processing"}
    # Keep active four-hour outcomes plus two hours of collection/worker margin.
    cutoff = min(now - 6 * 3_600_000, monitor["at_ms"] - 4 * 3_600_000)
    root = (store.root / "segments").resolve()
    state = store.get("recording_retention", {})
    through = state.get("through_ms", 0)
    usage = sum(p.stat().st_size for p in store.root.rglob("*") if p.is_file())
    budget = settings.max_storage_gb * 1e9
    manifests = [json.loads(r[0]) for r in store.db.execute("SELECT payload FROM segments")]
    manifests.sort(key=lambda m: m["max_receipt_ms"])
    selected, planned = [], 0
    pressure = usage >= budget * 0.8
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
        selected.append((m, paths))
        planned += size
    if not selected:
        return {"status": "within budget" if not pressure else "active history protected", "bytes": usage}
    through = max(through, max(m["max_receipt_ms"] for m, _ in selected))
    # Publish the replay boundary before deletion. A crash resumes the same prefix;
    # replay cannot mistake partially removed history for complete evidence.
    state.update(through_ms=through, at_ms=now, status="pruning", bytes_before=usage)
    store.put("recording_retention", state)
    freed = 0
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
