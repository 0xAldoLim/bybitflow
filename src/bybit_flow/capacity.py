"""Small local-only outcome/retention worker, independent of optional ML fitting.

Only existing prints-v1 materialization and verified safe retention run here.
No network, training, promotion, synthetic labels or evidence-policy changes.
"""

import json
import sqlite3
import time
from pathlib import Path

from .storage import Store, directory_bytes, now_ms


def manifest_batch(store, after_ms, boundary, limit=16, max_input_bytes=64_000_000):
    """Choose a chronological prefix; never split overlapping/equal receipt spans."""
    rows = store.db.execute(
        "SELECT s.payload,p.bytes FROM segments s LEFT JOIN segment_packs p ON p.segment_id=s.id "
        "WHERE s.at_ms<=? AND json_extract(s.payload,'$.max_receipt_ms')>? "
        "AND NOT EXISTS (SELECT 1 FROM kv WHERE key='pruned_segment:'||s.id) "
        "ORDER BY json_extract(s.payload,'$.min_receipt_ms'),s.id LIMIT ?",
        (boundary, after_ms, max(128, limit + 1) + 1),
    ).fetchall()
    chosen, size, end = [], 0, -1
    for payload, packed_bytes in rows:
        manifest = json.loads(payload)
        if len(chosen) >= limit and manifest["min_receipt_ms"] > end:
            break
        path = Path(manifest["raw"])
        size += path.stat().st_size if path.is_file() else packed_bytes or 0
        if len(chosen) >= 128 or size > max_input_bytes:
            raise ValueError("Recording overlap group or segment exceeds bounded upkeep budget")
        chosen.append(manifest)
        end = max(end, manifest["max_receipt_ms"])
    return chosen, len(chosen) < len(rows), size


def step(settings, store, *, at_ms=None, max_segments=16):
    from .ml.labels import label_recordings
    from .ml.recordings import replay_start, worker_rows
    from .retention import prune_recordings

    now = now_ms() if at_ms is None else at_ms
    used = directory_bytes(store.root)
    previous = store.get("capacity_health", {})
    if used < settings.max_storage_gb * 1e9 * 0.85:
        return dict(at_ms=now, state="NORMAL", used_bytes=used)
    # A healthy regular materializer owns the same work. Do not replay every poll.
    if now - previous.get("last_success_ms", 0) < 60_000:
        return previous | dict(at_ms=now)
    cursor = store.get("primary_materialization", {}).get("cursor_ms")
    start = replay_start(store, cursor)
    batch, more, input_bytes = manifest_batch(store, start, now, max_segments)
    result = dict(at_ms=now, state="MATERIALIZING", segments=len(batch), input_bytes=input_bytes)
    store.put("capacity_health", result)
    # A truncated batch is NOT the end of recorded history. In particular, do not
    # substitute wall time and incorrectly mark later positions incomplete.
    labels = label_recordings(
        store,
        worker_rows(store, after_ms=cursor, manifests=batch),
        settings,
        incremental=True,
        observed_until_ms=None if more else now,
    )
    prune = prune_recordings(store, settings, force=True, max_reclaim_bytes=256_000_000)
    used = directory_bytes(store.root)
    blocked = used >= settings.max_storage_gb * 1e9 and not prune.get("freed_bytes", 0)
    result.update(
        state="PROTECTED_CAPACITY_BLOCKED" if blocked else "CATCHING_UP" if more else "OBSERVED",
        last_success_ms=now_ms(),
        cursor_ms=store.get("primary_materialization", {}).get("cursor_ms"),
        more=more,
        outcomes=len(labels["outcomes"]),
        complete=labels["complete"],
        freed_bytes=prune.get("freed_bytes", 0),
        used_bytes=used,
        reason="Protected evidence exceeds budget; preserve it and report capacity" if blocked else None,
    )
    store.put("capacity_health", result)
    return result


def main():
    from .config import Settings
    from .db_runtime import busy

    settings = Settings()
    while True:
        store = None
        result = dict(at_ms=now_ms(), state="INTERRUPTED")
        try:
            store = Store(settings.data_dir)
            result = step(settings, store)
        except BlockingIOError:
            result = dict(at_ms=now_ms(), state="DEFERRED_PRIMARY_OWNER")
        except sqlite3.OperationalError as exc:
            result = dict(
                at_ms=now_ms(),
                state="DEFERRED_SQLITE_BUSY" if busy(exc) else "ERROR",
                error_type=type(exc).__name__,
            )
        except Exception as exc:
            result = dict(at_ms=now_ms(), state="ERROR", error_type=type(exc).__name__)
        finally:
            if store:
                try:
                    store.put("capacity_health", result)
                except sqlite3.OperationalError:
                    pass  # The log remains available when the diagnostic write is busy.
                store.close()
        print(json.dumps(result), flush=True)
        time.sleep(30)


if __name__ == "__main__":
    main()
