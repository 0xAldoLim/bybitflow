"""Preflight committed recordings; preserve clock-damaged bytes as explicit gaps.

Strict CLI replay still rejects unordered inputs. The unattended worker can move
past clock jumps without sorting events or learning outcomes across missing tape.
Integrity failures remain fatal and preflight finishes before any label is written.
"""

import hashlib
import json
from pathlib import Path

from ..packing import manifest_for, packed_record, raw_bytes, raw_stream
from ..replay import segment_rows
from ..storage import now_ms


def worker_rows(store):
    import uuid

    lease = uuid.uuid4().hex
    with store.db:
        store.db.execute(
            "INSERT INTO storage_leases VALUES(?,?,?,?,?)",
            (
                lease,
                store.get("recording_retention", {}).get("through_ms", 0),
                now_ms(),
                now_ms() + 86_400_000,
                "ML replay in progress",
            ),
        )
    try:
        yield from _worker_rows(store)
    finally:
        with store.db:
            store.db.execute("DELETE FROM storage_leases WHERE id=?", (lease,))


def committed_manifests(store, retained_after):
    """Bound memory without retaining a WAL read cursor across worker writes."""
    last_id, boundary = "", now_ms()
    while True:
        batch = store.db.execute(
            "SELECT id,payload FROM segments WHERE id>? AND at_ms<=? "
            "AND json_extract(payload,'$.max_receipt_ms')>? ORDER BY id LIMIT 1000",
            (last_id, boundary, retained_after),
        ).fetchall()
        if not batch:
            return
        last_id = batch[-1][0]
        for _, payload in batch:
            yield json.loads(payload)


def _worker_rows(store):
    # SQLite publication happens after gzip, parquet and manifest have closed.
    retained_after = store.get("recording_retention", {}).get("through_ms", 0)
    pending = store.db.execute(
        "SELECT min(s.decision_ms) FROM ml_snapshots s WHERE s.stage='decision' AND s.decision_ms>? AND NOT EXISTS "
        "(SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy='prints-v1')",
        (retained_after,),
    ).fetchone()
    if pending and pending[0] is not None:
        retained_after = max(retained_after, pending[0] - 900_000)
    manifests = committed_manifests(store, retained_after)
    spans = []
    for manifest in manifests:
        path = Path(manifest["raw"])
        start, end = manifest["min_receipt_ms"], manifest["max_receipt_ms"]
        reason = None
        if not path.exists() and not packed_record(path):
            reason = "committed recording missing from active storage"
        else:
            if hashlib.sha256(raw_bytes(path)).hexdigest() != manifest["sha256"]:
                raise ValueError("Raw segment integrity mismatch")
            if manifest_for(path) != manifest:
                raise ValueError("Committed recording manifest mismatch")
            previous, low, high, count = -1, None, None, 0
            with raw_stream(path) as stream:
                for line in stream:
                    receipt = json.loads(line)["receipt_ms"]
                    if receipt < previous:
                        reason = "non-monotonic receipt time"
                    previous = receipt
                    low = receipt if low is None else min(low, receipt)
                    high = receipt if high is None else max(high, receipt)
                    count += 1
            if (low, high, count) != (start, end, manifest["rows"]):
                raise ValueError("Committed recording bounds mismatch")
        spans.append(dict(start=start, end=end, manifest=manifest, reason=reason))
        if len(spans) % 1000 == 0:
            store.put(
                "ml_replay_progress",
                dict(phase="integrity preflight", segments=len(spans), receipt_ms=end, at_ms=now_ms()),
            )
    spans.sort(key=lambda s: (s["start"], s["manifest"]["id"]))
    groups = []
    for span in spans:
        if groups and span["start"] < groups[-1]["end"]:
            groups[-1]["items"].append(span)
            groups[-1]["end"] = max(groups[-1]["end"], span["end"])
        else:
            groups.append(dict(start=span["start"], end=span["end"], items=[span]))
    excluded = []
    for group in groups:
        group["bad"] = len(group["items"]) > 1 or any(s["reason"] for s in group["items"])
        if group["bad"]:
            excluded.extend(
                dict(
                    id=s["manifest"]["id"],
                    sha256=s["manifest"]["sha256"],
                    raw=s["manifest"]["raw"],
                    start=s["start"],
                    end=s["end"],
                    reason=s["reason"] or "overlapping recording interval",
                )
                for s in group["items"]
            )
    audit = dict(policy="clock-exclusion-v1", excluded=excluded, segments=len(spans))
    directory = store.root / "ml" / "recording-audits"
    directory.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(audit, sort_keys=True, indent=2)
    target = directory / (hashlib.sha256(encoded.encode()).hexdigest() + ".json")
    if not target.exists():
        target.write_text(encoded)
    store.put(
        "ml_recordings", dict(at_ms=now_ms(), segments=len(spans), excluded=len(excluded), audit=str(target))
    )
    previous = None
    for index, group in enumerate(groups):
        if index % 1000 == 0:
            store.put(
                "ml_replay_progress",
                dict(
                    phase="paper outcomes",
                    segments=index,
                    total=len(groups),
                    receipt_ms=group["start"],
                    at_ms=now_ms(),
                ),
            )
        if group["bad"]:
            for at in sorted({group["start"], group["end"]}):
                yield dict(
                    source="control/gap",
                    symbol="ALL",
                    event_ms=at,
                    receipt_ms=at,
                    schema_version=1,
                    complete=False,
                    payload=json.dumps(dict(reason="audited recording exclusion", audit=str(target))),
                )
            previous = None
            continue
        manifest = group["items"][0]["manifest"]
        if previous and manifest.get("previous_segment") != {
            "id": previous["id"],
            "sha256": previous["sha256"],
        }:
            at = group["start"]
            yield dict(
                source="control/gap",
                symbol="ALL",
                event_ms=at,
                receipt_ms=at,
                schema_version=1,
                complete=False,
                payload='{"reason":"unchained recording"}',
            )
        last_row = last_emitted = None
        for row in segment_rows([manifest["raw"]]):
            last_row = row
            source = row["source"]
            if source.startswith("native/"):
                source = source.split("/", 2)[2]
            # The prints-v1 policy consumes trades and continuity controls, not DOM.
            # Integrity preflight above still verifies every recorded envelope.
            if not row.get("complete", True) or source.startswith(("control/", "ws/publicTrade.")):
                last_emitted = row
                yield row
        if last_row is not None and last_row is not last_emitted:
            yield dict(
                source="control/clock",
                symbol="ALL",
                event_ms=last_row["event_ms"],
                receipt_ms=last_row["receipt_ms"],
                schema_version=1,
                complete=True,
                payload="{}",
            )
        previous = manifest
