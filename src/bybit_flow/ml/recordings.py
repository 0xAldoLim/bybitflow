"""Preflight committed recordings; preserve clock-damaged bytes as explicit gaps.

Strict CLI replay still rejects unordered inputs. The unattended worker can move
past clock jumps without sorting events or learning outcomes across missing tape.
Integrity failures remain fatal and preflight finishes before any label is written.
"""

import gzip
import hashlib
import json
from pathlib import Path

from ..replay import segment_rows
from ..storage import now_ms


def worker_rows(store):
    # SQLite publication happens after gzip, parquet and manifest have closed.
    manifests = [json.loads(r[0]) for r in store.db.execute("SELECT payload FROM segments")]
    retained_after = store.get("recording_retention", {}).get("through_ms", 0)
    manifests = [m for m in manifests if m["max_receipt_ms"] > retained_after]
    spans = []
    for manifest in manifests:
        path = Path(manifest["raw"])
        start, end = manifest["min_receipt_ms"], manifest["max_receipt_ms"]
        reason = None
        if not path.exists():
            reason = "committed recording missing from active storage"
        else:
            if hashlib.sha256(path.read_bytes()).hexdigest() != manifest["sha256"]:
                raise ValueError("Raw segment integrity mismatch")
            sidecar = path.with_name(path.name.removesuffix(".jsonl.gz") + ".manifest.json")
            if not sidecar.exists() or json.loads(sidecar.read_text()) != manifest:
                raise ValueError("Committed recording manifest mismatch")
            previous, low, high, count = -1, None, None, 0
            with gzip.open(path, "rt") as stream:
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
    for group in groups:
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
        yield from segment_rows([manifest["raw"]])
        previous = manifest
