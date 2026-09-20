"""Lossless segment packing. Original compressed bytes, hashes and manifests survive."""

import hashlib
import io
import json
import sqlite3
import uuid
import zipfile
from pathlib import Path


def migrate(store):
    with store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS segment_packs(segment_id TEXT PRIMARY KEY, archive TEXT NOT NULL, bytes INTEGER NOT NULL)"
        )


def packed_record(path):
    path = Path(path)
    database = path.parent.parent / "research.sqlite"
    if not database.exists():
        return None
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='segment_packs'").fetchone():
            return None
        ident = path.name.removesuffix(".jsonl.gz")
        return db.execute(
            "SELECT p.archive,s.payload FROM segment_packs p JOIN segments s ON s.id=p.segment_id WHERE p.segment_id=?",
            (ident,),
        ).fetchone()


def raw_bytes(path):
    path = Path(path)
    if path.exists():
        return path.read_bytes()
    record = packed_record(path)
    if not record:
        raise FileNotFoundError(path)
    with zipfile.ZipFile(record[0]) as archive:
        return archive.read(path.name)


def manifest_for(path):
    path = Path(path)
    sidecar = path.with_name(path.name.removesuffix(".jsonl.gz") + ".manifest.json")
    if sidecar.exists():
        return json.loads(sidecar.read_text())
    record = packed_record(path)
    if record:
        return json.loads(record[1])
    raise ValueError("Raw segment lacks manifest; verify provenance before replay")


def raw_stream(path):
    import gzip

    return gzip.open(io.BytesIO(raw_bytes(path)), "rt")


def compact(store, limit=300, dry_run=False):
    migrate(store)
    from .leases import overlaps
    from .storage import now_ms

    # Never compact the writer's current boundary. Published older files are immutable.
    rows = store.db.execute(
        "SELECT s.payload FROM segments s LEFT JOIN segment_packs p ON p.segment_id=s.id WHERE p.segment_id IS NULL AND json_extract(s.payload,'$.max_receipt_ms')>? ORDER BY s.at_ms LIMIT ?",
        (store.get("recording_retention", {}).get("through_ms", 0), limit * 10),
    ).fetchall()
    selected = []
    for row in rows:
        m = json.loads(row[0])
        if (
            not overlaps(store, m["min_receipt_ms"], m["max_receipt_ms"])
            and Path(m["raw"]).exists()
            and Path(m["parquet"]).exists()
        ):
            selected.append(m)
        if len(selected) >= limit:
            break
    if len(selected) < 2:
        return {"status": "no unpacked batch", "segments": 0}
    if dry_run:
        return {
            "status": "dry-run",
            "segments": len(selected),
            "files_to_pack": len(selected) * 3,
            "policy": "Lossless archive with checksum verification before removing redundant originals",
        }
    from contextlib import ExitStack

    from .leases import RangeLease

    # Recheck/claim each exact range atomically before touching files. A new reader
    # either owns the range first or waits for this lossless publication to finish.
    with ExitStack() as stack:
        protected = []
        for m in selected:
            try:
                lease = stack.enter_context(
                    RangeLease(
                        store, m["min_receipt_ms"], m["max_receipt_ms"], "Lossless compaction", exclusive=True
                    )
                )
                protected.append((m, lease))
            except BlockingIOError:
                continue
        if len(protected) < 2:
            return {"status": "overlapping evidence protected", "segments": 0}
        result = _pack(store, [m for m, _ in protected], [lease for _, lease in protected])
        store.put("last_compaction", dict(at_ms=now_ms(), **result))
        return result


def _pack(store, selected, leases):
    root = (store.root / "segments").resolve()
    directory = store.root / "packs"
    directory.mkdir(exist_ok=True)
    target = directory / (uuid.uuid4().hex + ".zip")
    files = []
    before = 0
    with zipfile.ZipFile(target, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for m in selected:
            for lease in leases:
                lease.heartbeat()
            paths = [Path(m["raw"]), Path(m["parquet"]), Path(m["raw"]).with_name(m["id"] + ".manifest.json")]
            for p in paths:
                if p.is_symlink() or p.resolve().parent != root:
                    raise ValueError("Compaction path outside managed segments")
                archive.write(p, p.name)
                before += p.stat().st_size
                files.append(p)
    # Verify every archived byte before publishing the mapping or removing originals.
    with zipfile.ZipFile(target) as archive:
        for p in files:
            if hashlib.sha256(archive.read(p.name)).digest() != hashlib.sha256(p.read_bytes()).digest():
                raise ValueError("Compaction checksum mismatch")
    with store.db:
        for m in selected:
            store.db.execute(
                "INSERT INTO segment_packs VALUES(?,?,?)",
                (m["id"], str(target), Path(m["raw"]).stat().st_size + Path(m["parquet"]).stat().st_size),
            )
    # A crash here leaves redundant originals; readers prefer them, and evidence survives.
    for lease in leases:
        lease.heartbeat(force=True)
    for p in files:
        p.unlink()
    return dict(
        status="packed",
        segments=len(selected),
        files_removed=len(files),
        archive=str(target),
        bytes_before=before,
        bytes_after=target.stat().st_size,
        saved_bytes=before - target.stat().st_size,
    )
