"""Lossless, bounded archival of immutable historical horizon research exports."""

import gzip
import hashlib
import re
import shutil
import uuid
from pathlib import Path

from ..storage import now_ms
from .locking import exclusive

CHUNK = 1_048_576


def fingerprint(path, *, packed=False):
    value, size = hashlib.sha256(), 0
    with gzip.open(path, "rb") if packed else path.open("rb") as stream:
        while block := stream.read(CHUNK):
            value.update(block)
            size += len(block)
    return value.hexdigest(), size


def current_export(store, path):
    current = store.get("horizon_model", {}).get("dataset")
    if not current:
        return False
    target = Path(current)
    if target.suffix == ".gz":
        target = target.with_suffix("")
    return target.resolve() == path.resolve()


def archive_one(store, path):
    archive = path.with_suffix(".json.gz")
    if path.is_symlink() or archive.is_symlink() or archive.resolve().parent != path.resolve().parent:
        raise ValueError("Unsafe horizon export path")
    expected, size = fingerprint(path)
    if expected != path.stem:
        raise ValueError("Horizon export content hash mismatch")
    temporary = path.parent / ("." + path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        if not archive.exists():
            with path.open("rb") as source, temporary.open("xb") as output:
                with gzip.GzipFile(
                    filename="", fileobj=output, mode="wb", compresslevel=3, mtime=0
                ) as packed:
                    shutil.copyfileobj(source, packed, CHUNK)
            if fingerprint(temporary, packed=True) != (expected, size):
                raise ValueError("Archived horizon export verification failed")
            temporary.replace(archive)
        if fingerprint(archive, packed=True) != (expected, size):
            raise ValueError("Existing horizon archive does not match original")
        if current_export(store, path):
            return 0  # A concurrent fit may have made this dataset current.
        if fingerprint(path) != (expected, size):
            raise ValueError("Horizon export changed during archival")
        receipt = dict(
            at_ms=now_ms(),
            original=str(path),
            archive=str(archive),
            sha256=expected,
            original_bytes=size,
            archive_bytes=archive.stat().st_size,
            status="VERIFIED_PENDING_SOURCE_REMOVAL",
        )
        key = "horizon_export_archive:" + expected
        store.put(key, receipt)
        path.unlink()  # Both copies were verified, and the archive remains intact.
        store.put(key, receipt | dict(status="ARCHIVED"))
        return max(0, size - archive.stat().st_size)
    finally:
        temporary.unlink(missing_ok=True)


def archive_horizon_exports(store, *, at_ms=None, max_input_bytes=128_000_000, min_age_ms=7_200_000):
    """Keep current/model files; preserve older export bytes in verified gzip files."""
    now = at_ms if at_ms is not None else now_ms()
    result = dict(at_ms=now, status="OBSERVED", archived=0, freed_bytes=0, input_bytes=0, errors=0)
    root = store.root / "ml" / "horizon"
    if not root.is_dir() or root.is_symlink():
        return result
    from .cli import LiveRecordingPressure, prioritize_live_recording

    try:
        with exclusive(store.root / "ml-export-archive.lock"):
            for path in sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime):
                if (
                    not re.fullmatch(r"[a-f0-9]{64}\.json", path.name)
                    or path.is_symlink()
                    or current_export(store, path)
                    or now - path.stat().st_mtime * 1000 < min_age_ms
                ):
                    continue
                size = path.stat().st_size
                if result["input_bytes"] + size > max_input_bytes:
                    result["status"] = "DEFERRED_BUDGET"
                    if size > max_input_bytes:
                        continue  # An oversized export must not starve smaller work.
                    break
                prioritize_live_recording(store)
                result["input_bytes"] += size
                try:
                    freed = archive_one(store, path)
                    result["archived"] += not path.exists()
                    result["freed_bytes"] += freed
                except (OSError, ValueError, EOFError) as exc:
                    result.update(status="DEFERRED", last_error_type=type(exc).__name__, reason=str(exc))
                    result["errors"] += 1
    except LiveRecordingPressure:
        result["status"] = "DEFERRED_LIVE_PRESSURE"
        raise
    except BlockingIOError:
        result["status"] = "DEFERRED_BUSY"
    finally:
        store.put("ml_export_archival", result)
    return result
