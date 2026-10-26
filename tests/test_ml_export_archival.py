import gzip
import hashlib
import os

import pytest

from bybit_flow.ml.export_archival import archive_horizon_exports
from bybit_flow.ml.horizon import fit
from bybit_flow.ml.store import canonical, digest
from bybit_flow.storage import Store, now_ms


def export(store, data, age_ms=10_800_000):
    root = store.root / "ml" / "horizon"
    root.mkdir(parents=True, exist_ok=True)
    path = root / (hashlib.sha256(data).hexdigest() + ".json")
    path.write_bytes(data)
    at = (now_ms() - age_ms) / 1000
    os.utime(path, (at, at))
    return path


def test_lossless_archival_preserves_current_models_and_original_training_records(settings):
    store = Store(settings.data_dir)
    data = canonical([dict(value="historical" * 1000)]).encode()
    old = export(store, data)
    current = export(store, b'["current"]')
    recent = export(store, b'["recent"]', age_ms=0)
    model = old.with_name(old.stem + "-model.json")
    model.write_text("fitted model", encoding="utf-8")
    store.put("horizon_model", dict(dataset=str(current)))
    store.put("frozen-learning-record", dict(features=[1, 2], label="loss"))
    result = archive_horizon_exports(store)
    assert result["archived"] == 1 and result["freed_bytes"] > 0
    assert not old.exists() and gzip.decompress(old.with_suffix(".json.gz").read_bytes()) == data
    assert current.exists() and recent.exists() and model.read_text(encoding="utf-8") == "fitted model"
    assert store.get("frozen-learning-record") == dict(features=[1, 2], label="loss")
    assert store.get("horizon_export_archive:" + old.stem)["status"] == "ARCHIVED"
    store.close()


def test_hash_mismatch_or_corrupt_archive_cannot_remove_original(settings):
    store = Store(settings.data_dir)
    changed = export(store, b'["original"]')
    changed.write_bytes(b'["changed"]')
    old = (now_ms() - 10_800_000) / 1000
    os.utime(changed, (old, old))
    intact = export(store, b'["intact"]')
    archive = intact.with_suffix(".json.gz")
    archive.write_bytes(b"invalid archive")
    result = archive_horizon_exports(store)
    assert result["errors"] == 2 and result["archived"] == 0
    assert changed.read_bytes() == b'["changed"]' and intact.read_bytes() == b'["intact"]'
    assert archive.read_bytes() == b"invalid archive"
    store.close()


def test_interrupted_archival_retries_without_losing_original(settings, monkeypatch):
    store = Store(settings.data_dir)
    original = export(store, b'["preserved"]' * 1000)
    data = original.read_bytes()
    import bybit_flow.ml.export_archival as module

    copy = module.shutil.copyfileobj
    monkeypatch.setattr(module.shutil, "copyfileobj", lambda *_: (_ for _ in ()).throw(OSError("fixture")))
    assert archive_horizon_exports(store)["errors"] == 1
    assert original.read_bytes() == data and not list(original.parent.glob(".*.tmp"))
    monkeypatch.setattr(module.shutil, "copyfileobj", copy)
    assert archive_horizon_exports(store)["archived"] == 1
    assert gzip.decompress(original.with_suffix(".json.gz").read_bytes()) == data
    store.close()


def test_batches_resume_and_oversized_files_do_not_block_smaller_exports(settings):
    store = Store(settings.data_dir)
    huge = export(store, b"x" * 3000)
    first = export(store, b"a" * 1000)
    second = export(store, b"b" * 1000)
    one = archive_horizon_exports(store, max_input_bytes=1000)
    assert one["archived"] == 1 and one["input_bytes"] == 1000
    two = archive_horizon_exports(store, max_input_bytes=1000)
    assert two["archived"] == 1 and two["input_bytes"] == 1000
    assert huge.exists() and not first.exists() and not second.exists()
    store.close()


def test_horizon_fit_reuses_archived_export_without_recreating_json(settings, monkeypatch):
    store = Store(settings.data_dir)
    rows = [dict(targets=dict(best_observed_horizon="SWING"), value="data" * 1000)]
    path = export(store, canonical(rows).encode())
    assert path.stem == digest(rows)
    assert archive_horizon_exports(store)["archived"] == 1
    monkeypatch.setattr("bybit_flow.ml.horizon.dataset", lambda *_: rows)
    report = fit(store, settings, now_ms())
    assert report["dataset"] == str(path.with_suffix(".json.gz")) and not path.exists()
    assert gzip.decompress(path.with_suffix(".json.gz").read_bytes()) == canonical(rows).encode()
    store.close()


def test_recording_pressure_defers_archival_without_touching_export(settings):
    from bybit_flow.ml.cli import LiveRecordingPressure

    store = Store(settings.data_dir)
    path = export(store, b"keep" * 1000)
    store.put("recorder_health", dict(at_ms=now_ms(), healthy=False, state="RECORDER_PRESSURE"))
    with pytest.raises(LiveRecordingPressure):
        archive_horizon_exports(store)
    assert path.read_bytes() == b"keep" * 1000
    assert store.get("ml_export_archival")["status"] == "DEFERRED_LIVE_PRESSURE"
    store.close()


def test_restart_finishes_verified_archive_without_recompressing(settings, monkeypatch):
    from pathlib import Path

    store = Store(settings.data_dir)
    data = b"saved" * 1000
    path = export(store, data)
    unlink = Path.unlink

    def busy(target, *args, **kwargs):
        if target == path:
            raise PermissionError("fixture busy source")
        return unlink(target, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", busy)
    assert archive_horizon_exports(store)["errors"] == 1
    archive = path.with_suffix(".json.gz")
    original_archive = archive.read_bytes()
    assert gzip.decompress(original_archive) == data and path.read_bytes() == data
    store.close()
    store = Store(settings.data_dir)
    monkeypatch.setattr(Path, "unlink", unlink)
    assert archive_horizon_exports(store)["archived"] == 1
    assert not path.exists() and archive.read_bytes() == original_archive
    store.close()
