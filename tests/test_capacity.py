"""Storage recovery must not depend on model fitting or invent missing outcomes."""

import json

import pytest

from bybit_flow.capacity import manifest_batch, step
from bybit_flow.ml.labels import label_recordings
from bybit_flow.ml.locking import exclusive
from bybit_flow.ml.recordings import worker_rows
from bybit_flow.ml.store import FeatureStore
from bybit_flow.storage import Recorder, Store, now_ms


def event(at, source="control/subscribed", payload=None):
    return dict(
        source=source,
        symbol="TESTUSDT",
        receipt_ms=at,
        event_ms=at,
        schema_version=1,
        complete=True,
        payload=json.dumps(payload or {"symbols": ["TESTUSDT"]}),
    )


def trade(at, price, ident):
    return event(at, "ws/publicTrade.TESTUSDT", {"data": [dict(T=at, p=price, v="1000", i=ident, S="Buy")]})


def prepare(settings, signal):
    store = Store(settings.data_dir)
    FeatureStore(store).capture(signal, 1000, "decision")
    recorder = Recorder(store, settings)
    recorder.flush([event(500), trade(1500, "100", "entry")])
    recorder.flush([trade(5000, "116", "exit")])
    return store


def test_pressure_upkeep_works_with_ml_disabled_and_matches_full_replay(settings, signal, monkeypatch):
    settings = settings.model_copy(update={"ml_enabled": False})
    store = prepare(settings, signal)
    full_settings = settings.model_copy(update={"data_dir": settings.data_dir / "reference"})
    full = prepare(full_settings, signal)
    expected = label_recordings(full, worker_rows(full), full_settings)["outcomes"][0]
    monkeypatch.setattr(
        "bybit_flow.capacity.directory_bytes", lambda root: settings.max_storage_gb * 1e9 * 0.9
    )
    first = step(settings, store, max_segments=1)
    assert first["more"] and first["state"] == "CATCHING_UP"
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == 0
    assert store.get("primary_materialization")["pending"] == 1
    # A truncated batch must not turn the still-open trade into an incomplete label.
    second = step(settings, store, at_ms=now_ms() + 61_000, max_segments=1)
    assert not second["more"]
    got = json.loads(store.db.execute("SELECT payload FROM ml_labels").fetchone()[0])
    for key in ("complete", "classification", "net_r", "entry_ms", "exit_ms", "data_gaps"):
        assert got.get(key) == expected.get(key)
    assert got["complete"]
    assert store.db.execute("SELECT count(*) FROM ml_models").fetchone()[0] == 0
    assert store.get("ml_champion") is None
    full.close()
    store.close()


def test_batch_never_splits_equal_receipt_or_overlapping_segments(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    recorder.flush([event(500), event(1500)])
    recorder.flush([event(1500), event(2500)])
    recorder.flush([event(3000)])
    batch, more, _ = manifest_batch(store, 0, now_ms(), limit=1)
    assert len(batch) == 2 and more
    assert max(x["max_receipt_ms"] for x in batch) == 2500
    with pytest.raises(ValueError, match="bounded upkeep"):
        manifest_batch(store, 0, now_ms(), limit=1, max_input_bytes=1)
    store.close()


def test_regular_labeler_and_capacity_worker_share_exclusive_checkpoint_owner(settings, signal):
    store = prepare(settings, signal)
    before = store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0]
    with exclusive(store.root / "primary-label.lock"):
        with pytest.raises((BlockingIOError, OSError)):
            label_recordings(store, worker_rows(store), settings, incremental=True)
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == before
    assert store.get("primary_materialization") is None
    store.close()


def test_low_pressure_does_no_replay_or_training(settings, monkeypatch):
    store = Store(settings.data_dir)
    monkeypatch.setattr("bybit_flow.capacity.manifest_batch", lambda *a, **kw: pytest.fail("unneeded replay"))
    assert step(settings, store)["state"] == "NORMAL"
    store.close()


def test_recording_failure_is_not_mislabeled_as_exchange_failure():
    from bybit_flow.operator_health import recovery_detail
    from bybit_flow.recovery import error_category

    category = error_category(RuntimeError("Recording circuit open"))
    assert category == "RECORDING_UNAVAILABLE"
    assert "storage health" in recovery_detail({"error_category": category}, 1000)
