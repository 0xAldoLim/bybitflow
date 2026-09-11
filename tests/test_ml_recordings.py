import json
from pathlib import Path

import pytest

from bybit_flow.ml.labels import label_recordings
from bybit_flow.ml.recordings import worker_rows
from bybit_flow.ml.store import FeatureStore
from bybit_flow.replay import segment_rows
from bybit_flow.storage import Recorder, Store


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


@pytest.mark.parametrize("damage", ["clock", "overlap", "missing"])
def test_clock_gap_preserves_bytes_and_excludes_outcome(settings, signal, damage):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    FeatureStore(store).capture(signal, 1000, "decision")
    recorder.flush(
        [
            event(500),
            event(
                1500,
                "ws/publicTrade.TESTUSDT",
                {"data": [dict(T=1500, p="100", v="1000", i="entry", S="Buy")]},
            ),
        ]
    )
    recorder.flush([event(2000), event(1900 if damage == "clock" else 2500)])
    damaged = Path(store.rows("segments")[0]["raw"])
    if damage == "overlap":
        recorder.flush([event(2100), event(2700)])
    if damage == "missing":
        damaged.rename(damaged.with_suffix(".preserved"))
    recorder.flush(
        [
            event(
                5000,
                "ws/publicTrade.TESTUSDT",
                {"data": [dict(T=5000, p="116", v="1000", i="exit", S="Buy")]},
            )
        ]
    )
    before = {p: p.read_bytes() for p in (store.root / "segments").iterdir()}
    rows = list(worker_rows(store))
    assert [r["receipt_ms"] for r in rows] == sorted(r["receipt_ms"] for r in rows)
    assert store.get("ml_recordings")["excluded"] == (2 if damage == "overlap" else 1)
    assert all(p.read_bytes() == data for p, data in before.items())
    result = label_recordings(store, rows, settings)
    assert result["complete"] == 0
    assert result["outcomes"][0]["net_r"] is None
    if damage == "clock":
        with pytest.raises(ValueError, match="Non-monotonic"):
            list(segment_rows([damaged]))


def test_integrity_failure_precedes_any_output(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    recorder.flush([event(500)])
    recorder.flush([event(2000)])
    path = Path(store.rows("segments")[0]["raw"])
    path.write_bytes(path.read_bytes() + b"corruption")
    with pytest.raises(ValueError, match="integrity mismatch"):
        next(worker_rows(store))


def test_unpublished_file_ignored_and_good_recording_unchanged(settings):
    store = Store(settings.data_dir)
    Recorder(store, settings).flush([event(500), event(1000)])
    path = Path(store.rows("segments")[0]["raw"])
    (path.parent / "still-writing.jsonl.gz").write_bytes(b"partial")
    assert list(worker_rows(store)) == list(segment_rows([path]))
    assert store.get("ml_recordings")["excluded"] == 0


def test_snapshot_iterator_does_not_hold_wal_read_lock(settings, signal):
    store = Store(settings.data_dir)
    features = FeatureStore(store)
    features.capture(signal, 1000, "decision")
    features.capture(signal.model_copy(update={"id": "second"}), 2000, "decision")
    pending = features.snapshots()
    next(pending)
    peer = Store(settings.data_dir)
    peer.put("recorder-commit", True)
    store.put("ml-audit", True)
    assert store.get("ml-audit") is True
    assert len(list(pending)) == 1
    peer.close()
    store.close()
