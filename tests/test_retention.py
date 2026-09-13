import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from bybit_flow.ml.recordings import worker_rows
from bybit_flow.ml.store import FeatureStore
from bybit_flow.retention import prune_recordings
from bybit_flow.storage import Recorder, Store


def test_retention_preserves_learning_and_active_history(settings, signal):
    store = Store(settings.data_dir)
    rec = Recorder(store, settings)
    now = 8 * 3_600_000
    for at in (1000, now - 1000):
        rec.flush(
            [
                dict(
                    source="control/subscribed",
                    symbol="TESTUSDT",
                    event_ms=at,
                    receipt_ms=at,
                    schema_version=1,
                    complete=True,
                    payload=json.dumps({"symbols": ["TESTUSDT"]}),
                )
            ]
        )
    manifests = sorted(store.rows("segments"), key=lambda m: m["max_receipt_ms"])
    FeatureStore(store).capture(signal, 2000, "decision")
    before = store.db.execute("SELECT * FROM ml_snapshots").fetchall()
    model = store.root / "ml" / "models" / "preserved.json"
    model.parent.mkdir(parents=True, exist_ok=True)
    model.write_text('{"fixture": "preserve"}')
    store.put("ml_monitor", dict(status="observed", at_ms=now))
    cfg = SimpleNamespace(recording_retention_enabled=True, max_storage_gb=0.000001)
    result = prune_recordings(store, cfg, now)
    assert result["freed_bytes"] > 0
    assert not Path(manifests[0]["raw"]).exists()
    assert Path(manifests[1]["raw"]).exists()
    assert store.db.execute("SELECT * FROM ml_snapshots").fetchall() == before
    assert model.read_text() == '{"fixture": "preserve"}'
    assert len(store.rows("segments")) == 2
    assert len(list(worker_rows(store))) == 1
    assert store.get("ml_recordings")["excluded"] == 0
    assert prune_recordings(store, cfg, now)["freed_bytes"] == 0
    store.close()


def test_retention_rejects_paths_outside_segments(settings, tmp_path):
    store = Store(settings.data_dir)
    outside = tmp_path / "important.jsonl.gz"
    outside.write_text("keep")
    with store.db:
        store.db.execute(
            "INSERT INTO segments VALUES(?,?,?)",
            ("unsafe", 1, json.dumps(dict(max_receipt_ms=1, raw=str(outside), parquet=str(outside)))),
        )
    now = 8 * 3_600_000
    store.put("ml_monitor", dict(status="observed", at_ms=now))
    cfg = SimpleNamespace(recording_retention_enabled=True, max_storage_gb=0.000001)
    with pytest.raises(ValueError, match="outside managed"):
        prune_recordings(store, cfg, now)
    assert outside.read_text() == "keep"
    store.close()
