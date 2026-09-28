import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from bybit_flow.ml.labels import close_pre_retention_decisions, label_recordings
from bybit_flow.ml.recordings import worker_rows
from bybit_flow.ml.store import FeatureStore
from bybit_flow.packing import compact
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
    ident = FeatureStore(store).capture(signal, 2000, "decision")
    FeatureStore(store).label(
        ident, {"policy": "prints-v1", "complete": False, "classification": "incomplete"}, 3000
    )
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


def test_retention_keeps_corrupt_pack_and_prunes_independent_pack(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    archives = []
    for batch in range(2):
        for at in (batch * 2 + 1, batch * 2 + 2):
            recorder.flush(
                [
                    dict(
                        source="control/subscribed",
                        symbol="TESTUSDT",
                        event_ms=at,
                        receipt_ms=at,
                        schema_version=1,
                        complete=True,
                        payload='{"symbols":["TESTUSDT"]}',
                    )
                ]
            )
        archives.append(Path(compact(store, limit=2)["archive"]))
    archives[0].write_bytes(b"damaged pack")
    cfg = SimpleNamespace(recording_retention_enabled=True, max_storage_gb=0.000001)
    result = prune_recordings(store, cfg, 8 * 3_600_000)
    assert result["skipped_unverifiable"] == 1
    assert result["freed_bytes"] > 0
    assert archives[0].exists()
    assert not archives[1].exists()
    assert store.get("recording_retention_errors")["items"][0]["error_type"] == "BadZipFile"
    store.close()


def test_pre_retention_decision_becomes_incomplete_not_a_trade_result(settings, signal):
    store = Store(settings.data_dir)
    ident = FeatureStore(store).capture(signal, 2000, "decision")
    store.put("recording_retention", dict(through_ms=3000))
    assert close_pre_retention_decisions(store) == 1
    assert close_pre_retention_decisions(store) == 0
    label = json.loads(
        store.db.execute("SELECT payload FROM ml_labels WHERE snapshot_id=?", (ident,)).fetchone()[0]
    )
    assert label["classification"] == "incomplete"
    assert label["net_r"] is None
    assert not label["complete"]
    store.close()


def test_force_retention_prunes_safe_evidence_below_budget(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    for at in (1000, 2000):
        recorder.flush(
            [
                dict(
                    source="control/subscribed",
                    symbol="TESTUSDT",
                    event_ms=at,
                    receipt_ms=at,
                    schema_version=1,
                    complete=True,
                    payload='{"symbols":["TESTUSDT"]}',
                )
            ]
        )
    manifests = sorted(store.rows("segments"), key=lambda m: m["max_receipt_ms"])
    cfg = SimpleNamespace(recording_retention_enabled=True, max_storage_gb=1)
    assert prune_recordings(store, cfg, 8 * 3_600_000)["freed_bytes"] == 0
    assert prune_recordings(store, cfg, 8 * 3_600_000, force=True, max_reclaim_bytes=1)["freed_bytes"] > 0
    assert not Path(manifests[0]["raw"]).exists()
    assert Path(manifests[1]["raw"]).exists()
    assert prune_recordings(store, cfg, 8 * 3_600_000, force=True, max_reclaim_bytes=1)["freed_bytes"] > 0
    assert not Path(manifests[1]["raw"]).exists()
    store.close()


def test_finalized_incomplete_outcome_releases_recent_raw_evidence(settings, signal):
    store = Store(settings.data_dir)
    now = 8 * 3_600_000
    decision = now - 3_600_000
    recorder = Recorder(store, settings)
    recorder.flush(
        [
            dict(
                source="control/subscribed",
                symbol=signal.symbol,
                event_ms=decision,
                receipt_ms=decision,
                schema_version=1,
                complete=True,
                payload=json.dumps({"symbols": [signal.symbol]}),
            )
        ]
    )
    manifest = store.rows("segments")[0]
    ident = FeatureStore(store).capture(signal, decision, "decision")
    FeatureStore(store).label(
        ident,
        dict(policy="prints-v1", complete=False, classification="incomplete", net_r=None),
        decision + 1,
    )
    cfg = SimpleNamespace(recording_retention_enabled=True, max_storage_gb=1)
    assert prune_recordings(store, cfg, now)["freed_bytes"] == 0
    assert prune_recordings(store, cfg, now, force=True)["freed_bytes"] > 0
    assert not Path(manifest["raw"]).exists()
    assert store.db.execute("SELECT 1 FROM ml_labels WHERE snapshot_id=?", (ident,)).fetchone()
    store.close()


def test_incomplete_label_does_not_release_active_setup_evidence(settings, signal):
    store = Store(settings.data_dir)
    now = 8 * 3_600_000
    decision = now - 3_600_000
    signal.created_ms = decision
    signal.expires_ms = now + 3_600_000
    signal.state = "ALERTED"
    with store.db:
        store.db.execute(
            "INSERT INTO signals VALUES(?,?,?,?,?)",
            (signal.id, signal.symbol, decision, signal.state, signal.model_dump_json()),
        )
    Recorder(store, settings).flush(
        [
            dict(
                source="control/subscribed",
                symbol=signal.symbol,
                event_ms=decision,
                receipt_ms=decision,
                schema_version=1,
                complete=True,
                payload=json.dumps({"symbols": [signal.symbol]}),
            )
        ]
    )
    raw = Path(store.rows("segments")[0]["raw"])
    ident = FeatureStore(store).capture(signal, decision, "decision")
    FeatureStore(store).label(
        ident,
        dict(policy="prints-v1", complete=False, classification="incomplete", net_r=None),
        decision + 1,
    )
    cfg = SimpleNamespace(recording_retention_enabled=True, max_storage_gb=1)
    assert prune_recordings(store, cfg, now, force=True)["freed_bytes"] == 0
    assert raw.exists()
    store.close()


def test_labels_restart_observed_coverage_after_pruned_subscription(settings, signal):
    store = Store(settings.data_dir)
    store.put("recording_retention", dict(through_ms=1000))
    FeatureStore(store).capture(signal, 2000, "decision")
    rows = [
        dict(
            source="ws/publicTrade.TESTUSDT",
            symbol="TESTUSDT",
            event_ms=at,
            receipt_ms=at,
            complete=True,
            payload=json.dumps({"data": [dict(T=at, p=str(price), v="1000", i=str(at), S="Buy")]}),
        )
        for at, price in ((1500, 100), (2500, 100), (5000, 116))
    ]
    result = label_recordings(store, rows, settings)
    assert result["complete"] == 1
    assert result["outcomes"][0]["data_gaps"] == []
    store.close()
