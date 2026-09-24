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


def test_unreadable_pack_becomes_audited_gap_without_fabricating_outcome(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    recorder.flush([event(500)])
    manifest = store.rows("segments")[0]
    archive = store.root / "packs" / "invalid.zip"
    archive.parent.mkdir(exist_ok=True)
    archive.write_bytes(b"")
    with store.db:
        store.db.execute("INSERT INTO segment_packs VALUES(?,?,?)", (manifest["id"], str(archive), 0))
    Path(manifest["raw"]).unlink()
    rows = list(worker_rows(store))
    assert rows and all(row["source"] == "control/gap" for row in rows)
    audit = store.get("ml_recordings")
    assert audit["excluded"] == 1
    assert "unreadable packed recording archive" in Path(audit["audit"]).read_text()
    assert archive.read_bytes() == b""
    store.close()


def test_live_cutoff_freezes_missing_history_as_incomplete_not_a_win(settings, signal):
    store = Store(settings.data_dir)
    FeatureStore(store).capture(signal, 1000, "decision")
    result = label_recordings(store, [event(500)], settings, observed_until_ms=20_000_000)
    assert result["complete"] == 0
    assert result["outcomes"][0]["classification"] == "incomplete"
    assert result["outcomes"][0]["net_r"] is None
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == 1
    store.close()


def test_live_cutoff_keeps_young_missing_outcomes_pending(settings, signal):
    store = Store(settings.data_dir)
    FeatureStore(store).capture(signal, 1000, "decision")
    label_recordings(store, [event(500)], settings, observed_until_ms=2000)
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == 0
    store.close()


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


def test_manifest_iterator_releases_read_cursor_before_worker_writes(settings):
    from bybit_flow.ml.recordings import committed_manifests

    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    recorder.flush([event(500)])
    recorder.flush([event(1000)])
    iterator = committed_manifests(store, 0)
    next(iterator)
    peer = Store(settings.data_dir)
    peer.put("concurrent-recorder", True)
    store.put("replay-progress", True)
    assert len(list(iterator)) == 1
    peer.close()
    store.close()


def test_outcome_replay_skips_dom_but_preserves_clock_and_gap(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    recorder.flush([event(500), event(1000, "ws/orderbook.50.TESTUSDT", {"type": "snapshot", "ts": 1000})])
    rows = list(worker_rows(store))
    assert [r["source"] for r in rows] == ["control/subscribed", "control/clock"]
    assert rows[-1]["receipt_ms"] == 1000
    assert store.get("ml_recordings")["segments"] == 1
    gap = event(1500, "ws/orderbook.50.TESTUSDT", {"type": "delta", "ts": 1500})
    gap["complete"] = False
    recorder.flush([gap])
    assert list(worker_rows(store))[-1]["complete"] is False
    store.close()


def test_exchange_event_leading_receipt_does_not_abort_labels(settings, signal):
    import json

    from bybit_flow.ml.labels import label_recordings
    from bybit_flow.ml.store import FeatureStore
    from bybit_flow.storage import Store

    store = Store(settings.data_dir)
    FeatureStore(store).capture(signal, 2000, "decision")
    rows = [
        dict(
            source="control/subscribed",
            symbol=signal.symbol,
            event_ms=1000,
            receipt_ms=1000,
            complete=True,
            payload=json.dumps({"symbols": [signal.symbol]}),
        )
    ]
    for event, receipt, price in ((2500, 2400, 100), (5000, 4900, 116)):
        rows.append(
            dict(
                source="ws/publicTrade." + signal.symbol,
                symbol=signal.symbol,
                event_ms=event,
                receipt_ms=receipt,
                complete=True,
                payload=json.dumps({"data": [dict(T=event, p=str(price), v="1000", i=str(event), S="Buy")]}),
            )
        )
    result = label_recordings(store, rows, settings)
    assert result["complete"] == 1
    available = store.db.execute("SELECT available_ms FROM ml_labels").fetchone()[0]
    assert available >= 5000
    store.close()
