import json
from pathlib import Path

import pytest

from bybit_flow.leases import RangeLease
from bybit_flow.lifecycle import advance_trades
from bybit_flow.ml.store import FeatureStore
from bybit_flow.models import D, Signal, Trade
from bybit_flow.orderflow import Tape
from bybit_flow.packing import compact
from bybit_flow.retention import active_evidence_start, protection_ranges, prune_recordings
from bybit_flow.storage import Recorder, Store


def stored_plan(store, signal, now):
    signal.created_ms = now - 3 * 3_600_000
    signal.expires_ms = signal.trigger_expires_ms = signal.created_ms + 60_000
    signal.holding_deadline_ms = now + 3_600_000
    signal.horizon_profile = "SWING"
    signal.state = "ALERTED"
    signal.evidence["observed_entry_ms"] = signal.created_ms + 1000
    with store.db:
        store.db.execute(
            "INSERT INTO signals VALUES(?,?,?,?,?)",
            (signal.id, signal.symbol, signal.created_ms, signal.state, signal.model_dump_json()),
        )


def recording(store, settings, at):
    Recorder(store, settings).flush(
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


@pytest.mark.parametrize("packed", [False, True])
def test_cleanup_releases_processed_tape_and_restart_keeps_original_lifecycle(settings, signal, packed):
    store = Store(settings.data_dir)
    now = 8 * 3_600_000
    stored_plan(store, signal, now)
    features = FeatureStore(store)
    ident = features.capture(signal, signal.created_ms, "decision")
    features.label(ident, dict(policy="prints-v1", complete=False, classification="incomplete"), now - 1)
    for at in (signal.created_ms, now - 1000):
        recording(store, settings, at)
        if packed:
            recording(store, settings, at + 1)
            assert compact(store, limit=2)["status"] == "packed"
    manifests = sorted(store.rows("segments"), key=lambda row: row["min_receipt_ms"])
    boundary_manifests = [manifests[0], manifests[-1]]
    if packed:
        paths = [
            Path(
                store.db.execute(
                    "SELECT archive FROM segment_packs WHERE segment_id=?", (m["id"],)
                ).fetchone()[0]
            )
            for m in boundary_manifests
        ]
    else:
        paths = [Path(m["raw"]) for m in boundary_manifests]
    model = store.root / "ml" / "models" / "preserved.json"
    model.parent.mkdir(parents=True, exist_ok=True)
    model.write_bytes(b'{"fixture":"saved model"}')
    snapshots = store.db.execute("SELECT * FROM ml_snapshots").fetchall()
    labels = store.db.execute("SELECT * FROM ml_labels").fetchall()
    tape = Tape()
    tape.add(Trade(signal.symbol, now - 2000, now - 1000, "processed", "Buy", D("100"), D("1")))
    advance_trades(signal, tape, now)
    store.monitor(signal)
    original = signal.model_dump()
    assert paths[0].exists() and paths[1].exists()
    assert prune_recordings(store, settings, now, force=True)["freed_bytes"] > 0
    assert not paths[0].exists() and paths[1].exists()
    assert dict(store.db.execute("SELECT id,payload FROM signals"))[signal.id] == signal.model_dump_json()
    assert store.db.execute("SELECT * FROM ml_snapshots").fetchall() == snapshots
    assert store.db.execute("SELECT * FROM ml_labels").fetchall() == labels
    assert model.read_bytes() == b'{"fixture":"saved model"}'
    assert len(store.rows("segments")) == len(manifests)
    store.close()
    reopened = Store(settings.data_dir)
    restored = Signal.model_validate(reopened.active_signals()[0])
    assert restored.model_dump() == original
    # The later target follows the saved entry; it does not depend on old raw files.
    tape = Tape()
    tape.add(Trade(signal.symbol, now + 1000, now + 1001, "target", "Buy", D("115"), D("1")))
    advance_trades(restored, tape, now + 1002)
    assert restored.state == "RESOLVED"
    assert restored.evidence["primary_outcome"] == "TARGET"
    assert restored.evidence["observed_entry_ms"] == original["evidence"]["observed_entry_ms"]
    for key in ("entry", "zone", "stop", "tp1", "tp2", "trigger_expires_ms", "holding_deadline_ms"):
        assert restored.model_dump()[key] == original[key]
    reopened.close()


@pytest.mark.parametrize(
    "change",
    [
        {"monitor_cursor_event_ms": None},
        {"monitor_cursor_receipt_ms": 9_000_001},
        {"monitor_cursor_event_ms": True},
        {"monitor_cursor_receipt_ms": 999},
        {"last_monitor_ms": 4_000_000},
        {"last_checked_trade_id": None},
    ],
)
def test_invalid_lifecycle_progress_keeps_original_protection(signal, change):
    signal.coverage.update(
        monitor_cursor_event_ms=4_900_000,
        monitor_cursor_receipt_ms=4_900_001,
        last_monitor_ms=5_000_000,
        last_checked_trade_id="last",
    )
    signal.coverage.update(change)
    assert active_evidence_start(signal.model_dump(), 9_000_000) == signal.created_ms - 900_000


def test_paused_progress_preserves_unprocessed_tape_ml_and_reader_protection(settings, signal):
    store = Store(settings.data_dir)
    now = 8 * 3_600_000
    stored_plan(store, signal, now)
    ident = FeatureStore(store).capture(signal, signal.created_ms, "decision")
    signal.coverage.update(
        monitor_cursor_event_ms=now - 2000,
        monitor_cursor_receipt_ms=now - 1000,
        last_monitor_ms=now,
        last_checked_trade_id="last",
        monitoring="paused",
        monitor_status="RECONCILIATION_PENDING",
    )
    store.monitor(signal)
    recording(store, settings, signal.created_ms)
    raw = Path(store.rows("segments")[0]["raw"])
    active = next(r for r in protection_ranges(store, now) if r[2] == "active setup")
    assert active[0] == now - 2000 - 900_000
    assert active[1] == signal.holding_deadline_ms
    # Advancing lifecycle progress cannot release unresolved ML evidence.
    assert prune_recordings(store, settings, now, force=True)["freed_bytes"] == 0
    assert raw.exists()
    FeatureStore(store).label(
        ident, dict(policy="prints-v1", complete=False, classification="incomplete"), now
    )
    with RangeLease(store, signal.created_ms - 1, signal.created_ms + 1, "reader"):
        assert prune_recordings(store, settings, now, force=True)["freed_bytes"] == 0
        assert raw.exists()
    assert prune_recordings(store, settings, now, force=True)["freed_bytes"] > 0
    assert not raw.exists()
    assert (
        json.loads(store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0])[
            "coverage"
        ]["monitoring"]
        == "paused"
    )
    store.close()
