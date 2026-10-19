import hashlib
import json
import os
import zipfile
from pathlib import Path

import pytest

from bybit_flow.diagnostics import failure_summary, record_failure
from bybit_flow.ml.store import FeatureStore
from bybit_flow.packing import compact, raw_bytes
from bybit_flow.recording_maintenance import classify, execute, planning
from bybit_flow.retention import prune_recordings
from bybit_flow.storage import Recorder, Store, now_ms


def tape(store, settings, at):
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
    return json.loads(
        store.db.execute("SELECT payload FROM segments ORDER BY at_ms DESC LIMIT 1").fetchone()[0]
    )


def packed(store, settings):
    ms = [tape(store, settings, i) for i in (1000, 2000, 3000)]
    return ms, Path(compact(store, limit=3)["archive"])


@pytest.mark.parametrize(
    "exc,code",
    [
        (zipfile.BadZipFile("broken"), "ARCHIVE_BAD_ZIP"),
        (KeyError("member missing in archive directory"), "ARCHIVE_MEMBER_MISSING"),
        (ValueError("Raw hash mismatch"), "RAW_HASH_MISMATCH"),
        (ValueError("Manifest mismatch"), "MANIFEST_MISMATCH"),
        (json.JSONDecodeError("invalid", "x", 0), "JSON_DECODE_ERROR"),
        (PermissionError("denied"), "PERMISSION_ERROR"),
        (TypeError("internal mistake"), "UNKNOWN_EXCEPTION"),
        (ValueError("Need at least 60 closed candles"), "CANDLE_HISTORY_INSUFFICIENT"),
    ],
)
def test_failure_causes_are_durable(settings, exc, code):
    store = Store(settings.data_dir)
    r = record_failure(
        store,
        exc,
        scan_id="one",
        phase="test",
        operation="read",
        candidate_id="candidate",
        source="binance",
        archive="a.zip",
        range_start_ms=1,
        range_end_ms=2,
    )
    assert r["cause_code"] == code and r["exception_type"] == type(exc).__name__
    assert r["exception_message"] and len(r["traceback_fingerprint"]) == 64
    assert failure_summary(store, "one")["without_durable_causes"] == 0
    store.close()
    store = Store(settings.data_dir)
    assert store.get("failure_causes_v212")[0]["candidate_id"] == "candidate"
    store.close()


@pytest.mark.parametrize("consumer", ["active", "unresolved", "lease"])
def test_corrupt_pack_protected(settings, signal, consumer):
    store = Store(settings.data_dir)
    ms, path = packed(store, settings)
    path.write_bytes(b"broken")
    if consumer == "active":
        signal.state = "ALERTED"
        store.signal(signal)
    elif consumer == "unresolved":
        FeatureStore(store).capture(signal, 1000, "decision")
    else:
        with store.db:
            store.db.execute(
                "INSERT INTO storage_leases VALUES(?,?,?,?,?)",
                ("reader", 0, 5000, now_ms() + 600000, "replay"),
            )
    prune_recordings(store, settings, 8 * 3600000, force=True)
    assert path.exists()
    health = json.loads(store.db.execute("SELECT payload FROM archive_health").fetchone()[0])
    assert health["disposition"] == "CORRUPT_BUT_PROTECTED" and health["protected_reason"]
    if consumer == "unresolved":
        ident = store.db.execute("SELECT id FROM ml_snapshots").fetchone()[0]
        FeatureStore(store).label(
            ident, dict(policy="prints-v1", complete=False, classification="incomplete", net_r=None), 10000
        )
        prune_recordings(store, settings, 8 * 3600000, force=True)
        assert not path.exists()
        assert (
            json.loads(store.db.execute("SELECT payload FROM ml_labels").fetchone()[0])["complete"] is False
        )
    store.close()


@pytest.mark.parametrize("interruption", [None, "before_mapping", "after_mapping"])
def test_mixed_pack_atomic_replacement(settings, monkeypatch, interruption):
    store = Store(settings.data_dir)
    ms, path = packed(store, settings)
    from bybit_flow import recording_maintenance as maintenance

    # Protection is retained by reachability, not by a transient reader lease.
    original = maintenance.classify

    def protect(st, m, *args, **kw):
        return ["PROTECTED_ACTIVE_SETUP"] if m["id"] == ms[0]["id"] else original(st, m, *args, **kw)

    monkeypatch.setattr(maintenance, "classify", protect)
    if interruption == "before_mapping":
        monkeypatch.setattr(
            maintenance,
            "replacement",
            lambda *args: (_ for _ in ()).throw(ValueError("simulated copy interruption")),
        )
    if interruption == "after_mapping":
        unlink = Path.unlink

        def failure(p, *args, **kw):
            if p == path:
                raise OSError("simulated unlink interruption")
            return unlink(p, *args, **kw)

        monkeypatch.setattr(Path, "unlink", failure)
    plan = planning(store, settings, 8 * 3600000, force=True)
    result = execute(store, settings, plan)
    assert hashlib.sha256(raw_bytes(ms[0]["raw"])).hexdigest() == ms[0]["sha256"]
    target = Path(
        store.db.execute("SELECT archive FROM segment_packs WHERE segment_id=?", (ms[0]["id"],)).fetchone()[0]
    )
    if interruption == "before_mapping":
        assert target == path and not store.get("pruned_segment:" + ms[1]["id"])
    else:
        assert target != path and target.exists()
        assert store.get("pruned_segment:" + ms[1]["id"])
        assert (
            store.db.execute("SELECT 1 FROM segment_packs WHERE segment_id=?", (ms[1]["id"],)).fetchone()
            is None
        )
        assert path.exists() == (interruption == "after_mapping")
    assert result["skipped_unverifiable"] == (interruption is not None)
    store.close()


def test_trainability_and_export_survive_raw_deletion(settings, signal):
    store = Store(settings.data_dir)
    tape(store, settings, 1000)
    signal.source = "binance"
    signal.feature_schema_version = "candidate-v20"
    features = FeatureStore(store)
    ident = features.capture(signal, 1000, "decision")
    features.label(
        ident, dict(policy="prints-v1", complete=True, classification="target", net_r=2, exit_ms=5000), 6000
    )
    before = features.dataset(10000, source="binance", schema_version="candidate-v20")
    assert len(before) == 1
    path = features.export(10000, source="binance")
    exported = path.read_bytes()
    result = prune_recordings(store, settings, 8 * 3600000, force=True)
    assert result["freed_bytes"] > 0
    assert features.dataset(10000, source="binance", schema_version="candidate-v20") == before
    assert features.export(10000, source="binance") == path and path.read_bytes() == exported
    assert store.get("cleanup_verification_v212")["status"] == "PASSED"
    store.close()


def test_unexpected_archive_fault_cannot_authorize_deletion(settings, monkeypatch):
    store = Store(settings.data_dir)
    ms, path = packed(store, settings)
    monkeypatch.setattr(
        zipfile.ZipFile, "read", lambda *a, **kw: (_ for _ in ()).throw(TypeError("software defect"))
    )
    plan = planning(store, settings, 8 * 3600000, force=True)
    assert all(m["classes"] == ["UNKNOWN_FAIL_CLOSED"] for m in plan["packs"][0]["members"])
    execute(store, settings, plan)
    assert path.exists() and not store.get("pruned_segment:" + ms[0]["id"])
    store.close()


def test_corrupt_pack_recovers_protected_member_from_healthy_original(settings, monkeypatch):
    store = Store(settings.data_dir)
    ms = [tape(store, settings, i) for i in (1000, 2000, 3000)]
    paths = [
        Path(ms[0]["raw"]),
        Path(ms[0]["parquet"]),
        Path(ms[0]["raw"]).with_name(ms[0]["id"] + ".manifest.json"),
    ]
    saved = {p: p.read_bytes() for p in paths}
    path = Path(compact(store, limit=3)["archive"])
    path.write_bytes(b"broken")
    for p, data in saved.items():
        p.write_bytes(data)
    from bybit_flow import recording_maintenance as maintenance

    original = maintenance.classify
    monkeypatch.setattr(
        maintenance,
        "classify",
        lambda st, m, *a, **kw: (
            ["PROTECTED_ACTIVE_SETUP"] if m["id"] == ms[0]["id"] else original(st, m, *a, **kw)
        ),
    )
    execute(store, settings, planning(store, settings, 8 * 3600000, force=True))
    assert not path.exists()
    for p in paths:
        p.unlink()  # Verify the replacement, not the redundant original.
    assert hashlib.sha256(raw_bytes(ms[0]["raw"])).hexdigest() == ms[0]["sha256"]
    store.close()


def test_scheduled_and_manual_cleanup_cannot_overlap(settings, monkeypatch):
    from bybit_flow import recording_maintenance as maintenance

    store = Store(settings.data_dir)
    ms, path = packed(store, settings)
    plan = planning(store, settings, 8 * 3600000, force=True)
    inventory = maintenance.ml_inventory
    observed = []

    def concurrent(*args, **kwargs):
        if not observed:
            observed.append(execute(store, settings, plan))
            assert path.exists() and not store.get("pruned_segment:" + ms[0]["id"])
        return inventory(*args, **kwargs)

    monkeypatch.setattr(maintenance, "ml_inventory", concurrent)
    result = execute(store, settings, plan)
    assert observed[0]["status"] == "cleanup already running"
    assert observed[0]["freed_bytes"] == 0
    assert result["freed_bytes"] > 0 and not path.exists()
    assert not store.get("cleanup_run_claim_v212")
    assert (store.root / "ml" / "maintenance" / (plan["run_id"].replace(":", "-") + "-result.json")).exists()
    store.close()


@pytest.mark.parametrize("addition", ["snapshot", "historical_label"])
def test_cleanup_freezes_existing_rows_while_learning_continues(settings, signal, monkeypatch, addition):
    from bybit_flow import recording_maintenance as maintenance

    store = Store(settings.data_dir)
    tape(store, settings, 1000)
    signal.source, signal.feature_schema_version = "binance", "candidate-v20"
    features = FeatureStore(store)
    ident = features.capture(signal, 2_000_000, "decision")
    plan = planning(store, settings, 8 * 3600000, force=True)
    other = Store(settings.data_dir)
    original = maintenance.classify
    added = []

    def publish_label(*args, **kwargs):
        if not added:
            target = ident
            if addition == "snapshot":
                target = FeatureStore(other).capture(
                    signal.model_copy(update={"id": "later-historical-candidate"}), 2_100_000, "decision"
                )
            FeatureStore(other).label(
                target,
                dict(policy="prints-v1", complete=True, classification="target", net_r=2, exit_ms=2_200_000),
                2_300_000,
            )
            added.append(target)
        return original(*args, **kwargs)

    monkeypatch.setattr(maintenance, "classify", publish_label)
    result = execute(store, settings, plan)
    assert result["freed_bytes"] > 0
    verification = store.get("cleanup_verification_v212")
    assert verification["status"] == "PASSED"
    assert verification["before"]["eligible"] == verification["after"]["eligible"]
    assert (
        FeatureStore(store).dataset(now_ms(), source="binance", schema_version="candidate-v20")[0]["id"]
        == added[0]
    )
    other.close()
    store.close()


def test_cleanup_still_stops_if_existing_label_disappears(settings, signal, monkeypatch):
    from bybit_flow import recording_maintenance as maintenance

    store = Store(settings.data_dir)
    manifest = tape(store, settings, 1000)
    features = FeatureStore(store)
    ident = features.capture(signal, 1000, "decision")
    features.label(
        ident, dict(policy="prints-v1", complete=True, classification="target", net_r=2, exit_ms=5000), 6000
    )
    plan = planning(store, settings, 8 * 3600000, force=True)
    original = maintenance.managed
    removed = []

    def lose_label(*args, **kwargs):
        if not removed:
            with store.db:
                store.db.execute("DELETE FROM ml_labels WHERE snapshot_id=?", (ident,))
            removed.append(ident)
        return original(*args, **kwargs)

    monkeypatch.setattr(maintenance, "managed", lose_label)
    with pytest.raises(RuntimeError, match="ML row identities changed"):
        execute(store, settings, plan)
    assert store.get("cleanup_verification_v212")["status"] == "FAILED"
    assert Path(manifest["raw"]).exists()
    assert not store.get("cleanup_run_claim_v212")
    store.close()


def test_stale_inventory_cannot_undo_archive_retirement(settings):
    from bybit_flow.recording_maintenance import migrate, save_health

    store = Store(settings.data_dir)
    migrate(store)
    stale = dict(archive="old.zip", disposition="ELIGIBLE_FOR_RETIREMENT", deleted_ms=None)
    save_health(store, stale | dict(disposition="RETIREMENT_COMMITTED"))
    save_health(store, stale)
    assert (
        json.loads(store.db.execute("SELECT payload FROM archive_health").fetchone()[0])["disposition"]
        == "RETIREMENT_COMMITTED"
    )
    save_health(store, stale | dict(disposition="RETIRED_CORRUPT", deleted_ms=1000))
    save_health(store, stale)
    assert (
        json.loads(store.db.execute("SELECT payload FROM archive_health").fetchone()[0])["deleted_ms"] == 1000
    )
    store.close()


def test_orphan_grace_writer_and_unknown_fail_closed(settings):
    store = Store(settings.data_dir)
    root = store.root / "segments"
    root.mkdir(exist_ok=True)
    orphan = root / "abandoned.tmp"
    active = root / "active.tmp"
    for p in (orphan, active):
        p.write_bytes(b"unused")
        os.utime(p, (1, 1))
    store.put("writer_batch_v212:test", dict(paths=[str(active)]))
    plan = planning(store, settings, now_ms(), force=True)
    assert str(orphan) in [r["path"] for r in plan["orphans"]]
    assert str(active) not in [r["path"] for r in plan["orphans"]]
    execute(store, settings, plan)
    assert not orphan.exists() and active.exists()
    assert classify(store, dict(id="unknown", min_receipt_ms=None, max_receipt_ms=1), 10, 5, []) == [
        "UNKNOWN_FAIL_CLOSED"
    ]
    store.close()
