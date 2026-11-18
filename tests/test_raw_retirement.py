import json
from pathlib import Path

import pytest
from test_recording_maintenance import tape

from bybit_flow import raw_retirement as retirement
from bybit_flow.cleanup_reconciliation import LATCH
from bybit_flow.leases import RangeLease
from bybit_flow.ml.store import FeatureStore
from bybit_flow.packing import compact, raw_bytes
from bybit_flow.storage import Store, now_ms


def add_decision(store, signal, at=1_000_000, classification=None, complete=False):
    features = FeatureStore(store)
    ident = features.capture(signal, at, "decision")
    if classification is not None:
        features.label(
            ident,
            dict(
                policy="prints-v1",
                complete=complete,
                classification=classification,
                exit_ms=at + 100_000,
                net_r=2 if complete else None,
            ),
            at + 100_000,
        )
    return ident


@pytest.mark.parametrize("classification", ["incomplete", "technical_duplicate", "no_entry", None])
def test_retires_only_target_raw_and_preserves_old_latch_and_proofs(settings, signal, classification):
    store = Store(settings.data_dir)
    at = 1_000_000
    if classification is not None:
        add_decision(store, signal, at, classification)
    manifest = tape(store, settings, at + 1000)
    old = dict(status="FAILED", run_id="historical", reason="legacy audit")
    store.put(LATCH, old)
    store.put("cleanup_reconciliation", dict(state="AUDIT_UNAVAILABLE"))
    before = {t: [tuple(r) for r in store.db.execute(f"SELECT * FROM {t}")] for t in retirement.TABLES}
    summary = retirement.dry_run(store.root, settings)
    category = retirement.ELIGIBLE[0 if classification is not None else 1]
    assert summary["counts"][category] == 1
    assert store.get(retirement.STATE) is None and Path(manifest["raw"]).exists()
    result = retirement.run(store, settings)
    assert result["status"] == "PASSED" and result["retired_segments"] == 1
    assert result["freed_bytes"] > 0 and not Path(manifest["raw"]).exists()
    assert result["freed_by_class"][category] == result["freed_bytes"]
    assert store.get(LATCH) == old
    assert store.get("cleanup_reconciliation") == dict(state="AUDIT_UNAVAILABLE")
    assert before == {
        t: [tuple(r) for r in store.db.execute(f"SELECT * FROM {t}")] for t in retirement.TABLES
    }
    tombstone = store.get("pruned_segment:" + manifest["id"])
    assert (
        tombstone["policy"] == retirement.POLICY and tombstone["reason_code"] == retirement.REASONS[category]
    )
    assert tombstone["sha256"] == manifest["sha256"] and tombstone["raw_bytes_freed"] == result["freed_bytes"]
    root = store.root / "ml" / "raw-retirement"
    pre = json.loads((root / (result["run_id"] + "-pre.json")).read_bytes())
    post = json.loads((root / (result["run_id"] + "-post.json")).read_bytes())
    assert pre["frozen_rows"] == post["frozen_rows"]
    assert pre["ml"]["eligible"] == post["ml"]["eligible"]
    store.close()


@pytest.mark.parametrize("protection", ["active", "unresolved", "writer", "recent", "complete"])
def test_committed_consumers_and_complete_raw_are_excluded(settings, signal, protection):
    store = Store(settings.data_dir)
    at = now_ms() - 1000 if protection == "recent" else 1_000_000
    manifest = tape(store, settings, at)
    if protection == "active":
        store.signal(signal)
    if protection in {"unresolved", "complete"}:
        add_decision(store, signal, at, "win" if protection == "complete" else None, protection == "complete")
    if protection == "writer":
        store.put("writer_batch_v212:test", dict(paths=[manifest["raw"]]))
    assert retirement.run(store, settings)["status"] == "NO_ELIGIBLE_RAW"
    assert Path(manifest["raw"]).exists()
    assert store.get("pruned_segment:" + manifest["id"]) is None
    store.close()


def test_retirement_yields_to_lease_and_recorder_pressure(settings):
    store = Store(settings.data_dir)
    manifest = tape(store, settings, 1000)
    with RangeLease(store, 500, 1500, "replay"):
        assert retirement.run(store, settings)["status"] == "NO_ELIGIBLE_RAW"
    store.put("recorder_health", dict(queue_depth=75, queue_capacity=100))
    assert retirement.run(store, settings)["status"] == "DEFERRED"
    assert Path(manifest["raw"]).exists()
    store.close()


@pytest.mark.parametrize("change", ["mutated", "new"])
def test_frozen_update_stops_next_segment_but_new_rows_allowed(settings, signal, monkeypatch, change):
    store = Store(settings.data_dir)
    frozen = add_decision(store, signal, classification="incomplete")
    first = tape(store, settings, 1_010_000)
    second = tape(store, settings, 1_020_000)
    original = retirement.retire_unit
    calls = 0

    def changed(*args):
        nonlocal calls
        original(*args)
        calls += 1
        if calls == 1:
            if change == "mutated":
                with store.db:
                    store.db.execute(
                        "UPDATE ml_labels SET payload=json_set(payload,'$.mutated',1) WHERE snapshot_id=?",
                        (frozen,),
                    )
            else:
                add_decision(store, signal.model_copy(update={"id": "later"}), now_ms())

    monkeypatch.setattr(retirement, "retire_unit", changed)
    result = retirement.run(store, settings)
    assert not Path(first["raw"]).exists()
    if change == "mutated":
        assert result["status"] == "FAILED" and "ml_labels" in result["reason"]
        assert Path(second["raw"]).exists()
        assert retirement.run(store, settings)["status"] == "BLOCKED"
    else:
        assert result["status"] == "PASSED" and not Path(second["raw"]).exists()
    store.close()


def test_mixed_pack_rebuild_preserves_protected_member(settings, signal):
    store = Store(settings.data_dir)
    first, second = [tape(store, settings, at) for at in (1000, 10_000_000)]
    old = Path(compact(store, limit=2)["archive"])
    protected = raw_bytes(Path(second["raw"]))
    store.signal(signal.model_copy(update={"created_ms": 10_000_000, "expires_ms": 20_000_000}))
    assert retirement.dry_run(store.root, settings)["packs_requiring_repack"] == 1
    result = retirement.run(store, settings)
    assert result["status"] == "PASSED" and result["retired_segments"] == result["packs_rebuilt"] == 1
    assert not old.exists()
    assert store.get("pruned_segment:" + first["id"])
    assert store.get("pruned_segment:" + second["id"]) is None
    assert raw_bytes(Path(second["raw"])) == protected
    store.close()
