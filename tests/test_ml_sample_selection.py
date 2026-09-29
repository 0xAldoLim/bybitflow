import json

from bybit_flow.ml import SCHEMA_VERSION
from bybit_flow.ml.operations import trainability
from bybit_flow.ml.store import FeatureStore
from bybit_flow.storage import Store


def add_snapshot(store, ident, at_ms, *, source="binance", schema=SCHEMA_VERSION, identity=None, label=None):
    signal_id = "signal-" + ident
    payload = dict(
        signal_id=signal_id,
        decision_ms=at_ms,
        schema_version=schema,
        source=source,
        sequence=[{}] * 16,
    )
    with store.db:
        store.db.execute(
            "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
            (ident, signal_id, "decision", at_ms, schema, json.dumps(payload)),
        )
        store.db.execute(
            "INSERT INTO candidate_identities VALUES(?,?,?,?,?)",
            (signal_id, ident, identity or signal_id, signal_id, at_ms),
        )
        if label is not None:
            store.db.execute(
                "INSERT INTO ml_labels VALUES(?,?,?,?)",
                (ident, "prints-v1", at_ms + 1, json.dumps(label)),
            )


def completed(at_ms):
    return dict(complete=True, classification="win", net_r=1.0, exit_ms=at_ms + 1)


def test_limit_applies_after_eligibility_and_identity(settings):
    store = Store(settings.data_dir)
    for i in range(4):
        add_snapshot(store, f"good-{i}", 100 + i, label=completed(100 + i))
    add_snapshot(store, "incomplete", 200, identity="signal-good-3", label={"complete": False})
    add_snapshot(
        store,
        "duplicate",
        201,
        label=dict(complete=False, classification="technical_duplicate", net_r=None),
    )
    add_snapshot(store, "wrong-source", 202, source="bybit", label=completed(202))
    add_snapshot(store, "wrong-schema", 203, schema="candidate-old", label=completed(203))
    add_snapshot(store, "unresolved", 204)

    rows = FeatureStore(store).dataset(1000, limit=3, source="binance", schema_version=SCHEMA_VERSION)
    assert [r["id"] for r in rows] == ["good-1", "good-2", "good-3"]
    assert (
        FeatureStore(store).dataset(1000, limit=10, source="binance", schema_version=SCHEMA_VERSION)[-1]["id"]
        == "good-3"
    )
    readiness = trainability(store, 1000)["by_source"]
    assert readiness["binance"]["trainable_current_schema"] == 4
    assert readiness["binance"]["sequence_ready_16"] == 4
    assert readiness["bybit"]["trainable_current_schema"] == 1
    store.close()


def test_trainability_counts_shared_identity_once(settings):
    store = Store(settings.data_dir)
    add_snapshot(store, "first", 100, identity="same", label=completed(100))
    add_snapshot(store, "second", 200, identity="same", label=completed(200))
    rows = FeatureStore(store).dataset(1000, limit=10, source="binance", schema_version=SCHEMA_VERSION)
    assert [r["id"] for r in rows] == ["second"]
    readiness = trainability(store, 1000)["by_source"]["binance"]
    assert readiness["complete_labels"] == 2
    assert readiness["unique_complete_candidates"] == 1
    assert readiness["baseline_trainable"] == 1
    store.close()
