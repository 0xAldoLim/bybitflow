import json

from bybit_flow.ml import SCHEMA_VERSION, bootstrap, cli
from bybit_flow.ml.operations import cache_partition_feasibility, trainability
from bybit_flow.ml.registry import Registry
from bybit_flow.storage import Store

HOUR = 3_600_000


def timing_rows(count=700):
    return [
        dict(
            id=str(i),
            candidate_identity=str(i),
            source="binance",
            decision_ms=1_000_000_000 + i * HOUR,
            label_available_ms=1_000_060_000 + i * HOUR,
            label=dict(complete=True, exit_ms=1_000_060_000 + i * HOUR, net_r=1),
        )
        for i in range(count)
    ]


def test_partition_cache_respects_track_source_and_legacy_holdouts(settings):
    store = Store(settings.data_dir)
    prior = 1_000_000_000 + 300 * HOUR
    with store.db:
        store.db.executemany(
            "INSERT INTO ml_holdouts(start_ms,end_ms,experiment_id,scope) VALUES(?,?,?,?)",
            [
                (1, prior, "legacy", "legacy-primary"),
                (1, prior + HOUR, "other-source", f"primary|bybit|{SCHEMA_VERSION}|prints-v1"),
                (1, prior + 2 * HOUR, "bootstrap", "bootstrap|binance|bootstrap-core-v1|ohlc-path-v1"),
            ],
        )
    primary = cache_partition_feasibility(
        store, timing_rows(), "primary", "binance", SCHEMA_VERSION, "prints-v1"
    )
    proxy = cache_partition_feasibility(
        store, timing_rows(), "bootstrap", "binance", "bootstrap-core-v1", "ohlc-path-v1"
    )
    assert primary["previous_holdout_end"] == prior
    assert proxy["previous_holdout_end"] == prior + 2 * HOUR
    assert primary["status"] == proxy["status"] == "READY"
    assert primary["chosen_boundaries"]["validation_end"] > prior + 4 * HOUR
    assert proxy["chosen_boundaries"]["validation_end"] > prior + 6 * HOUR
    store.close()


def test_bootstrap_readiness_plans_already_loaded_rows_once(settings, monkeypatch):
    store = Store(settings.data_dir)
    calls = []

    def dataset(_store, source, asof_ms):
        calls.append(source)
        return timing_rows() if source == "binance" else []

    monkeypatch.setattr(bootstrap, "dataset", dataset)
    result = bootstrap.readiness(store)
    assert calls == ["binance", "bybit", "okx"]
    plan = result["binance"]["partition_feasibility"]
    assert plan["status"] == "READY" and plan["total_rows"] == 700
    assert plan == store.get("ml_partition_feasibility:bootstrap:binance:bootstrap-core-v1:ohlc-path-v1")
    assert "partition_feasibility" not in result["bybit"]
    store.close()


def test_primary_scalar_readiness_uses_causal_unique_rows_and_caches_plan(settings):
    store = Store(settings.data_dir)
    rows = timing_rows()
    with store.db:
        for row in rows:
            payload = row | dict(schema_version=SCHEMA_VERSION, sequence=[])
            store.db.execute(
                "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
                (
                    row["id"],
                    "signal-" + row["id"],
                    "decision",
                    row["decision_ms"],
                    SCHEMA_VERSION,
                    json.dumps(payload),
                ),
            )
            store.db.execute(
                "INSERT INTO ml_labels VALUES(?,?,?,?)",
                (row["id"], "prints-v1", row["label_available_ms"], json.dumps(row["label"])),
            )
        # A second observation of the same candidate must not inflate planning.
        row = rows[-1]
        with_identity = row | dict(decision_ms=row["decision_ms"] + 1, schema_version=SCHEMA_VERSION)
        store.db.execute(
            "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
            (
                "duplicate",
                "signal-duplicate",
                "decision",
                with_identity["decision_ms"],
                SCHEMA_VERSION,
                json.dumps(with_identity),
            ),
        )
        store.db.execute(
            "INSERT INTO ml_labels VALUES(?,?,?,?)",
            ("duplicate", "prints-v1", row["label_available_ms"], json.dumps(row["label"])),
        )
        for signal_id in ("signal-" + row["id"], "signal-duplicate"):
            store.db.execute(
                "INSERT INTO candidate_identities VALUES(?,?,?,?,?)",
                (signal_id, signal_id, "shared", signal_id, row["decision_ms"]),
            )
    result = trainability(store)["by_source"]["binance"]
    assert result["raw_complete_snapshots"] == 701
    assert result["baseline_trainable"] == 700
    assert result["partition_feasibility"]["total_rows"] == 700
    assert result["partition_feasibility"]["status"] == "READY"
    store.close()


def test_worker_persists_partition_abstention_reason_and_status_uses_cache(settings, monkeypatch):
    store = Store(settings.data_dir)
    plan = dict(status="NOT_READY", total_rows=700, blocker="calibration", shortfall={"calibration": 17})
    store.put("ml_bootstrap_trainability", {"binance": {"trainable": 700}})
    store.put("ml_partition_feasibility:bootstrap:binance:bootstrap-core-v1:ohlc-path-v1", plan)
    reason = "No causal 200/100/100/100 partition exists after 4h embargo: train=201, calibration=83, validation=26, holdout=404"

    def fail(*_args):
        raise ValueError(reason)

    monkeypatch.setattr(cli, "monitor", lambda *_: None)
    monkeypatch.setattr(bootstrap, "train", fail)
    result = cli.worker_step(settings, store, manual=True)
    fit = store.get("ml_fit:bootstrap:binance:bootstrap-core-v1:ohlc-path-v1")
    assert fit["status"] == "abstained" and fit["partition_feasibility"] == plan
    assert store.get("ml_cycle")["reason"] == result["reason"] == "bootstrap:binance: " + reason

    def no_payload_scan(*_args, **_kwargs):
        raise AssertionError("Normal status must consume cached planning diagnostics")

    monkeypatch.setattr(bootstrap, "readiness", no_payload_scan)
    monkeypatch.setattr(bootstrap, "dataset", no_payload_scan)
    report = Registry(store).summary()
    assert report["last_training_reason"] == "bootstrap:binance: " + reason
    assert report["partition_feasibility"]["bootstrap"]["binance"] == plan
    assert Registry(store).cached_summary()["partition_feasibility"]["bootstrap"]["binance"] == plan
    store.close()


def test_status_does_not_invent_ready_before_worker_plans(settings):
    store = Store(settings.data_dir)
    store.put("ml_bootstrap_trainability", {"binance": {"trainable": 700}, "okx": {"trainable": 7}})
    report = Registry(store).summary()["partition_feasibility"]
    assert report["bootstrap"]["binance"]["status"] == "AWAITING_WORKER_PLAN"
    assert "okx" not in report["bootstrap"]
    store.close()
