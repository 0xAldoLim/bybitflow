from bybit_flow.ml import cli
from bybit_flow.ml.maturity import progress, start_epoch
from bybit_flow.ml.operations import trainability
from bybit_flow.ml.registry import Registry
from bybit_flow.ml.store import FeatureStore
from bybit_flow.storage import Store


def test_verified_deployment_creates_epoch_and_restart_cycle_preserve_it(settings, monkeypatch):
    store = Store(settings.data_dir)
    cli.run(["maturity-start", "--code-commit", "a" * 40], settings, store)
    original = store.get("ml_maturity_epoch")
    assert original["code_commit"] == "a" * 40
    assert original["candidate_schema"] == "candidate-v10"
    assert original["primary_label_policy"] == "prints-v1"
    assert original["partition_policy"] == "adaptive-causal-v2"
    store.close()
    store = Store(settings.data_dir)
    assert start_epoch(store, "b" * 40) == original
    monkeypatch.setattr(cli, "monitor", lambda *_: None)
    cli.worker_step(settings, store, first_monitor=True)
    assert store.get("ml_maturity_epoch") == original
    assert Registry(store).cached_summary()["ml_maturity_epoch"] == original
    assert Registry(store).summary()["maturity"]["epoch_started_ms"] == original["started_ms"]
    store.close()


def test_maturity_progress_is_source_specific_and_uses_cached_diagnostics(settings):
    store = Store(settings.data_dir)
    epoch = start_epoch(store, "a" * 40)
    store.put(
        "ml_trainability_cache",
        dict(
            active_source="binance",
            by_source={
                "binance": dict(
                    trainable_current_schema=300,
                    sequence_ready_16=20,
                    current_schema_decisions=700,
                    epoch_current_schema_decisions=5,
                ),
                "okx": dict(
                    trainable_current_schema=300,
                    sequence_ready_16=10,
                    current_schema_decisions=400,
                    epoch_current_schema_decisions=2,
                ),
            },
        ),
    )
    store.put("ml_bootstrap_trainability", {"binance": {"complete_unique": 3724}})
    store.put("storage_status", dict(used_bytes=23_000_000_000, budget_bytes=30_000_000_000))
    store.put("recorder_health", dict(events_dropped=0))
    report = progress(store, "primary-test", "bootstrap-test", epoch["started_ms"] + 86_400_000)
    assert report["epoch_age_days"] == 1
    assert report["candidate_v10_decisions"] == 1100 and report["epoch_candidate_v10_decisions"] == 7
    assert report["primary_progress_pct"] == 60  # 300 per source cannot be pooled into 600.
    assert report["sequence_progress_pct"] == 4
    assert report["storage_state"] == "HEALTHY" and report["recorder_drop_count"] == 0
    assert report["bootstrap_model_id"] == "bootstrap-test"
    store.close()


def test_worker_reuses_scalar_timing_pass_for_epoch_decisions(settings, signal):
    store = Store(settings.data_dir)
    store.put("ml_maturity_epoch", {"started_ms": 2000})
    signal.source = "binance"
    FeatureStore(store).capture(signal, 1000, "decision")
    signal.id = "after-epoch"
    FeatureStore(store).capture(signal, 3000, "decision")
    detail = trainability(store, 4000)["by_source"]["binance"]
    assert detail["current_schema_decisions"] == 2
    assert detail["epoch_current_schema_decisions"] == 1
    store.close()
