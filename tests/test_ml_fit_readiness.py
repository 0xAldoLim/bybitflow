from copy import deepcopy

import pytest

from bybit_flow.ml import cli, training
from bybit_flow.ml.operations import cache_partition_feasibility
from bybit_flow.ml.readiness import model_fit_feasibility
from bybit_flow.ml.registry import Registry
from bybit_flow.ml.validation import HOUR, plan_partitions
from bybit_flow.storage import Store


def evidence(count=900):
    return [
        dict(
            id=str(i),
            candidate_identity=str(i),
            source="binance",
            decision_ms=1_000_000_000 + i * HOUR,
            label_available_ms=1_000_060_000 + i * HOUR,
            label=dict(complete=True, exit_ms=1_000_060_000 + i * HOUR, net_r=1 if i % 2 else -1),
        )
        for i in range(count)
    ]


@pytest.mark.parametrize(
    "single,blocker",
    [(None, None), ("training", "TRAIN_SINGLE_CLASS"), ("calibration", "CALIBRATION_SINGLE_CLASS")],
)
def test_classes_diagnose_fit_without_changing_partitions(single, blocker):
    data = evidence()
    plan, groups = plan_partitions(data)
    modified = deepcopy(data)
    if single:
        ids = {r["id"] for r in groups[0 if single == "training" else 1]}
        for r in modified:
            if r["id"] in ids:
                r["label"]["net_r"] = -1
    later, later_groups = plan_partitions(modified)
    assert later["chosen_boundaries"] == plan["chosen_boundaries"]
    report = model_fit_feasibility(later, later_groups)
    assert report["partition_ready"]
    assert report["blocker"] == blocker
    assert report["fit_ready"] == (blocker is None)
    for name in ("training", "calibration", "validation", "holdout"):
        assert report[name + "_positive"] + report[name + "_negative"] == report[name + "_rows"]


def test_partition_failure_is_not_misreported_as_single_class():
    plan, groups = plan_partitions(evidence(400))
    report = model_fit_feasibility(plan, groups)
    assert not report["fit_ready"] and report["blocker"] == "PARTITION_NOT_READY"
    assert report["training_positive"] is None


def test_training_abstains_before_fit_and_holdout_on_single_class(settings, monkeypatch):
    store = Store(settings.data_dir)
    data = evidence()
    _, groups = plan_partitions(data)
    for r in groups[1]:
        r["label"]["net_r"] = -1
    for r in data:
        r.update(schema_version="bootstrap-core-v1")
    monkeypatch.setattr(training, "read_dataset", lambda *_a, **_k: data)
    monkeypatch.setattr(
        training, "fit", lambda *_a, **_k: pytest.fail("Single-class data must never reach fitting")
    )
    with pytest.raises(ValueError, match="CALIBRATION_SINGLE_CLASS"):
        training.train(store, settings.data_dir / "unused.parquet", track="bootstrap")
    assert not store.db.execute("SELECT 1 FROM ml_holdouts").fetchone()
    assert not store.db.execute("SELECT 1 FROM ml_models").fetchone()
    store.close()


def test_cached_fit_status_and_model_library_error(settings, monkeypatch):
    store = Store(settings.data_dir)
    store.put("ml_bootstrap_trainability", {"binance": {"trainable": 900}})
    cache_partition_feasibility(
        store, evidence(), "bootstrap", "binance", "bootstrap-core-v1", "ohlc-path-v1"
    )
    assert Registry(store).cached_summary()["model_fit_feasibility"]["bootstrap"]["binance"]["fit_ready"]
    from bybit_flow.ml import bootstrap

    def fail(*_):
        raise RuntimeError("Test model library error")

    monkeypatch.setattr(bootstrap, "train", fail)
    result = cli.train_ready(settings, store, manual=True)
    report = result["tracks"]["bootstrap:binance"]["model_fit_feasibility"]
    assert report["blocker"] == "MODEL_FIT_ERROR" and not report["fit_ready"]
    assert "Test model library error" in result["reason"]
    store.close()


def test_first_fit_ready_bootstrap_attempt_is_immediate(settings, monkeypatch):
    from bybit_flow.ml import bootstrap

    store = Store(settings.data_dir)
    store.put("ml_bootstrap_trainability", {"binance": {"trainable": 900}})
    cache_partition_feasibility(
        store, evidence(), "bootstrap", "binance", "bootstrap-core-v1", "ohlc-path-v1"
    )
    calls = []

    def fit(_store, source):
        calls.append(source)
        return {"id": "test-challenger"}

    monkeypatch.setattr(bootstrap, "train", fit)
    result = cli.train_ready(settings, store)
    assert calls == ["binance"]
    assert result["tracks"]["bootstrap:binance"]["status"] == "challenger"
    assert cli.train_ready(settings, store)["status"] == "collecting"
    assert calls == ["binance"]
    store.close()
