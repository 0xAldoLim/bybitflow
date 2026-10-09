import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bybit_flow.ml.features import snapshot
from bybit_flow.ml.models import calibrate, fit, predict, records
from bybit_flow.ml.registry import Registry
from bybit_flow.ml.store import FeatureStore, canonical
from bybit_flow.ml.training import train
from bybit_flow.ml.validation import HOUR, accepted, next_cycle_partitions, partitions, walk_forward
from bybit_flow.storage import Store

pytest.importorskip("sklearn")


def dataset(signal, n=1600):
    rng = np.random.default_rng(11)
    rows = []
    for i in range(n):
        s = signal.model_copy(deep=True)
        s.id, s.created_ms = f"synthetic-{i}", 1_600_000_000_000 + i * 6 * HOUR
        s.expires_ms = s.created_ms + HOUR
        x = float(rng.normal())
        s.evidence = {"flow": {"delta_pct": x * 30}, "h4": {"efficiency": max(0.0, min(1.0, x / 4 + 0.5))}}
        s.quality = 96 if x > 0 else 70
        s.risk = {"accepted": True, "net_rr": 2.5}
        row = snapshot(s, s.created_ms, "decision")
        row["id"] = s.id
        row["label"] = dict(
            complete=True,
            net_r=2.0 if x + rng.normal() * 0.3 > 0 else -1.0,
            exit_ms=s.created_ms + HOUR,
            data_kind="SYNTHETIC SOFTWARE TEST",
            costs_verified=False,
        )
        row["label_available_ms"] = s.created_ms + 2 * HOUR
        rows.append(row)
    return rows


@pytest.mark.parametrize("kind", ["logistic", "lightgbm", "random_forest"])
def test_safe_json_prediction_parity_and_calibration(signal, kind):
    if kind == "lightgbm":
        pytest.importorskip("lightgbm")
    rows = dataset(signal, 500)
    artifact, pipe = fit(rows[:250], kind)
    decoded = json.loads(canonical(artifact))
    assert np.allclose(predict(decoded, rows[350:]), pipe.predict_proba(records(rows[350:]))[:, 1])
    calibrate(decoded, rows[250:350])
    assert len(predict(decoded, rows[350:])) == 150
    with pytest.raises(ValueError, match="1000"):
        calibrate(decoded, rows[250:350], "isotonic")


def test_purging_and_same_timestamp_grouping(signal):
    rows = dataset(signal, 700)
    rows[145]["label_available_ms"] = rows[250]["decision_ms"]
    parts = partitions(rows)
    assert rows[145] not in parts[0]
    for a, b in zip(parts, parts[1:]):
        assert max(r["label_available_ms"] for r in a) + 4 * HOUR < min(r["decision_ms"] for r in b)
    for tr, ca, te in walk_forward(rows):
        assert max(r["label_available_ms"] for r in tr) + 4 * HOUR < ca[0]["decision_ms"]
        assert max(r["label_available_ms"] for r in ca) + 4 * HOUR < te[0]["decision_ms"]
    with pytest.raises(ValueError, match="safety"):
        accepted(rows[0], 0.9, {"leverage": 20})


def test_new_cycle_never_reuses_old_holdout(signal):
    rows = dataset(signal, 700)
    consumed = rows[350]["label_available_ms"]
    tr, ca, va, ho = next_cycle_partitions(rows, consumed)
    assert all(r["decision_ms"] > consumed + 4 * HOUR for r in ho)
    assert max(r["label_available_ms"] for r in va) + 4 * HOUR < ho[0]["decision_ms"]
    assert not {r["id"] for r in ho} & {r["id"] for r in tr + ca + va}


def test_reproducible_training_registry_holdout_and_no_fake_promotion(settings, signal, monkeypatch):
    from bybit_flow.ml import training

    rows = dataset(signal)
    # Reverse ID order at a shared decision time; no tied holdout observation
    # may enter walk-forward research through an input-order list slice.
    boundary = partitions(rows)[3][0]["decision_ms"]
    first = next(i for i, row in enumerate(rows) if row["decision_ms"] == boundary)
    rows[first + 1]["decision_ms"] = boundary
    rows[first + 1]["feature_metadata"] = rows[first]["feature_metadata"]
    rows[first]["id"], rows[first + 1]["id"] = "z-holdout", "a-holdout"
    research_inputs = []
    original_walk_forward = training.walk_forward

    def observed_walk_forward(development):
        research_inputs.extend(development)
        return original_walk_forward(development)

    monkeypatch.setattr(training, "walk_forward", observed_walk_forward)
    path = settings.data_dir / "synthetic.parquet"
    pq.write_table(pa.Table.from_pylist([{"payload": canonical(r)} for r in rows]), path)
    store = Store(settings.data_dir)
    result = train(store, path, kinds=("logistic",))
    registry = Registry(store)
    assert registry.get(result["id"])["dataset_hash"] == result["dataset_hash"]
    assert result["report"]["out_of_sample"]
    assert result["model"]["kind"] == "logistic"
    assert len(result["report"]["experiments"]) == 3
    assert research_inputs
    assert all(row["decision_ms"] < result["periods"]["holdout"]["start"] for row in research_inputs)
    assert (
        result["training_spec"]["partition_planner"]
        == result["report"]["partition_feasibility"]["partition_policy_used"]
    )
    assert result["report"]["model_fit_feasibility"]["fit_ready"]
    with pytest.raises(ValueError, match="real data"):
        registry.promote(result["id"], "test reviewer")
    assert registry.champion() is None
    with pytest.raises(ValueError, match="consumed"):
        train(store, path, kinds=("logistic",))
    with store.db:
        store.db.execute("UPDATE ml_models SET sha256='bad'")
    with pytest.raises(ValueError, match="integrity"):
        registry.get(result["id"])
    store.close()


def test_bootstrap_fit_keeps_holdouts_separate_and_never_promotes(settings, signal):
    from bybit_flow.ml.bootstrap_features import project

    rows = dataset(signal)
    store = Store(settings.data_dir)
    primary = train(store, FeatureStore(store).write_dataset(rows), kinds=("logistic",))
    proxy_rows = []
    for row in rows:
        row["label"].update(
            policy="ohlc-path-v1",
            data_kind="original-venue-public-1m-OHLC",
            execution_fidelity="proxy",
            source=row["source"],
            event_available_ms=row["label_available_ms"],
            production_execution_verified=False,
        )
        # Bootstrap retains its supported legacy schema; overlapping field
        # names do not make the new v20 score compatible with that projection.
        proxy_rows.append(project(row | dict(schema_version="candidate-v10")))
    bootstrap = train(
        store, FeatureStore(store).write_dataset(proxy_rows), kinds=("logistic",), track="bootstrap"
    )
    assert bootstrap["holdout_scope"] != primary["holdout_scope"]
    assert bootstrap["periods"]["holdout"] == primary["periods"]["holdout"]
    assert bootstrap["label_fidelity"] == "OHLC_PROXY"
    assert not bootstrap["promotion_eligible"] and not bootstrap["production_filter_eligible"]
    assert "quality_score" not in bootstrap["model"]["vocabulary"]
    with pytest.raises(ValueError, match="cannot be promoted"):
        Registry(store).promote(bootstrap["id"], "Test reviewer")
    with pytest.raises(ValueError, match="consumed"):
        train(store, FeatureStore(store).write_dataset(proxy_rows), kinds=("logistic",), track="bootstrap")
    store.close()
