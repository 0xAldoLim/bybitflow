import json

import numpy as np
import pytest
from test_ml_training import dataset

from bybit_flow.ml.models import calibrate, fit, margins, predict, records
from bybit_flow.ml.stacking import SEQUENCE_KEYS, fit_stack, sequence_values
from bybit_flow.ml.store import FeatureStore
from bybit_flow.storage import Store


def sequential(rows):
    for row in rows:
        row["sequence"] = [
            dict(
                at_ms=row["decision_ms"] - (15 - i) * 60_000,
                values={k: row["values"].get(k) for k in SEQUENCE_KEYS},
            )
            for i in range(16)
        ]
    return rows


def test_svm_json_margins_match_library(signal):
    rows = dataset(signal, 400)
    artifact, pipe = fit(rows[:250], "svm")
    decoded = json.loads(json.dumps(artifact))
    assert np.allclose(margins(decoded, rows[250:]), pipe.decision_function(records(rows[250:])))
    calibrate(decoded, rows[250:350])
    assert np.all(np.isfinite(predict(decoded, rows[350:])))


def test_sequence_capture_is_causal_and_keeps_history(settings, signal):
    store = Store(settings.data_dir)
    fs = FeatureStore(store)
    for i in range(3):
        s = signal.model_copy(deep=True)
        s.id = f"sequence-{i}"
        s.created_ms = 1000 + i * 60_000
        fs.capture(s, s.created_ms, "decision")
    rows = list(fs.snapshots())
    assert [len(r["sequence"]) for r in rows] == [1, 2, 3]
    assert rows[-1]["sequence"][0]["at_ms"] == rows[0]["decision_ms"]
    with pytest.raises(ValueError, match="16 causal"):
        sequence_values(rows)
    store.close()


@pytest.mark.parametrize("meta", ["logistic", "svm", "random_forest"])
def test_two_stage_json_predictions_and_temporal_separation(signal, meta):
    pytest.importorskip("torch")
    rows = sequential(dataset(signal, 650))
    artifact = fit_stack(rows[:500], meta)
    split = artifact["stage_split"]
    assert split["last_base_label_ms"] + 14_400_000 < split["boundary_ms"]
    decoded = json.loads(json.dumps(artifact))
    calibrate(decoded, rows[500:600])
    p = predict(decoded, rows[600:])
    assert p.shape == (50,) and np.all(np.isfinite(p)) and np.all((0 <= p) & (p <= 1))
    bad = sequential(dataset(signal, 1))
    bad[0]["sequence"][-1]["at_ms"] += 1
    with pytest.raises(ValueError, match="causal"):
        predict(decoded, bad)


def test_two_stage_full_training_registry_and_holdout(settings, signal):
    pytest.importorskip("torch")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from bybit_flow.ml.registry import Registry
    from bybit_flow.ml.training import train

    rows = sequential(dataset(signal, 1600))
    path = settings.data_dir / "synthetic-sequences.parquet"
    pq.write_table(pa.Table.from_pylist([{"payload": json.dumps(r)} for r in rows]), path)
    store = Store(settings.data_dir)
    result = train(store, path, kinds=("two_stage_logistic",))
    saved = Registry(store).get(result["id"])
    assert saved["model"]["kind"] == "two_stage_logistic"
    assert set(saved["model"]["base_models"]) == {"lightgbm", "random_forest"}
    assert saved["model"]["lstm"]["training_samples"] >= 200
    assert saved["report"]["out_of_sample"]
    assert "torch" in saved["packages"]
    assert Registry(store).champion() is None
    store.close()
