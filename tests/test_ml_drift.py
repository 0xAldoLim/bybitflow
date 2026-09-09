import json

import pytest

from bybit_flow.ml.drift import check
from bybit_flow.ml.models import fit
from bybit_flow.ml.registry import Registry
from bybit_flow.ml.store import FeatureStore
from bybit_flow.storage import Store

pytest.importorskip("sklearn")


def test_distribution_shift_degrades_and_sparse_data_abstains(settings, signal, monkeypatch):
    store = Store(settings.data_dir)
    fs = FeatureStore(store)
    training = []
    for i in range(100):
        s = signal.model_copy(deep=True)
        s.id = "drift-training-" + str(i)
        s.evidence = {"flow": {"delta_pct": i % 2}}
        ident = fs.capture(s, 2000, "decision")
        row = json.loads(
            store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
        )
        row["label"] = dict(net_r=1 if i % 2 else -1)
        training.append(row)
    model, _ = fit(training)
    model["reference_prediction_histogram"] = [10] * 10
    fake = {"model": model, "report": {"all_holdout": {"brier": 0.2}}}
    monkeypatch.setattr(Registry, "get", lambda self, ident: fake)
    assert check(store, "mock")["status"] == "insufficient"
    for i in range(200):
        s = signal.model_copy(deep=True)
        s.id = "drift-current-" + str(i)
        ident = fs.capture(s, 3000, "decision")
        with store.db:
            store.db.execute("INSERT INTO ml_predictions VALUES(?,?,?,?,?)", (ident, "mock", 3000, 0.99, 1))
    result = check(store, "mock")
    assert result["status"] == "degraded" and result["prediction_psi"] > 0.25
    assert store.get("ml_degraded")["model_id"] == "mock"
    check(store, "mock")
    assert store.db.execute("SELECT COUNT(*) FROM ml_history WHERE action='degraded'").fetchone()[0] == 1
    store.close()
