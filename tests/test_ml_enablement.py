from bybit_flow.ml import cli
from bybit_flow.ml.inference import apply
from bybit_flow.ml.operations import status
from bybit_flow.storage import Store


def test_disabled_worker_is_idle_and_desk_inference_abstains(settings, signal, monkeypatch):
    store = Store(settings.data_dir)
    calls = []
    monkeypatch.setattr(cli, "monitor", lambda *_: calls.append("monitor"))
    monkeypatch.setattr(cli, "cycle", lambda *_: calls.append("cycle"))
    disabled = settings.model_copy(update={"ml_enabled": False})
    cli.run(["cycle"], disabled, store)
    apply(signal, disabled, store)
    assert calls == []
    assert store.get("ml_worker_state")["status"] == "DISABLED"
    assert store.get("ml_monitor") is None
    assert store.get("ml_cycle") is None
    assert not store.db.execute("SELECT 1 FROM ml_labels LIMIT 1").fetchone()
    assert status(store, {}, enabled=False)["status"] == "DISABLED"
    assert signal.calibrated_probability is None
    store.close()


def test_enabled_worker_runs_monitor_and_training_cycle(settings, monkeypatch):
    store = Store(settings.data_dir)
    store.put("ml_trainability_cache", {"by_source": {"binance": {"baseline_trainable": 500}}})
    calls = []
    monkeypatch.setattr(cli, "monitor", lambda *_: calls.append("monitor"))
    monkeypatch.setattr(cli, "cycle", lambda *_: calls.append("cycle") or "fixture-model")
    cli.run(["cycle"], settings.model_copy(update={"ml_enabled": True}), store)
    assert calls == ["monitor", "cycle"]
    assert store.get("ml_cycle")["tracks"]["primary:binance"]["model_id"] == "fixture-model"
    store.close()
