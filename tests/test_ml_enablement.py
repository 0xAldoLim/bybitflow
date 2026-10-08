import pytest

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


@pytest.mark.parametrize("next_status", ["collecting", "abstained"])
def test_worker_replaces_storage_backpressure_after_capacity_recovers(settings, monkeypatch, next_status):
    store = Store(settings.data_dir)
    monkeypatch.setattr(cli, "now_ms", lambda: 1_000_000)
    store.put("ml_monitor", dict(at_ms=1_000_000, status="observed"))
    store.put("ml_cycle", dict(at_ms=999_000, status="STORAGE_BACKPRESSURE"))
    result = dict(at_ms=1_000_000, status=next_status, tracks={})
    monkeypatch.setattr(cli, "train_ready", lambda *_: result)
    assert cli.worker_step(settings, store) == result
    assert store.get("ml_cycle") == result
    assert not store.db.execute("SELECT 1 FROM ml_models LIMIT 1").fetchone()
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
