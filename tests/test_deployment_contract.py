"""Reviewable source-only deployment safety and disposable smoke boundaries."""

import importlib.util
from pathlib import Path

import pytest

from bybit_flow.storage import Store

ROOT = Path(__file__).resolve().parents[1]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runtime_continuity_checks_terminal_plans_and_sent_receipts(settings, signal):
    module = load_script("scripts/runtime_continuity.py")
    store = Store(settings.data_dir)
    store.signal(signal)
    with store.db:
        store.db.execute("INSERT INTO outbox VALUES('x:initial',?,'sent','{}','message',2000)", (signal.id,))
    before = module.capture(store.root)
    signal.state = "EXPIRED"
    store.signal(signal)
    after = module.capture(store.root, before["active"])
    assert module.validate(before, after)
    after["sent_initial"] = []
    with pytest.raises(ValueError, match="receipt"):
        module.validate(before, after)
    store.close()


def test_deploy_has_no_surprise_learning_or_destructive_rollback():
    deploy = (ROOT / "scripts/deploy.ps1").read_text()
    assert "head_sha=$commit" in deploy and "container-smoke" in deploy and "git/ref/heads/main" in deploy
    assert (
        "maturity-start" not in deploy and "bootstrap-labels" not in deploy and "'ml','cycle'" not in deploy
    )
    for name in ("deploy.ps1", "rollback.ps1"):
        text = (ROOT / "scripts" / name).read_text()
        assert "reset --hard" not in text and "down -v" not in text and "volume rm" not in text
    assert "Assert-Continuity" in (ROOT / "scripts/rollback.ps1").read_text()


def test_compose_smoke_has_no_live_configuration_or_network():
    module = load_script("examples/compose_smoke.py")
    spec = module.specification(ROOT, "unique-test-image")
    desk = spec["services"]["desk"]
    assert desk["image"] == "unique-test-image" and desk["network_mode"] == "none"
    assert "ports" not in desk and "env_file" not in desk
    assert desk["environment"]["FLOW_SCAN_ENABLED"] == "false"
    assert not any(value for key, value in desk["environment"].items() if "WEBHOOK" in key)
    assert desk["volumes"] == ["smoke-data:/app/data"]
