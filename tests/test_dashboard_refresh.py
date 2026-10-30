import json
import shutil
import subprocess
from pathlib import Path

import pytest

from bybit_flow.storage import Store


@pytest.mark.parametrize(
    "outcome,state,ambiguous",
    [("TARGET", "RESOLVED", False), ("STOP", "INVALIDATED", False), ("UNCLEAR", "INVALIDATED", True)],
)
def test_summary_preserves_evidenced_result_without_changing_history(
    settings, signal, outcome, state, ambiguous
):
    store = Store(settings.data_dir)
    signal.state = state
    signal.evidence.update(
        primary_outcome=outcome,
        terminal_event=dict(method="LIVE_EXECUTED_TRADE", effective_ms=2000, ambiguous=ambiguous),
    )
    signal.evidence["expiry_mark"] = {"estimated_net_r": 0.2}
    store.signal(signal)
    before = store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0]
    summary = store.signal_summaries()[0]
    assert summary["evidence"]["primary_outcome"] == outcome
    assert summary["evidence"]["terminal_event"]["ambiguous"] is ambiguous
    assert summary["evidence"]["expiry_mark"] == {"estimated_net_r": 0.2}
    assert json.loads(before)["coverage"] == summary["coverage"]
    assert store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0] == before
    store.close()


def test_dashboard_renderer_handles_partial_reports_and_refresh_failures():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js not installed")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [node, str(root / "tests/dashboard_ui.cjs")], cwd=root, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr
