from decimal import Decimal

import pytest

from bybit_flow.levels import targets
from bybit_flow.ml.policy_research import run
from bybit_flow.storage import Store


def test_structural_targets_causal_and_fallback_documented(bars):
    result = targets(100, 95, "LONG", "range_rejection", bars, Decimal(".01"), bars[-1].end)
    assert result["tp2"] > result["tp1"] > 100
    assert result["source_timeframe_ms"] == bars[-1].interval
    assert result["available_ms"] == bars[-1].end
    with pytest.raises(ValueError, match="decision-time"):
        targets(100, 95, "LONG", "range_rejection", bars, Decimal(".01"), bars[-1].end - 1)
    fallback = targets(1000, 950, "LONG", "trend_pullback", bars, Decimal(".01"), bars[-1].end)
    assert fallback["tp1"] == 1150
    assert "fallback" in fallback["target_method"]


def test_insufficient_policy_evidence_never_changes_production(settings):
    store = Store(settings.data_dir)
    result = run(store, 100000)
    assert result["status"] == "INSUFFICIENT_EVIDENCE"
    assert not result["production_enabled"]
    assert result["challenger_score_profile"] is None
    assert store.get("swing_stop_study")["decision"] == "Current Swing stop policy retained."
    store.close()


@pytest.mark.parametrize(
    "fraction,compaction,pruning",
    [(0.6, False, False), (0.7, True, False), (0.85, True, True), (0.96, True, True)],
)
def test_ten_gb_pressure_policy_protects_evidence(settings, monkeypatch, fraction, compaction, pruning):
    from unittest.mock import Mock

    import bybit_flow.retention as retention

    store = Store(settings.data_dir)
    cfg = settings.model_copy(update={"max_storage_gb": 10})
    monkeypatch.setattr(retention, "directory_bytes", lambda _: 10_000_000_000 * fraction)
    pack = Mock(return_value={"segments": 0})
    prune = Mock(return_value={"freed_bytes": 0})
    monkeypatch.setattr("bybit_flow.packing.compact", pack)
    monkeypatch.setattr(retention, "prune_recordings", prune)
    result = retention.maintain(store, cfg)
    assert pack.called == compaction and prune.called == pruning
    if fraction >= 0.95:
        assert result["state"] == "STORAGE_BACKPRESSURE"
    store.close()


def test_horizon_snapshot_lookup_uses_signal_scoped_index(settings):
    store = Store(settings.data_dir)
    query = "SELECT id,decision_ms,payload FROM ml_snapshots WHERE signal_id=? AND stage='decision' AND decision_ms<=? ORDER BY decision_ms LIMIT 1"
    plan = " ".join(
        str(tuple(row)) for row in store.db.execute("EXPLAIN QUERY PLAN " + query, ("candidate", 1000))
    )
    assert "ml_snapshot_signal_time" in plan
    assert "TEMP B-TREE" not in plan
    store.close()
