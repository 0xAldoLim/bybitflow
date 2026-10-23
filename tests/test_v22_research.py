"""Fixed prospective exits stay outside primary labels and survive downtime."""

import json
from copy import deepcopy

from test_ml_monitored import bar, plan, touch
from test_setup_v22 import NOW, tagged

from bybit_flow.ml import SCHEMA_VERSION
from bybit_flow.ml.features import snapshot
from bybit_flow.ml.monitored import evaluate
from bybit_flow.models import Candle
from bybit_flow.quality_v22 import apply
from bybit_flow.setup_research import advance
from bybit_flow.storage import Store


def registered(signal, settings, instrument):
    s, b = tagged(signal, settings, instrument)
    s.holding_deadline_ms = NOW + 300000
    s.expected_hold_min = 2
    s.trigger_expires_ms = NOW + 120000
    apply(s, 100.01, NOW, book=b, settings=settings)
    store = Store(settings.data_dir)
    store.signal(s)
    assert store.db.execute("SELECT count(*) FROM setup_path_jobs").fetchone()[0] == 1
    return s, store


def candle(start, opening=100, high=100.1, low=99.9, close=100):
    return Candle(start, 60000, opening, high, low, close, 100, 10000)


def result(store, ident):
    raw = store.db.execute("SELECT payload FROM setup_path_labels WHERE signal_id=?", (ident,)).fetchone()
    return json.loads(raw[0]) if raw else None


def test_fixed_exits_share_entry_costs_and_do_not_touch_primary_labels(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    before = store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0]
    bars = [
        candle(NOW),
        candle(NOW + 60000, high=101.7, close=101.5),
        candle(NOW + 120000, opening=101.5, high=102.5, low=101.2, close=102),
        candle(NOW + 180000, opening=102, high=106.2, low=101.5, close=106),
    ]
    advance(store, s.id, bars, NOW + 240000)
    p = result(store, s.id)
    assert p["complete"] and p["production_ml"] is False
    assert p["policies"]["r050"]["net_r"] == 0.5 - p["cost_per_base"] / 2
    assert p["policies"]["r100"]["net_r"] == 1.0 - p["cost_per_base"] / 2
    assert p["policies"]["partial075_structural"]["net_r"] == 1.875 - p["cost_per_base"] / 2
    assert p["time_to_execution_tp1_ms"] == 180000
    assert not p["stop_before_0.5r"] and not p["stop_before_1r"]
    assert all(str(v) in p["milestones"] for v in (0.25, 0.5, 0.75, 1.0))
    assert set(p["ablations"]["variants"]) == {
        "v22_full",
        "without_ofi",
        "without_target_reachability",
        "without_stop_viability",
    }
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == before
    assert SCHEMA_VERSION == "candidate-v20"
    store.close()


def test_same_candle_stop_and_target_never_manufacture_complete_wins(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    advance(store, s.id, [candle(NOW), candle(NOW + 60000, high=107, low=97)], NOW + 120000)
    p = result(store, s.id)
    assert not p["complete"]
    assert p["policies"]["r100"]["ambiguous"] and p["policies"]["r100"]["net_r"] is None
    assert p["stop_before_1r"] is None
    assert p["mfe_lower_bound_r"] == 0
    store.close()


def test_entry_candle_stop_keeps_later_ordering_unknown(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    advance(store, s.id, [candle(NOW, low=97), candle(NOW + 60000, high=107)], NOW + 120000)
    p = result(store, s.id)
    assert p["entry_stop_order_unknown"]
    assert p["stop_before_1r"] is None
    assert not p["complete"] and p["mfe_lower_bound_r"] == 0
    assert all(r["net_r"] is None for r in p["policies"].values())
    store.close()


def test_entry_candle_possible_milestone_is_not_a_proven_stop_first(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    advance(store, s.id, [candle(NOW, high=101.2), candle(NOW + 60000, low=97)], NOW + 120000)
    p = result(store, s.id)
    assert p["stop_before_0.5r"] is None and p["stop_before_1r"] is True
    assert p["policies"]["time_decay"]["net_r"] is None
    assert p["policies"]["execution_tp1"]["complete"]
    store.close()


def test_research_resumes_from_saved_cursor_without_relabeling_old_data(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    advance(store, s.id, [candle(NOW)], NOW + 60000)
    store.close()
    resumed = Store(settings.data_dir)
    advance(resumed, s.id, [candle(NOW + 60000, high=100.1, low=97.9)], NOW + 120000)
    p = result(resumed, s.id)
    assert p["policies"]["execution_tp1"]["outcome"] == "STOP"
    assert p["policies"]["execution_tp1"]["complete"]
    saved = json.dumps(p, sort_keys=True)
    # A later invented rally cannot overwrite an existing result.
    assert advance(resumed, s.id, [candle(NOW + 120000, high=110)], NOW + 180000) is None
    assert json.dumps(result(resumed, s.id), sort_keys=True) == saved
    resumed.close()


def test_coverage_gap_is_unknown_not_a_training_loss(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    advance(store, s.id, [candle(NOW), candle(NOW + 120000, high=100.1, low=97.9)], NOW + 180000)
    p = result(store, s.id)
    assert p["state"] == "INCOMPLETE" and not p["complete"]
    assert all(r["net_r"] is None for r in p["policies"].values())
    store.close()


def test_no_entry_is_not_a_loss_and_old_policies_have_no_new_jobs(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    advance(
        store,
        s.id,
        [
            candle(NOW, opening=101, high=101.2, low=100.5, close=101),
            candle(NOW + 60000, opening=101, high=101.2, low=100.5, close=101),
            candle(NOW + 120000, opening=101, high=101.2, low=100.5, close=101),
        ],
        NOW + 180000,
    )
    p = result(store, s.id)
    assert p["state"] == "NO_ENTRY"
    assert all(r["net_r"] is None for r in p["policies"].values())
    old = deepcopy(s)
    old.id = "historical-v211"
    old.evidence["production_policies"]["trade_quality"] = "trade-validity-v21.1"
    store.signal(old)
    assert not store.db.execute("SELECT 1 FROM setup_path_jobs WHERE signal_id=?", (old.id,)).fetchone()
    store.close()


def test_v211_markouts_remain_available_without_changing_proxy_labels(signal):
    s = plan(signal, "CORE_INTRADAY")
    s.evidence["production_policies"] = dict(trade_quality="trade-validity-v21.1")
    row = snapshot(s, 121000, "decision") | dict(id="snapshot")
    touch(s)
    bars = [bar(120000), bar(180000), bar(240000, low=94)]
    old = evaluate(row, s.model_dump(), bars, "binance", 360000, materialized_ms=360000)
    new_row = deepcopy(row)
    new_row["signal"]["evidence"]["production_policies"]["trade_quality"] = "trade-validity-v22"
    new = evaluate(new_row, s.model_dump(), bars, "binance", 360000, materialized_ms=360000)
    assert old["research_path"] == new["research_path"]
    assert {k: v for k, v in old.items() if k != "research_path"} == {
        k: v for k, v in new.items() if k != "research_path"
    }
