import json
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from bybit_flow.backtest import PaperPosition
from bybit_flow.ml.features import snapshot
from bybit_flow.ml.store import FeatureStore
from bybit_flow.models import Trade
from bybit_flow.scoring import tier
from bybit_flow.storage import Store


def test_immutable_rejected_candidate_and_label(settings, signal):
    store = Store(settings.data_dir)
    fs = FeatureStore(store)
    signal.gates = ["flow rejected"]
    signal.evidence = {"h4": {"efficiency": 0.4, "asof": 1000}, "flow": {"delta_pct": -30}}
    ident = fs.capture(signal, 2000, "decision")
    signal.evidence["flow"]["delta_pct"] = 99
    assert fs.capture(signal, 3000, "decision") == ident
    row = next(fs.snapshots())
    assert row["values"]["delta_pct"] == -30
    assert row["signal"]["gates"] == ["flow rejected"]
    assert "gates" not in row["values"]
    outcome = dict(policy="prints-v1", complete=True, net_r=-1.0, exit_ms=5000)
    with pytest.raises(ValueError, match="future"):
        fs.label(ident, outcome, 4000)
    fs.label(ident, outcome, 6000)
    assert fs.dataset(5500) == []
    assert len(fs.dataset(6000)) == 1
    with pytest.raises(ValueError, match="Immutable"):
        fs.label(ident, outcome | {"net_r": 5}, 7000)
    exported = fs.export(6000)
    assert json.loads(pq.read_table(exported)["payload"][0].as_py())["values"]["delta_pct"] == -30
    store.close()


def test_unavailable_future_and_source_distinction(signal):
    signal.source = "tradingview"
    signal.evidence = {"flow": {"delta_pct": 90, "event_ms": 5000}}
    s = snapshot(signal, 2000, "decision")
    assert s["values"]["delta_pct"] is None
    assert s["feature_metadata"]["delta_pct"]["missing"]
    assert "classified-footprint" in s["feature_metadata"]["delta_pct"]["source"]
    assert s["values"]["book_spread_bps"] is None
    with pytest.raises(ValueError):
        snapshot(signal, 999, "decision")


def test_excursions_funding_dedup_and_grades(signal):
    p = PaperPosition(signal, 1, slippage_bps=0)
    p.on_trade(Trade(signal.symbol, 1500, 1500, "1", "Buy", Decimal(100), Decimal(100)))
    p.on_funding(2000, 0.001, 100)
    p.on_funding(2000, 0.001, 100)
    assert p.funding == 0.1
    p.on_trade(Trade(signal.symbol, 3000, 3000, "2", "Sell", Decimal(98), Decimal(100)))
    p.on_trade(Trade(signal.symbol, 4000, 4000, "3", "Buy", Decimal(116), Decimal(100)))
    result = p.outcome()
    assert result["actual_entry_ms"] == 1500
    assert result["mfe_r"] == 3.2 and result["mae_r"] == -0.4
    assert result["classification"] == "win"
    assert tier(19) == "F" and tier(20) == "E" and tier(35) == "D"
