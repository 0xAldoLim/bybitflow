import pytest

from bybit_flow.fill_review import review


def test_reported_fills_keep_original_risk_and_signed_costs(tmp_path):
    path = tmp_path / "fills.csv"
    path.write_text(
        "signal_id,source,direction,planned_entry,original_stop,entry,exit,quantity,fees,funding_cost,delivered_ms,entry_ms,exit_ms\na,binance,LONG,100,95,101,110,2,1,-0.2,1000,61000,120000\n"
    )
    result = review(path)
    assert result["rows"][0]["net_r"] == pytest.approx(1.72)
    assert result["rows"][0]["entry_delay_seconds"] == 60
    assert result["production_ml"] is False
    path.write_text(path.read_text().replace(",101,110,", ",nan,110,"))
    with pytest.raises(ValueError, match="row 2"):
        review(path)
