from test_tradingview import NOW, payload

from bybit_flow.models import Candle
from bybit_flow.tradingview import TVEvent
from bybit_flow.tv_research import family_replay


def test_family_replay_costs_and_no_overlap(settings):
    events = []
    for i, family in enumerate(("tv_sweep_reclaim", "tv_continuation")):
        p = payload()
        p.update(event_id=f"synthetic-research-{i}", signal_id=f"synthetic-research-{i}")
        p["observation"]["family"] = family
        if i:
            p["observation"]["prior_close"] = 99  # accepted above 98 continuation level
        events.append(TVEvent.model_validate(p))
    bars = [Candle(NOW - 1000 + i * 900_000, 900_000, 100, 116, 99, 110, 0, 0) for i in range(100)]
    report = family_replay(events + events, bars, settings, "TESTUSDT")
    assert len(report["outcomes"]) == 2
    assert all(0 < r["net_r"] < 3 for r in report["outcomes"])
    assert report["calibration"]["status"] == "Uncalibrated" and not report["validated"]
    assert set(report["families"]) == {
        f + ":" + d for f in ("tv_sweep_reclaim", "tv_continuation") for d in ("LONG", "SHORT")
    }
    expensive = settings.model_copy(update={"slippage_bps": 10})
    assert (
        family_replay(events, bars, expensive, "TESTUSDT")["outcomes"][0]["net_r"]
        < report["outcomes"][0]["net_r"]
    )


def test_missing_footprint_never_backfilled(settings):
    p = payload()
    p["observation"].update(footprint_source="unavailable", buy_volume=None, sell_volume=None)
    bars = [Candle(NOW - 1000 + i * 900_000, 900_000, 100, 116, 99, 110, 0, 0) for i in range(100)]
    result = family_replay([TVEvent.model_validate(p)], bars, settings, "TESTUSDT")
    assert not result["outcomes"] and result["rejections"]
