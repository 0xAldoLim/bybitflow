from types import SimpleNamespace

from bybit_flow.strategy import candidates


def test_each_closed_execution_window_gets_its_own_candidate_id(instrument, monkeypatch):
    features = dict(
        regime="trending up",
        atr=2,
        ma20=100,
        low=99,
        high=110,
        sweep_long=False,
        sweep_short=False,
    )
    monkeypatch.setattr("bybit_flow.strategy.candle_features", lambda *args: features)
    bar = SimpleNamespace(low=99, high=101, close=100.1, end=3_600_000)
    first = [SimpleNamespace(close=100.1, end=3_600_000)]
    next_window = [SimpleNamespace(close=100.2, end=4_500_000)]
    args = (instrument, [bar], [bar] * 24)
    a = candidates(*args, first, 3_600_100, families=("trend_pullback",))[0]
    repeat = candidates(*args, first, 3_600_200, families=("trend_pullback",))[0]
    b = candidates(*args, next_window, 4_500_100, families=("trend_pullback",))[0]
    assert a.id == repeat.id
    assert a.id != b.id
    assert a.version == b.version == "rules-0.2.0"
