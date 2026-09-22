import pytest

from bybit_flow.thesis_health import evaluate, market_gate


def observation(sign=1, structural=True):
    return dict(
        coverage_complete=True,
        flow=dict(delta_pct=-40 * sign, cvd_slope=-10 * sign, delta_persistence=1),
        obi=-0.5 * sign,
        structural_failure=structural,
        window_end_ms=1,
    )


@pytest.mark.parametrize("direction,sign", [("LONG", 1), ("SHORT", -1)])
def test_persistence_structure_and_symmetry(signal, direction, sign):
    signal.direction, signal.horizon_profile, signal.version = direction, "CORE_INTRADAY", "rules:autonomy-v1"
    state = {}
    for now in range(1000, 601000, 60000):
        state = evaluate(signal, observation(sign) | dict(window_end_ms=now), state, now)
        assert not state["withdraw"]
    assert evaluate(signal, observation(sign) | dict(window_end_ms=601000), state, 601000)["withdraw"]
    assert not evaluate(signal, observation(sign, False) | dict(window_end_ms=181000), state, 181000)[
        "withdraw"
    ]
    assert not evaluate(signal, {"coverage_complete": False}, state, 181000)["withdraw"]


def test_swing_noise_and_legacy_policy_do_not_withdraw(signal):
    signal.horizon_profile, signal.version = "SWING", "rules:autonomy-v1"
    state = {}
    for now in range(1000, 181001, 60000):
        state = evaluate(signal, observation() | dict(window_end_ms=now), state, now)
    assert not state["withdraw"]
    signal.horizon_profile, signal.version = "CORE_INTRADAY", "old-policy"
    assert not evaluate(signal, observation() | dict(window_end_ms=241000), state, 241000)["withdraw"]


def test_market_conflict_requires_independent_participation(signal):
    signal.direction, signal.horizon_profile = "SHORT", "CORE_INTRADAY"
    signal.evidence = dict(
        market_factor=dict(
            available=True,
            beta_to_btc=1,
            correlation_to_btc=0.8,
            beta_stability_btc=0.1,
            expected_return_btc=0.01,
            residual_btc=0,
        ),
        flow={},
    )
    btc = dict(regime="trending up", efficiency=0.7)
    assert market_gate(signal, btc, btc)["blocked"]
    signal.evidence["market_factor"]["residual_btc"] = -0.02
    signal.evidence["flow"] = dict(delta_pct=-40, delta_persistence=1, cvd_slope=-1, initiative_short=True)
    assert not market_gate(signal, btc, btc)["blocked"]
    signal.evidence["market_factor"]["beta_stability_btc"] = 2
    assert market_gate(signal, btc, btc)["alignment"] == "UNCERTAIN"
