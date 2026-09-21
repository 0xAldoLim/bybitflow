from bybit_flow.scoring import score


def test_reversal_scores_defended_absorption_not_continuation_stack(signal):
    signal.family = "liquidity_sweep"
    signal.evidence = {
        "flow": {
            "delta_pct": -50,
            "sell_notional": 1000,
            "defended_notional": {"LONG": 250},
            "absorption_long": True,
            "stacked_buy": 0,
            "stacked_sell": 4,
        }
    }
    score(signal, True, False)
    assert signal.evidence["score_components"]["orderflow"]["earned"] == 25
    signal.evidence["flow"]["defended_notional"] = {}
    score(signal, True, False)
    assert signal.evidence["score_components"]["orderflow"]["earned"] == 0
    assert signal.calibrated_probability is None


def test_native_score_can_reach_sss_with_actual_component_observations(signal):
    signal.risk = {"accepted": True, "net_rr": 2.8}
    signal.evidence = {
        "h4": {"efficiency": 0.6},
        "h1": {"bos": "up", "atr": 3},
        "flow": {"delta_pct": 30, "stacked_buy": 4},
        "derivatives": {"funding_rate": -0.0001, "oi_change_pct": 1.5},
        "cross_market": {"BTCUSDT": "trending up", "ETHUSDT": "trending up"},
        "fundamentals": [
            {
                "category": k,
                "source": "https://example.org/fixture",
                "definition": "Synthetic source-attributed test fact",
                "known_ms": 1,
                "expires_ms": 999999,
            }
            for k in ("economic_purpose", "value_accrual", "dilution", "security", "governance")
        ],
    }
    score(signal, True, True)
    assert signal.quality == 100 and signal.raw_tier == "SSS"
    assert signal.final_tier == "RESEARCH" and signal.qualification["probability"] is None
    signal.evidence["fundamentals"] = []
    score(signal, True, True)
    assert signal.quality == 90
    signal.evidence["cross_market"] = {"BTCUSDT": "unavailable"}
    score(signal, True, True)
    assert signal.quality == 85


def test_confirmed_directional_reversal_earns_flow_without_absorption(signal):
    signal.family = "liquidity_sweep"
    signal.evidence.update(
        flow=dict(delta_pct=25, delta_persistence=0.75, cvd_slope=10, absorption_long=False),
        confirmation=dict(passed=True, flow_support="FLOW_SUPPORTIVE"),
    )
    score(signal, True, False)
    assert signal.score_components["orderflow"]["earned"] == 0  # Grandfathered scoring.
    signal.version += ":flow-score-v2"
    score(signal, True, False)
    assert signal.score_components["orderflow"]["earned"] == 12.5
    assert signal.score_components["orderflow"]["weight"] == 25
    assert signal.quality == round(sum(v["earned"] for v in signal.score_components.values()), 1)
    assert signal.evidence["score_profile"].startswith("native-evidence-4")
    signal.evidence["flow"]["delta_pct"] = -25
    score(signal, True, False)
    assert signal.score_components["orderflow"]["earned"] == 0


def test_new_flow_score_is_capped_and_requires_confirmed_evidence(signal):
    signal.family = "range_rejection"
    signal.direction = "SHORT"
    signal.version += ":flow-score-v2"
    signal.evidence.update(
        flow=dict(delta_pct=-100, delta_persistence=1, cvd_slope=-100),
        confirmation=dict(passed=True, flow_support="FLOW_SUPPORTIVE"),
    )
    score(signal, True, False)
    assert signal.score_components["orderflow"]["earned"] == 25
    signal.evidence["confirmation"]["passed"] = False
    score(signal, True, False)
    assert signal.score_components["orderflow"]["earned"] == 0
