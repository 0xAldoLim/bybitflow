from bybit_flow.scoring import score


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
