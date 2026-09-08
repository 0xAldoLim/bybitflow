WEIGHTS = {
    "regime": 15,
    "structure": 20,
    "orderflow": 25,
    "derivatives": 10,
    "execution": 15,
    "fundamentals": 10,
    "cross_market": 5,
}


def tier(score):
    return next(
        name
        for cutoff, name in (
            (95, "SSS"),
            (90, "SS"),
            (85, "S"),
            (75, "A"),
            (65, "B"),
            (50, "C"),
            (35, "D"),
            (0, "F"),
        )
        if score >= cutoff
    )


def score(signal, flow_confirmed, derivatives_available, fundamental=None, cross=None):
    # Price-event group receives one score; sweep/BOS/wick are never summed as independent votes.
    fractions = {
        "regime": 1 if signal.regime.startswith("trending") else 0.7,
        "structure": 0.85,
        "orderflow": 1 if flow_confirmed else 0,
        "derivatives": 0.6 if derivatives_available else 0,
        "execution": 1 if signal.risk.get("accepted") else 0,
        "fundamentals": 0,
        "cross_market": 0,
    }
    # Facts are not automatically converted into an unsupported asset-quality number.
    signal.quality = round(sum(WEIGHTS[k] * v for k, v in fractions.items()), 1)
    signal.raw_tier = "F" if signal.gates else tier(signal.quality)
    signal.evidence["score_components"] = {
        k: {"weight": WEIGHTS[k], "earned": WEIGHTS[k] * v} for k, v in fractions.items()
    }
    signal.final_tier = "REJECTED" if signal.gates else "RESEARCH"
    signal.qualification = {
        "status": "Uncalibrated",
        "probability": None,
        "reason": "No independently validated deployment model; all high-tier alerts locked",
        "data_cap": "Missing asset-quality and cross-market validation; no weight redistribution",
    }
    return signal
