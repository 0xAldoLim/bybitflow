"""Human prioritization of simultaneously valid setups; never a delivery gate."""

from math import sqrt

POLICY = "opportunity-priority-v1"


def _value(signal, name, default=None):
    return signal.get(name, default) if isinstance(signal, dict) else getattr(signal, name, default)


def _evidence(signal):
    return _value(signal, "evidence", {}) or {}


def _correlation(a, b):
    left = {bar.end: bar.close for bar in a[-31:]}
    right = {bar.end: bar.close for bar in b[-31:]}
    times = sorted(set(left) & set(right))[-21:]
    if len(times) < 11:
        return None
    x = [left[t2] / left[t1] - 1 for t1, t2 in zip(times[:-1], times[1:])]
    y = [right[t2] / right[t1] - 1 for t1, t2 in zip(times[:-1], times[1:])]
    xm, ym = sum(x) / len(x), sum(y) / len(y)
    numerator = sum((u - xm) * (v - ym) for u, v in zip(x, y))
    denominator = sqrt(sum((u - xm) ** 2 for u in x) * sum((v - ym) ** 2 for v in y))
    return numerator / denominator if denominator else None


def cluster(signal):
    evidence = _evidence(signal)
    factor = evidence.get("market_factor", {})
    beta_btc = factor.get("beta_to_btc")
    beta_eth = factor.get("beta_to_eth")
    direction = _value(signal, "direction")
    if beta_btc is not None and abs(beta_btc) >= 0.8:
        return f"BTC_HIGH_BETA_{direction}_CLUSTER"
    if beta_eth is not None and abs(beta_eth) >= 0.8:
        return "ETH_ALT_CLUSTER"
    return "LOW_BETA_IDIOSYNCRATIC"


def rank(signals, asof_ms, bars_by_symbol=None, maximum=30):
    candidates = list(signals)[:maximum]
    bars = bars_by_symbol or {}
    results = {}
    for signal in candidates:
        ident = _value(signal, "id")
        symbol = _value(signal, "symbol")
        direction = _value(signal, "direction")
        group = cluster(signal)
        related = []
        for other in candidates:
            if _value(other, "id") == ident or _value(other, "direction") != direction:
                continue
            if cluster(other) != group or group == "LOW_BETA_IDIOSYNCRATIC":
                continue
            pairwise = _correlation(bars.get(symbol, []), bars.get(_value(other, "symbol"), []))
            # Missing pairwise history cannot establish redundancy.
            if pairwise is not None and pairwise >= 0.8:
                related.append(other)
        evidence = _evidence(signal)
        context = evidence.get("context_ev", {})
        quality = float(_value(signal, "quality", 0))
        net_rr = (_value(signal, "risk", {}) or {}).get("net_rr")
        liquidity = evidence.get("normal_spread", {})
        spread = liquidity.get("median_bps", liquidity.get("spread_bps"))
        liquidity_score = max(0, 100 - 10 * spread) if isinstance(spread, (float, int)) else None
        entry_state = "READY" if _value(signal, "state") in {"CONFIRMED", "ALERTED"} else "NEAR_CONFIRMED"
        components = [
            (quality, 0.30),
            (min(100, max(0, float(net_rr) * 25)), 0.20) if net_rr is not None else (None, 0.20),
            (100 if entry_state == "READY" else 50, 0.15),
            (liquidity_score, 0.15),
        ]
        ev = context.get("shrunk_expectancy_r") if context.get("status") != "INSUFFICIENT" else None
        components.append((max(0, min(100, 50 + ev * 25)) if ev is not None else None, 0.10))
        weight = sum(weight for value, weight in components if value is not None)
        base = (
            sum(value * weight for value, weight in components if value is not None) / weight
            if weight
            else quality
        )
        penalty = min(10, len(related) * 5)
        results[ident] = dict(
            policy=POLICY,
            source=_value(signal, "source"),
            source_ms=asof_ms,
            available_ms=asof_ms,
            priority_score_0_100=round(max(0, min(100, base - penalty)), 1),
            rank=None,
            cluster_id=group,
            cluster_size=len(related) + 1,
            redundancy_penalty=penalty,
            liquidity_quality=liquidity_score,
            quality_score=quality,
            net_rr=net_rr,
            entry_state=entry_state,
            context_ev=context.get("shrunk_expectancy_r"),
            expected_hold_minutes=_value(signal, "expected_hold_max"),
            reason="Pairwise closed-bar correlation and factor cluster; informational only",
        )
    for position, ident in enumerate(
        sorted(results, key=lambda key: results[key]["priority_score_0_100"], reverse=True), 1
    ):
        results[ident]["rank"] = position
        results[ident]["rank_of"] = len(results)
    return results
