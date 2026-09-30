"""Cheap closed-bar cross-sectional context from the scanner's existing universe."""

from statistics import median, pstdev

POLICY = "crypto-breadth-v1"


def _return(bars, asof_ms, max_age_ms):
    if len(bars) < 2 or bars[-1].end > asof_ms or asof_ms - bars[-1].end > max_age_ms:
        return None
    return bars[-1].close / bars[-2].close - 1 if bars[-2].close else None


def _beta(bars, reference):
    own = {bar.end: bar.close for bar in bars[-31:]}
    other = {bar.end: bar.close for bar in reference[-31:]}
    times = sorted(set(own) & set(other))
    if len(times) < 21:
        return None
    x = [other[b] / other[a] - 1 for a, b in zip(times[-21:-1], times[-20:])]
    y = [own[b] / own[a] - 1 for a, b in zip(times[-21:-1], times[-20:])]
    xm, ym = sum(x) / len(x), sum(y) / len(y)
    variance = sum((v - xm) ** 2 for v in x)
    return sum((a - xm) * (b - ym) for a, b in zip(x, y)) / variance if variance > 0 else None


def assess(rows, asof_ms, source, previous=None, minimum=8):
    """Rows contain only already-admitted symbols and their cached closed candles."""
    prior = previous or {}
    fresh = {}
    for row in rows[:150]:
        if row.get("source") != source or not row.get("eligible"):
            continue
        h1, h4 = row.get("h1", []), row.get("h4", [])
        one = _return(h1, asof_ms, 3_660_000)
        four = _return(h4, asof_ms, 14_460_000)
        if one is not None and four is not None:
            fresh[row["symbol"]] = (row, one, four)
    n = len(fresh)
    base = dict(
        policy=POLICY,
        state="INSUFFICIENT",
        source=source,
        source_ms=max((r[0]["h1"][-1].end for r in fresh.values()), default=None),
        available_ms=asof_ms,
        eligible_symbols=n,
        denominator=n,
    )
    if n < minimum:
        return base
    one = [item[1] for item in fresh.values()]
    four = [item[2] for item in fresh.values()]
    btc = fresh.get("BTCUSDT")
    eth = fresh.get("ETHUSDT")
    btc_residuals, eth_residuals = [], []
    above, below = 0, 0
    m15_returns = []
    for row, return_1h, _ in fresh.values():
        bars = row["h1"]
        day = (bars[-1].end - 1) // 86_400_000 * 86_400_000
        session = [bar for bar in bars if bar.start >= day]
        volume = sum(bar.volume for bar in session)
        if volume:
            vwap = sum(bar.turnover for bar in session) / volume
            above += bars[-1].close > vwap
            below += bars[-1].close < vwap
        m15_return = _return(row.get("m15", []), asof_ms, 960_000)
        if m15_return is not None:
            m15_returns.append(m15_return)
        for reference, output in ((btc, btc_residuals), (eth, eth_residuals)):
            if reference and row["symbol"] != reference[0]["symbol"]:
                beta = _beta(bars, reference[0]["h1"])
                if beta is not None:
                    output.append(return_1h - beta * reference[1])
    up1 = sum(value > 0 for value in one) / n
    down1 = sum(value < 0 for value in one) / n
    up4 = sum(value > 0 for value in four) / n
    down4 = sum(value < 0 for value in four) / n
    positive_btc = (
        sum(value > 0.0001 for value in btc_residuals) / len(btc_residuals) if btc_residuals else None
    )
    negative_btc = (
        sum(value < -0.0001 for value in btc_residuals) / len(btc_residuals) if btc_residuals else None
    )
    positive_eth = (
        sum(value > 0.0001 for value in eth_residuals) / len(eth_residuals) if eth_residuals else None
    )
    negative_eth = (
        sum(value < -0.0001 for value in eth_residuals) / len(eth_residuals) if eth_residuals else None
    )
    impulse = sum(value > 0 for value in m15_returns) / len(m15_returns) if m15_returns else None
    dispersion = pstdev(one) if n > 1 else 0.0
    residual_dispersion = pstdev(btc_residuals) if len(btc_residuals) > 1 else None
    state = "MIXED"
    if btc and btc[1] > 0 and negative_btc is not None and negative_btc > 0.65:
        state = "BTC_LED_RALLY"
    elif btc and btc[1] < 0 and positive_btc is not None and positive_btc > 0.65:
        state = "BTC_LED_SELLOFF"
    elif up1 >= 0.7 and up4 >= 0.6:
        state = "BROAD_RISK_ON"
    elif down1 >= 0.7 and down4 >= 0.6:
        state = "BROAD_RISK_OFF"
    elif residual_dispersion is not None and residual_dispersion >= 0.025:
        state = "HIGH_DISPERSION"
    elif positive_btc is not None and positive_btc >= 0.7:
        state = "ALT_STRENGTH"
    elif negative_btc is not None and negative_btc >= 0.7:
        state = "ALT_WEAKNESS"
    return base | dict(
        state=state,
        pct_trending_up_1h=up1,
        pct_trending_down_1h=down1,
        pct_trending_up_4h=up4,
        pct_trending_down_4h=down4,
        pct_above_session_vwap=above / (above + below) if above + below else None,
        pct_below_session_vwap=below / (above + below) if above + below else None,
        positive_btc_residual_fraction=positive_btc,
        negative_btc_residual_fraction=negative_btc,
        positive_eth_residual_fraction=positive_eth,
        negative_eth_residual_fraction=negative_eth,
        median_btc_residual=median(btc_residuals) if btc_residuals else None,
        median_eth_residual=median(eth_residuals) if eth_residuals else None,
        cross_sectional_return_dispersion=dispersion,
        cross_sectional_residual_dispersion=residual_dispersion,
        breadth_impulse_15m=impulse,
        breadth_change_1h=up1 - prior["pct_trending_up_1h"]
        if prior.get("source") == source
        and prior.get("available_ms", 0) <= asof_ms - 3_600_000
        and prior.get("pct_trending_up_1h") is not None
        else None,
        residual_samples_btc=len(btc_residuals),
        residual_samples_eth=len(eth_residuals),
        vwap_samples=above + below,
        m15_samples=len(m15_returns),
    )
