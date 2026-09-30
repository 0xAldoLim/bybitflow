"""Closed-bar volatility and jump descriptors; never a directional trade signal."""

import math
from statistics import mean, pstdev

POLICY = "volatility-jump-v1"


def assess(bars, asof_ms, source):
    base = dict(
        policy=POLICY,
        state="INSUFFICIENT",
        source=source,
        source_ms=None,
        available_ms=asof_ms,
        short_rv=None,
        long_rv=None,
        rv_ratio=None,
        vol_of_vol=None,
        parkinson_volatility=None,
        realized_bipower_variation=None,
        jump_variation=None,
        jump_ratio=None,
        range_expansion_percentile=None,
    )
    closed = [bar for bar in bars if bar.end <= asof_ms]
    if len(closed) < 61 or closed[-1].end != bars[-1].end:
        return base
    returns = [math.log(b.close / a.close) for a, b in zip(closed[-61:-1], closed[-60:])]
    short_rv = math.sqrt(mean(value * value for value in returns[-10:]))
    long_rv = math.sqrt(mean(value * value for value in returns[-50:]))
    windows = [math.sqrt(mean(value * value for value in returns[i : i + 10])) for i in range(0, 50, 5)]
    parkinson = math.sqrt(mean(math.log(bar.high / bar.low) ** 2 for bar in closed[-20:]) / (4 * math.log(2)))
    realized = sum(value * value for value in returns[-20:])
    bipower = math.pi / 2 * sum(abs(a * b) for a, b in zip(returns[-20:-1], returns[-19:]))
    jump = max(0.0, realized - bipower)
    ranges = [(bar.high - bar.low) / bar.close for bar in closed[-51:]]
    range_percentile = sum(value <= ranges[-1] for value in ranges[:-1]) / 50
    ratio = short_rv / long_rv if long_rv else None
    jump_ratio = jump / realized if realized else 0.0
    state = "NORMAL"
    if jump_ratio >= 0.35 and range_percentile >= 0.9:
        state = "JUMP_SHOCK"
    elif ratio is not None and ratio >= 1.6:
        state = "VOLATILITY_EXPANSION"
    elif ratio is not None and ratio <= 0.65:
        state = "VOLATILITY_COMPRESSION"
    elif ratio is not None and 0.65 < ratio < 1 and max(windows) > 1.6 * long_rv:
        state = "POST_SHOCK_NORMALIZATION"
    return base | dict(
        state=state,
        source_ms=closed[-1].end,
        short_rv=short_rv,
        long_rv=long_rv,
        rv_ratio=ratio,
        vol_of_vol=pstdev(windows),
        parkinson_volatility=parkinson,
        realized_bipower_variation=bipower,
        jump_variation=jump,
        jump_ratio=jump_ratio,
        range_expansion_percentile=range_percentile,
    )
