"""Honest OHLCV-only baseline, chronological partitions and immutable experiment provenance."""

import hashlib
import json
import subprocess
import uuid
from pathlib import Path

from .features import candle_features, validate_bars
from .storage import now_ms


def baseline(bars, settings, atr_multiple=2.0, hold_bars=4):
    if not bars:
        return {"mode": "OHLCV-only", "outcomes": [], "status": "insufficient data"}
    validate_bars(bars, bars[-1].end)
    outcomes = []
    i = 60
    while i + hold_bars < len(bars):
        f = candle_features(bars[:i], bars[i].start)
        # Transparent moving-average/slope baseline, independent of SMC classification.
        direction = 1 if f["slope"] > 0.04 else -1 if f["slope"] < -0.04 else 0
        if direction == 0:
            i += 1
            continue
        entry = bars[i].open * (1 + direction * settings.slippage_bps / 10000)
        distance = atr_multiple * f["atr"]
        stop, target = entry - direction * distance, entry + direction * 3 * distance
        exit_price, reason, j = bars[i + hold_bars - 1].close, "time_stop", i + hold_bars - 1
        for k in range(i, i + hold_bars):
            c = bars[k]
            stopped = c.low <= stop if direction == 1 else c.high >= stop
            won = c.high >= target if direction == 1 else c.low <= target
            if stopped:  # Explicit pessimistic resolution when both touched inside one candle.
                exit_price = min(stop, c.open) if direction == 1 else max(stop, c.open)
                reason, j = "stop_or_ambiguous_bar", k
                break
            if won:
                exit_price, reason, j = target, "target", k
                break
        exit_price *= 1 - direction * settings.slippage_bps / 10000
        costs = (
            entry + exit_price
        ) * settings.taker_fee_bps / 10000 + entry * settings.funding_reserve_bps / 10000
        outcomes.append(
            dict(
                entry_ms=bars[i].start,
                exit_ms=bars[j].end,
                net_r=(direction * (exit_price - entry) - costs) / distance,
                direction="LONG" if direction == 1 else "SHORT",
                family="ATR_baseline",
                regime=f["regime"],
                cluster=bars[i].start // 604_800_000,
                complete=True,
                reason=reason,
            )
        )
        i = j + 1
    from .backtest import performance

    stats = performance(outcomes)
    stats["outcome"] = "next-bar market open; 2 ATR stop; 3R target; four bars; stop-first ambiguous candles"
    stats["limitations"] = "OHLCV-only, funding reserve assumption, no actual depth/fills/PIT universe"
    return dict(
        mode="OHLCV-only baseline",
        outcomes=outcomes,
        statistics=stats,
        qualification="not eligible to validate order-flow strategies or high-tier alerts",
    )


def research_run(bars, settings):
    # Preregistered chronological partition: development 60%, evaluation 20%, untouched holdout 20%.
    # No selection occurs based on holdout. Repeated accesses are separate tracked experiments.
    n = len(bars)
    cut1, cut2 = int(n * 0.6), int(n * 0.8)
    sensitivity = {str(a): baseline(bars[:cut1], settings, a) for a in (1.5, 2.0, 2.5)}
    evaluation = baseline(bars[max(0, cut1 - 60) : cut2], settings)
    holdout = baseline(bars[max(0, cut2 - 60) :], settings)
    benchmark = (bars[-1].close / bars[0].open - 1) if bars else None
    return dict(
        development_sensitivity=sensitivity,
        evaluation=evaluation,
        holdout=holdout,
        holdout_policy="fixed defaults; never used to select parameters; repeated runs tracked",
        market_benchmark=dict(
            gross_buy_hold_return=benchmark,
            leverage=1,
            limitations="gross spot-like price return; not a cost-adjusted perpetual strategy",
        ),
        ablations="Unavailable for missing order-flow/depth/fundamental datasets; no substitutes",
        verdict="Research only; no strategy approval",
    )


def save_experiment(store, kind, params, result, paths=()):
    def digest(path):
        h = hashlib.sha256()
        with Path(path).open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    root = Path(__file__).parent
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        commit = "uncommitted"
    code_hash = hashlib.sha256("".join(digest(p) for p in sorted(root.glob("*.py"))).encode()).hexdigest()
    ident = uuid.uuid4().hex[:16]
    experiment = dict(
        id=ident,
        kind=kind,
        at_ms=now_ms(),
        parameters=params,
        data_hashes={str(p): digest(p) for p in paths},
        code_commit=commit,
        code_sha256=code_hash,
        results=result,
    )
    directory = store.root / "experiments"
    directory.mkdir(exist_ok=True)
    path = directory / f"{ident}.json"
    with path.open("x") as f:
        json.dump(experiment, f, indent=2, allow_nan=False)
    with store.db:
        store.db.execute(
            "INSERT INTO experiments VALUES(?,?,?)", (ident, experiment["at_ms"], json.dumps(experiment))
        )
    return path
