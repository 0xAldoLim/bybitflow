"""Historical chart-observation replay; no footprint reconstructed from candles.

Inputs must contain actual saved observations and actual OHLC records. This evaluates
chart research rules, not exchange-depth execution and not unverified journal labels.
"""

import csv
from bisect import bisect_left
from datetime import datetime

from .calibration import clustered_statistics, reliability, walk_forward_predictions
from .features import validate_bars
from .models import Candle
from .tradingview import TVEvent, evaluate


def read_candles(path):
    bars = []
    with path.open(newline="") as stream:
        for row in csv.DictReader(stream):
            raw = row["time"]
            try:
                stamp = float(raw)
                start = int(stamp * 1000) if stamp < 100_000_000_000 else int(stamp)
            except ValueError:
                date = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if date.tzinfo is None:
                    raise ValueError("CSV timestamps must have an explicit timezone") from None
                start = int(date.timestamp() * 1000)
            bars.append(
                Candle(start, 900_000, *(float(row[k]) for k in ("open", "high", "low", "close")), 0, 0)
            )
    if not bars:
        raise ValueError("No actual 15M candles supplied")
    validate_bars(bars, bars[-1].end)
    return bars


def read_events(path):
    with path.open() as stream:
        return [TVEvent.model_validate_json(line) for line in stream if line.strip()]


def family_replay(events, bars, settings, symbol):
    validate_bars(bars, bars[-1].end)
    starts = [c.start for c in bars]
    seen, occupied, outcomes, rejected = set(), {}, [], []
    research = settings.model_copy(update={"tv_proxy_research": True, "sss_research": False, "equity": None})
    for event in sorted(events, key=lambda e: e.source_ms):
        if event.event != "setup" or event.native_symbol != symbol or event.internal_id in seen:
            continue
        seen.add(event.internal_id)
        signal = evaluate(event, research, at_ms=event.source_ms)
        if signal.gates:
            rejected.append({"signal_id": signal.id, "reasons": signal.gates})
            continue
        # Separate experimental books per family/direction; no overlapping labels within a book.
        key = (signal.family, signal.direction)
        if event.source_ms <= occupied.get(key, 0):
            continue
        i = bisect_left(starts, event.source_ms)
        if i + 15 >= len(bars):
            rejected.append({"signal_id": signal.id, "reasons": ["incomplete 4H outcome window"]})
            continue
        if starts[i] > signal.trigger_expires_ms or not signal.zone[0] <= bars[i].open <= signal.zone[1]:
            rejected.append({"signal_id": signal.id, "reasons": ["missed entry; no touch-implies-fill"]})
            continue
        sign = 1 if signal.direction == "LONG" else -1
        entry = bars[i].open * (1 + sign * settings.slippage_bps / 10000)
        distance = sign * (entry - signal.stop)
        if distance <= 0:
            continue
        exit_price, j, reason = bars[i + 15].close, i + 15, "4H time stop"
        for k in range(i, i + 16):
            c = bars[k]
            stopped = c.low <= signal.stop if sign > 0 else c.high >= signal.stop
            targeted = c.high >= signal.tp1 if sign > 0 else c.low <= signal.tp1
            if stopped:
                exit_price = min(signal.stop, c.open) if sign > 0 else max(signal.stop, c.open)
                j, reason = k, "stop-first including ambiguous OHLC"
                break
            if targeted:
                exit_price, j, reason = signal.tp1, k, "TP1 whole position"
                break
        exit_price *= 1 - sign * settings.slippage_bps / 10000
        costs = (
            entry + exit_price
        ) * settings.taker_fee_bps / 10000 + entry * settings.funding_reserve_bps / 10000
        occupied[key] = bars[j].end
        outcomes.append(
            dict(
                signal_id=signal.id,
                entry_ms=bars[i].start,
                exit_ms=bars[j].end,
                family=signal.family,
                direction=signal.direction,
                regime=signal.regime,
                score=signal.quality,
                liquidity_bucket="chart-turnover-proxy",
                cluster=bars[i].start // 604_800_000,
                net_r=(sign * (exit_price - entry) - costs) / distance,
                complete=True,
                reason=reason,
            )
        )
    span = bars[-1].end - bars[0].start
    cut1, cut2 = bars[0].start + int(span * 0.6), bars[0].start + int(span * 0.8)
    groups = {}
    for family in ("tv_sweep_reclaim", "tv_continuation"):
        for direction in ("LONG", "SHORT"):
            rows = [r for r in outcomes if r["family"] == family and r["direction"] == direction]
            partitions = {
                "development": [r for r in rows if r["exit_ms"] < cut1],
                "out_of_sample": [
                    r for r in rows if r["entry_ms"] >= cut1 + 14_400_000 and r["exit_ms"] < cut2
                ],
                "holdout": [r for r in rows if r["entry_ms"] >= cut2 + 14_400_000],
            }
            groups[family + ":" + direction] = {k: clustered_statistics(v) for k, v in partitions.items()}
    # Preregistered minimum: 200 matured prior labels AND 26 weekly clusters in each stratum.
    predictions = walk_forward_predictions(outcomes)
    return {
        "mode": "recorded TradingView observations + actual 15M OHLC",
        "outcomes": outcomes,
        "rejections": rejected,
        "families": groups,
        "predictions": predictions,
        "calibration": reliability(predictions),
        "validated": False,
        "assumptions": {
            "entry": "first subsequent bar open within trigger/zone; slippage on both sides",
            "fee_bps_per_side": settings.taker_fee_bps,
            "slippage_bps_per_side": settings.slippage_bps,
            "funding_reserve_bps": settings.funding_reserve_bps,
            "partition": "60/20/20 chronological, 4H embargo",
            "limits": "No historical depth, partial fills or actual funding payments; chart evidence source-attested. Not deployment validation.",
        },
    }
