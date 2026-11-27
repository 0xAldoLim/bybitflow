"""Structure-only test of the production entry rules on a year of public candles.

Runs the same `strategy.candidates` function the scanner uses (trade-validity-v22,
structural targets, staged reversal policy) at every closed setup bar, then
measures what price did from the next 5m open. No order-flow confirmation, volume
profile, score or later gate is applied: this isolates whether the candle
structure by itself predicts direction. It is the widest funnel stage, not the
delivered product.
"""

import bisect
import json
import os
import sys
import time
from decimal import Decimal
from multiprocessing import Pool

HERE = os.path.dirname(os.path.abspath(__file__))
C = os.path.join(HERE, "candles")
sys.path.insert(0, os.path.join(HERE, "..", "..", "src"))
M5 = 300_000
TF = {"15": 900_000, "60": 3_600_000, "240": 14_400_000, "D": 86_400_000}
# context, setup, execution timeframe, setup bars supplied, hold minutes
HZ = {"CORE_INTRADAY": ("240", "60", "15", 200, 240), "SWING": ("D", "240", "60", 120, 2880)}


def aggregate(rows, ms):
    from bybit_flow.models import Candle

    out, cur = [], None
    need = ms // M5

    def close():
        if cur and cur[7] == need:
            out.append(Candle(cur[0], ms, cur[1], cur[2], cur[3], cur[4], cur[5], cur[6]))

    for t, o, h, low, c, v, q in rows:
        s = t // ms * ms
        if cur is None or cur[0] != s:
            close()
            cur = [s, o, h, low, c, v, q, 1]
        else:
            cur[2] = max(cur[2], h)
            cur[3] = min(cur[3], low)
            cur[4] = c
            cur[5] += v
            cur[6] += q
            cur[7] += 1
    close()
    return out


def tail(bars, ends, asof, n):
    i = bisect.bisect_right(ends, asof)
    return bars[max(0, i - n) : i]


def work(symbol):
    import logging

    logging.disable(logging.CRITICAL)
    from bybit_flow.models import Instrument
    from bybit_flow.strategy import candidates

    meta = json.load(open(os.path.join(C, "_meta.json")))[symbol]
    rows = json.load(open(os.path.join(C, symbol + ".json")))
    inst = Instrument(
        exchange="binance",
        exchange_symbol=symbol,
        symbol=symbol,
        base=symbol[:-4],
        settle="USDT",
        launch_ms=meta["onboard"],
        tick=Decimal(meta["tick"]),
        qty_step=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5"),
        max_qty=Decimal("1000000000"),
        max_leverage=50,
        funding_interval_minutes=480,
        metadata={},
    )
    bars = {k: aggregate(rows, ms) for k, ms in TF.items()}
    ends = {k: [b.end for b in v] for k, v in bars.items()}
    times = [r[0] for r in rows]
    out, seen, errors = [], set(), 0
    for hz, (ctx, setup, exe, nsetup, hold) in HZ.items():
        for bar in bars[setup][max(nsetup, 130) :]:
            asof = bar.end
            try:
                plans = candidates(
                    inst,
                    tail(bars[ctx], ends[ctx], asof, 120),
                    tail(bars[setup], ends[setup], asof, nsetup),
                    tail(bars[exe], ends[exe], asof, 120),
                    asof,
                    execution_window_ms=60_000,
                    structural_targets=True,
                    volume_profile=None,
                    horizon=hz,
                    quality_policy="trade-validity-v22",
                    reversal_timing=True,
                )
            except Exception:
                errors += 1
                continue
            for s in plans:
                if s.state == "INVALIDATED" or s.id in seen:
                    continue
                seen.add(s.id)
                i = bisect.bisect_left(times, asof)
                if i >= len(rows) - 12:
                    continue
                sign = 1 if s.direction == "LONG" else -1
                e = rows[i][1]
                risk = sign * (e - s.stop)
                if risk <= 0 or sign * (s.tp1 - e) <= 0:
                    continue
                res = {}
                tests = (
                    ("b1", 1, 1, None),
                    ("b2", 2, 1, None),
                    ("plan", sign * (s.tp1 - e) / risk, 1, asof + hold * 60_000),
                )
                for name, up, dn, limit in tests:
                    upper, lower, r = e + sign * up * risk, e - sign * dn * risk, None
                    for t, o, h, low, c, v, q in rows[i : i + 12 * 24 * 10]:
                        if limit and t > limit:
                            r = ("TIME", sign * (o - e) / risk)
                            break
                        # Same-bar tie counts as the stop.
                        if (low <= lower) if sign > 0 else (h >= lower):
                            r = ("DN", -dn)
                            break
                        if (h >= upper) if sign > 0 else (low <= upper):
                            r = ("UP", up)
                            break
                    res[name] = r or ("OPEN", 0)

                def ret(minutes):
                    j = bisect.bisect_right(times, asof + minutes * 60_000) - 1
                    return sign * (rows[j][4] - e) / e * 1e4 if j < len(rows) - 1 else None

                out.append(
                    dict(
                        sym=symbol,
                        t=asof,
                        hz=hz,
                        fam=s.family,
                        dir=s.direction,
                        stop_bps=risk / e * 1e4,
                        tpR=sign * (s.tp1 - e) / risk,
                        regime=s.regime,
                        r60=ret(60),
                        r240=ret(240),
                        r1440=ret(1440),
                        **res,
                    )
                )
    return symbol, out, errors, len(rows)


if __name__ == "__main__":
    symbols = [f[:-5] for f in sorted(os.listdir(C)) if f.endswith(".json") and not f.startswith("_")]
    if len(sys.argv) > 1:
        symbols = sys.argv[1].split(",")
    started, collected = time.time(), []
    with Pool(min(6, len(symbols))) as pool:
        for symbol, out, errors, n in pool.imap_unordered(work, symbols):
            collected += out
            print(
                symbol,
                "candidates",
                len(out),
                "errors",
                errors,
                "bars",
                n,
                round(time.time() - started),
                "s",
                flush=True,
            )
    json.dump(collected, open(os.path.join(HERE, "results.json"), "w"))
