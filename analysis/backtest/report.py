"""Report for run.py: does the candle structure alone predict direction?"""

import collections
import datetime
import json
import math
import os
import statistics as st

HERE = os.path.dirname(os.path.abspath(__file__))
R = json.load(open(os.path.join(HERE, "results.json")))
COST = 15  # bps round trip: the project's own taker fee and slippage assumptions


def day(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC).date()


print(
    "structure-only candidates:",
    len(R),
    "| symbols:",
    len({r["sym"] for r in R}),
    "| span:",
    day(min(r["t"] for r in R)),
    "to",
    day(max(r["t"] for r in R)),
)


def line(name, rs, key="b1", up=1):
    c = collections.Counter(r[key][0] for r in rs)
    n = c["UP"] + c["DN"]
    if n < 20:
        return
    null = 1 / (1 + up)
    p = c["UP"] / n
    z = (p - null) / math.sqrt(null * (1 - null) / n)
    net = st.mean(r[key][1] - COST / r["stop_bps"] for r in rs)
    # Same-day candidates across symbols move together, so cluster the error by day.
    days = collections.defaultdict(list)
    for r in rs:
        days[r["t"] // 86_400_000].append(r[key][1] - COST / r["stop_bps"])
    sums = [sum(v) for v in days.values()]
    se = st.pstdev(sums) * math.sqrt(len(sums)) / len(rs) if len(sums) > 1 else float("nan")
    print(
        f"{name:34s} n={len(rs):5d}  P(+{up}R first)={100 * p:5.1f}% vs {100 * null:4.1f}% z={z:+5.1f}"
        f"   net {net:+.3f}R/trade (day-clustered ±{1.96 * se:.3f})"
    )


for hz in sorted({r["hz"] for r in R}):
    rs = [r for r in R if r["hz"] == hz]
    print("\n==", hz)
    line("ALL  +1R before -1R", rs)
    line("ALL  +2R before -1R", rs, "b2", 2)
    c = collections.Counter(r["plan"][0] for r in rs)
    print(
        "   published plan within hold window:",
        dict(c),
        " net %.3fR/trade" % st.mean(r["plan"][1] - COST / r["stop_bps"] for r in rs),
        " | median planned RR %.2f, median stop %.0f bps"
        % (st.median(r["tpR"] for r in rs), st.median(r["stop_bps"] for r in rs)),
    )
    for f in sorted({r["fam"] for r in rs}):
        line("family " + f, [r for r in rs if r["fam"] == f])
    for d in ("LONG", "SHORT"):
        line(d, [r for r in rs if r["dir"] == d])
    for g in sorted({str(r["regime"]) for r in rs}):
        line("regime " + g, [r for r in rs if str(r["regime"]) == g])
    ts = sorted(r["t"] for r in rs)
    for q in range(4):
        a, b = ts[len(ts) * q // 4], ts[min(len(ts) - 1, len(ts) * (q + 1) // 4)]
        line(f"time quarter {q + 1}", [r for r in rs if a <= r["t"] < b or (q == 3 and r["t"] == b)])
    for k, label in (("r60", "1h"), ("r240", "4h"), ("r1440", "24h")):
        v = [r[k] for r in rs if r[k] is not None]
        print(
            f"   signed return after {label:3s}: mean {st.mean(v):+6.1f} bps,"
            f" right direction {100 * st.mean(x > 0 for x in v):.1f}% (n={len(v)})"
        )
