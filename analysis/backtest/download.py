"""Download 12 months of public Binance USDT-M 5m candles for the structure-only entry test."""

import json, time, urllib.request, urllib.parse, os, sys

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "candles")
os.makedirs(OUT, exist_ok=True)
SYMS = (
    sys.argv[1].split(",")
    if len(sys.argv) > 1
    else "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,LINKUSDT,ADAUSDT,AVAXUSDT,LTCUSDT,BCHUSDT,TRXUSDT".split(
        ","
    )
)
DAYS = int(sys.argv[2]) if len(sys.argv) > 2 else 365


def get(path, **q):
    u = "https://fapi.binance.com" + path + "?" + urllib.parse.urlencode(q)
    for a in range(5):
        try:
            with urllib.request.urlopen(
                urllib.request.Request(u, headers={"User-Agent": "research-backtest"}), timeout=25
            ) as r:
                return json.load(r)
        except Exception as e:
            err = e
            time.sleep(2 * (a + 1))
    raise err


info = {s["symbol"]: s for s in get("/fapi/v1/exchangeInfo")["symbols"]}
json.dump(
    {
        s: dict(
            tick=next(f["tickSize"] for f in info[s]["filters"] if f["filterType"] == "PRICE_FILTER"),
            onboard=info[s]["onboardDate"],
        )
        for s in SYMS
        if s in info
    },
    open(os.path.join(OUT, "_meta.json"), "w"),
)
end = int(time.time() * 1000) // 300000 * 300000
start = end - DAYS * 86400000
for s in SYMS:
    f = os.path.join(OUT, s + ".json")
    if os.path.exists(f):
        continue
    rows = []
    t = start
    while t < end:
        k = get("/fapi/v1/klines", symbol=s, interval="5m", startTime=t, limit=1500)
        if not k:
            break
        rows += [
            [x[0], float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), float(x[7])] for x in k
        ]
        t = k[-1][0] + 300000
        time.sleep(0.25)
    json.dump(rows, open(f, "w"))
    print(s, len(rows), flush=True)
print("done")
