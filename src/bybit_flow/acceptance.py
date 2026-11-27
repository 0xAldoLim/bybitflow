"""Proof of profitability, or its absence, for each entry rule.

`run` replays the production entry function (`strategy.candidates`) over public
candles and measures what price did next. `evaluate` turns those rows into a
verdict per rule. `fit_grades` derives the outcome-based grade from the same
rows. The results are written next to this module and shipped with the code, so
the running system can state, for every alert, what its rule has demonstrated.

Protocol, fixed before looking at results:

- Entry at the next 5-minute open after the setup bar closes. R is the distance
  to the published stop. A bar touching both barriers counts as the stop.
- Costs: taker fee and slippage on both sides, from the default settings.
- The first 75% of the period is the test sample, the last 25% is held out.
- A rule is ACCEPTED only if, in the test sample, "+1R before -1R" beats 50%
  with a z-score corrected for the number of rules tried, and the traded plan
  nets a positive R with a day-clustered lower bound above zero; and, in the
  held-out sample, it still beats 50% and nets a positive R.
- Variants are declared here, not chosen after the fact: each rule as published,
  and its mirror (same trigger, opposite direction, 2R target, 1R stop).
- Three literature baselines are replayed beside the scanner's rules: follow the
  sign of the prior 24h, 72h and 168h return at each daily close (time-series
  momentum), stop 1.5 x ATR(4h), 2R target, 48h hold. Their mirrors are the
  matching reversal rules. They are not scanner rules: a baseline that passes
  is a lead to implement, and it counts toward the multiple-testing correction.

Second round, declared 2026-11-27 before it was run:

- Swing continuation families (trend pullback, breakout retest) with four
  exits: fixed 1R, 1.5R and 2R targets, and no target at all (stop or the 48h
  time exit).
- Resting-order entries for core intraday and swing: a limit at the setup-bar
  close ("maker_close") or 0.25R better ("maker_pullback"), filled only if price
  trades through it within one setup bar. Entry and target exits pay the maker
  fee; stops and time exits pay taker fee and slippage. Unfilled orders are not
  trades.
- Funding extremes: at each funding settlement, fade a rate in the top or bottom
  tenth of its own trailing 270 settlements. Its mirror follows the crowd.
- Cross-sectional momentum: at each daily close, long the three strongest and
  short the three weakest symbols by 7-day return. Its mirror is the reversal.

Every rule ever tried counts toward the multiple-testing correction.

This is a structure-only test: order-flow confirmation and later gates need
recorded ticks and are not replayed. It bounds what the rule itself contributes.
"""

import bisect
import collections
import json
import math
import os
import statistics as st
import time
import urllib.parse
import urllib.request
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).parent
REPORT_FILE = HERE / "acceptance_report.json"
MODEL_FILE = HERE / "outcome_model.json"
PROTOCOL = "barrier-acceptance-v1"
M5 = 300_000
DAY_MS = 86_400_000
TIMEFRAMES = {"5": M5, "15": 900_000, "60": 3_600_000, "240": 14_400_000, "D": DAY_MS}
# context, setup, execution timeframe, setup bars supplied, hold minutes, days replayed
HORIZONS = {
    "SHORT_INTRADAY": ("60", "15", "5", 120, 120, 120),
    "CORE_INTRADAY": ("240", "60", "15", 200, 240, 365),
    "SWING": ("D", "240", "60", 120, 2880, 1095),
}
VARIANTS = ("as_published", "mirrored")
EXIT_TARGETS = {"target_1r": 1.0, "target_1.5r": 1.5, "target_2r": 2.0}
CONTINUATION = {"trend_pullback", "breakout_retest"}
MAKER_HORIZONS = {"CORE_INTRADAY", "SWING"}
MAKER_OFFSETS = {"maker_close": 0.0, "maker_pullback": 0.25}  # in units of planned risk
MAKER_FEE_BPS = 2.0
# Replication set, declared 2026-11-27 before it was replayed: the 48 highest-volume
# USDT perpetuals listed for at least three years, excluding DEFAULT_SYMBOLS. None
# of them was used to form any rule or to notice the swing trend pullback lead.
REPLICATION_SYMBOLS = "ZECUSDT,NEARUSDT,SSVUSDT,1000PEPEUSDT,XMRUSDT,BIGTIMEUSDT,UNIUSDT,SUIUSDT,SEIUSDT,AAVEUSDT,WLDUSDT,INJUSDT,ONEUSDT,BLURUSDT,FETUSDT,ARBUSDT,FILUSDT,QNTUSDT,DOTUSDT,HBARUSDT,TIAUSDT,XLMUSDT,ATOMUSDT,APTUSDT,ICPUSDT,1000SHIBUSDT,OPUSDT,CRVUSDT,DASHUSDT,1000BONKUSDT,RIFUSDT,SANDUSDT,ETCUSDT,1000LUNCUSDT,TRBUSDT,GALAUSDT,LDOUSDT,DODOXUSDT,ALGOUSDT,PYTHUSDT,PENDLEUSDT,KAVAUSDT,CYBERUSDT,RUNEUSDT,BEAMXUSDT,STXUSDT,CAKEUSDT,ORDIUSDT"
# Mid-cap set, declared 2026-11-27 before it was replayed: USDT perpetuals ranked
# 101 to 250 by 24h quote volume, listed at least one year, not used above; the
# first 40 by rank. Their median half-spread measured 2.06 bps against 0.52 bps
# for DEFAULT_SYMBOLS, so slippage per side is declared as 4 bps instead of 2.
MIDCAP_SYMBOLS = "SPKUSDT,VIRTUALUSDT,AEROUSDT,ACEUSDT,PROMUSDT,FARTCOINUSDT,VELVETUSDT,PIPPINUSDT,RENDERUSDT,AVAAIUSDT,SENTUSDT,SAGAUSDT,MOODENGUSDT,AGTUSDT,REZUSDT,BABYUSDT,SYRUPUSDT,SKYAIUSDT,AINUSDT,LAYERUSDT,RAYSOLUSDT,ATUSDT,JTOUSDT,ENSOUSDT,CVCUSDT,BEATUSDT,HYPERUSDT,QUSDT,BARDUSDT,JSTUSDT,RIVERUSDT,KGENUSDT,IRYSUSDT,POLUSDT,ONUSDT,EIGENUSDT,LABUSDT,GRASSUSDT,METUSDT,ERAUSDT"
MIDCAP_SLIPPAGE_BPS = 4.0
DEFAULT_SYMBOLS = (
    "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,LINKUSDT,ADAUSDT,AVAXUSDT,LTCUSDT,BCHUSDT,TRXUSDT"
)
BASELINE = "BASELINE_DAILY"
BASELINE_LOOKBACKS = {"momentum_24h": 6, "momentum_72h": 18, "momentum_168h": 42}  # 4h bars
TIERS = ("SSS", "SS", "S", "A", "B", "C", "D", "E", "F")
FAMILIES = ("liquidity_sweep", "trend_pullback", "range_rejection", "breakout_retest")
FUNDING_WINDOW, FUNDING_MIN = 270, 90
MIN_TEST, MIN_HOLDOUT = 200, 50


def taker_cost_bps():
    """One taker side: fee plus slippage, from the default settings."""
    from .config import Settings

    fields = Settings.model_fields
    return fields["taker_fee_bps"].default + fields["slippage_bps"].default


# ---------------------------------------------------------------- features


def outcome_features(signal):
    """Inputs known when a setup is created; identical in replay and live."""
    e = signal.evidence
    context = e.get("context_features") or e.get("h4") or {}
    setup = e.get("setup_features") or e.get("h1") or {}
    sign = 1 if signal.direction == "LONG" else -1
    risk = abs(signal.entry - signal.stop)
    atr = setup.get("atr") or 0
    regime = context.get("regime") or signal.regime
    aligned = "trending up" if sign > 0 else "trending down"
    features = dict(
        stop_atr=risk / atr if atr else 0.0,
        planned_rr=abs(signal.tp1 - signal.entry) / risk if risk else 0.0,
        trend_alignment=1.0 if regime == aligned else -1.0 if str(regime).startswith("trending") else 0.0,
        context_efficiency=float(context.get("efficiency") or 0),
        setup_efficiency=float(setup.get("efficiency") or 0),
        context_slope=sign * float(context.get("slope") or 0),
        setup_slope=sign * float(setup.get("slope") or 0),
        ma_gap=sign * (signal.entry - setup["ma20"]) / atr if atr and setup.get("ma20") else 0.0,
        realized_volatility=float(setup.get("realized_volatility") or 0),
        long=1.0 if sign > 0 else 0.0,
    )
    for family in FAMILIES[1:]:
        features["family_" + family] = 1.0 if signal.family == family else 0.0
    return features


# ---------------------------------------------------------------- candles


def download(symbols, days, directory, pause=0.25):
    """Public Binance USDT-M 5m candles. No key, no account."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    def get(path, **query):
        url = "https://fapi.binance.com" + path + "?" + urllib.parse.urlencode(query)
        error = None
        for attempt in range(5):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "research-backtest"})
                with urllib.request.urlopen(request, timeout=25) as response:
                    return json.load(response)
            except Exception as exc:  # noqa: BLE001 - retried, then raised
                error = exc
                time.sleep(2 * (attempt + 1))
        raise error

    info = {s["symbol"]: s for s in get("/fapi/v1/exchangeInfo")["symbols"]}
    meta_path = directory / "_meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    end = int(time.time() * 1000) // M5 * M5

    def fetch(symbol):
        target = directory / (symbol + ".json")
        if target.exists():
            return
        rows, cursor = [], end - days * DAY_MS
        while cursor < end:
            batch = get("/fapi/v1/klines", symbol=symbol, interval="5m", startTime=cursor, limit=1500)
            if not batch:
                break
            rows += [
                [k[0], float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]), float(k[7])]
                for k in batch
            ]
            cursor = batch[-1][0] + M5
            time.sleep(pause)
        target.write_text(json.dumps(rows))

    wanted = [s for s in symbols if s in info]
    for symbol in wanted:
        tick = next(f["tickSize"] for f in info[symbol]["filters"] if f["filterType"] == "PRICE_FILTER")
        meta[symbol] = dict(tick=tick, onboard=info[symbol]["onboardDate"])
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(4) as pool:  # Well inside the public request-weight limit.
        list(pool.map(fetch, wanted))
    meta_path.write_text(json.dumps(meta))
    return sorted(meta)


def aggregate(rows, ms):
    from .models import Candle

    out, current, need = [], None, ms // M5

    def close():
        if current and current[7] == need:
            out.append(Candle(*current[:1], ms, *current[1:7]))

    for t, o, h, low, c, v, q in rows:
        start = t // ms * ms
        if current is None or current[0] != start:
            close()
            current = [start, o, h, low, c, v, q, 1]
        else:
            current[2], current[3], current[4] = max(current[2], h), min(current[3], low), c
            current[5] += v
            current[6] += q
            current[7] += 1
    close()
    return out


def first_touch(rows, start, entry, sign, up, down, risk, limit=None, span=2880):
    """First barrier reached, in R. Same-bar tie is the stop."""
    upper, lower = entry + sign * up * risk, entry - sign * down * risk
    for t, o, h, low, _c, _v, _q in rows[start : start + span]:
        if limit and t > limit:
            return "TIME", sign * (o - entry) / risk
        if (low <= lower) if sign > 0 else (h >= lower):
            return "DN", -down
        if (h >= upper) if sign > 0 else (low <= upper):
            return "UP", up
    return "OPEN", 0.0


def maker_entry(rows, start, signal, sign, offset, wait_bars, hold_ms, taker_side=None):
    """A resting limit order. No fill, no trade; the fill bar can only stop it out."""
    planned = abs(signal.entry - signal.stop)
    limit = signal.entry - sign * offset * planned
    risk = sign * (limit - signal.stop)
    target = sign * (signal.tp1 - limit)
    if risk <= 0 or target <= 0:
        return None
    for j in range(start, min(start + wait_bars, len(rows) - 2)):
        t, _o, high, low = rows[j][:4]
        if not ((low < limit) if sign > 0 else (high > limit)):
            continue  # Must trade through the price, not merely touch it.
        stopped = (low <= signal.stop) if sign > 0 else (high >= signal.stop)
        even = ("DN", -1) if stopped else first_touch(rows, j + 1, limit, sign, 1, 1, risk)
        plan = (
            ("DN", -1)
            if stopped
            else first_touch(rows, j + 1, limit, sign, target / risk, 1, risk, t + hold_ms)
        )
        exit_cost = MAKER_FEE_BPS if plan[0] == "UP" else (taker_side or taker_cost_bps())
        return dict(even=even, plan=plan, stop_bps=risk / limit * 1e4, cost_bps=MAKER_FEE_BPS + exit_cost)
    return None


def replay_symbol(args):
    """One symbol, every horizon. Top-level so worker processes can import it."""
    symbol, directory, horizons, taker_side = args
    import logging

    logging.disable(logging.CRITICAL)
    from .models import Instrument
    from .strategy import candidates

    directory = Path(directory)
    meta = json.loads((directory / "_meta.json").read_text())[symbol]
    rows = json.loads((directory / (symbol + ".json")).read_text())
    instrument = Instrument(
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
    bars = {k: aggregate(rows, ms) for k, ms in TIMEFRAMES.items()}
    ends = {k: [b.end for b in v] for k, v in bars.items()}
    times = [r[0] for r in rows]

    def tail(tf, asof, n):
        i = bisect.bisect_right(ends[tf], asof)
        return bars[tf][max(0, i - n) : i]

    out, seen, errors = [], set(), 0
    for horizon in horizons:
        context, setup, execution, supplied, hold, days = HORIZONS[horizon]
        earliest = times[-1] - days * DAY_MS
        for bar in bars[setup][max(supplied, 130) :]:
            asof = bar.end
            if asof < earliest:
                continue
            try:
                plans = candidates(
                    instrument,
                    tail(context, asof, 120),
                    tail(setup, asof, supplied),
                    tail(execution, asof, 120),
                    asof,
                    execution_window_ms=60_000,
                    structural_targets=True,
                    volume_profile=None,
                    horizon=horizon,
                    quality_policy="trade-validity-v22",
                    reversal_timing=True,
                )
            except Exception:  # noqa: BLE001 - counted and reported
                errors += 1
                continue
            for signal in plans:
                if signal.state == "INVALIDATED" or signal.id in seen:
                    continue
                seen.add(signal.id)
                i = bisect.bisect_left(times, asof)
                if i >= len(rows) - 12:
                    continue
                sign = 1 if signal.direction == "LONG" else -1
                entry = rows[i][1]
                risk = sign * (entry - signal.stop)
                target = sign * (signal.tp1 - entry)
                if risk <= 0 or target <= 0:
                    continue
                deadline = asof + hold * 60_000
                even = first_touch(rows, i, entry, sign, 1, 1, risk)
                row = dict(
                    symbol=symbol,
                    t=asof,
                    horizon=horizon,
                    family=signal.family,
                    direction=signal.direction,
                    stop_bps=risk / entry * 1e4,
                    features=outcome_features(signal),
                    as_published=dict(
                        even=even,
                        plan=first_touch(rows, i, entry, sign, target / risk, 1, risk, deadline),
                    ),
                    mirrored=dict(
                        even=first_touch(rows, i, entry, -sign, 1, 1, risk),
                        plan=first_touch(rows, i, entry, -sign, 2, 1, risk, deadline),
                    ),
                )
                if horizon == "SWING" and signal.family in CONTINUATION:
                    for name, multiple in EXIT_TARGETS.items():
                        row[name] = dict(
                            even=even, plan=first_touch(rows, i, entry, sign, multiple, 1, risk, deadline)
                        )
                    row["time_exit"] = dict(
                        even=even, plan=first_touch(rows, i, entry, sign, 1e9, 1, risk, deadline)
                    )
                if horizon in MAKER_HORIZONS:
                    wait = TIMEFRAMES[setup] // M5
                    for name, offset in MAKER_OFFSETS.items():
                        filled = maker_entry(rows, i, signal, sign, offset, wait, hold * 60_000, taker_side)
                        if filled:
                            row[name] = filled
                out.append(row)
    return symbol, out, errors


def _atr_risk(bars, k):
    window = bars[k - 14 : k + 1]
    ranges = [
        max(b.high - b.low, abs(b.high - a.close), abs(b.low - a.close)) for a, b in zip(window, window[1:])
    ]
    return 1.5 * sum(ranges) / len(ranges)


def _baseline_row(symbol, family, rows, times, at_ms, sign, risk):
    """Shared geometry for every baseline: 1.5 x ATR(4h) stop, 2R target, 48h hold."""
    i = bisect.bisect_left(times, at_ms)
    if i >= len(rows) - 12 or risk <= 0:
        return None
    entry, deadline = rows[i][1], at_ms + 2880 * 60_000
    return dict(
        symbol=symbol,
        t=at_ms,
        horizon=BASELINE,
        family=family,
        direction="LONG" if sign > 0 else "SHORT",
        stop_bps=risk / entry * 1e4,
        features={},
        as_published=dict(
            even=first_touch(rows, i, entry, sign, 1, 1, risk),
            plan=first_touch(rows, i, entry, sign, 2, 1, risk, deadline),
        ),
        mirrored=dict(
            even=first_touch(rows, i, entry, -sign, 1, 1, risk),
            plan=first_touch(rows, i, entry, -sign, 2, 1, risk, deadline),
        ),
    )


def _load_symbol(symbol, directory):
    rows = json.loads((Path(directory) / (symbol + ".json")).read_text())
    return rows, aggregate(rows, TIMEFRAMES["240"]), [r[0] for r in rows]


def baseline_rows(symbol, directory):
    """Time-series momentum at each daily close, on the same candles and protocol."""
    rows, bars, times = _load_symbol(symbol, directory)
    out = []
    for k in range(max(BASELINE_LOOKBACKS.values()) + 15, len(bars)):
        bar = bars[k]
        if bar.end % DAY_MS:
            continue
        risk = _atr_risk(bars, k)
        for family, lookback in BASELINE_LOOKBACKS.items():
            move = bar.close - bars[k - lookback].close
            if move:
                row = _baseline_row(symbol, family, rows, times, bar.end, 1 if move > 0 else -1, risk)
                if row:
                    out.append(row)
    return out


def download_funding(symbols, directory):
    """Public funding-rate history; one file per symbol."""
    directory = Path(directory)
    for symbol in symbols:
        target = directory / ("_funding_" + symbol + ".json")
        if target.exists():
            continue
        first = json.loads((directory / (symbol + ".json")).read_text())[0][0]
        records, cursor = [], first
        while True:
            query = urllib.parse.urlencode(dict(symbol=symbol, startTime=cursor, limit=1000))
            request = urllib.request.Request(
                "https://fapi.binance.com/fapi/v1/fundingRate?" + query,
                headers={"User-Agent": "research-backtest"},
            )
            batch = None
            for attempt in range(5):  # Public endpoint; brief stalls are common.
                try:
                    with urllib.request.urlopen(request, timeout=25) as response:
                        batch = json.load(response)
                    break
                except Exception:  # noqa: BLE001 - retried, then raised
                    if attempt == 4:
                        raise
                    time.sleep(2 * (attempt + 1))
            records += [[int(r["fundingTime"]), float(r["fundingRate"])] for r in batch]
            if len(batch) < 1000:
                break
            cursor = records[-1][0] + 1
            time.sleep(0.5)
        target.write_text(json.dumps(records))


def funding_rows(symbol, directory):
    """Fade a funding rate that is extreme against its own recent history."""
    path = Path(directory) / ("_funding_" + symbol + ".json")
    if not path.exists():
        return []
    funding = json.loads(path.read_text())
    rows, bars, times = _load_symbol(symbol, directory)
    ends = [b.end for b in bars]
    out = []
    for n in range(FUNDING_MIN, len(funding)):
        at_ms, rate = funding[n]
        history = sorted(r for _t, r in funding[max(0, n - FUNDING_WINDOW) : n])
        low, high = history[len(history) // 10], history[len(history) * 9 // 10]
        if low < rate < high or low == high:
            continue
        k = bisect.bisect_right(ends, at_ms) - 1
        if k < 16:
            continue
        # A high rate means longs pay: the crowd is long, so the fade is short.
        row = _baseline_row(
            symbol, "funding_extreme", rows, times, at_ms, -1 if rate >= high else 1, _atr_risk(bars, k)
        )
        if row:
            out.append(row)
    return out


def cross_sectional_rows(symbols, directory, lookback=42, legs=3):
    """Long the strongest and short the weakest symbols by 7-day return, daily."""
    data = {s: _load_symbol(s, directory) for s in symbols}
    index = {s: {b.end: k for k, b in enumerate(data[s][1])} for s in symbols}
    days = sorted({end for s in symbols for end in index[s] if not end % DAY_MS})
    out = []
    for day in days:
        ranked = []
        for s in symbols:
            k = index[s].get(day)
            if k is not None and k >= lookback + 15:
                bars = data[s][1]
                ranked.append((bars[k].close / bars[k - lookback].close - 1, s, k))
        if len(ranked) < 4 * legs:
            continue
        ranked.sort()
        for sign, group in ((1, ranked[-legs:]), (-1, ranked[:legs])):
            for _ret, s, k in group:
                rows, bars, times = data[s]
                row = _baseline_row(s, "cross_momentum_7d", rows, times, day, sign, _atr_risk(bars, k))
                if row:
                    out.append(row)
    return out


def all_baselines(directory):
    directory = Path(directory)
    symbols = sorted(p.stem for p in directory.glob("*.json") if not p.stem.startswith("_"))
    out = cross_sectional_rows(symbols, directory)
    for symbol in symbols:
        out += baseline_rows(symbol, directory) + funding_rows(symbol, directory)
    return out


def run(symbols, directory, workers=None, horizons=tuple(HORIZONS), progress=print, taker_side=None):
    from multiprocessing import Pool

    symbols = download(symbols, max(HORIZONS[h][5] for h in horizons), directory)
    rows, errors, started = [], 0, time.time()
    jobs = [(s, str(directory), tuple(horizons), taker_side) for s in symbols]
    with Pool(workers or min(len(jobs), max(1, (os.cpu_count() or 2) - 2))) as pool:
        for symbol, out, failed in pool.imap_unordered(replay_symbol, jobs):
            rows += out
            errors += failed
            progress(f"{symbol}: {len(out)} candidates, {failed} errors, {round(time.time() - started)}s")
    return rows, errors


# ---------------------------------------------------------------- statistics


def net_r(row, variant, cost_bps):
    """Net R of one variant. Variants may carry their own stop distance and cost."""
    v = row[variant]
    return v["plan"][1] - v.get("cost_bps", cost_bps) / v.get("stop_bps", row["stop_bps"])


def variants_of(rows):
    return sorted({k for r in rows for k, v in r.items() if isinstance(v, dict) and "plan" in v})


def clustered(values_by_day):
    """Mean and standard error, treating each UTC day as one observation cluster."""
    n = sum(len(v) for v in values_by_day.values())
    if not n:
        return 0.0, float("inf"), 0
    mean = sum(sum(v) for v in values_by_day.values()) / n
    residual = [sum(x - mean for x in v) for v in values_by_day.values()]
    k = len(residual)
    se = math.sqrt(sum(r * r for r in residual) * k / (k - 1)) / n if k > 1 else float("inf")
    return mean, se, n


def summarize(rows, variant, cost_bps):
    wins = sum(r[variant]["even"][0] == "UP" for r in rows)
    resolved = sum(r[variant]["even"][0] in {"UP", "DN"} for r in rows)
    by_day = collections.defaultdict(list)
    for r in rows:
        by_day[r["t"] // DAY_MS].append(net_r(r, variant, cost_bps))
    mean, se, n = clustered(by_day)
    hit = wins / resolved if resolved else 0.0
    return dict(
        n=n,
        hit_rate=round(hit, 4),
        z=round((hit - 0.5) / math.sqrt(0.25 / resolved), 2) if resolved else 0.0,
        plan_net_r=round(mean, 4),
        plan_net_r_lower=round(mean - 1.96 * se, 4) if math.isfinite(se) else None,
        days=len(by_day),
    )


def verdict(test, holdout, z_required):
    if test["n"] < MIN_TEST or holdout["n"] < MIN_HOLDOUT:
        return "INSUFFICIENT_SAMPLE"
    passed = (
        test["z"] >= z_required
        and (test["plan_net_r_lower"] or -1) > 0
        and holdout["hit_rate"] > 0.5
        and holdout["plan_net_r"] > 0
    )
    return "ACCEPTED" if passed else "REJECTED"


def split(rows, fraction=0.75):
    """Chronological split per horizon; later data is never used to choose rules."""
    cut = {}
    for horizon in {r["horizon"] for r in rows}:
        times = sorted(r["t"] for r in rows if r["horizon"] == horizon)
        cut[horizon] = times[int(len(times) * fraction)] if times else 0
    test = [r for r in rows if r["t"] < cut[r["horizon"]]]
    holdout = [r for r in rows if r["t"] >= cut[r["horizon"]]]
    return test, holdout


def evaluate(rows, cost_bps, errors=0, tried=None):
    test, holdout = split(rows)
    keys = sorted({(r["horizon"], r["family"]) for r in rows})
    names = {key: variants_of([r for r in rows if (r["horizon"], r["family"]) == key]) for key in keys}
    # A replication keeps the bar set by every rule tried so far.
    tried = max(tried or 0, 1, sum(len(v) for v in names.values()))
    z_required = round(st.NormalDist().inv_cdf(1 - 0.05 / tried), 2)
    segments = {}
    for horizon, family in keys:
        for variant in names[(horizon, family)]:

            def pick(sample):
                return [
                    r for r in sample if r["horizon"] == horizon and r["family"] == family and variant in r
                ]

            a, b = summarize(pick(test), variant, cost_bps), summarize(pick(holdout), variant, cost_bps)
            segments[f"{horizon}|{family}|{variant}"] = dict(
                scanner_rule=horizon in HORIZONS and variant == "as_published",
                horizon=horizon,
                family=family,
                variant=variant,
                test=a,
                holdout=b,
                all=summarize(pick(rows), variant, cost_bps),
                verdict=verdict(a, b, z_required),
            )
    stamps = [r["t"] for r in rows]
    return dict(
        protocol=PROTOCOL,
        generated_ms=int(time.time() * 1000),
        cost_bps=cost_bps,
        candidates=len(rows),
        replay_errors=errors,
        symbols=sorted({r["symbol"] for r in rows}),
        span_ms=[min(stamps), max(stamps)] if stamps else None,
        rules_tried=tried,
        z_required=z_required,
        scope="Structure only: order-flow confirmation and later gates are not replayed",
        accepted=sorted(k for k, v in segments.items() if v["verdict"] == "ACCEPTED"),
        segments=segments,
    )


# ---------------------------------------------------------------- outcome grades


def grade_from(mean, lower):
    """Grade a demonstrated expectancy. Upper grades need a proven lower bound."""
    for threshold, grade in ((0.50, "SSS"), (0.40, "SS"), (0.30, "S"), (0.15, "A"), (0.05, "B")):
        if lower is not None and lower >= threshold:
            return grade
    if mean >= 0:
        return "C"
    return "D" if mean >= -0.15 else "E" if mean >= -0.30 else "F"


def _ridge(matrix, target, penalty):
    import numpy as np

    x, y = np.asarray(matrix, float), np.asarray(target, float)
    mean, scale = x.mean(axis=0), x.std(axis=0)
    scale[scale == 0] = 1.0
    z = (x - mean) / scale
    weights = np.linalg.solve(z.T @ z + penalty * np.eye(z.shape[1]), z.T @ (y - y.mean()))
    return dict(mean=mean.tolist(), scale=scale.tolist(), weights=weights.tolist(), intercept=float(y.mean()))


def predict(model, features):
    return model["intercept"] + sum(
        w * (features.get(name, 0.0) - m) / s
        for name, w, m, s in zip(model["names"], model["weights"], model["mean"], model["scale"])
    )


def fit_grades(rows, cost_bps, bins=10, penalty=200.0):
    """Walk-forward ridge on the published plan's net R, then calibrate by bin.

    The model is trusted only if its out-of-sample top bin beats its bottom bin
    by more than two clustered standard errors. Otherwise the grade falls back
    to the rule's own measured expectancy, which needs no model.
    """
    rows = sorted((r for r in rows if r["horizon"] in HORIZONS), key=lambda r: r["t"])
    names = sorted(rows[0]["features"]) + ["horizon_SHORT_INTRADAY", "horizon_SWING"]

    def vector(r):
        f = dict(r["features"])
        f["horizon_SHORT_INTRADAY"] = float(r["horizon"] == "SHORT_INTRADAY")
        f["horizon_SWING"] = float(r["horizon"] == "SWING")
        return f

    def fit(sample):
        model = _ridge(
            [[vector(r)[n] for n in names] for r in sample],
            [max(-1.5, min(6.0, net_r(r, "as_published", cost_bps))) for r in sample],
            penalty,
        )
        return model | dict(names=names)

    scored, n = [], len(rows)
    for fold in range(3):  # train on the past, score the next sixth of the period
        a, b = n * (3 + fold) // 6, n * (4 + fold) // 6
        model = fit(rows[:a])
        scored += [(predict(model, vector(r)), r) for r in rows[a:b]]
    scored.sort(key=lambda item: item[0])
    table = []
    for k in range(bins):
        chunk = scored[len(scored) * k // bins : len(scored) * (k + 1) // bins]
        by_day = collections.defaultdict(list)
        for _score, r in chunk:
            by_day[r["t"] // DAY_MS].append(net_r(r, "as_published", cost_bps))
        mean, se, count = clustered(by_day)
        table.append(
            dict(
                upper_score=chunk[-1][0] if k < bins - 1 else None,
                n=count,
                mean_r=round(mean, 4),
                lower_r=round(mean - 1.96 * se, 4),
                se=round(se, 4),
                grade=grade_from(mean, mean - 1.96 * se),
            )
        )
    spread = table[-1]["mean_r"] - table[0]["mean_r"]
    spread_z = spread / math.sqrt(table[-1]["se"] ** 2 + table[0]["se"] ** 2)
    # Ordered overall, without demanding that every noisy neighbouring bin agrees.
    rising = sum(y["mean_r"] > x["mean_r"] for x, y in zip(table, table[1:]))
    monotonic = rising >= 0.6 * (len(table) - 1)
    rules = {}
    for horizon, family in sorted({(r["horizon"], r["family"]) for r in rows}):
        by_day = collections.defaultdict(list)
        for r in rows:
            if r["horizon"] == horizon and r["family"] == family:
                by_day[r["t"] // DAY_MS].append(net_r(r, "as_published", cost_bps))
        mean, se, count = clustered(by_day)
        rules[f"{horizon}|{family}"] = dict(
            n=count,
            mean_r=round(mean, 4),
            lower_r=round(mean - 1.96 * se, 4),
            grade=grade_from(mean, mean - 1.96 * se),
        )
    return dict(
        version="outcome-grade-v1",
        generated_ms=int(time.time() * 1000),
        cost_bps=cost_bps,
        target="Net R of the published plan within its hold window, public candles",
        skill=bool(spread_z >= 2 and monotonic),
        out_of_sample=dict(n=len(scored), top_minus_bottom_r=round(spread, 4), z=round(spread_z, 2)),
        calibration=table,
        rules=rules,
        model=fit(rows),
    )


# ---------------------------------------------------------------- runtime lookups

_cache = {}


def _load(path):
    path = Path(path)
    stamp = path.stat().st_mtime_ns if path.exists() else None
    if _cache.get(path, (None, None))[0] != stamp:
        _cache[path] = (stamp, json.loads(path.read_text()) if stamp else None)
    return _cache[path][1]


def rule_evidence(signal, report=None):
    """What the alert's rule has demonstrated. Unknown rules are not accepted."""
    report = report if report is not None else _load(REPORT_FILE)
    key = f"{signal.horizon_profile}|{signal.family}|as_published"
    segment = (report or {}).get("segments", {}).get(key)
    if not segment:
        return dict(verdict="NOT_EVALUATED", key=key, protocol=PROTOCOL)
    return dict(
        verdict=segment["verdict"],
        key=key,
        protocol=report["protocol"],
        n=segment["all"]["n"],
        hit_rate=segment["all"]["hit_rate"],
        plan_net_r=segment["all"]["plan_net_r"],
        holdout_net_r=segment["holdout"]["plan_net_r"],
        generated_ms=report["generated_ms"],
    )


def outcome_grade(signal, model=None):
    """Grade from measured outcomes of setups like this one."""
    model = model if model is not None else _load(MODEL_FILE)
    if not model:
        return dict(grade="F", basis="NO_OUTCOME_MODEL", expected_r=None, lower_r=None)
    rule = model["rules"].get(f"{signal.horizon_profile}|{signal.family}")
    if model.get("skill"):
        features = outcome_features(signal)
        features["horizon_SHORT_INTRADAY"] = float(signal.horizon_profile == "SHORT_INTRADAY")
        features["horizon_SWING"] = float(signal.horizon_profile == "SWING")
        score = predict(model["model"], features)
        row = next(
            (b for b in model["calibration"] if b["upper_score"] is None or score <= b["upper_score"]),
            model["calibration"][-1],
        )
        basis = "CALIBRATED_MODEL"
    elif rule:
        row, basis = rule, "RULE_EXPECTANCY"
    else:
        return dict(grade="F", basis="RULE_NOT_EVALUATED", expected_r=None, lower_r=None)
    return dict(
        grade=row["grade"],
        basis=basis,
        expected_r=row["mean_r"],
        lower_r=row["lower_r"],
        n=row["n"],
        version=model["version"],
    )


def render(report, model=None):
    def day(ms):
        return time.strftime("%Y-%m-%d", time.gmtime(ms / 1000))

    lines = [
        f"{report['candidates']} candidates, {len(report['symbols'])} symbols, "
        f"{day(report['span_ms'][0])} to {day(report['span_ms'][1])}, cost {report['cost_bps']} bps",
        report["scope"],
        f"Required z after trying {report['rules_tried']} rules: {report['z_required']}",
        "",
        f"{'rule':46s}{'n':>6}{'hit%':>7}{'z':>7}{'netR':>8}{'lower':>8}{'held-out':>10}  verdict",
    ]
    for key, s in report["segments"].items():
        t, h = s["test"], s["holdout"]
        lower = "" if t["plan_net_r_lower"] is None else f"{t['plan_net_r_lower']:+.3f}"
        lines.append(
            f"{key:46s}{t['n']:>6}{100 * t['hit_rate']:>7.1f}{t['z']:>+7.1f}{t['plan_net_r']:>+8.3f}"
            f"{lower:>8}{h['plan_net_r']:>+10.3f}  {s['verdict']}"
        )
    lines += ["", "ACCEPTED: " + (", ".join(report["accepted"]) or "none")]
    if model:
        oos = model["out_of_sample"]
        lines += [
            "",
            f"Outcome grade model: skill={'yes' if model['skill'] else 'no'} "
            f"(top minus bottom bin {oos['top_minus_bottom_r']:+.3f}R, z {oos['z']:+.1f}, n {oos['n']})",
        ]
        lines += [
            f"  {k:34s} {v['mean_r']:+.3f}R (lower {v['lower_r']:+.3f})  grade {v['grade']}  n {v['n']}"
            for k, v in model["rules"].items()
        ]
    return "\n".join(lines)
