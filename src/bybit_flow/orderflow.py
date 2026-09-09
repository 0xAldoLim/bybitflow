"""Executed-trade footprint and visible-book support. No inferred hidden orders."""

from bisect import bisect_left
from collections import OrderedDict, defaultdict, deque
from decimal import ROUND_FLOOR, Decimal
from statistics import median

from .models import Trade

D = Decimal


class BookGap(ValueError):
    pass


class Book:
    def __init__(self):
        self.reset()

    def reset(self):
        self.bids, self.asks = {}, {}
        self.valid = False
        self.update = self.seq = self.event_ms = self.receipt_ms = 0
        self.changes = deque(maxlen=5000)

    def apply(self, message, receipt_ms):
        d = message["data"]
        snapshot = message["type"] == "snapshot" or int(d["u"]) == 1
        if snapshot:
            self.reset()
        elif not self.valid:
            raise BookGap("Delta before snapshot")
        elif int(d["u"]) <= self.update or int(d["seq"]) < self.seq:
            self.valid = False
            raise BookGap("Non-monotonic book update; resubscribe")
        # u/seq are NOT documented as consecutive; a jump alone cannot prove loss.
        for side, levels in (("bid", d["b"]), ("ask", d["a"])):
            book = self.bids if side == "bid" else self.asks
            for p, q in levels:
                p, q = D(p), D(q)
                if p <= 0 or q < 0:
                    self.valid = False
                    raise BookGap("Invalid book price/quantity")
                old = book.get(p, D(0))
                if q == 0:
                    book.pop(p, None)
                else:
                    book[p] = q
                if not snapshot:
                    self.changes.append((receipt_ms, side, p, q - old))
        self.update, self.seq = int(d["u"]), int(d["seq"])
        self.event_ms, self.receipt_ms = int(message.get("cts", message["ts"])), receipt_ms
        self.valid = bool(self.bids and self.asks) and max(self.bids) < min(self.asks)
        if not self.valid:
            raise BookGap("Empty or crossed book")

    def fresh(self, now, max_age=5000):
        return (
            self.valid and 0 <= now - self.receipt_ms <= max_age and -1000 <= now - self.event_ms <= max_age
        )

    def impact(self, side, notional):
        levels = sorted(self.asks.items()) if side == "LONG" else sorted(self.bids.items(), reverse=True)
        remaining, qty, spent = D(str(notional)), D(0), D(0)
        for price, size in levels:
            take = min(size, remaining / price)
            qty += take
            spent += take * price
            remaining -= take * price
            if remaining <= D("0.000001"):
                break
        if not qty or remaining > D("0.000001"):
            return None
        mid = (max(self.bids) + min(self.asks)) / 2
        return dict(
            vwap=float(spent / qty),
            impact_bps=float(abs(spent / qty - mid) / mid * 10000),
            quantity=float(qty),
        )

    def features(self, now):
        if not self.valid:
            return {"available": False}
        bid, ask = max(self.bids), min(self.asks)
        mid = (bid + ask) / 2
        depth = {}
        for bps in (5, 10, 25):
            b = sum(p * q for p, q in self.bids.items() if p >= mid * (1 - D(bps) / 10000))
            a = sum(p * q for p, q in self.asks.items() if p <= mid * (1 + D(bps) / 10000))
            depth[str(bps)] = dict(
                bid=float(b), ask=float(a), imbalance=float((b - a) / (b + a)) if b + a else 0
            )
        changes = [c for c in self.changes if now - 60_000 <= c[0] <= now]
        replenish = {
            side: float(sum(p * q for _, s, p, q in changes if s == side and q > 0))
            for side in ("bid", "ask")
        }
        depletion = {
            side: float(-sum(p * q for _, s, p, q in changes if s == side and q < 0))
            for side in ("bid", "ask")
        }
        # Depletion combines fills and cancellations; it is not cancellation volume.
        return dict(
            available=True,
            event_ms=self.event_ms,
            receipt_ms=self.receipt_ms,
            spread_bps=float((ask - bid) / mid * 10000),
            mid=float(mid),
            depth=depth,
            microprice=float(
                (ask * self.bids[bid] + bid * self.asks[ask]) / (self.bids[bid] + self.asks[ask])
            ),
            replenishment_60s=replenish,
            depletion_60s=depletion,
            visible_flow_imbalance=sum((1 if s == "bid" else -1) * float(p * q) for _, s, p, q in changes),
            bids=[[str(p), str(q)] for p, q in sorted(self.bids.items(), reverse=True)[:50]],
            asks=[[str(p), str(q)] for p, q in sorted(self.asks.items())[:50]],
        )


def value_area(levels, fraction=0.70):
    """Contiguous expansion from PoC, larger adjacent volume first, lower price wins ties."""
    if not 0 < fraction <= 1:
        raise ValueError("Invalid value area fraction")
    if not levels:
        return {"poc": None, "val": None, "vah": None}
    prices = sorted(levels)
    poc = max(range(len(prices)), key=lambda i: (levels[prices[i]], -prices[i]))
    lo = hi = poc
    covered, target = levels[prices[poc]], sum(levels.values()) * fraction
    while covered < target and (lo > 0 or hi < len(prices) - 1):
        lv = levels[prices[lo - 1]] if lo else -1
        hv = levels[prices[hi + 1]] if hi < len(prices) - 1 else -1
        if lv >= hv:
            lo -= 1
            covered += levels[prices[lo]]
        else:
            hi += 1
            covered += levels[prices[hi]]
    return dict(poc=float(prices[poc]), val=float(prices[lo]), vah=float(prices[hi]))


def footprint(trades, tick, atr, book=None):
    if not trades:
        return {"available": False}
    bucket = max(tick, (D(str(atr)) / 100 / tick).to_integral_value(rounding=ROUND_FLOOR) * tick)
    levels = defaultdict(lambda: [0.0, 0.0])
    buy = sell = buy_n = sell_n = 0.0
    cvd, cvd_path = 0.0, []
    trades = sorted(trades, key=lambda t: t.event_ms)
    for t in trades:
        p = (t.price / bucket).to_integral_value(rounding=ROUND_FLOOR) * bucket
        size, notional = float(t.size), float(t.size * t.price)
        levels[p][0 if t.side == "Buy" else 1] += size
        sign = 1 if t.side == "Buy" else -1
        if sign == 1:
            buy += size
            buy_n += notional
        else:
            sell += size
            sell_n += notional
        cvd += sign * size
        cvd_path.append([t.event_ms, cvd])
    typical = median(sum(v) for v in levels.values())
    floor = max(typical * 0.1, 1e-12)
    # Explicit liquidity-dependent denominator floor and volatility-dependent ratio.
    ratio = min(6.0, max(3.0, 3 + atr / float(trades[-1].price) * 100))
    buy_stack = sell_stack = max_buy = max_sell = 0
    previous = None
    imbalances = []
    for p in sorted(levels):
        if previous is None or p - previous != bucket:
            buy_stack = sell_stack = 0
        b, s = levels[p]
        below = levels.get(p - bucket, [0.0, 0.0])[1]
        above = levels.get(p + bucket, [0.0, 0.0])[0]
        ib = below >= floor and b >= ratio * below
        iss = above >= floor and s >= ratio * above
        buy_stack = buy_stack + 1 if ib else 0
        sell_stack = sell_stack + 1 if iss else 0
        max_buy, max_sell = max(max_buy, buy_stack), max(max_sell, sell_stack)
        if ib or iss:
            imbalances.append(dict(price=float(p), buy=ib, sell=iss))
        previous = p
    first, last = trades[0], trades[-1]
    displacement = float(last.price - first.price)
    delta_pct = 100 * (buy - sell) / (buy + sell)
    # Local price-level additions must overlap aggressive opposite-side trades.
    defended = {"LONG": 0.0, "SHORT": 0.0}
    if book and book.valid:
        receipt_index = defaultdict(list)
        for t in trades:
            receipt_index[(t.side, int(t.price / bucket))].append((t.receipt_ms, t.price))
        for key in receipt_index:
            receipt_index[key].sort()
        for ts, side, p, q in book.changes:
            if q <= 0:
                continue
            matched = False
            for price_bucket in range(int(p / bucket) - 1, int(p / bucket) + 2):
                series = receipt_index.get(("Sell" if side == "bid" else "Buy", price_bucket), [])
                index = bisect_left(series, (ts - 2000, D(0)))
                while index < len(series) and series[index][0] <= ts:
                    if abs(series[index][1] - p) <= bucket:
                        matched = True
                        break
                    index += 1
            if matched:
                defended["LONG" if side == "bid" else "SHORT"] += float(q * p)
    absorption_long = (
        sell_n > 2 * buy_n and abs(displacement) < 0.15 * atr and defended["LONG"] > 0.1 * sell_n
    )
    absorption_short = (
        buy_n > 2 * sell_n and abs(displacement) < 0.15 * atr and defended["SHORT"] > 0.1 * buy_n
    )
    total = {p: sum(v) for p, v in levels.items()}
    profile = value_area(total)
    return dict(
        available=True,
        buy_base=buy,
        sell_base=sell,
        buy_notional=buy_n,
        sell_notional=sell_n,
        delta_base=buy - sell,
        delta_notional=buy_n - sell_n,
        delta_pct=delta_pct,
        delta_denominator="100 × (buy_base − sell_base) / (buy_base + sell_base)",
        cvd=cvd,
        cvd_scope="this complete execution window",
        cvd_path=cvd_path[:: max(1, len(cvd_path) // 200)],
        vwap=(buy_n + sell_n) / (buy + sell),
        bucket=str(bucket),
        **profile,
        profile=[dict(price=float(p), buy=v[0], sell=v[1]) for p, v in sorted(levels.items())],
        imbalances=imbalances,
        imbalance_ratio=ratio,
        denominator_floor=floor,
        stacked_buy=max_buy,
        stacked_sell=max_sell,
        defended_notional=defended,
        absorption_long=absorption_long,
        absorption_short=absorption_short,
        initiative_long=delta_pct >= 20 and displacement > 0.15 * atr,
        initiative_short=delta_pct <= -20 and displacement < -0.15 * atr,
        delta_divergence=displacement * (buy - sell) < 0,
        trades=len(trades),
        duration_ms=last.event_ms - first.event_ms,
        trades_per_second=len(trades) / max(1, (last.event_ms - first.event_ms) / 1000),
    )


class Tape:
    def __init__(self, max_trades=200_000):
        self.trades = deque(maxlen=max_trades)
        self.ids = OrderedDict()
        self.coverage_start = 0
        self.last_event = self.last_receipt = 0

    def reset(self, at_ms):
        self.trades.clear()
        self.ids.clear()
        self.coverage_start = at_ms
        self.last_event = self.last_receipt = 0

    def add(self, t: Trade):
        if t.trade_id in self.ids:
            return False
        if t.event_ms < self.last_event:
            self.reset(t.receipt_ms)
            raise BookGap("Out-of-order trade: continuity unknown")
        if len(self.trades) == self.trades.maxlen:
            self.coverage_start = max(self.coverage_start, self.trades[0].event_ms + 1)
        self.ids[t.trade_id] = None
        if len(self.ids) > self.trades.maxlen:
            self.ids.popitem(last=False)
        self.trades.append(t)
        self.last_event, self.last_receipt = t.event_ms, t.receipt_ms
        return True

    def window(self, start, end):
        return [t for t in self.trades if start <= t.event_ms < end]
