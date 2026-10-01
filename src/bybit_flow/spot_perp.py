"""Selective public Binance spot observations, separate from perpetual evidence."""

import re
from collections import deque

import httpx

from .storage import now_ms

POLICY = "spot-perp-v2"
# Binance's public-market-data host avoids account/trading endpoints.
SPOT_BASE = "https://data-api.binance.vision"


def compare(spot, perp, asof_ms, previous=None):
    base = dict(
        policy=POLICY,
        state="UNAVAILABLE",
        spot_source="binance-spot",
        perp_source=perp.get("source") if perp else None,
        source=perp.get("source") if perp else None,
        symbol=perp.get("symbol") if perp else None,
        price_coverage_complete=False,
        spot_trade_window_complete=False,
        source_ms=None,
        receipt_ms=asof_ms,
        available_ms=asof_ms,
        spot_return_1m=None,
        perp_return_1m=None,
        spot_return_5m=None,
        perp_return_5m=None,
        spot_perp_return_spread=None,
        spot_delta=None,
        perp_delta=None,
        spot_perp_delta_agreement=None,
        spot_perp_basis_bps=None,
        basis_change_bps=None,
        spot_price_response=None,
        perp_price_response=None,
        spot_lead_lag_ms=None,
        spot_perp_price_agreement=None,
        spot_flow_trust=None,
        perp_flow_trust=None,
    )
    if (
        not spot
        or not perp
        or spot.get("price") is None
        or perp.get("price") is None
        or not 0 <= asof_ms - spot.get("available_ms", 0) <= 90_000
        or not 0 <= asof_ms - perp.get("available_ms", 0) <= 90_000
        or not 0 <= asof_ms - spot.get("source_ms", asof_ms + 1) <= 90_000
        or not 0 <= asof_ms - perp.get("source_ms", asof_ms + 1) <= 90_000
        or spot.get("symbol") != perp.get("symbol")
    ):
        return base
    sr, pr = spot.get("return_1m"), perp.get("return_1m")
    basis = (perp["price"] / spot["price"] - 1) * 10_000
    prior = previous or {}
    basis_change = (
        basis - prior["spot_perp_basis_bps"]
        if prior.get("spot_perp_basis_bps") is not None and prior.get("available_ms", 0) < asof_ms
        else None
    )
    spot_delta, perp_delta = spot.get("delta"), perp.get("delta")
    agreement = (spot_delta * perp_delta > 0) if spot_delta is not None and perp_delta is not None else None
    price_agreement = (sr * pr > 0) if sr is not None and pr is not None else None
    lead_lag = (
        spot["impulse_ms"] - perp["impulse_ms"]
        if spot.get("impulse_ms") is not None and perp.get("impulse_ms") is not None and price_agreement
        else None
    )
    state = "UNAVAILABLE"
    if sr is not None and pr is not None:
        state = "SPOT_PERP_DIVERGENCE" if sr * pr < 0 else "SPOT_PERP_CONFIRMED"
        if (
            abs(pr) >= 0.002
            and abs(sr) <= abs(pr) * 0.25
            and (perp.get("oi_change_1m_pct") or 0) > 0
            and basis_change is not None
            and basis_change * pr > 0
        ):
            state = "LEVERAGED_PERP_ONLY_MOVE"
        elif sr * pr >= 0 and abs(sr) >= 1.5 * max(abs(pr), 0.0001):
            state = "SPOT_LED_MOVE"
        elif sr * pr >= 0 and abs(pr) >= 1.5 * max(abs(sr), 0.0001):
            state = "PERP_LED_MOVE"
    return base | dict(
        state=state,
        price_coverage_complete=bool(
            spot.get("price_coverage_complete")
            and perp.get("price_coverage_complete")
            and abs(spot["source_ms"] - perp["source_ms"]) <= 1000
        ),
        spot_trade_window_complete=bool(spot.get("trade_window_complete")),
        perp_oi_change_pct=perp.get("oi_change_1m_pct"),
        source_ms=min(spot["source_ms"], perp["source_ms"]),
        spot_return_1m=sr,
        perp_return_1m=pr,
        spot_return_5m=spot.get("return_5m"),
        perp_return_5m=perp.get("return_5m"),
        spot_perp_return_spread=sr - pr if sr is not None and pr is not None else None,
        spot_delta=spot_delta,
        perp_delta=perp_delta,
        spot_perp_delta_agreement=agreement,
        spot_perp_basis_bps=basis,
        basis_change_bps=basis_change,
        spot_price_response=sr,
        perp_price_response=pr,
        spot_lead_lag_ms=lead_lag,
        spot_perp_price_agreement=price_agreement,
        spot_flow_trust=spot.get("flow_trust"),
        perp_flow_trust=perp.get("flow_trust"),
    )


class Collector:
    """One pooled public client; each request shares the scanner REST budget."""

    def __init__(self, bounded_rest=None, *, transport=None, clock=now_ms):
        self.client = httpx.AsyncClient(
            base_url=SPOT_BASE,
            timeout=8,
            transport=transport,
            limits=httpx.Limits(max_connections=3, max_keepalive_connections=3),
        )
        self.bounded_rest = bounded_rest
        self.clock = clock
        self.requests = deque(maxlen=200)
        self.errors = deque(maxlen=200)
        self.backoff_until = 0

    async def close(self):
        await self.client.aclose()

    def metrics(self):
        now = self.clock()
        return dict(
            spot_requests_1m=sum(t >= now - 60_000 for t in self.requests),
            spot_errors_1m=sum(t >= now - 60_000 for t in self.errors),
            spot_rate_limited=now < self.backoff_until,
        )

    async def _get(self, path, params):
        if self.clock() < self.backoff_until:
            raise httpx.HTTPError("spot rate backoff")

        async def request():
            self.requests.append(self.clock())
            response = await self.client.get(path, params=params)
            if response.status_code in {418, 429}:
                self.backoff_until = self.clock() + 60_000
            response.raise_for_status()
            return response.json()

        return await self.bounded_rest(request()) if self.bounded_rest else await request()

    async def collect(self, symbol, asof_ms):
        if not re.fullmatch(r"[A-Z0-9]{2,25}USDT", symbol):
            return None
        try:
            rows = await self._get(
                "/api/v3/klines", dict(symbol=symbol, interval="1m", endTime=asof_ms, limit=8)
            )
            bars = sorted((r for r in rows if int(r[6]) <= asof_ms), key=lambda r: int(r[0]))
            if len(bars) < 6 or asof_ms - int(bars[-1][6]) > 90_000:
                return None
            closes = [float(r[4]) for r in bars]
            price_complete = all(int(b[0]) - int(a[0]) == 60_000 for a, b in zip(bars[-6:-1], bars[-5:]))
            result = dict(
                source="binance-spot",
                symbol=symbol,
                source_ms=int(bars[-1][6]),
                available_ms=self.clock(),
                receipt_ms=self.clock(),
                price=closes[-1],
                return_1m=closes[-1] / closes[-2] - 1,
                return_5m=closes[-1] / closes[-6] - 1,
                price_coverage_complete=price_complete,
                delta=None,
                flow_trust=None,
                trade_window_complete=False,
                impulse_ms=None,
            )
            # Inclusive time pagination deliberately never skips trades sharing a millisecond.
            # Exhaustion of a page proves the requested interval is complete. Saturated
            # same-ms pages or the hard bound leave delta unknown, while prices survive.
            trades, cursor, complete = {}, asof_ms - 60_000, False
            try:
                for _ in range(3):
                    page = await self._get(
                        "/api/v3/aggTrades",
                        dict(symbol=symbol, startTime=cursor, endTime=asof_ms, limit=1000),
                    )
                    before = len(trades)
                    for row in page:
                        if asof_ms - 60_000 <= int(row["T"]) <= asof_ms:
                            trades[int(row["a"])] = row
                    if len(page) < 1000:
                        complete = True
                        break
                    latest = max(int(r["T"]) for r in page)
                    if latest < cursor or len(trades) == before:
                        break
                    cursor = latest
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                self.errors.append(self.clock())
            result.update(
                delta=sum(float(r["q"]) * (-1 if r["m"] else 1) for r in trades.values())
                if complete
                else None,
                flow_trust=1.0 if complete else None,
                trade_window_complete=complete,
                available_ms=self.clock(),
                receipt_ms=self.clock(),
            )
            return result
        except (httpx.HTTPError, ValueError, KeyError, TypeError, IndexError, ZeroDivisionError):
            self.errors.append(self.clock())
            return None


async def collect(symbol, asof_ms, *, transport=None):
    """Compatibility wrapper for offline callers; scanner owns a persistent Collector."""
    collector = Collector(transport=transport)
    try:
        return await collector.collect(symbol, asof_ms)
    finally:
        await collector.close()
