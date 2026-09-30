"""Selective public Binance spot observations, separate from perpetual evidence."""

import asyncio
import re

import httpx

from .storage import now_ms

POLICY = "spot-perp-v1"
# Binance's public-market-data host avoids account/trading endpoints.
SPOT_BASE = "https://data-api.binance.vision"


def compare(spot, perp, asof_ms, previous=None):
    base = dict(
        policy=POLICY,
        state="UNAVAILABLE",
        spot_source="binance-spot",
        perp_source=perp.get("source") if perp else None,
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
        or spot.get("source_ms", asof_ms + 1) > asof_ms
        or perp.get("source_ms", asof_ms + 1) > asof_ms
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
            and (perp.get("oi_change_pct") or 0) > 0
            and basis_change is not None
            and basis_change * pr > 0
            and (perp.get("flow_trust") or 0) >= 0.6
        ):
            state = "LEVERAGED_PERP_ONLY_MOVE"
        elif sr * pr >= 0 and abs(sr) >= 1.5 * max(abs(pr), 0.0001):
            state = "SPOT_LED_MOVE"
        elif sr * pr >= 0 and abs(pr) >= 1.5 * max(abs(sr), 0.0001):
            state = "PERP_LED_MOVE"
    return base | dict(
        state=state,
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


async def collect(symbol, asof_ms, *, transport=None):
    """Two bounded public GETs; missing trade-window coverage leaves spot delta null."""
    if not re.fullmatch(r"[A-Z0-9]{2,25}USDT", symbol):
        return None
    try:
        async with httpx.AsyncClient(base_url=SPOT_BASE, timeout=8, transport=transport) as client:
            bars_response, trades_response = await asyncio.gather(
                client.get("/api/v3/klines", params={"symbol": symbol, "interval": "1m", "limit": 8}),
                client.get("/api/v3/trades", params={"symbol": symbol, "limit": 1000}),
            )
            if bars_response.status_code != 200 or trades_response.status_code != 200:
                return None
            bars = [row for row in bars_response.json() if int(row[0]) + 60_000 <= asof_ms]
            if len(bars) < 6:
                return None
            closes = [float(row[4]) for row in bars]
            trades = sorted(
                (row for row in trades_response.json() if asof_ms - 60_000 <= int(row["time"]) <= asof_ms),
                key=lambda row: int(row["time"]),
            )
            coverage = bool(
                trades
                and int(trades[0]["time"]) <= asof_ms - 55_000
                and int(trades[-1]["time"]) >= asof_ms - 5_000
            )
            delta = (
                sum(float(row["qty"]) * (-1 if row["isBuyerMaker"] else 1) for row in trades)
                if coverage
                else None
            )
            received = now_ms()
            first_price = float(trades[0]["price"]) if coverage else None
            move = float(trades[-1]["price"]) / first_price - 1 if first_price else None
            impulse = next(
                (
                    int(row["time"])
                    for row in trades
                    if move is not None
                    and abs(move) >= 0.001
                    and (float(row["price"]) / first_price - 1) * move > 0
                    and abs(float(row["price"]) / first_price - 1) >= 0.001
                ),
                None,
            )
            return dict(
                source="binance-spot",
                source_ms=int(trades[-1]["time"]) if trades else int(bars[-1][0]) + 60_000,
                receipt_ms=received,
                available_ms=received,
                price=float(trades[-1]["price"]) if trades else closes[-1],
                return_1m=float(trades[-1]["price"]) / float(trades[0]["price"]) - 1 if coverage else None,
                return_5m=closes[-1] / closes[-6] - 1,
                delta=delta,
                flow_trust=1.0 if coverage else None,
                trade_window_complete=coverage,
                impulse_ms=impulse,
            )
    except (httpx.HTTPError, ValueError, KeyError, TypeError, IndexError, ZeroDivisionError):
        return None
