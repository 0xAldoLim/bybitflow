"""Reproducible REAL archive calculation, not a signal/backtest/verified historical tick table."""

import argparse
import hashlib
import json
from decimal import Decimal

import pyarrow.parquet as pq

from bybit_flow.features import candle_features
from bybit_flow.history import aggregate_trades
from bybit_flow.models import Candle, Trade
from bybit_flow.orderflow import footprint


def main():
    from pathlib import Path

    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument(
        "--bucket-increment",
        type=Decimal,
        required=True,
        help="Explicit analytical increment; not a verified historical exchange tick",
    )
    args = parser.parse_args()
    if args.bucket_increment <= 0:
        parser.error("positive increment required")
    bars = [Candle(**r) for r in aggregate_trades(args.archive, 15)]
    # The last aggregate could be partial. This demonstration uses the preceding interval.
    bars = bars[:-1]
    end = bars[-1].end
    atr = candle_features(bars, end)["atr"]
    rows = pq.read_table(
        args.archive, filters=[("event_ms", ">=", end - 900_000), ("event_ms", "<", end)]
    ).to_pylist()
    trades = [
        Trade(
            r["symbol"],
            r["event_ms"],
            r["collected_ms"],
            r["trade_id"],
            r["side"],
            Decimal(r["price"]),
            Decimal(r["size"]),
            r.get("exchange", "bybit"),
        )
        for r in rows
    ]
    result = footprint(trades, args.bucket_increment, atr)
    summary = {
        k: result[k]
        for k in (
            "buy_base",
            "sell_base",
            "delta_base",
            "delta_notional",
            "delta_pct",
            "cvd",
            "poc",
            "vah",
            "val",
            "vwap",
            "trades",
            "bucket",
        )
    }
    print(
        json.dumps(
            dict(
                status="HISTORICAL DATA CALCULATION — NOT A TRADE SIGNAL",
                input_sha256=hashlib.sha256(args.archive.read_bytes()).hexdigest(),
                source=rows[0]["source"],
                window_start_ms=end - 900_000,
                window_end_ms=end,
                analytical_bucket_increment=str(args.bucket_increment),
                footprint=summary,
                limitations="Historical tick metadata unavailable; increment explicitly supplied. No book/absorption, PIT universe, own fills, predictive calibration or profitability validation.",
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
