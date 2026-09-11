"""Typed observation rows. Decimal strings preserve exchange precision in Parquet."""

import json

import pyarrow as pa

NORMALIZATION_VERSION = 2
SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("exchange", pa.string()),
        ("connection_id", pa.string()),
        ("kind", pa.string()),
        ("symbol", pa.string()),
        ("event_ms", pa.int64()),
        ("receipt_ms", pa.int64()),
        ("schema_version", pa.int32()),
        ("normalization_version", pa.int32()),
        ("complete", pa.bool_()),
        ("trade_id", pa.string()),
        ("side", pa.string()),
        ("price", pa.string()),
        ("quantity", pa.string()),
        ("notional", pa.string()),
        ("payload", pa.string()),
    ]
)


def normalize(envelope):
    from decimal import Decimal

    p = json.loads(envelope["payload"])
    source = envelope["source"]
    exchange = p.get("exchange")
    if source.startswith("native/"):
        _, exchange, source = source.split("/", 2)
    elif source.startswith("raw/"):
        exchange = source.split("/", 2)[1]
    elif "tradingview" in source:
        exchange = "tradingview"
    elif source.startswith(("ws/", "rest/")):
        exchange = "bybit"  # legacy V5 wire sources; generic observations remain nullable
    base = {
        k: envelope[k] for k in ("source", "symbol", "event_ms", "receipt_ms", "schema_version", "complete")
    }
    base |= {
        "normalization_version": NORMALIZATION_VERSION,
        "exchange": exchange,
        "connection_id": p.get("connection_id"),
        "kind": "observation",
        "trade_id": None,
        "side": None,
        "price": None,
        "quantity": None,
        "notional": None,
        "payload": envelope["payload"],
    }
    if source.startswith(("ws/publicTrade.", "ws/allLiquidation.")):
        is_trade = source.startswith("ws/publicTrade.")
        rows = p["data"] if isinstance(p["data"], list) else [p["data"]]
        for r in rows:
            yield base | {
                "kind": "trade" if is_trade else "liquidation",
                "event_ms": int(r["T"]),
                "trade_id": r.get("i"),
                "side": r["S"]
                if is_trade
                else ("liquidated_long" if r["S"] == "Buy" else "liquidated_short"),
                "price": r["p"],
                "quantity": r["v"],
                "notional": str(Decimal(r["p"]) * Decimal(r["v"])),
                "payload": json.dumps(r),
            }
    elif source.startswith("ws/orderbook."):
        yield base | {"kind": "book_" + p["type"], "event_ms": int(p.get("cts", p["ts"]))}
    else:
        yield base
