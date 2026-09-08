import csv
import gzip
import hashlib
import json
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from .storage import now_ms


async def download_trades(symbol: str, day: str, directory: Path):
    if not re.fullmatch(r"[A-Z0-9]{2,30}", symbol):
        raise ValueError("Invalid symbol")
    date.fromisoformat(day)
    directory.mkdir(parents=True, exist_ok=True)
    name = f"{symbol}{day}.csv.gz"
    url = f"https://public.bybit.com/trading/{symbol}/{name}"
    raw = directory / name
    if raw.exists():
        raise ValueError("Archive already exists; select a new destination or use the existing verified file")
    partial = raw.with_suffix(".gz.partial")
    sha, size = hashlib.sha256(), 0
    async with httpx.AsyncClient(timeout=60, follow_redirects=False) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            with partial.open("xb") as f:
                async for block in response.aiter_bytes():
                    size += len(block)
                    if size > 2_000_000_000:
                        raise ValueError("Archive exceeds 2GB download budget")
                    f.write(block)
                    sha.update(block)
    partial.rename(raw)
    output = directory / f"{symbol}{day}.trades.parquet"
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("event_ms", pa.int64()),
            ("event_index", pa.int64()),
            ("trade_id", pa.string()),
            ("side", pa.string()),
            ("price", pa.string()),
            ("size", pa.string()),
            ("notional", pa.string()),
            ("source", pa.string()),
            ("collected_ms", pa.int64()),
            ("schema_version", pa.int32()),
        ]
    )
    collected, count, first, last, previous = now_ms(), 0, None, None, -1
    monotonic = True
    batch = []
    with pq.ParquetWriter(output, schema, compression="zstd") as writer, gzip.open(raw, "rt") as stream:
        reader = csv.DictReader(stream)
        required = {"timestamp", "symbol", "side", "size", "price", "trdMatchID"}
        if not required <= set(reader.fieldnames or []):
            raise ValueError("Official archive schema changed; inspect raw file")
        for row in reader:
            t = int(Decimal(row["timestamp"]) * 1000)
            p, q = Decimal(row["price"]), Decimal(row["size"])
            if row["symbol"] != symbol or row["side"] not in {"Buy", "Sell"} or min(p, q) <= 0:
                raise ValueError("Archive row validation failed")
            monotonic = monotonic and t >= previous
            previous = t
            first, last = t if first is None else min(first, t), t if last is None else max(last, t)
            batch.append(
                dict(
                    symbol=symbol,
                    event_ms=t,
                    event_index=count,
                    trade_id=row["trdMatchID"],
                    side=row["side"],
                    price=str(p),
                    size=str(q),
                    notional=str(p * q),
                    source=url,
                    collected_ms=collected,
                    schema_version=1,
                )
            )
            count += 1
            if len(batch) >= 50_000:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch.clear()
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
    manifest = dict(
        source=url,
        raw=str(raw),
        parquet=str(output),
        sha256=sha.hexdigest(),
        checksum_status="locally computed integrity hash; not an exchange-signed checksum",
        rows=count,
        min_event_ms=first,
        max_event_ms=last,
        monotonic=monotonic,
        collected_ms=collected,
        schema_version=1,
        size_bytes=size,
        completeness="gzip CRC and row schema verified; no proof every exchange trade is present",
        book_coverage=False,
        liquidation_coverage=False,
        universe_coverage="historical instrument status not supplied by this archive",
    )
    (directory / f"{name}.manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def aggregate_trades(path: Path, interval_minutes=60):
    """Aggregate real archive prints to OHLCV. Empty periods remain missing, never forward-filled."""
    import duckdb

    width = interval_minutes * 60_000
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
        SELECT CAST(floor(event_ms / ?) * ? AS BIGINT) AS start,
               arg_min(CAST(price AS DOUBLE),struct_pack(t := event_ms, i := event_index)) AS open,
               max(CAST(price AS DOUBLE)) AS high, min(CAST(price AS DOUBLE)) AS low,
               arg_max(CAST(price AS DOUBLE),struct_pack(t := event_ms, i := event_index)) AS close,
               sum(CAST(size AS DOUBLE)) AS volume, sum(CAST(notional AS DOUBLE)) AS turnover
        FROM read_parquet(?) GROUP BY start ORDER BY start
        """,
            [width, width, str(path)],
        ).fetchall()
        return [
            dict(zip(("start", "open", "high", "low", "close", "volume", "turnover"), r))
            | {"interval": width}
            for r in rows
        ]
    finally:
        connection.close()
