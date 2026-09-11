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


async def download_binance_trades(symbol: str, day: str, directory: Path):
    """Official USD-M aggregate executions, verified against the published SHA-256 sidecar."""
    from .exchanges import symbol_id

    symbol_id(symbol, "binance")
    date.fromisoformat(day)
    directory.mkdir(parents=True, exist_ok=True)
    name = f"{symbol}-aggTrades-{day}.zip"
    url = f"https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol}/{name}"
    raw = directory / name
    if raw.exists():
        raise ValueError("Archive already exists; do not overwrite research inputs")
    partial = raw.with_suffix(".zip.partial")
    sha, size = hashlib.sha256(), 0
    async with httpx.AsyncClient(timeout=60, follow_redirects=False) as client:
        checksum = await client.get(url + ".CHECKSUM")
        checksum.raise_for_status()
        expected = checksum.text.split()[0]
        if not re.fullmatch(r"[a-fA-F0-9]{64}", expected):
            raise ValueError("Invalid official checksum sidecar")
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            with partial.open("xb") as f:
                async for block in response.aiter_bytes():
                    size += len(block)
                    if size > 2_000_000_000:
                        raise ValueError("Archive exceeds 2GB download budget")
                    f.write(block)
                    sha.update(block)
    if sha.hexdigest() != expected.lower():
        raise ValueError("Archive checksum mismatch; partial file retained for investigation")
    partial.rename(raw)
    output = directory / (name + ".trades.parquet")
    manifest = convert_binance_archive(raw, output, symbol, day, url)
    manifest |= dict(
        sha256=sha.hexdigest(),
        size_bytes=size,
        checksum_status="matches official HTTPS sidecar, not exchange-signed",
    )
    (directory / (name + ".manifest.json")).write_text(json.dumps(manifest, indent=2))
    return manifest


def convert_binance_archive(raw, output, symbol, day, source):
    import io
    import zipfile
    from datetime import UTC, datetime

    if output.exists():
        raise ValueError("Normalized archive already exists")
    first = int(datetime.combine(date.fromisoformat(day), datetime.min.time(), UTC).timestamp() * 1000)
    schema = pa.schema(
        [
            ("exchange", pa.string()),
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
    count, previous, previous_id, collected, batch = 0, -1, -1, now_ms(), []
    with zipfile.ZipFile(raw) as archive:
        files = archive.infolist()
        if len(files) != 1 or not files[0].filename.endswith(".csv") or files[0].file_size > 8_000_000_000:
            raise ValueError("Unexpected or excessive ZIP contents")
        # Stream a member without extracting paths from an untrusted archive.
        with (
            archive.open(files[0]) as member,
            io.TextIOWrapper(member) as stream,
            pq.ParquetWriter(output, schema, compression="zstd") as writer,
        ):
            for index, row in enumerate(csv.reader(stream)):
                if index == 0 and row[0] == "agg_trade_id":
                    continue
                if len(row) != 7 or row[6].lower() not in {"true", "false"}:
                    raise ValueError("Binance aggregate trade schema changed")
                ident, timestamp, price, quantity = int(row[0]), int(row[5]), Decimal(row[1]), Decimal(row[2])
                if (
                    not first <= timestamp < first + 86_400_000
                    or timestamp < previous
                    or ident <= previous_id
                    or price <= 0
                    or quantity <= 0
                ):
                    raise ValueError("Invalid/non-monotonic trade or unexpected timestamp units")
                previous, previous_id = timestamp, ident
                batch.append(
                    dict(
                        exchange="binance",
                        symbol=symbol,
                        event_ms=timestamp,
                        event_index=count,
                        trade_id=str(ident),
                        side="Sell" if row[6].lower() == "true" else "Buy",
                        price=str(price),
                        size=str(quantity),
                        notional=str(price * quantity),
                        source=source,
                        collected_ms=collected,
                        schema_version=2,
                    )
                )
                count += 1
                if len(batch) >= 50_000:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
    if not count:
        raise ValueError("Empty execution archive")
    return dict(
        exchange="binance",
        source=source,
        raw=str(raw),
        parquet=str(output),
        rows=count,
        collected_ms=collected,
        schema_version=2,
        book_coverage=False,
        liquidation_coverage=False,
        completeness="validated schema, UTC date, monotonic IDs/time and ZIP CRC; not proof of all venue fills",
        universe_coverage="restricted symbol archive, no historical membership",
        methodology="venue aggregate trades; no inferred taker sides",
    )


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
