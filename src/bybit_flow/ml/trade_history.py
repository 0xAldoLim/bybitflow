"""Bounded Binance aggregate trades, including checksummed official daily archives."""

import csv
import hashlib
import re
import time
import zipfile
from datetime import UTC, datetime, timedelta

from ..exchanges import VenueAPI, symbol_id
from ..storage import directory_bytes

CACHE_LIMIT = 128_000_000


def sha256(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1048576):
            value.update(chunk)
    return value.hexdigest()


class BinanceHistory:
    name = "binance-aggregate-trades:REST-or-checksummed-official-daily-archive"

    def __init__(self, store, settings, api=None):
        self.store, self.settings = store, settings
        self.api = api or VenueAPI("binance", settings.model_copy(update={"rest_requests_per_second": 1}))
        self.cache = store.root / "ml" / "print-history-cache"
        self.verified = {}

    async def close(self):
        await self.api.close()

    async def page(self, snapshot, state, metrics, asof):
        symbol = snapshot["signal"]["symbol"]
        symbol_id(symbol, "binance")
        start = state.get("last_event_ms") or snapshot["decision_ms"] - 60000
        if asof - start < 47 * 3600000:
            params = dict(symbol=symbol, limit=1000)
            if state.get("last_id") is None:
                params.update(startTime=start, endTime=snapshot["decision_ms"])
            else:
                params["fromId"] = state["last_id"] + 1
            metrics["requests"] += 1
            rows = await self.api.request("/fapi/v1/aggTrades", **params)
            if not isinstance(rows, list) or len(rows) > 1000:
                raise ValueError("Invalid aggregate trade history page")
            return dict(trades=rows)
        return await self.archive_page(snapshot, state, metrics, asof)

    async def archive(self, symbol, day, metrics):
        name = f"{symbol}-aggTrades-{day}.zip"
        url = f"https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol}/{name}"
        if name in self.verified:
            path, expected = self.verified[name]
            if path.is_file():
                return path, expected
        metrics["requests"] += 1
        response = await self.api.client.get(url + ".CHECKSUM")
        response.raise_for_status()
        words = response.text.split()
        if (
            len(response.content) > 4096
            or len(words) != 2
            or not re.fullmatch("[a-fA-F0-9]{64}", words[0])
            or words[1].lstrip("*") != name
        ):
            raise ValueError("Invalid official archive checksum")
        expected = words[0].lower()
        self.cache.mkdir(parents=True, exist_ok=True)
        path = self.cache / name
        if path.is_symlink():
            raise ValueError("Archive cache symlink rejected")
        if path.is_file() and sha256(path) != expected:
            path.unlink()
        if not path.exists():
            for old in self.cache.iterdir():
                if old != path and old.suffix in {".zip", ".download"} and not old.is_symlink():
                    old.unlink()
            temporary = path.with_suffix(".download")
            if temporary.is_symlink():
                raise ValueError("Archive temporary symlink rejected")
            try:
                metrics["requests"] += 1
                written = 0
                async with self.api.client.stream("GET", url) as download:
                    download.raise_for_status()
                    if int(download.headers.get("Content-Length", "0")) > CACHE_LIMIT:
                        raise ValueError("Official trade archive exceeds bounded recovery cache")
                    with temporary.open("wb") as output:
                        async for chunk in download.aiter_bytes(1048576):
                            written += len(chunk)
                            if (
                                written > CACHE_LIMIT
                                or directory_bytes(self.store.root) + len(chunk)
                                >= self.settings.max_storage_gb * 1e9 * 0.95
                            ):
                                raise ValueError("Recovery archive storage budget reached")
                            output.write(chunk)
                if sha256(temporary) != expected:
                    raise ValueError("Official trade archive checksum mismatch")
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
        self.verified[name] = (path, expected)
        return path, expected

    async def archive_page(self, snapshot, state, metrics, asof):
        symbol = snapshot["signal"]["symbol"]
        start = state.get("last_event_ms") or snapshot["decision_ms"] - 60000
        day = datetime.fromtimestamp(start / 1000, UTC).date()
        cursor = state.get("archive", {})
        if cursor.get("day"):
            cursor_day = datetime.strptime(cursor["day"], "%Y-%m-%d").date()
            if cursor_day >= day:
                day = cursor_day
        if day >= datetime.fromtimestamp(asof / 1000, UTC).date():
            return dict(trades=[])
        path, checksum = await self.archive(symbol, str(day), metrics)
        offset = cursor.get("offset", 0) if cursor.get("day") == str(day) else 0
        if offset and cursor.get("sha256") != checksum:
            raise ValueError("Official archive revised; full replay required")
        records = []
        deadline = time.monotonic() + 10
        with zipfile.ZipFile(path) as archive:
            expected_name = path.stem + ".csv"
            if archive.namelist() != [expected_name]:
                raise ValueError("Unexpected official archive member")
            with archive.open(expected_name) as stream:
                stream.seek(offset)
                while len(records) < 1000 and time.monotonic() < deadline:
                    line = stream.readline(4097)
                    if not line:
                        return dict(
                            trades=records,
                            cursor=dict(day=str(day + timedelta(days=1)), offset=0, sha256=None),
                        )
                    if len(line) > 4096:
                        raise ValueError("Invalid archive row size")
                    row = next(csv.reader([line.decode("utf-8-sig")]))
                    if row and row[0] in {"agg_trade_id", "aggregate_trade_id"} and offset == 0:
                        offset = stream.tell()
                        continue
                    if len(row) != 7 or row[6].lower() not in {"true", "false"}:
                        raise ValueError("Invalid futures aggregate-trade archive row")
                    offset = stream.tell()
                    ident, event = int(row[0]), int(row[5])
                    if (state.get("last_id") is not None and ident <= state["last_id"]) or event < start:
                        continue
                    pointer = dict(day=str(day), offset=offset, sha256=checksum)
                    records.append(
                        dict(
                            a=ident,
                            p=row[1],
                            q=row[2],
                            f=int(row[3]),
                            l=int(row[4]),
                            T=event,
                            m=row[6].lower() == "true",
                            _archive=pointer,
                        )
                    )
        return dict(trades=records, cursor=dict(day=str(day), offset=offset, sha256=checksum))
