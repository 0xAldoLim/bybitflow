"""Replay actual recorder envelopes in receipt order using the live feature/strategy modules."""

import gzip
import hashlib
import itertools
import json
from decimal import Decimal

from .backtest import PaperPosition, performance
from .features import validate_bars
from .ingestion import parse_eligible_metadata
from .liquidity import SpreadHistory
from .models import Candle, Trade
from .orderflow import Book, Tape, footprint
from .risk import evaluate_risk
from .strategy import candidates, confirm


def segment_rows(paths):
    def read(path):
        from pathlib import Path

        path = Path(path)
        manifest_path = path.with_name(path.name.removesuffix(".jsonl.gz") + ".manifest.json")
        if not manifest_path.exists():
            raise ValueError("Raw segment lacks manifest; verify provenance before replay")
        manifest = json.loads(manifest_path.read_text())
        if hashlib.sha256(path.read_bytes()).hexdigest() != manifest["sha256"]:
            raise ValueError("Raw segment integrity mismatch")
        previous = -1
        with gzip.open(path, "rt") as f:
            for line in f:
                row = json.loads(line)
                if row["receipt_ms"] < previous:
                    raise ValueError("Non-monotonic segment receipt time")
                previous = row["receipt_ms"]
                yield row

    from pathlib import Path

    manifests = []
    for path in paths:
        path = Path(path)
        manifest_path = path.with_name(path.name.removesuffix(".jsonl.gz") + ".manifest.json")
        if not manifest_path.exists():
            raise ValueError("Raw segment lacks manifest; verify provenance before replay")
        manifests.append((json.loads(manifest_path.read_text()), path))
    manifests.sort(key=lambda item: (item[0].get("min_receipt_ms", item[0]["collected_ms"]), item[0]["id"]))
    previous, last_receipt = None, -1
    for manifest, path in manifests:
        iterator = iter(read(path))
        first = next(iterator, None)
        if first is None:
            continue
        if previous and manifest.get("previous_segment") != {
            "id": previous["id"],
            "sha256": previous["sha256"],
        }:
            yield {
                "source": "control/gap",
                "symbol": "ALL",
                "event_ms": first["receipt_ms"],
                "receipt_ms": first["receipt_ms"],
                "schema_version": 1,
                "complete": False,
                "payload": json.dumps({"reason": "missing or unchained raw recording segment"}),
            }
        for row in itertools.chain([first], iterator):
            if row["receipt_ms"] < last_receipt:
                raise ValueError("Overlapping or non-monotonic recording segments")
            last_receipt = row["receipt_ms"]
            yield row
        previous = manifest


def replay(rows, settings, families=None):
    from .strategy import FAMILIES

    families = families or FAMILIES
    instruments, candles, books, tapes = {}, {}, {}, {}
    membership, spreads = {}, {}
    positions, seen = [], set()
    gaps = 0
    last_receipt = -1
    decisions = 0
    funding_seen = set()
    for row in rows:
        if row["source"].startswith(("native/", "raw/binance/", "raw/okx/")):
            raise ValueError(
                "Legacy strategy replay is Bybit-only; use source-separated ml label for native candidate outcomes"
            )
        now = row["receipt_ms"]
        if now < last_receipt:
            raise ValueError("Replay requires receipt order")
        last_receipt = now
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        source, symbol = row["source"], row["symbol"]
        if source == "universe/membership":
            membership[symbol] = {"known_ms": now, **payload}
            source = "replay/evaluate"
        elif source == "features/signal":
            source = "replay/evaluate"
        if source == "control/gap":
            gaps += 1
            for p in positions:
                if p.exit_ms is None:
                    p.data_gaps.append("recording discontinuity during fill/outcome window")
            for book in books.values():
                book.reset()
            for tape in tapes.values():
                tape.reset(now)
            continue
        if source == "control/subscribed":
            for subscribed in payload["symbols"]:
                tapes.setdefault(subscribed, Tape(settings.tape_max_trades)).reset(now)
            continue
        if source == "control/rotation":
            for removed in payload["removed"]:
                if removed in books:
                    books[removed].reset()
                if removed in tapes:
                    tapes[removed].reset(now)
                for position in positions:
                    if position.signal.symbol == removed and position.exit_ms is None:
                        position.data_gaps.append("subscription removed during outcome window")
            continue
        if source == "rest/instruments-info":
            for raw in payload["response"]["result"]["list"]:
                inst = parse_eligible_metadata(raw, settings, now)
                if inst:
                    instruments[inst.symbol] = inst
                else:
                    instruments.pop(raw["symbol"], None)
        elif source == "rest/tickers":
            response = payload["response"]
            for t in response["result"]["list"]:
                spreads.setdefault(t["symbol"], SpreadHistory()).add(
                    int(response["time"]), now, float(t.get("bid1Price") or 0), float(t.get("ask1Price") or 0)
                )
        elif source == "rest/funding/history":
            for f in payload["response"]["result"]["list"]:
                key = (symbol, int(f["fundingRateTimestamp"]))
                if key in funding_seen:
                    continue
                funding_seen.add(key)
                # Cannot accurately charge historical funding without simultaneous historical mark/position.
                # Such trades are marked incomplete below if any settlement occurred during their lifetime.
                for p in positions:
                    if (
                        p.signal.symbol == symbol
                        and p.quantity
                        and p.signal.created_ms <= key[1] <= (p.exit_ms or now)
                    ):
                        p.data_gaps.append("unverified_historical_funding")
        elif source.startswith("ws/orderbook."):
            try:
                books.setdefault(symbol, Book()).apply(payload, now)
            except ValueError:
                gaps += 1
                tapes.setdefault(symbol, Tape()).reset(now)
                for p in positions:
                    if p.signal.symbol == symbol and p.exit_ms is None:
                        p.data_gaps.append("book integrity lost during outcome window")
        elif source.startswith("ws/publicTrade."):
            tape = tapes.setdefault(symbol, Tape(settings.tape_max_trades))
            if not tape.coverage_start:
                tape.reset(now)
            for t in payload["data"]:
                if t.get("BT"):
                    continue
                trade = Trade(symbol, int(t["T"]), now, t["i"], t["S"], Decimal(t["p"]), Decimal(t["v"]))
                try:
                    if not tape.add(trade):
                        continue
                except ValueError:
                    gaps += 1
                    for p in positions:
                        if p.signal.symbol == symbol and p.exit_ms is None:
                            p.data_gaps.append("trade ordering lost during outcome window")
                    continue
                for position in positions:
                    position.on_trade(trade)
        elif source in {"rest/kline", "replay/evaluate"}:
            if source == "rest/kline":
                params = payload["params"]
                interval = params["interval"]
                width = 86_400_000 if interval == "D" else int(interval) * 60_000
                bucket = candles.setdefault((symbol, interval), {})
                for c in payload["response"]["result"]["list"]:
                    bar = Candle(int(c[0]), width, *map(float, c[1:7]))
                    if bar.end <= now:
                        bucket[bar.start] = bar
                for old in sorted(bucket)[:-300]:
                    del bucket[old]
                continue
            if symbol not in instruments or symbol not in tapes or symbol not in books:
                continue
            bars = [
                sorted(candles.get((symbol, i), {}).values(), key=lambda c: c.start)
                for i in ("240", "60", "15")
            ]
            daily = sorted(candles.get((symbol, "D"), {}).values(), key=lambda c: c.start)
            from statistics import median

            if min(map(len, bars)) < 60 or len(daily) < 30:
                continue
            member = membership.get(symbol)
            if (
                not member
                or not member["eligible"]
                or now - member["known_ms"] > settings.scan_seconds * 1000 + 60_000
            ):
                continue
            if (
                not spreads.get(symbol)
                or not spreads[symbol].assess(now, settings.max_spread_bps, settings.spread_min_samples)[
                    "eligible"
                ]
            ):
                continue
            try:
                validate_bars(daily[-30:], now)
            except ValueError:
                continue
            if median(c.turnover for c in daily[-7:]) < settings.min_daily_turnover:
                continue
            if any(c.volume <= 0 for c in daily[-30:]) or now - daily[-1].end >= 86_400_000:
                continue
            book, tape = books[symbol], tapes[symbol]
            end = bars[2][-1].end
            if not book.fresh(now) or not (0 < tape.coverage_start <= end - 900_000) or now - end > 960_000:
                continue
            if not (
                0 <= now - tape.last_receipt <= settings.trade_stale_ms
                and -1000 <= now - tape.last_event <= settings.trade_stale_ms
            ):
                continue
            if book.features(now)["spread_bps"] > settings.max_spread_bps:
                continue
            try:
                plans = candidates(instruments[symbol], *bars, now, families=families)
            except ValueError:
                continue
            for signal in plans:
                if signal.id in seen or signal.expires_ms <= now:
                    continue
                window_book = Book()
                window_book.valid = book.valid
                window_book.changes.extend(c for c in book.changes if c[0] <= end)
                flow = footprint(
                    tape.window(end - 900_000, end),
                    instruments[symbol].tick,
                    signal.evidence["m15"]["atr"],
                    window_book,
                )
                if not confirm(signal, flow, bars[2]):
                    continue
                signal.risk = evaluate_risk(signal, instruments[symbol], settings, book)
                if not signal.risk["accepted"]:
                    continue
                if any(p.signal.symbol == symbol and p.exit_ms is None for p in positions):
                    continue
                decisions += 1
                seen.add(signal.id)
                positions.append(
                    PaperPosition(
                        signal,
                        settings.hypothetical_notional / signal.entry,
                        fee_bps=settings.taker_fee_bps,
                        slippage_bps=settings.slippage_bps,
                    )
                )
    outcomes = [p.outcome() for p in positions]
    return {
        "mode": "recorded-event research",
        "decisions": decisions,
        "gaps": gaps,
        "outcomes": outcomes,
        "statistics": performance(outcomes),
        "qualification": "Uncalibrated; no deployment approval",
        "limitations": [
            "No survivorship-free period before recorded instrument metadata",
            "Missing historical membership/normal-spread coverage abstains; older recordings may not qualify",
            "Missing funding/mark histories preclude full-cost qualification",
            "No claim of profitable strategy or fully realistic account liquidation",
        ],
    }
