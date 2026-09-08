"""Replay actual recorder envelopes in receipt order using the live feature/strategy modules."""

import gzip
import hashlib
import heapq
import json
from decimal import Decimal

from .backtest import PaperPosition, performance
from .features import validate_bars
from .ingestion import parse_eligible_metadata
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

    # Limit open file handles by replaying chronologically nonoverlapping segments in batches externally.
    if len(paths) > 256:
        raise ValueError("Replay accepts at most 256 segments per invocation; compact research data first")
    yield from heapq.merge(*(read(p) for p in paths), key=lambda r: r["receipt_ms"])


def replay(rows, settings, families=None):
    from .strategy import FAMILIES

    families = families or FAMILIES
    instruments, candles, books, tapes = {}, {}, {}, {}
    positions, seen = [], set()
    gaps = 0
    last_receipt = -1
    decisions = 0
    funding_seen = set()
    for row in rows:
        now = row["receipt_ms"]
        if now < last_receipt:
            raise ValueError("Replay requires receipt order")
        last_receipt = now
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        source, symbol = row["source"], row["symbol"]
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
        if source == "rest/instruments-info":
            for raw in payload["response"]["result"]["list"]:
                inst = parse_eligible_metadata(raw, settings, now)
                if inst:
                    instruments[inst.symbol] = inst
                else:
                    instruments.pop(raw["symbol"], None)
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
        elif source.startswith("ws/publicTrade."):
            tape = tapes.setdefault(symbol, Tape())
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
                    continue
                for position in positions:
                    position.on_trade(trade)
        elif source == "rest/kline":
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
            if interval != "15" or symbol not in instruments or symbol not in tapes or symbol not in books:
                continue
            bars = [
                sorted(candles.get((symbol, i), {}).values(), key=lambda c: c.start)
                for i in ("240", "60", "15")
            ]
            daily = sorted(candles.get((symbol, "D"), {}).values(), key=lambda c: c.start)
            from statistics import median

            if min(map(len, bars)) < 60 or len(daily) < 30:
                continue
            try:
                validate_bars(daily[-30:], now)
            except ValueError:
                continue
            if median(c.turnover for c in daily[-7:]) < settings.min_daily_turnover:
                continue
            book, tape = books[symbol], tapes[symbol]
            end = bars[2][-1].end
            if not book.fresh(now) or not (0 < tape.coverage_start <= end - 900_000) or now - end > 960_000:
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
                flow = footprint(
                    tape.window(end - 900_000, end), instruments[symbol].tick, signal.evidence["m15"]["atr"]
                )  # no retrospective replenishment inference
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
            "Reversal absorption replay not enabled without matched replenishment history",
            "Missing funding/mark histories preclude full-cost qualification",
            "No claim of profitable strategy or fully realistic account liquidation",
        ],
    }
