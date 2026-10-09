"""One causal, bounded activity summary per original-venue symbol tape.

All numeric settings are implementation inferences, not calibrated trade rules.
Quantiles refresh from strictly prior prints every 64 updates (512-print history).
Only compact second aggregates and 128 immutable closed rotations are retained.
"""

from collections import deque
from copy import deepcopy
from math import isfinite
from statistics import median

import numpy as np

BUCKETS = ("micro", "small", "medium", "large", "very_large")


class EventFeatures:
    def __init__(self, source, symbol):
        self.source, self.symbol = source, symbol
        self.sizes = deque(maxlen=512)
        self.seconds = deque(maxlen=181)
        self.rotations = deque(maxlen=128)
        self.quantiles = None
        self.quantile_ms = 0
        self.updates = 0
        self.current = None
        self.last_event = self.last_receipt = 0
        self.retained_from_ms = 0

    def add(self, trade):
        if trade.symbol != self.symbol or trade.exchange != self.source or trade.event_ms < self.last_event:
            raise ValueError("Event features require a chronological, symbol-specific tape")
        notional = float(trade.price * trade.size)
        if not isfinite(notional) or notional <= 0:
            raise ValueError("Event features require positive finite print notional")
        if len(self.sizes) >= 64 and self.updates % 64 == 0:
            self.quantiles = tuple(float(v) for v in np.quantile(self.sizes, (0.50, 0.75, 0.90, 0.97)))
            self.quantile_ms = self.last_event
        bucket = None if self.quantiles is None else BUCKETS[sum(notional > q for q in self.quantiles)]
        second = trade.event_ms // 1000 * 1000
        if not self.seconds or self.seconds[-1]["start_ms"] != second:
            if len(self.seconds) == self.seconds.maxlen:
                self.retained_from_ms = self.seconds[0]["start_ms"] + 1000
            self.seconds.append(
                dict(
                    start_ms=second,
                    source_ms=trade.event_ms,
                    receipt_ms=trade.receipt_ms,
                    counts=0,
                    classified=0,
                    buy={b: 0.0 for b in BUCKETS},
                    sell={b: 0.0 for b in BUCKETS},
                    quantiles=self.quantiles,
                    quantile_ms=self.quantile_ms,
                    first=float(trade.price),
                    last=float(trade.price),
                )
            )
        row = self.seconds[-1]
        row["counts"] += 1
        row["source_ms"], row["receipt_ms"], row["last"] = (
            trade.event_ms,
            max(row["receipt_ms"], trade.receipt_ms),
            float(trade.price),
        )
        if bucket is not None:
            row["classified"] += 1
            row["buy" if trade.side == "Buy" else "sell"][bucket] += notional
            row["quantiles"], row["quantile_ms"] = self.quantiles, self.quantile_ms
        if self.current is None and self.quantiles is not None:
            # A relative activity clock, frozen before including its opening print.
            self.current = dict(
                start_ms=trade.event_ms,
                threshold_notional=self.quantiles[0] * 32,
                threshold_known_ms=self.quantile_ms,
                first=float(trade.price),
                count=0,
                notional=0.0,
                delta=0.0,
                path=0.0,
                last=float(trade.price),
            )
        if self.current is not None:
            event = self.current
            event["count"] += 1
            event["notional"] += notional
            event["delta"] += notional * (1 if trade.side == "Buy" else -1)
            event["path"] += abs(float(trade.price) - event["last"])
            event["last"] = float(trade.price)
            if event["notional"] >= event["threshold_notional"]:
                self.rotations.append(
                    dict(
                        event,
                        end_ms=trade.event_ms,
                        receipt_ms=trade.receipt_ms,
                        duration_ms=trade.event_ms - event["start_ms"],
                        direction="UP"
                        if event["last"] > event["first"]
                        else "DOWN"
                        if event["last"] < event["first"]
                        else "FLAT",
                        efficiency=abs(event["last"] - event["first"]) / max(event["path"], 1e-12),
                    )
                )
                self.current = None
        # Current print enters the baseline only after its classification/threshold.
        self.sizes.append(notional)
        self.updates += 1
        self.last_event, self.last_receipt = trade.event_ms, trade.receipt_ms

    def snapshot(self, start, end, available_ms, coverage_complete):
        window_rows = [r for r in self.seconds if start <= r["start_ms"] < end]
        coverage_complete = bool(
            coverage_complete
            and not any(r["receipt_ms"] > available_ms for r in window_rows)
            and end <= available_ms
            and start >= self.retained_from_ms
            and start % 1000 == 0
            and end % 1000 == 0
        )
        common = dict(
            source=self.source,
            symbol=self.symbol,
            source_mode="native",
            source_ms=end,
            receipt_ms=available_ms,
            available_ms=available_ms,
            coverage_complete=coverage_complete,
            freshness_ms=available_ms - end,
            policy="relative-event-features-v1",
        )
        rows = [r for r in window_rows if r["receipt_ms"] <= available_ms]
        common["receipt_ms"] = max((r["receipt_ms"] for r in rows), default=None)
        count = sum(r["counts"] for r in rows)
        classified = sum(r["classified"] for r in rows)
        complete = bool(coverage_complete and rows and count == classified and classified >= 1)
        buy = {b: sum(r["buy"][b] for r in rows) for b in BUCKETS}
        sell = {b: sum(r["sell"][b] for r in rows) for b in BUCKETS}
        delta = {b: buy[b] - sell[b] for b in BUCKETS}
        large = sum(delta[b] for b in BUCKETS[-2:])
        small = sum(delta[b] for b in BUCKETS[:2])
        quantiles = rows[-1]["quantiles"] if rows else None
        size = common | dict(
            available=complete,
            quality="READY" if complete else "WARMING_OR_INCOMPLETE",
            count=count,
            classified=classified,
            buy_notional_by_size=buy,
            sell_notional_by_size=sell,
            delta_by_size_bucket=delta,
            quantiles=dict(zip(("q50", "q75", "q90", "q97"), quantiles)) if quantiles else {},
            quantile_known_ms=rows[-1]["quantile_ms"] if rows else None,
            large_trade_direction="BUY" if large > 0 else "SELL" if large < 0 else "MIXED",
            small_trade_direction="BUY" if small > 0 else "SELL" if small < 0 else "MIXED",
            large_small_divergence=large * small < 0,
            participant_identity="unknown; exchange-reported prints may be aggregated",
        )
        closed = [
            r
            for r in self.rotations
            if start <= r["start_ms"] and r["end_ms"] < end and r["receipt_ms"] <= available_ms
        ]
        prior = [
            r["duration_ms"]
            for r in self.rotations
            if r["end_ms"] < start and r["receipt_ms"] <= available_ms
        ]
        durations = [r["duration_ms"] for r in closed]
        duration_ratio = median(durations) / max(1, median(prior)) if durations and prior else None
        rotation = common | dict(
            available=bool(coverage_complete and closed),
            quality="READY" if coverage_complete and closed else "WARMING_OR_INCOMPLETE",
            closed_count=len(closed),
            latest=deepcopy(closed[-1]) if closed else None,
            activity_duration_ratio=duration_ratio,
            direction=closed[-1]["direction"] if closed else "UNKNOWN",
            price_efficiency=closed[-1]["efficiency"] if closed else None,
            buffer_limits=dict(baseline=512, seconds=181, rotations=128),
            construction="32 prior-median print notionals; crossing print closes; no partial event features",
        )
        return size, rotation
