"""Time-sampled quote history; current liquidity is never backdated."""

import math
from statistics import median


class SpreadHistory:
    def __init__(self, rows=(), bucket_ms=300_000, window_ms=21_600_000):
        self.rows = {int(r["bucket"]): dict(r) for r in rows}
        self.bucket_ms, self.window_ms = bucket_ms, window_ms

    def add(self, event_ms, receipt_ms, bid, ask):
        bucket = receipt_ms // self.bucket_ms
        # One observation per time bucket, not a trade/message-frequency-weighted mean.
        if bucket in self.rows:
            return
        valid = (
            all(math.isfinite(v) for v in (bid, ask))
            and ask > bid > 0
            and -1000 <= receipt_ms - event_ms <= 10_000
        )
        spread = 10000 * (ask - bid) / ((ask + bid) / 2) if valid else None
        self.rows[bucket] = {"bucket": bucket, "known_ms": receipt_ms, "event_ms": event_ms, "bps": spread}
        cutoff = (receipt_ms - self.window_ms) // self.bucket_ms
        self.rows = {k: v for k, v in self.rows.items() if k >= cutoff}

    def assess(self, asof, max_bps=5, min_samples=12, min_coverage=0.8):
        rows = sorted(
            (r for r in self.rows.values() if asof - self.window_ms <= r["known_ms"] <= asof),
            key=lambda r: r["bucket"],
        )
        values = sorted(r["bps"] for r in rows if r["bps"] is not None)
        expected = asof // self.bucket_ms - rows[0]["bucket"] + 1 if rows else 0
        coverage = len(values) / expected if expected else 0
        med = median(values) if values else None
        p90 = values[max(0, math.ceil(0.9 * len(values)) - 1)] if values else None
        reasons = []
        if len(values) < min_samples:
            reasons.append("normal spread warming up")
        if coverage < min_coverage:
            reasons.append("normal spread observation coverage insufficient")
        if med is not None and med > max_bps:
            reasons.append("normal median spread exceeds gate")
        if p90 is not None and p90 > 2 * max_bps:
            reasons.append("normal spread tail exceeds gate")
        return {
            "eligible": not reasons,
            "samples": len(values),
            "expected_samples": expected,
            "coverage": coverage,
            "median_bps": med,
            "p90_bps": p90,
            "reasons": reasons,
            "method": "first observed quote per five-minute bucket; rolling six-hour median and p90",
            "asof": asof,
            "window_ms": self.window_ms,
        }

    def export(self):
        return sorted(self.rows.values(), key=lambda r: r["bucket"])
