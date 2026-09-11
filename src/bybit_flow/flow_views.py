"""Causal, bounded views over genuinely retained prints. Missing windows stay missing."""

from datetime import UTC, datetime

from .orderflow import footprint


def profiles(tape, tick, atr, now, exchange):
    minute, day = 60_000, 86_400_000
    midnight = now // day * day
    monday = midnight - datetime.fromtimestamp(now / 1000, UTC).weekday() * day
    windows = {
        f"{m}m": (now // (m * minute) * m * minute - m * minute, now // (m * minute) * m * minute)
        for m in (1, 5, 15, 60)
    }
    windows |= {
        "rolling_4h": (now // minute * minute - 240 * minute, now // minute * minute),
        "day": (midnight, now),
        "prior_day": (midnight - day, midnight),
        "session_utc_8h": (now // (8 * 60 * minute) * (8 * 60 * minute), now),
        "week": (monday, now),
        "prior_week": (monday - 7 * day, monday),
    }
    result = {}
    for name, (start, end) in windows.items():
        complete = (
            tape.coverage_start > 0
            and tape.coverage_start <= start
            and tape.last_event >= end - 15_000
            and tape.last_receipt >= now - 15_000
        )
        row = dict(exchange=exchange, start_ms=start, end_ms=end, complete=complete)
        if complete:
            row.update(footprint(tape.window(start, end), tick, atr))
        else:
            row.update(
                available=False,
                reason="insufficient retained continuous executed-trade coverage; no candle substitution",
            )
        result[name] = row
    return result
