"""Keep application time aligned with the exchange when the host clock drifts.

The scanner refuses to scan when the local clock is more than two seconds from
exchange time, and it should: stale-data checks are meaningless otherwise. But
on a desktop host the operating system's time service is often not running, and
a clock that gains a quarter of a second a day stops the product within a week.

This module holds a correction that `storage.now_ms` adds to the host clock. It
is steered from the scanner's own exchange-time measurements and slewed slowly,
so application time never jumps and never runs backwards. A host clock that is
badly wrong is not masked: beyond MAX_OFFSET_MS the correction stops and the
scanner's two-second check fails as before.
"""

import math
import time

SLEW_RATE = 0.02  # At most 20 ms of correction per second of real time.
DEADBAND_MS = 150  # Ignore differences smaller than measurement noise.
MAX_ROUND_TRIP_MS = 1500  # A slow request cannot locate exchange time well enough.
MAX_OFFSET_MS = 30_000

_base = 0.0
_target = 0.0
_since = time.monotonic()


def offset_ms():
    """Current correction in milliseconds, part-way to the target while slewing."""
    moved = (time.monotonic() - _since) * 1000 * SLEW_RATE
    gap = _target - _base
    return int(round(_base + (gap if abs(gap) <= moved else math.copysign(moved, gap))))


def target_ms():
    return int(round(_target))


def reset():
    global _base, _target, _since
    _base = _target = 0.0
    _since = time.monotonic()


def settled_seconds():
    """Seconds since the last correction finished being applied (negative while slewing)."""
    return (time.monotonic() - _since) - abs(_target - _base) / (1000 * SLEW_RATE)


def discipline(skew_ms, round_trip_ms, measured_age_s=0.0):
    """Steer toward exchange time. `skew_ms` is corrected local time minus exchange time.

    A measurement taken while an earlier correction was still being applied
    describes a clock that no longer exists; acting on it overshoots. Such
    measurements are skipped and the next one, taken on the settled clock, is used.

    Returns True when the target changed.
    """
    global _base, _target, _since
    if settled_seconds() < measured_age_s:
        return False
    if not isinstance(skew_ms, (int, float)) or not isinstance(round_trip_ms, (int, float)):
        return False
    if round_trip_ms < 0 or round_trip_ms > MAX_ROUND_TRIP_MS:
        return False
    if abs(skew_ms) <= max(DEADBAND_MS, round_trip_ms / 2):
        return False
    current = offset_ms()
    wanted = current - skew_ms
    if abs(wanted) > MAX_OFFSET_MS:
        return False  # The host clock is broken, not drifting. Let the scan check say so.
    _base, _target, _since = float(current), float(wanted), time.monotonic()
    return True
