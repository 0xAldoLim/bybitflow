"""Versioned, causal horizon policies. Missing profile always means legacy."""

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Horizon:
    context: str
    setup: str
    execution: str
    hold_min: int  # minutes
    hold_max: int
    checkpoints: tuple[int, ...]  # minutes from creation
    research_only: bool = False


PROFILES = {
    "SHORT_INTRADAY": Horizon("60", "15", "5", 15, 120, (240, 480, 1440)),
    "CORE_INTRADAY": Horizon("240", "60", "15", 60, 240, (480, 720, 1440, 2880)),
    "SWING": Horizon("D", "240", "60", 240, 2880, (4320, 7200, 10080)),
    "EXTENDED_SWING": Horizon("D", "240", "60", 2880, 10080, (14400, 20160), True),
    "LEGACY": Horizon("240", "60", "15", 60, 240, (480, 720, 1440, 2880)),
}
DURATIONS = {"5": 300_000, "15": 900_000, "60": 3_600_000, "240": 14_400_000, "D": 86_400_000}


def session_context(at_ms):
    utc = datetime.fromtimestamp(at_ms / 1000, UTC)
    flags = {}
    for name, zone, end in (
        ("asia", "Asia/Singapore", 16),
        ("europe", "Europe/London", 16),
        ("new_york", "America/New_York", 17),
    ):
        flags[name + "_active"] = 8 <= utc.astimezone(ZoneInfo(zone)).hour < end
    a, e, n = flags.values()
    primary = (
        "EU_NY_OVERLAP"
        if e and n
        else "ASIA_EU_OVERLAP"
        if a and e
        else "NEW_YORK"
        if n
        else "EUROPE"
        if e
        else "ASIA"
        if a
        else "OFF_SESSION"
    )
    return flags | {"primary": primary, "at_ms": at_ms, "weekend": utc.weekday() >= 5}


def assign(signal, name):
    p = PROFILES[name]
    sign = 1 if signal.direction == "LONG" else -1
    if sign * (signal.tp2 - signal.tp1) <= 0:
        signal.tp2 = signal.tp1 + sign * abs(signal.entry - signal.stop)
        signal.evidence["tp2_method"] = "one additional risk unit beyond the first target"
    signal.horizon_profile = name
    signal.context_timeframe, signal.setup_timeframe, signal.execution_timeframe = (
        p.context,
        p.setup,
        p.execution,
    )
    signal.expected_hold_min, signal.expected_hold_max = p.hold_min, p.hold_max
    signal.holding_deadline_ms = signal.primary_tracking_deadline = signal.created_ms + p.hold_max * 60_000
    signal.session = session_context(signal.created_ms)
    signal.entry_session = signal.session["primary"]
    signal.lifecycle_version = "horizon-v1"
    signal.feature_schema_version = "candidate-v5"
    signal.version += ":horizons-v1:" + name
    signal.id = hashlib.sha256((signal.id + ":" + name).encode()).hexdigest()[:24]
    # Group the actual structural event across horizons; alert comparison also checks prices.
    signal.setup_thesis_id = hashlib.sha256(
        f"{signal.source}|{signal.symbol}|{signal.family}|{signal.direction}|{signal.evidence['trigger_bar_end']}".encode()
    ).hexdigest()[:24]
    signal.reason = (
        f"{signal.family}: causal {p.setup} setup, {p.context} context; executed-flow confirmation required"
    )
    signal.evidence["research_only_horizon"] = p.research_only
    return signal


def same_thesis(a, b, max_age_ms=3_600_000):
    if (a.source, a.symbol, a.direction, a.family) != (b.source, b.symbol, b.direction, b.family):
        return False
    risk = max(abs(a.entry - a.stop), abs(b.entry - b.stop), 1e-12)
    return (
        abs(a.created_ms - b.created_ms) <= max_age_ms
        and abs(a.entry - b.entry) <= 0.2 * risk
        and abs(a.stop - b.stop) <= 0.2 * risk
        and abs(a.tp1 - b.tp1) <= 0.3 * risk
    )
