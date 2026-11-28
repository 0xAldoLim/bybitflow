import time

import pytest

from bybit_flow import clock
from bybit_flow.storage import now_ms


@pytest.fixture(autouse=True)
def fresh_clock():
    clock.reset()
    yield
    clock.reset()


def advance(monkeypatch, seconds):
    start = time.monotonic()
    monkeypatch.setattr(clock.time, "monotonic", lambda: start + seconds)


def test_no_correction_by_default():
    assert clock.offset_ms() == 0
    assert abs(now_ms() - time.time_ns() // 1_000_000) <= 2


def test_small_or_unreliable_differences_are_ignored():
    assert clock.discipline(120, 80) is False  # Inside the deadband.
    assert clock.discipline(600, 1400) is False  # Within half the round trip.
    assert clock.discipline(900, 4000) is False  # Request too slow to locate exchange time.
    assert clock.discipline(None, 80) is False and clock.discipline(900, None) is False
    assert clock.target_ms() == 0


def test_host_ahead_is_slewed_back_without_a_jump(monkeypatch):
    # Local time 1.7 s ahead of the exchange, as measured on the production host.
    assert clock.discipline(1700, 120) is True
    assert clock.target_ms() == -1700
    assert clock.offset_ms() in (0, -1)  # Nothing applied yet: no jump.
    advance(monkeypatch, 10)
    assert clock.offset_ms() == -200  # 20 ms per second.
    advance(monkeypatch, 200)
    assert clock.offset_ms() == -1700  # Reaches the target and stays there.


def test_application_time_never_runs_backwards_while_slewing(monkeypatch):
    clock.discipline(1700, 120)
    real = time.time_ns()
    readings = []
    for step in range(0, 120):
        advance(monkeypatch, step)
        monkeypatch.setattr(
            clock.time, "time_ns", lambda step=step: real + step * 1_000_000_000, raising=False
        )
        readings.append(real // 1_000_000 + step * 1000 + clock.offset_ms())
    assert all(b > a for a, b in zip(readings, readings[1:]))


def test_follow_up_measurement_refines_from_the_current_offset(monkeypatch):
    clock.discipline(1700, 100)
    advance(monkeypatch, 200)
    assert clock.offset_ms() == -1700
    # A later check finds corrected time still 300 ms ahead.
    assert clock.discipline(300, 100) is True
    assert clock.target_ms() == -2000
    assert clock.discipline(-400, 100) is True  # Now behind: correct the other way.
    assert clock.target_ms() > -2000


def test_a_broken_host_clock_is_not_masked():
    assert clock.discipline(45_000, 100) is False
    assert clock.target_ms() == 0
