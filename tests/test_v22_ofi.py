"""Manual event arithmetic, causal windows and bounded quote continuity."""

from decimal import Decimal

import pytest

from bybit_flow.ofi import event_assess, l1_event
from bybit_flow.orderflow import Book


@pytest.mark.parametrize(
    "bid,ask,expected",
    [
        ((100, 15), (102, 12), 5),  # bid addition
        ((100, 7), (102, 12), -3),  # bid cancellation/depletion
        ((100, 10), (102, 15), -3),  # ask addition
        ((100, 10), (102, 7), 5),  # ask cancellation/depletion
        ((101, 8), (102, 12), 8),  # improved bid contributes new queue
        ((99, 20), (102, 12), -10),  # worse bid removes previous queue
        ((100, 10), (101, 7), -7),  # improved ask contributes new supply
        ((100, 10), (103, 20), 12),  # retreating ask removes previous supply
        ((101, 8), (103, 20), 20),  # mixed quote events
    ],
)
def test_manual_touch_ofi(bid, ask, expected):
    assert l1_event((100, 10), (102, 12), bid, ask) == expected


def message(at, update, bid_quantity, *, snapshot=False, extra=()):
    return dict(
        type="snapshot" if snapshot else "delta",
        ts=at,
        cts=at,
        data=dict(
            u=update,
            seq=update,
            b=[["99.99", str(bid_quantity)], *extra],
            a=[["100.01", "100"]] if snapshot else [],
        ),
    )


def warm(now=120000):
    book = Book()
    book.apply(message(now - 66000, 1, 100, snapshot=True), now - 66000)
    for i in range(66):
        at = now - 65000 + i * 1000
        book.apply(message(at, i + 2, 101 + i), at)
    return book


def test_all_windows_normalized_and_symmetric():
    book = warm()
    long = event_assess(book, 120000, "binance", "TESTUSDT", "LONG")
    short = event_assess(book, 120000, "binance", "TESTUSDT", "SHORT")
    assert long["state"] == "READY"
    for seconds in (1, 5, 15, 30, 60):
        row = long["windows"][str(seconds)]
        assert row["state"] == "READY" and row["observed_seconds"] == seconds
        assert row["touch"]["raw"] == seconds
        assert row["touch"]["directional"] == -short["windows"][str(seconds)]["touch"]["directional"]
        assert row["touch"]["depth_normalized"] > 0
        assert row["end_ms"] <= long["available_ms"]
    assert len(book.ofi_buckets) <= 120


def test_touch_and_band_changes_are_distinct():
    book = warm()
    book.apply(message(121000, 100, 166, extra=[["99.80", "10"]]), 121000)
    book.apply(message(122000, 101, 166), 122000)
    result = event_assess(book, 122000, "binance", "TESTUSDT")
    band = result["windows"]["1"]
    assert band["touch"]["raw"] == 0
    assert band["5bps"]["raw"] == 0 and band["10bps"]["raw"] == 0
    assert band["25bps"]["raw"] == pytest.approx(998)
    assert band["25bps"]["units"] == "quote notional"


@pytest.mark.parametrize("reset", ["snapshot", "reconnect", "gap", "epoch"])
def test_reset_never_bridges_previous_ofi(reset):
    book = warm()
    before = book.event_ofi_epoch
    if reset == "reconnect":
        book.reset()
        assert event_assess(book, 120000, "binance", "TESTUSDT")["state"] == "GAPPED"
        book.apply(message(121000, 1, 10, snapshot=True), 121000)
    elif reset == "snapshot":
        book.apply(message(121000, 100, 10, snapshot=True), 121000)
    elif reset == "gap":
        book.apply(message(126001, 100, 200), 126001)
    else:
        book.event_ofi_epoch += 1
    at = book.receipt_ms
    result = event_assess(book, at, "binance", "TESTUSDT")
    assert book.event_ofi_epoch > before
    assert not result["coverage_complete"]
    assert result["windows"]["60"]["event_count"] <= 1


def test_stale_and_future_quotes_are_not_ready():
    book = warm()
    assert event_assess(book, 130000, "binance", "TESTUSDT")["state"] == "STALE"
    book.event_ms = 120001
    assert event_assess(book, 120000, "binance", "TESTUSDT")["reason"] == "FUTURE_TIMESTAMP"


def test_exact_second_gap_does_not_include_cross_gap_queue_change():
    book = warm()
    book.apply(message(126000, 100, 10000), 126000)
    book.apply(message(127000, 101, 10001), 127000)
    at_boundary = event_assess(book, 127000, "binance", "TESTUSDT")
    assert at_boundary["windows"]["1"]["touch"]["raw"] == 0
    book.apply(message(128000, 102, 10002), 128000)
    next_window = event_assess(book, 128000, "binance", "TESTUSDT")
    assert next_window["windows"]["1"]["touch"]["raw"] == 1
    # Historical reader data is retained; only the v22 reader excludes it.
    assert book.ofi_buckets[-3]["ofi_l1_sum"] > 9000


def test_bucket_bound_and_no_retrospective_mutation_leak():
    book = warm()
    old = event_assess(book, 120000, "binance", "TESTUSDT")
    for i in range(1, 160):
        at = 120000 + i * 1000
        book.apply(message(at, 100 + i, 166 + i), at)
    assert len(book.ofi_buckets) == 120
    assert old["windows"]["60"]["touch"]["raw"] == 60
    assert not event_assess(book, 120000, "binance", "TESTUSDT")["coverage_complete"]
    assert book.bids[Decimal("99.99")] == Decimal(325)
