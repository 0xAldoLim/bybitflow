import json
from decimal import Decimal as D

import httpx
import pytest

from bybit_flow.ingestion import Bybit, parse_eligible_metadata
from bybit_flow.models import Trade
from bybit_flow.normalization import normalize
from bybit_flow.orderflow import Book, BookGap, Tape, footprint, value_area


def message(kind="snapshot", u=10, seq=100, bids=None, asks=None, ts=1000):
    return {
        "type": kind,
        "ts": ts,
        "cts": ts,
        "data": {
            "s": "TESTUSDT",
            "u": u,
            "seq": seq,
            "b": [["99.99", "1000"]] if bids is None else bids,
            "a": [["100.01", "1000"]] if asks is None else asks,
        },
    }


def test_snapshot_delta_restart():
    b = Book()
    b.apply(message(), 1000)
    b.apply(message("delta", 20, 103, [["99.99", "0"], ["99.98", "7"]], []), 1001)
    assert D("99.99") not in b.bids and b.bids[D("99.98")] == 7
    # Numeric jumps are legal: exchange does not document contiguous counters.
    b.apply(message("delta", 1, 1, [["90", "1"]], [["91", "1"]]), 1002)
    assert b.bids == {D(90): D(1)}


def test_delta_before_snapshot_and_nonmonotonic():
    b = Book()
    with pytest.raises(BookGap):
        b.apply(message("delta"), 1000)
    b.apply(message(), 1000)
    with pytest.raises(BookGap):
        b.apply(message("delta", 9), 1001)
    assert not b.valid


def test_crossed_stale_impact():
    b = Book()
    with pytest.raises(BookGap):
        b.apply(message(bids=[["101", "1"]]), 1000)
    b.apply(message(), 1000)
    assert b.fresh(2000) and not b.fresh(7000)
    assert not b.fresh(0)  # future receipt rejected
    assert b.impact("LONG", 1000)["vwap"] == 100.01
    assert b.impact("LONG", 10_000_000) is None


def test_tape_duplicate_and_overflow():
    tape = Tape(max_trades=2)
    tape.reset(1)
    a = Trade("TESTUSDT", 10, 10, "a", "Buy", D(100), D(2))
    assert tape.add(a)
    assert not tape.add(a)
    tape.add(Trade("TESTUSDT", 11, 11, "b", "Sell", D(100), D(1)))
    tape.add(Trade("TESTUSDT", 12, 12, "c", "Buy", D(100), D(1)))
    assert tape.coverage_start == 11
    with pytest.raises(BookGap):
        tape.add(Trade("TESTUSDT", 9, 13, "d", "Buy", D(100), D(1)))
    assert not tape.trades


def test_executed_delta_profile_and_absorption_not_delta_alone():
    trades = [Trade("T", 1, 1, "1", "Buy", D(100), D(3)), Trade("T", 2, 2, "2", "Sell", D(100), D(1))]
    f = footprint(trades, D(".01"), 1)
    assert f["delta_pct"] == 50 and f["delta_base"] == 2 and f["delta_notional"] == 200
    assert f["poc"] == 100 and f["vwap"] == 100
    assert not f["absorption_short"] and not f["absorption_long"]
    assert f["stacked_buy"] == 0  # near-zero opposite volume is not infinite imbalance


def test_value_area_contiguous_ties():
    result = value_area({D(99): 3, D(100): 10, D(101): 3}, 0.7)
    assert result == {"poc": 100, "val": 99, "vah": 100}


def test_liquidation_direction_is_position_side():
    envelope = dict(
        source="ws/allLiquidation.T",
        symbol="T",
        event_ms=100,
        receipt_ms=110,
        schema_version=1,
        complete=True,
        payload=json.dumps({"data": [{"T": 99, "S": "Buy", "p": "100", "v": "2"}]}),
    )
    r = list(normalize(envelope))[0]
    assert r["side"] == "liquidated_long" and r["notional"] == "200"
    assert r["event_ms"] == 99 and r["receipt_ms"] == 110


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "PreLaunch"),
        ("contractType", "LinearFutures"),
        ("settleCoin", "BTC"),
        ("isPreListing", True),
        ("marketRegion", "US"),
    ],
)
def test_metadata_exclusions(instrument_row, settings, field, value):
    instrument_row[field] = value
    assert parse_eligible_metadata(instrument_row, settings, 100 * 86_400_000) is None


async def test_cursor_pagination(settings, instrument_row):
    calls = []

    def handle(request):
        calls.append(dict(request.url.params))
        cursor = request.url.params.get("cursor")
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "time": 100,
                "result": {
                    "list": [instrument_row | {"symbol": "SECOND" if cursor else "FIRST"}],
                    "nextPageCursor": "next" if not cursor else "",
                },
            },
        )

    api = Bybit(settings, transport=httpx.MockTransport(handle))
    try:
        rows = await api.instruments()
        assert len(rows) == 2 and calls[1]["cursor"] == "next"
        with pytest.raises(ValueError):
            await api.get("order/create")
    finally:
        await api.close()


async def test_candles_exclude_open_and_paginate(settings):
    calls = []

    def handle(request):
        end = int(request.url.params["end"])
        calls.append(end)
        data = [
            [str(i * 60000), "100", "101", "99", "100", "1", "100"]
            for i in reversed(range(5))
            if i * 60000 <= end
        ]
        return httpx.Response(200, json={"retCode": 0, "time": 270000, "result": {"list": data[:2]}})

    api = Bybit(settings, transport=httpx.MockTransport(handle))
    try:
        bars = await api.candles("T", "1", 270000, limit=4)
        assert [c.start for c in bars] == [0, 60000, 120000, 180000]
        assert len(calls) == 3
    finally:
        await api.close()


async def test_forbidden_region_not_retried(settings):
    count = 0

    def handle(request):
        nonlocal count
        count += 1
        return httpx.Response(403)

    api = Bybit(settings, transport=httpx.MockTransport(handle))
    try:
        with pytest.raises(PermissionError):
            await api.get("time")
        assert count == 1
    finally:
        await api.close()
