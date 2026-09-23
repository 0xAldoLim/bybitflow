"""Synthetic official-schema payload tests. Not real connectivity evidence."""

from decimal import Decimal as D

import httpx
import pytest

from bybit_flow.config import Settings
from bybit_flow.diagnostics import discord_test, doctor
from bybit_flow.exchanges import VenueAPI, instrument_from_wire, symbol_id, trade_from_wire
from bybit_flow.flow_views import profiles
from bybit_flow.native_streams import SequencedBook
from bybit_flow.orderflow import Book, BookGap, Tape
from bybit_flow.storage import Store


def okx_instrument():
    return instrument_from_wire(
        "okx",
        dict(
            instType="SWAP",
            ctType="linear",
            settleCcy="USDT",
            state="live",
            instId="BTC-USDT-SWAP",
            ctValCcy="BTC",
            ctVal="0.01",
            ctMult="1",
            listTime="1",
            tickSz="0.1",
            lotSz="0.1",
            minSz="0.1",
            maxMktSz="1000",
            lever="100",
        ),
    )


def test_contract_units_and_aggressor():
    i = okx_instrument()
    assert i.qty_step == D("0.001") and i.max_qty == D("10")
    t = trade_from_wire(
        "okx", dict(instId=i.exchange_symbol, ts="10", tradeId="1", side="sell", px="100", sz="5"), 12, i
    )
    assert t.size == D("0.05") and t.side == "Sell" and t.exchange == "okx"
    t = trade_from_wire("binance", dict(s="BTCUSDT", T=10, a=2, m=True, p="100", q="2"), 12)
    assert t.side == "Sell" and t.size == 2  # buyer maker -> seller aggressor
    assert symbol_id("BTCUSDT", "okx") == "BTC-USDT-SWAP"
    with pytest.raises(ValueError):
        symbol_id("BTC/USDT", "okx")


def test_binance_snapshot_bridge_and_gap():
    b = Book()
    state = SequencedBook("binance", b)
    state.apply(dict(lastUpdateId=100, T=1000, bids=[["99", "2"]], asks=[["101", "3"]]), 1001, True)
    assert not state.bridged
    assert not state.apply(dict(U=90, u=99, pu=89, T=1002, b=[], a=[]), 1003)
    state.apply(dict(U=99, u=102, pu=98, T=1004, b=[["99", "4"]], a=[]), 1005)
    assert state.bridged and b.bids[D("99")] == 4
    with pytest.raises(BookGap):
        state.apply(dict(U=105, u=108, pu=104, T=1006, b=[], a=[]), 1007)
    assert not b.valid


def test_okx_book_contracts_and_heartbeat():
    b = Book()
    state = SequencedBook("okx", b, D("0.01"))
    state.apply(
        dict(
            seqId=10,
            prevSeqId=-1,
            ts="1000",
            bids=[["99", "10", "0", "1"]],
            asks=[["101", "20", "0", "1"]],
            checksum=0,
        ),
        1001,
        True,
    )
    assert b.bids[D("99")] == D("0.1")
    state.apply(dict(seqId=10, prevSeqId=10, ts="1100", bids=[], asks=[], checksum=0), 1101)
    assert b.valid
    with pytest.raises(BookGap):
        state.apply(dict(seqId=12, prevSeqId=9, ts="1200", bids=[], asks=[]), 1201)
    assert not b.valid


def test_profiles_never_fabricate_missing_history():
    tape = Tape(1000)
    tape.reset(1_000_000)
    result = profiles(tape, D("0.1"), 1, 1_100_000, "binance")
    assert all(not row["available"] and not row["complete"] for row in result.values())
    assert result["prior_week"]["exchange"] == "binance"


async def test_auto_source_selection_records_transition_and_resets(settings, monkeypatch):
    import bybit_flow.scanner as module
    from bybit_flow.storage import Recorder

    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    scanner = module.Scanner(settings, store, recorder)
    scanner.context["OLD"] = {"stale": True}

    async def probe(name, settings):
        return {"exchange": name, "status": "HEALTHY" if name == "binance" else "UNAVAILABLE"}

    monkeypatch.setattr(module, "market_probe", probe)
    try:
        await scanner.select_source()
        assert scanner.exchange == "binance" and not scanner.context and scanner.source_ready
        assert store.get("active_exchange")["current"] == "binance"
        assert not scanner.streams.connected
    finally:
        await scanner.stop()
        store.close()


def test_native_normalized_trade_preserves_venue():
    import json

    from bybit_flow.normalization import normalize

    event = dict(
        source="native/binance/ws/publicTrade.BTCUSDT",
        symbol="BTCUSDT",
        event_ms=10,
        receipt_ms=12,
        schema_version=1,
        complete=True,
        payload=json.dumps(
            {
                "exchange": "binance",
                "connection_id": "test",
                "data": [{"T": 10, "i": "1", "S": "Sell", "p": "100", "v": "2"}],
            }
        ),
    )
    row = list(normalize(event))[0]
    assert row["exchange"] == "binance" and row["notional"] == "200" and row["connection_id"] == "test"
    assert row["normalization_version"] == 2 and row["schema_version"] == 1
    generic = event | {"source": "features/unknown", "payload": "{}"}
    assert list(normalize(generic))[0]["exchange"] is None


def test_cross_venue_requires_independent_fresh_sources():
    from bybit_flow.cross_venue import compare, fresh_comparison

    one = dict(
        exchange="binance",
        event_ms=10000,
        mid=100,
        spread_bps=1,
        window_end=9000,
        window_start=1000,
        flow={"available": True, "delta_pct": 25},
    )
    two = one | {"exchange": "bybit", "mid": 101}
    assert not compare([one, one], 11000)["available"]
    assert not compare([one, two], 100000)["available"]
    result = compare([one, two], 11000)
    assert result["delta_agreement"] is True and result["predictive_weight"] == 0
    assert result["funding_dispersion"] is None and result["price_dislocation_bps"] > 0
    assert fresh_comparison(result, 11000, "binance")["available"]
    assert not fresh_comparison(result, 26000, "binance")["available"]
    assert not fresh_comparison(result, 11000, "okx")["available"]


async def test_cross_market_context_excludes_old_venue_future_and_stale(settings):
    from bybit_flow.scanner import Scanner
    from bybit_flow.storage import Recorder

    store = Store(settings.data_dir)
    scanner = Scanner(settings, store, Recorder(store, settings))
    now = 2_000_000
    row = dict(exchange=scanner.exchange, asof=now, h4={"regime": "trending up"})
    try:
        store.put("market:BTCUSDT", row)
        store.put("market:ETHUSDT", row | {"exchange": "okx"})
        assert scanner.market_context(now) == {"BTCUSDT": "trending up", "ETHUSDT": "unavailable"}
        store.put("market:BTCUSDT", row | {"asof": now + 1})
        assert scanner.market_context(now)["BTCUSDT"] == "unavailable"
        store.put("market:BTCUSDT", row | {"asof": now - 960_000})
        assert scanner.market_context(now)["BTCUSDT"] == "unavailable"
    finally:
        await scanner.stop()
        store.close()


async def test_public_allowlist_and_closed_candles(tmp_path):
    def response(request):
        assert request.url.path == "/fapi/v1/klines"
        return httpx.Response(
            200,
            json=[[0, "1", "3", "1", "2", "4", 899999, "8"], [900000, "2", "3", "1", "2", "1", 1799999, "2"]],
        )

    api = VenueAPI("binance", Settings(data_dir=tmp_path), transport=httpx.MockTransport(response))
    try:
        with pytest.raises(ValueError):
            await api.request("/fapi/v1/order")
        bars = await api.candles("BTCUSDT", "15", 950000, limit=1)
        assert len(bars) == 1 and bars[0].turnover == 8 and bars[0].end == 900000
    finally:
        await api.close()


async def test_quote_refresh_does_not_refresh_old_funding(settings):
    from bybit_flow.scanner import Scanner
    from bybit_flow.storage import Recorder

    store = Store(settings.data_dir)
    scanner = Scanner(settings, store, Recorder(store, settings))
    scanner.context["BTCUSDT"] = {"derivatives": {}}
    ticker = dict(
        symbol="BTCUSDT",
        bid1Price="99",
        ask1Price="101",
        observed_ms=2_000_000,
        funding_observed_ms=1_000_000,
        fundingRate="0.0001",
    )
    try:
        scanner.record_quotes({"time": 2_000_000, "result": {"list": [ticker]}}, 2_000_000)
        assert scanner.context["BTCUSDT"]["derivatives"]["ticker_observed_ms"] == 1_000_000
    finally:
        await scanner.stop()
        store.close()


async def test_diagnostics_and_nontrade_discord(tmp_path):
    settings = Settings(data_dir=tmp_path, research_webhook="https://discord.com/api/webhooks/test/SECRET")
    store = Store(tmp_path)
    payloads = []

    def response(request):
        import json

        payloads.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "mock"})

    try:
        result = await discord_test(settings, store, httpx.MockTransport(response))
        assert result["status"] == "sent"
        assert payloads[0]["embeds"][0]["title"] == "BYBITFLOW CONNECTION TEST"
        assert "NOT A TRADE SIGNAL" in payloads[0]["embeds"][0]["description"]
        assert store.db.execute("SELECT count(*) FROM signals").fetchone()[0] == 0
        assert store.db.execute("SELECT count(*) FROM ml_snapshots").fetchone()[0] == 0
        report = await doctor(settings, store, network=False)
        assert "SECRET" not in str(report) and report["checks"]["database"]["status"] == "OK"
    finally:
        store.close()
