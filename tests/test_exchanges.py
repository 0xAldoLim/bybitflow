"""Synthetic official-schema payload tests. Not real connectivity evidence."""

from decimal import Decimal as D

import httpx
import pytest

from bybit_flow.config import Settings
from bybit_flow.diagnostics import discord_test, doctor
from bybit_flow.exchanges import VenueAPI, instrument_from_wire, symbol_id, trade_from_wire
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
