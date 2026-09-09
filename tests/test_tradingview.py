import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

from bybit_flow.app import create_app
from bybit_flow.models import Signal
from bybit_flow.storage import Store
from bybit_flow.tradingview import Gateway, LiquidityObservation, TVEvent, evaluate, trapped_participants

NOW = 1_800_000_901_000


def payload():
    return dict(
        event_id="synthetic-event-0001",
        signal_id="synthetic-signal-0001",
        symbol="BYBIT:TESTUSDT.P",
        event="setup",
        source_ms=NOW,
        bar_close_ms=NOW - 1000,
        price=100,
        observation=dict(
            direction="LONG",
            family="tv_sweep_reclaim",
            setup_ms=NOW - 901_000,
            entry=100,
            stop=95,
            tp1=115,
            tp2=120,
            atr=3,
            regime_slope_atr=0.4,
            regime_efficiency=0.6,
            level=98,
            setup_low=96,
            setup_high=102,
            setup_close=100,
            prior_close=99,
            bos_level=99,
            buy_volume=700,
            sell_volume=300,
            stacked_buy=4,
            stacked_sell=0,
            poc=100,
            val=99,
            vah=101,
            cvd=1000,
            turnover_median_7d=30_000_000,
            continuous_days=100,
            footprint_source="tradingview_intrabar_classification",
            prior_buy_volume=200,
            prior_sell_volume=800,
            prior_low=97,
            prior_high=100,
        ),
    )


def observed(instrument):
    return LiquidityObservation(
        instrument=instrument,
        source="SYNTHETIC TEST FIXTURE — not live data",
        observed_ms=NOW,
        continuous_days=100,
        median_turnover_7d=30_000_000,
        normal_spread_bps=2,
        spread_samples=12,
        bid=99.99,
        ask=100.01,
        executable_notional=10000,
        long_impact_bps=2,
        short_impact_bps=2,
    )


@pytest.fixture
def cfg(settings, monkeypatch):
    monkeypatch.setattr("bybit_flow.tradingview.now_ms", lambda: NOW)
    monkeypatch.setattr("bybit_flow.app.now_ms", lambda: NOW)
    return settings.model_copy(
        update={
            "tv_enabled": True,
            "tv_token": SecretStr("test-integration-key-" + "x" * 32),
            "tv_symbols": ["TESTUSDT"],
            "sss_research": True,
            "research_webhook": SecretStr("https://discord.com/api/webhooks/TEST/SYNTHETIC"),
        }
    )


def test_measured_sss_never_calibrated(cfg, instrument):
    event = TVEvent.model_validate(payload())
    signal = evaluate(event, cfg, observed(instrument), at_ms=NOW)
    assert signal.quality == 100 and not signal.gates
    assert signal.final_tier == "SSS RESEARCH · UNCALIBRATED"
    assert signal.qualification["probability"] is None and not signal.qualification["validated"]
    assert signal.trigger_expires_ms < signal.expires_ms < signal.holding_deadline_ms
    assert signal.evidence["trapped_participants"]["potential_trapped_sellers"]


def test_default_locked_missing_data_and_proxy_cap(cfg, instrument):
    event = TVEvent.model_validate(payload())
    assert evaluate(event, cfg, at_ms=NOW).final_tier == "REJECTED"
    proxy = cfg.model_copy(update={"tv_proxy_research": True})
    signal = evaluate(event, proxy, at_ms=NOW)
    assert not signal.gates and signal.final_tier == "TV PROXY RESEARCH" and signal.quality <= 84
    assert signal.risk["quantity"] is None
    no_opt_in = cfg.model_copy(update={"sss_research": False})
    assert evaluate(event, no_opt_in, observed(instrument), at_ms=NOW).final_tier == "RESEARCH"
    o = event.observation.model_copy(
        update={"footprint_source": "unavailable", "buy_volume": None, "sell_volume": None}
    )
    assert evaluate(event.model_copy(update={"observation": o}), proxy, at_ms=NOW).gates
    assert evaluate(
        event, cfg, observed(instrument).model_copy(update={"observed_ms": NOW - 61000}), at_ms=NOW
    ).gates


def test_each_evidence_component_changes_score(cfg, instrument):
    e = TVEvent.model_validate(payload())
    for change in (
        {"regime_slope_atr": 0.1},
        {"stacked_buy": 3},
        {"buy_volume": 550, "sell_volume": 450},
        {"setup_low": 98.5},
        {"tp1": 108},
    ):
        signal = evaluate(
            e.model_copy(update={"observation": e.observation.model_copy(update=change)}),
            cfg,
            observed(instrument),
            at_ms=NOW,
        )
        assert signal.quality < 100


def test_symmetric_short_and_traps(cfg, instrument):
    p = payload()
    o = p["observation"]
    for field in ("entry", "stop", "tp1", "tp2", "level", "setup_close", "prior_close", "bos_level", "poc"):
        o[field] = 200 - o[field]
    o["setup_low"], o["setup_high"] = 200 - o["setup_high"], 200 - o["setup_low"]
    o.update(
        direction="SHORT",
        regime_slope_atr=-0.4,
        buy_volume=300,
        sell_volume=700,
        stacked_buy=0,
        stacked_sell=4,
        prior_buy_volume=800,
        prior_sell_volume=200,
        prior_high=103,
        prior_low=100,
    )
    signal = evaluate(TVEvent.model_validate(p), cfg, observed(instrument), at_ms=NOW)
    assert signal.quality == 100 and not signal.gates
    assert signal.evidence["trapped_participants"]["potential_trapped_buyers"]
    missing = TVEvent.model_validate(payload()).observation.model_copy(
        update={"prior_buy_volume": None, "prior_sell_volume": None}
    )
    assert not trapped_participants(missing)["available"]


def test_auth_http_queue_discord_invalidation_journal(cfg, instrument, monkeypatch):
    delivered = []

    def discord(request):
        body = json.loads(request.content)
        assert body["allowed_mentions"] == {"parse": []}
        delivered.append(body)
        return httpx.Response(200, json={"id": str(len(delivered))})

    original = Gateway.__init__

    def gateway_init(self, settings, store, transport=None):
        original(self, settings, store, httpx.MockTransport(discord))

    monkeypatch.setattr(Gateway, "__init__", gateway_init)
    headers = {"x-tv-key": cfg.tv_token.get_secret_value()}
    with TestClient(create_app(cfg)) as client:
        assert client.post("/webhooks/tradingview", json=payload()).status_code == 401
        assert (
            client.post(
                "/api/tradingview/liquidity", json=observed(instrument).model_dump(mode="json")
            ).status_code
            == 200
        )
        response = client.post("/webhooks/tradingview", headers=headers, json=payload())
        assert response.status_code == 202
        assert client.post("/webhooks/tradingview", headers=headers, json=payload()).status_code == 200
        # Execute work through TestClient's event-loop portal, without arbitrary sleeps or SQLite cross-thread access.
        client.portal.call(client.app.state.gateway.process_one)
        signals = client.get("/api/overview").json()["signals"]
        assert signals[0]["state"] == "ALERTED" and len(delivered) == 1
        assert "SSS RESEARCH · UNCALIBRATED" in delivered[0]["embeds"][0]["title"]
        ident = signals[0]["id"]
        base = {k: v for k, v in payload().items() if k != "observation"}
        event = base | {"event": "invalidate", "event_id": "synthetic-invalidate-1", "price": 94}
        assert client.post("/webhooks/tradingview", headers=headers, json=event).status_code == 202
        client.portal.call(client.app.state.gateway.process_one)
        assert client.get("/api/signals/" + ident).json()["signal"]["state"] == "INVALIDATED"
        assert len(delivered) == 2
        monkeypatch.setattr("bybit_flow.tradingview.now_ms", lambda: NOW + 900_000)
        event = base | {
            "event": "outcome",
            "event_id": "synthetic-outcome-1",
            "outcome_net_r": -1.1,
            "source_ms": NOW + 900_000,
            "bar_close_ms": NOW + 899_000,
        }
        assert client.post("/webhooks/tradingview", headers=headers, json=event).status_code == 202
        client.portal.call(client.app.state.gateway.process_one)
        assert len(client.get("/api/journal").json()) == 1
        assert len(delivered) == 2  # outcome on invalidated plan journals but never resurrects it


def test_input_validation_and_durable_recovery(cfg, monkeypatch):
    store = Store(cfg.data_dir)
    g = Gateway(cfg, store)
    e = TVEvent.model_validate(payload())
    assert g.enqueue(e)
    assert not g.enqueue(e)
    with pytest.raises(ValueError):
        g.enqueue(e.model_copy(update={"price": 99}))
    with store.db:
        store.db.execute("UPDATE tv_inbox SET status='processing'")
    store.close()
    reopened = Store(cfg.data_dir)
    g = Gateway(cfg, reopened)
    assert reopened.db.execute("SELECT status FROM tv_inbox").fetchone()[0] == "pending"
    monkeypatch.setattr("bybit_flow.tradingview.now_ms", lambda: NOW + 180_000)
    asyncio.run(g.process_one())
    assert reopened.db.execute("SELECT result FROM tv_inbox").fetchone()[0] == "expired in queue"
    reopened.close()
    for change in (
        {"symbol": "BINANCE:BTCUSDT.P"},
        {"price": float("nan")},
        {"bar_close_ms": NOW},
        {"extra_secret": "never-echo"},
    ):
        with pytest.raises(ValidationError):
            TVEvent.model_validate(payload() | change)


def test_heartbeat_loss_and_expiry(cfg, instrument, monkeypatch):
    store = Store(cfg.data_dir)
    signal = evaluate(TVEvent.model_validate(payload()), cfg, observed(instrument), at_ms=NOW)
    store.signal(signal)
    monkeypatch.setattr("bybit_flow.tradingview.now_ms", lambda: NOW + 1_000_000)
    asyncio.run(Gateway(cfg, store).maintain())
    assert Signal.model_validate(store.signals()[0]).state == "INVALIDATED"
    store.close()


async def test_connection_test_is_not_a_signal(cfg):
    store = Store(cfg.data_dir)
    calls = []

    def transport(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "synthetic-test-message"})

    g = Gateway(cfg.model_copy(update={"research_alerts": True}), store, httpx.MockTransport(transport))
    p = {k: v for k, v in payload().items() if k != "observation"}
    e = TVEvent.model_validate(p | {"event": "connection_test"})
    assert g.enqueue(e)
    await g.process_one()
    assert not store.signals() and not store.rows("journal")
    assert calls[0]["embeds"][0]["title"] == "CONNECTION TEST · NOT A TRADE"
    assert not g.enqueue(e)
    store.close()
