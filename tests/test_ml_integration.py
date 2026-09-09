import json

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from bybit_flow.app import create_app
from bybit_flow.ml.inference import apply, delivery_eligible
from bybit_flow.ml.labels import label_recordings
from bybit_flow.ml.store import FeatureStore
from bybit_flow.notifications import Notifier
from bybit_flow.storage import Store


def envelope(source, time, payload, symbol="TESTUSDT"):
    return dict(
        source=source,
        event_ms=time,
        receipt_ms=time,
        symbol=symbol,
        complete=True,
        payload=json.dumps(payload),
        schema_version=1,
    )


def print_event(time, price, ident="print-1"):
    return envelope(
        "ws/publicTrade.TESTUSDT", time, {"data": [dict(T=time, p=str(price), v="1000", i=ident, S="Buy")]}
    )


def test_counterfactual_rejected_fill_and_gap_exclusion(settings, signal):
    store = Store(settings.data_dir)
    signal.gates = ["rejected order flow"]
    signal.quality = 25
    ident = FeatureStore(store).capture(signal, 1000, "decision")
    rows = [
        envelope("control/subscribed", 500, {"symbols": [signal.symbol]}),
        print_event(1500, 100),
        print_event(5000, 116, "exit"),
    ]
    report = label_recordings(store, rows, settings)
    assert report["complete"] == 1
    result = report["outcomes"][0]
    assert result["snapshot_id"] == ident and result["classification"] == "win"
    assert not result["costs_verified"] and result["funding_reserve"] > 0
    assert len(FeatureStore(store).dataset(6000)) == 1
    signal.id = "gap-test"
    FeatureStore(store).capture(signal, 1000, "decision")
    rows.insert(2, envelope("control/gap", 2000, {"reason": "lost"}))
    report = label_recordings(store, rows, settings)
    assert not report["outcomes"][0]["complete"]
    assert report["outcomes"][0]["net_r"] is None
    store.close()


def test_chart_fallback_keeps_rejected_candidate_without_fake_trades(settings, tmp_path):
    from test_tradingview import NOW, payload

    from bybit_flow.ml.labels import label_chart

    event = payload()
    event["observation"].update(footprint_source="unavailable", buy_volume=None, sell_volume=None)
    events, candles = tmp_path / "events.jsonl", tmp_path / "bars.csv"
    events.write_text(json.dumps(event) + "\n")
    candles.write_text(
        "time,open,high,low,close\n"
        + "".join(f"{NOW - 1000 + i * 900000},100,116,99,110\n" for i in range(100))
    )
    store = Store(settings.data_dir)
    result = label_chart(store, events, candles, settings, "TESTUSDT")
    assert result["candidates"] == result["complete"] == 1
    rows = FeatureStore(store).dataset(NOW + 100_000_000, policy="chart-v1", stage="chart")
    assert rows[0]["signal"]["gates"]
    assert rows[0]["values"]["buy_base"] is None
    assert rows[0]["label"]["data_kind"] == "source-attested-TV-OHLC"
    assert not rows[0]["label"]["costs_verified"]
    store.close()


def test_no_model_and_no_flag_can_unlock_public(settings, signal):
    store = Store(settings.data_dir)
    cfg = settings.model_copy(update={"ml_enabled": True, "sss_research": True})
    apply(signal, cfg, store, 1000)
    assert signal.calibrated_probability is None
    assert signal.model_version is None
    signal.validation_status, signal.final_tier = "validated", "SSS"
    signal.calibrated_probability = 0.99
    signal.expected_net_r_uncertainty = (1.0, 2.0)
    signal.risk = {"accepted": True}
    assert not delivery_eligible(signal, cfg, store)
    store.put("ml_champion", "untrusted")
    assert not delivery_eligible(signal, cfg, store)
    store.close()


def test_disabling_ml_clears_stale_approval(settings, signal):
    store = Store(settings.data_dir)
    signal.validation_status, signal.final_tier = "validated", "SSS"
    signal.calibrated_probability, signal.expected_net_r = 0.9, 1.0
    apply(signal, settings, store)
    assert signal.validation_status == "unvalidated" and signal.final_tier == "RESEARCH"
    assert signal.calibrated_probability is None and signal.expected_net_r is None
    store.close()


def test_ml_dashboard_auth_and_no_secret_echo(settings):
    cfg = settings.model_copy(
        update={"admin_token": SecretStr("test-admin"), "ops_webhook": SecretStr("secret-operations-url")}
    )
    with TestClient(create_app(cfg)) as client:
        assert client.get("/api/ml").status_code == 401
        response = client.get("/api/ml", auth=("research", "test-admin"))
        assert response.status_code == 200
        assert response.json()["champion"] is None
        assert response.json()["models"] == []
        assert "ops_webhook" not in client.get("/api/settings", auth=("research", "test-admin")).text


@pytest.mark.asyncio
async def test_ops_dedup_and_public_rejection(settings, signal):
    store = Store(settings.data_dir)
    calls = []

    def transport(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "mock-delivery"})

    cfg = settings.model_copy(update={"ops_webhook": SecretStr("https://discord.com/api/webhooks/MOCK/TEST")})
    notifier = Notifier(cfg, store, httpx.MockTransport(transport))
    assert await notifier.send_public(signal) == "blocked: no validated deployment model"
    result = {"status": "degraded", "reason": "never echo arbitrary exception content"}
    assert await notifier.send_operational("drift", result) == "sent"
    assert await notifier.send_operational("drift", result) == "already-attempted"
    assert "reason" not in calls[0]["embeds"][0]["description"]
    assert calls[0]["allowed_mentions"] == {"parse": []}
    store.close()


@pytest.mark.asyncio
async def test_tv_to_ml_snapshot_discord_invalidation_and_journal(settings, instrument, monkeypatch):
    from test_tradingview import NOW, observed, payload

    from bybit_flow.tradingview import Gateway, TVEvent

    clock = [NOW]
    monkeypatch.setattr("bybit_flow.tradingview.now_ms", lambda: clock[0])
    store = Store(settings.data_dir)
    cfg = settings.model_copy(
        update={
            "ml_enabled": True,
            "sss_research": True,
            "tv_symbols": ["TESTUSDT"],
            "research_webhook": SecretStr("https://discord.com/api/webhooks/MOCK/TEST"),
        }
    )
    store.put("tv_liquidity:TESTUSDT", observed(instrument).model_dump(mode="json"))
    cards = []

    def transport(request):
        cards.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "mock-card"})

    gateway = Gateway(cfg, store, httpx.MockTransport(transport))
    event = TVEvent.model_validate(payload())
    assert await gateway.process(event) == "sent"
    snapshot = next(FeatureStore(store).snapshots())
    assert snapshot["values"]["tv_buy_volume"] == 700
    assert snapshot["values"]["buy_base"] is None
    assert "SSS RESEARCH" in cards[0]["embeds"][0]["title"]
    clock[0] += 30_000
    invalidation = event.model_copy(
        update={
            "event": "invalidate",
            "event_id": "synthetic-invalid-ML",
            "observation": None,
            "source_ms": clock[0],
            "price": 94,
        }
    )
    assert await gateway.process(invalidation) == "INVALIDATED"
    assert len(cards) == 2
    clock[0] += 900_000
    outcome = event.model_copy(
        update={
            "event": "outcome",
            "event_id": "synthetic-outcome-ML",
            "observation": None,
            "source_ms": clock[0],
            "bar_close_ms": event.bar_close_ms + 900_000,
            "outcome_net_r": -1.0,
        }
    )
    await gateway.process(outcome)
    assert len(store.rows("journal")) == 1
    assert FeatureStore(store).dataset(clock[0]) == []  # Manual outcome never becomes verified ML label.
    store.close()
