import json

import httpx
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from bybit_flow.app import create_app
from bybit_flow.fundamentals import Fact, add_fact, facts_asof
from bybit_flow.notifications import Notifier, embed
from bybit_flow.replay import replay
from bybit_flow.storage import Recorder, Store


async def test_recorder_raw_and_normalized_roundtrip(settings):
    store = Store(settings.data_dir)
    rec = Recorder(store, settings)
    rec.offer("control/gap", "ALL", 100, {"reason": "test"}, receipt_ms=110, complete=False)
    rec.running = False
    await rec.run()
    manifest = store.rows("segments")[0]
    rows = pq.read_table(manifest["parquet"]).to_pylist()
    assert rows[0]["event_ms"] == 100 and rows[0]["receipt_ms"] == 110
    from bybit_flow.replay import segment_rows

    result = replay(segment_rows([manifest["raw"]]), settings)
    assert result["gaps"] == 1 and result["statistics"]["trades"] == 0
    store.close()


def test_backpressure_opens_circuit(settings):
    store = Store(settings.data_dir)
    rec = Recorder(store, settings)
    for _ in range(settings.queue_size):
        rec.offer("test", "T", 1, {})
    with pytest.raises(RuntimeError):
        rec.offer("test", "T", 1, {})
    assert not rec.healthy and store.get("recorder_gap")
    store.close()


def test_pit_membership_and_facts(settings):
    store = Store(settings.data_dir)
    store.membership(100, "T", True, {})
    store.membership(200, "T", False, {})
    assert store.membership_asof("T", 99) is None
    assert store.membership_asof("T", 150)["eligible"] == 1
    assert store.membership_asof("T", 250)["eligible"] == 0
    fact = Fact(
        asset="BTC",
        category="governance",
        definition="Synthetic test definition",
        value="test",
        source="https://bitcoin.org",
        known_ms=100,
        effective_ms=500,
        expires_ms=1000,
    )
    add_fact(store, fact, 150)
    assert not facts_asof(store, "BTC", 120)
    assert facts_asof(store, "BTC", 200)  # known upcoming event, no future-knowledge leak
    assert not facts_asof(store, "BTC", 1000)
    store.close()


async def test_discord_deduplicates_and_omits_mentions(settings, signal):
    from pydantic import SecretStr

    cfg = settings.model_copy(
        update={
            "research_alerts": True,
            "research_webhook": SecretStr("https://discord.com/api/webhooks/123/test"),
        }
    )
    store = Store(settings.data_dir)
    store.signal(signal)
    calls = []

    def handle(r):
        calls.append(r)
        return httpx.Response(200, json={"id": "discord-id"})

    n = Notifier(cfg, store, httpx.MockTransport(handle))
    assert await n.send_research(signal) == "sent"
    assert await n.send_research(signal) == "already-attempted"
    assert len(calls) == 1
    payload = json.loads(calls[0].content)
    assert payload["allowed_mentions"] == {"parse": []}
    assert "Uncalibrated" in json.dumps(payload)
    assert (await n.send_public(signal)).startswith("blocked")
    store.close()


def test_embed_within_discord_limits(settings, signal):
    payload = embed(signal, settings.dashboard_url)["embeds"][0]
    assert len(payload["fields"]) <= 25
    total = sum(len(f["name"]) + len(f["value"]) for f in payload["fields"])
    assert total + len(payload["title"]) + len(payload["description"]) + len(payload["footer"]["text"]) < 6000


def test_dashboard_local_auth_secrets_and_persistence(settings):
    with TestClient(create_app(settings)) as client:
        assert client.get("/").status_code == 200
        assert client.get("/assets/app.js").status_code == 200
        assert client.get("/healthz").json()["alerts_only"]
        assert client.get("/api/overview").json()["watchlist"] == []
        cfg = client.get("/api/settings").json()
        assert not any(k in cfg for k in ("discord_webhook", "research_webhook", "admin_token"))
        assert client.post("/api/journal", json={"note": "Manual paper observation"}).status_code == 200
        assert (
            client.post("/api/portfolio", json={"positions": [], "daily_loss_fraction": 0.01}).status_code
            == 200
        )
        assert (
            client.post(
                "/api/journal", json={"note": "bad"}, headers={"origin": "https://evil.example"}
            ).status_code
            == 403
        )
        assert client.get("/", headers={"host": "evil.example"}).status_code == 403
    with TestClient(create_app(settings)) as client:
        assert len(client.get("/api/journal").json()) == 1


def test_password_protects_entire_dashboard(settings):
    from pydantic import SecretStr

    cfg = settings.model_copy(update={"admin_token": SecretStr("test-password")})
    with TestClient(create_app(cfg)) as client:
        assert client.get("/").status_code == 401
        assert client.get("/api/overview").status_code == 401
        assert client.get("/", auth=("research", "test-password")).status_code == 200


def test_manual_asset_fact_is_source_attributed_not_a_signal(settings):
    from bybit_flow.storage import now_ms

    now = now_ms()
    fact = dict(
        asset="EXAMPLE",
        category="economic_purpose",
        definition="Synthetic API test only",
        value="No financial evidence",
        source="https://example.com/test",
        known_ms=now - 1000,
        effective_ms=now - 1000,
        expires_ms=now + 86400000,
    )
    with TestClient(create_app(settings)) as client:
        assert client.post("/api/facts", json=fact).status_code == 200
        recorded = client.get("/api/research").json()["facts"]
        assert len(recorded) == 1 and recorded[0]["verification"] == "user-sourced; review source"
        assert recorded[0]["collected_ms"] >= now
        assert client.post("/api/facts", json=fact | {"known_ms": now + 3600000}).status_code == 422
        assert client.get("/api/overview").json()["signals"] == []


def test_backup_is_recoverable(settings, tmp_path):
    store = Store(settings.data_dir)
    store.put("test", {"saved": True})
    target = tmp_path / "backup.sqlite"
    store.backup(target)
    assert target.exists()
    with pytest.raises(ValueError):
        store.backup(target)
    store.close()
