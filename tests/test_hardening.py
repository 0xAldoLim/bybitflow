import asyncio
import json

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.config import Settings
from bybit_flow.discovery import depth_shortlist, horizon_pre_ranks
from bybit_flow.fundamentals import quality_evidence
from bybit_flow.identity import delivery_status, fingerprint, relationship
from bybit_flow.ml.horizon import dataset, fit
from bybit_flow.notifications import Notifier
from bybit_flow.scoring import score
from bybit_flow.storage import Store


def configured(settings):
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/test/example")
    return settings


def test_retention_defaults_and_explicit_false():
    assert Settings(_env_file=None).recording_retention_enabled
    assert not Settings(_env_file=None, recording_retention_enabled=False).recording_retention_enabled


def test_fingerprint_ignores_minute_and_score_but_tracks_plan(signal):
    signal.evidence.update(trigger_bar_end=1000, price_tick=".01")
    other = signal.model_copy(deep=True)
    other.id = "next-minute"
    other.created_ms += 60000
    other.quality = 99
    other.evidence["execution_window_end_ms"] = 60000
    assert fingerprint(signal) == fingerprint(other)
    other.stop -= 1
    assert fingerprint(signal) != fingerprint(other)
    other.stop = signal.stop
    other.evidence["trigger_bar_end"] = 2000
    assert fingerprint(signal) != fingerprint(other)


@pytest.mark.asyncio
async def test_atomic_same_plan_across_connections_and_restart(settings, signal):
    configured(settings)
    first = Store(settings.data_dir)
    first.signal(signal)
    other = signal.model_copy(update={"id": "minute-two", "created_ms": 61000})
    first.signal(other)
    second = Store(settings.data_dir)
    calls = []

    async def handle(request):
        calls.append(request)
        await asyncio.sleep(0.01)
        return httpx.Response(200, json={"id": "one"})

    transport = httpx.MockTransport(handle)
    results = await asyncio.gather(
        Notifier(settings, first, transport).send_research(signal),
        Notifier(settings, second, transport).send_research(other),
    )
    assert sorted(results) == ["duplicate-plan-suppressed", "sent"]
    assert len(calls) == 1
    first.close()
    second.close()
    reopened = Store(settings.data_dir)
    third = other.model_copy(update={"id": "after-restart"})
    assert await Notifier(settings, reopened, transport).send_research(third) == "duplicate-plan-suppressed"
    assert len(calls) == 1
    assert delivery_status(reopened)["sent"] == 1
    reopened.close()


@pytest.mark.asyncio
async def test_clustering_does_not_mutate_either_lifecycle(settings, signal):
    configured(settings)
    store = Store(settings.data_dir)
    signal.horizon_profile = "CORE_INTRADAY"
    signal.state = "ALERTED"
    signal.holding_deadline_ms = 10000000
    store.signal(signal)
    sent = []

    def handle(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={"id": str(len(sent))})

    notifier = Notifier(settings, store, httpx.MockTransport(handle))
    assert await notifier.send_research(signal) == "sent"
    other = signal.model_copy(deep=True)
    other.id = "swing"
    other.horizon_profile = "SWING"
    other.quality = signal.quality + 10
    store.signal(other)
    before = dict(store.db.execute("SELECT id,payload FROM signals"))
    assert await notifier.send_research(other) == "sent"
    assert "PRIMARY THESIS UPDATE" in sent[-1]["embeds"][0]["title"]
    assert before == dict(store.db.execute("SELECT id,payload FROM signals"))
    assert len(store.active_signals()) == 2
    assert delivery_status(store)["thesis_cluster_updates"] == 1
    store.close()


@pytest.mark.parametrize(
    "change,expected",
    [
        ({"horizon_profile": "SWING"}, "CONFIRMING_HORIZON"),
        ({"stop": 80}, "MATERIALLY_DIFFERENT_PLAN"),
        ({"direction": "SHORT"}, "CONFLICTING_HORIZON"),
    ],
)
def test_cluster_relationships(signal, change, expected):
    assert relationship(signal.model_copy(update=change), signal) == expected


def test_quality_is_not_documentation_coverage():
    facts = [
        dict(
            category=k,
            source="https://example.org/source",
            definition="Sourced assessment",
            known_ms=1,
            expires_ms=100,
            assessment="positive",
        )
        for k in (
            "economic_purpose",
            "value_accrual",
            "dilution",
            "concentration",
            "security",
            "governance",
            "events",
        )
    ]
    good = quality_evidence(facts, 10)
    facts[2]["assessment"] = "adverse"
    assert quality_evidence(facts, 10)["fraction"] < good["fraction"] * 0.6
    assert quality_evidence([], 10)["fraction"] == 0
    assert quality_evidence(facts, 0)["fraction"] == 0
    assert quality_evidence([f | {"assessment": "unassessed"} for f in facts], 10)["fraction"] <= 0.2


def test_beta_residual_primary_and_fallback_capped(signal):
    signal.horizon_profile = "SWING"
    signal.evidence["cross_market"] = {"BTCUSDT": "trending up", "ETHUSDT": "trending up"}
    score(signal, False, False)
    assert signal.score_components["cross_market"]["earned"] <= 1.25
    signal.evidence["market_factor"] = {
        "available": True,
        "residual_return": 0,
        "beta_stability_btc": 0,
        "beta_stability_eth": 0,
    }
    score(signal, False, False)
    matching = signal.score_components["cross_market"]["earned"]
    signal.evidence["market_factor"]["residual_return"] = 0.01
    score(signal, False, False)
    assert signal.score_components["cross_market"]["earned"] > matching
    assert signal.quality <= 100


def test_horizon_abstains_and_is_research_only(settings):
    store = Store(settings.data_dir)
    result = fit(store, settings, 100000)
    assert result["status"] == "INSUFFICIENT_EVIDENCE"
    assert result["recommended_horizon"] is None and not result["production_enabled"]
    assert dataset(store, 100000) == []
    store.close()


def test_same_plan_ml_identity_preserves_distinct_horizon(settings, signal):
    store = Store(settings.data_dir)
    store.signal(signal)
    store.signal(signal.model_copy(update={"id": "duplicate", "created_ms": 61000}))
    store.signal(signal.model_copy(update={"id": "different-horizon", "horizon_profile": "SWING"}))
    ids = dict(store.db.execute("SELECT signal_id,candidate_identity FROM candidate_identities"))
    assert ids[signal.id] == ids["duplicate"]
    assert ids[signal.id] != ids["different-horizon"]
    assert store.db.execute("SELECT count(*) FROM signals").fetchone()[0] == 3
    store.close()


def test_shortlist_protects_core_pending_and_explores():
    ranked = [dict(symbol=str(i), eligible=True, rank_score=100 - i) for i in range(100)]
    chosen, meta = depth_shortlist(ranked, ["90"], ["91"], 10, 20, 4, 0.15)
    assert len(chosen) == 20 and {"90", "91"} <= set(chosen)
    assert any(v["selection_reason"] == "EXPLORE" for v in meta.values())


@pytest.mark.parametrize("winner", ["SWING", "SHORT_INTRADAY"])
def test_noncore_priority_reaches_shortlist(monkeypatch, winner):
    features = [dict(regime="range", efficiency=0.0, volume_expansion=0.0) for _ in range(4)]
    if winner == "SWING":
        features[0]["efficiency"] = 0.5
        features[1]["efficiency"] = 0.5
        features[1]["volume_expansion"] = 2
    else:
        features[2]["efficiency"] = 0.5
        features[3]["efficiency"] = 0.5
        features[3]["volume_expansion"] = 2
    monkeypatch.setattr("bybit_flow.discovery.candle_features", lambda bars, now: features[bars])
    pre = horizon_pre_ranks(0, 1, 2, 3, 1, 1, 10, True)
    assert pre["best_pre_rank_horizon"] == winner
    ranked = [dict(symbol="WIN", eligible=True, **pre), dict(symbol="LOW", eligible=True, rank_score=0)]
    selected, _ = depth_shortlist(ranked, [], [], 2, 2, 1, 0)
    assert "WIN" in selected


def test_late_dataset_uses_only_frozen_decision_features(settings, signal):
    from bybit_flow.ml.features import snapshot
    from bybit_flow.ml.store import canonical

    store = Store(settings.data_dir)
    signal.evidence["h1"] = {"atr": 2}
    store.signal(signal)
    frozen = snapshot(signal, 2000, "decision", None)
    frozen["values"]["late_target_hit"] = 999
    with store.db:
        store.db.execute(
            "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
            ("frozen", signal.id, "decision", 2000, frozen["schema_version"], canonical(frozen)),
        )
        store.db.execute(
            "INSERT INTO research_labels VALUES(?,?,?)",
            (
                signal.id,
                9000,
                json.dumps(
                    dict(
                        coverage_complete=True,
                        terminal_ms=4000,
                        late_target_hit=True,
                        extended_same_rules_outcome="TARGET",
                        best_observed_horizon="SWING",
                        timing_classification="late",
                    )
                ),
            ),
        )
    assert dataset(store, 8000) == []
    rows = dataset(store, 10000)
    assert len(rows) == 1 and rows[0]["values"]["h1_atr"] == 2
    assert "late_target_hit" not in rows[0]["values"]
    assert rows[0]["targets"]["late_target_hit"]
    assert store.signals()[0]["state"] == signal.state
    store.close()


def test_horizon_model_chronological_purge_and_reserved_holdout(settings, monkeypatch):
    settings.horizon_model_min_samples = 50
    store = Store(settings.data_dir)
    rows = [
        dict(
            signal_id=str(i),
            snapshot_id=str(i),
            candidate_identity=str(i),
            decision_ms=i * 3 * 86400000,
            label_available_ms=i * 3 * 86400000 + 1000,
            values={"h1_atr": i % 2},
            targets={"best_observed_horizon": "SWING" if i % 2 else "CORE_INTRADAY"},
        )
        for i in range(80)
    ]
    monkeypatch.setattr("bybit_flow.ml.horizon.dataset", lambda *_: rows)
    report = fit(store, settings, 10**12)
    assert report["status"] == "RESEARCH_ONLY" and not report["production_enabled"]
    assert report["recommended_horizon"] is None
    artifact = json.loads(
        (store.root / "ml" / "horizon" / (report["experiment_id"] + "-model.json")).read_text()
    )
    partitions = artifact["partitions"]
    assert max(map(int, partitions["train"])) < min(map(int, partitions["validation"]))
    assert max(map(int, partitions["validation"])) < min(map(int, partitions["test"]))
    assert len(store.get("horizon_holdouts")) == 1
    assert fit(store, settings, 10**12) == report
    store.close()


def test_protected_post_terminal_observation(settings, signal):
    from bybit_flow.retention import protection_cutoff

    store = Store(settings.data_dir)
    with store.db:
        store.db.execute(
            "INSERT INTO observations VALUES(?,?,?,?)",
            (
                signal.id,
                "FOLLOWING_LATE_OUTCOME",
                9000,
                json.dumps({"cursor_ms": 1000000, "terminal_ms": 1000000}),
            ),
        )
    assert protection_cutoff(store, 9000000, 8000000) <= 1000000
    store.close()


@pytest.mark.asyncio
async def test_same_signal_twice_one_http_and_tests_excluded(settings, signal):
    configured(settings)
    store = Store(settings.data_dir)
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json={"id": "1"})

    notifier = Notifier(settings, store, httpx.MockTransport(handle))
    store.signal(signal)
    assert await notifier.send_research(signal) == "sent"
    assert await notifier.send_research(signal) in {"already-attempted", "duplicate-plan-suppressed"}
    assert len(calls) == 1
    before = delivery_status(store)
    await notifier.send_connection_test("test", 1000)
    assert delivery_status(store) == before
    assert "TradingView gateway" not in calls[-1].content.decode()
    store.close()


@pytest.mark.asyncio
async def test_stage_a_shortlist_avoids_universe_depth_and_verifies_each_selected(
    settings, instrument_row, monkeypatch
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from bybit_flow.models import Candle
    from bybit_flow.scanner import Scanner
    from bybit_flow.storage import now_ms

    settings.market_source = "bybit"
    settings.deep_symbols = 3
    settings.core_watchlist = []
    settings.stage_a_depth_candidates = 6
    store = Store(settings.data_dir)
    scanner = Scanner(settings, store, SimpleNamespace(healthy=True, offer=Mock()))
    await scanner.api.close()
    rows = [instrument_row | {"symbol": f"TEST{i}USDT", "baseCoin": f"TEST{i}"} for i in range(12)]
    requests = []

    async def get(endpoint, **kwargs):
        now = now_ms()
        if endpoint == "time":
            return {"time": now}
        if endpoint == "tickers":
            return {
                "time": now,
                "result": {
                    "list": [dict(symbol=r["symbol"], bid1Price="99.99", ask1Price="100.01") for r in rows]
                },
            }
        assert endpoint == "orderbook"
        requests.append(kwargs["symbol"])
        return {
            "time": now,
            "result": {"u": 1, "seq": 1, "b": [["99.99", "1000"]], "a": [["100.01", "1000"]]},
        }

    scanner.api = SimpleNamespace(get=get, instruments=AsyncMock(return_value=rows), close=AsyncMock())
    scanner.select_source = AsyncMock()
    scanner.record_quotes = Mock()
    scanner.spread_history = lambda _: SimpleNamespace(assess=lambda *args: {"eligible": True, "reasons": []})

    async def candles(symbol, tf, now, count):
        width = {"D": 86400000, "240": 14400000, "60": 3600000, "15": 900000}[tf]
        end = now // width * width
        return [
            Candle(end - (count - i) * width, width, 100, 102, 99, 101, 300000, 30000000)
            for i in range(count)
        ]

    scanner.cached_candles = candles
    scanner.depth_context = AsyncMock(return_value={})
    scanner.save_candidates = Mock(return_value=[])
    scanner.discover_horizons = AsyncMock()
    scanner.streams.select = AsyncMock()
    result = await scanner.scan_once()
    assert result["preeligible"] == 12
    assert result["depth_requests"] == 3 < 12
    assert result["depth_verified"] == result["deep_selected"] == 3
    assert set(result["deep_symbols"]) <= set(requests)
    assert result["selection_counts"]["EXPLORE"] >= 1
    store.close()


@pytest.mark.asyncio
async def test_restart_seeds_older_runtime_send_without_mutating_active(settings, signal):
    store = Store(settings.data_dir)
    signal.state = "ALERTED"
    store.signal(signal)
    before = store.db.execute("SELECT payload FROM signals").fetchone()[0]
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            ("research:" + signal.id + ":initial", signal.id, "sent", "{}", "older-runtime", 5000),
        )
    store.close()
    store = Store(settings.data_dir)
    assert store.db.execute("SELECT payload FROM signals").fetchone()[0] == before
    assert delivery_status(store)["last_message_id"] == "older-runtime"
    duplicate = signal.model_copy(update={"id": "new-slot"})
    configured(settings)
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json={"id": "unexpected"})

    assert (
        await Notifier(settings, store, httpx.MockTransport(handle)).send_research(duplicate)
        == "duplicate-plan-suppressed"
    )
    assert calls == []
    store.close()
