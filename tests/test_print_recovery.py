import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime

import httpx
import pytest

from bybit_flow.backtest import PaperPosition
from bybit_flow.exchanges import VenueAPI
from bybit_flow.ml import SCHEMA_VERSION
from bybit_flow.ml.completeness import cache_readiness
from bybit_flow.ml.labels import primary_outcome
from bybit_flow.ml.operations import trainability
from bybit_flow.ml.print_recovery import POLICY, backfill, pending, recover_one, validate_recovered
from bybit_flow.ml.store import FeatureStore, digest
from bybit_flow.ml.trade_history import BinanceHistory
from bybit_flow.ml.training import read_dataset
from bybit_flow.storage import Store, now_ms


def trade(ident, at, price=100, quantity=1000):
    return dict(a=ident, T=at, p=str(price), q=str(quantity), m=False)


class History:
    name = "test-original-venue-aggregate-trades"

    def __init__(self, pages):
        self.pages = list(pages)

    async def page(self, snapshot, state, metrics, asof):
        metrics["requests"] += 1
        return dict(trades=self.pages.pop(0) if self.pages else [])


def candidate(settings, signal, complete=False):
    store = Store(settings.data_dir)
    signal.source = "binance"
    signal.feature_schema_version = SCHEMA_VERSION
    store.put(
        "primary_recorded_coverage",
        dict(
            receipt_ms=900,
            trade_stale_ms=15000,
            markets={
                "binance:TESTUSDT": dict(
                    subscribed=True, coverage_start_ms=800, last_trade_ms=900, subscription_start_ms=800
                )
            },
        ),
    )
    fs = FeatureStore(store)
    ident = fs.capture(signal, 1000, "decision")
    snapshot = json.loads(
        store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
    )
    position = PaperPosition(
        signal.model_copy(update={"created_ms": 1000}),
        10,
        fee_bps=settings.taker_fee_bps,
        slippage_bps=settings.slippage_bps,
    )
    from bybit_flow.exchanges import trade_from_wire

    records = [trade(10, 900), trade(11, 1600), trade(12, 5000, 94)]
    for raw in records if complete else records[:2]:
        position.on_trade(trade_from_wire("binance", raw | dict(s=signal.symbol), raw["T"]))
    if not complete:
        position.data_gaps.append("recording continuity lost")
    old = primary_outcome(snapshot, position, settings, "a" * 64)
    fs.label(ident, old, max(6000, old.get("exit_ms") or 0))
    return store, fs, ident, snapshot, old, records


async def test_interrupted_replay_resumes_and_joins_same_primary_pool(settings, signal):
    store, fs, ident, snapshot, old, records = candidate(settings, signal)
    original = store.db.execute("SELECT payload FROM ml_labels WHERE snapshot_id=?", (ident,)).fetchone()[0]
    metrics = dict(requests=0)
    assert await recover_one(store, settings, ident, History([records[:2]]), metrics, 1) == "PENDING"
    checkpoint = store.get("primary_print_recovery:" + ident)
    assert checkpoint["position"]["quantity"] == 10
    assert not fs.dataset(now_ms(), schema_version=SCHEMA_VERSION, source="binance")
    store.close()
    store = Store(settings.data_dir)
    fs = FeatureStore(store)
    assert await recover_one(store, settings, ident, History([records[2:]]), metrics, 1) == "VERIFIED"
    assert (
        store.db.execute(
            "SELECT payload FROM ml_labels WHERE snapshot_id=? AND policy='prints-v1'", (ident,)
        ).fetchone()[0]
        == original
    )
    rows = fs.dataset(now_ms(), schema_version=SCHEMA_VERSION, source="binance")
    assert len(rows) == 1 and rows[0]["label"]["policy"] == POLICY
    assert rows[0]["label"]["classification"] == "loss"
    validate_recovered(rows[0])
    assert not fs.dataset(10000, schema_version=SCHEMA_VERSION, source="binance")
    assert read_dataset(fs.write_dataset(rows))[0]["id"] == ident
    readiness = trainability(store)
    assert readiness["by_source"]["binance"]["trainable_current_schema"] == 1
    cache_readiness(store)
    assert (
        store.get("primary_data_quality")["binance"]["prospective_readiness"]["complete_eligible_outcomes"]
        == 1
    )
    # Original label identities and frozen decision features remain unchanged.
    assert (
        digest(
            json.loads(
                store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
            )
        )
        == ident
    )
    store.close()


async def test_recovered_fill_cost_and_target_math_matches_complete_local_replay(settings, signal):
    store, fs, ident, snapshot, old, records = candidate(settings, signal)
    assert await recover_one(store, settings, ident, History([records]), dict(requests=0), 1) == "VERIFIED"
    recovered = fs.dataset(now_ms(), source="binance", schema_version=SCHEMA_VERSION)[0]["label"]
    from bybit_flow.exchanges import trade_from_wire

    position = PaperPosition(
        signal.model_copy(update={"created_ms": 1000}),
        10,
        fee_bps=settings.taker_fee_bps,
        slippage_bps=settings.slippage_bps,
    )
    for raw in records:
        position.on_trade(trade_from_wire("binance", raw | dict(s=signal.symbol), raw["T"]))
    local = primary_outcome(snapshot, position, settings, "b" * 64)
    ignore = {"policy", "outcome_policy", "data_kind", "certificate", "input_event_hash"}
    assert {k: v for k, v in recovered.items() if k not in ignore} == {
        k: v for k, v in local.items() if k not in ignore
    }
    store.close()


@pytest.mark.parametrize(
    "records,reason",
    [
        ([trade(10, 900), trade(12, 1600)], "ID gap"),
        ([trade(10, 900), trade(10, 1600)], "ID gap"),
        ([trade(10, 900), trade(11, 800)], "time reversed"),
        ([trade(10, 1600)], "anchor missing"),
        ([trade(10, 900), trade(11, 1600, quantity=-1)], "Invalid historical"),
        ([trade(10, 900), trade(11, 1600, price="invalid")], "Invalid historical"),
        ([trade(10, 900), dict(a=11, T=1600, p="100", m=False)], "Invalid historical"),
    ],
)
async def test_unverified_history_never_becomes_primary_training_data(settings, signal, records, reason):
    store, fs, ident, _, _, _ = candidate(settings, signal)
    with pytest.raises(ValueError, match=reason):
        await recover_one(store, settings, ident, History([records]), dict(requests=0), 1)
    assert not store.db.execute(
        "SELECT 1 FROM ml_labels WHERE snapshot_id=? AND policy=?", (ident, POLICY)
    ).fetchone()
    assert not fs.dataset(now_ms(), source="binance", schema_version=SCHEMA_VERSION)
    store.close()


async def test_existing_complete_labels_are_never_replaced(settings, signal):
    store, fs, ident, _, old, records = candidate(settings, signal, complete=True)
    assert await recover_one(store, settings, ident, History([records]), dict(requests=0), 1) == "INELIGIBLE"
    fs.label(
        ident,
        old | dict(policy=POLICY, outcome_policy="prints-v1", certificate=dict(status="VERIFIED")),
        6000,
    )
    assert fs.dataset(now_ms(), source="binance", schema_version=SCHEMA_VERSION)[0]["label"] == old
    store.close()


async def test_recovered_certificate_cannot_hide_candle_proxy_or_changed_features(settings, signal):
    store, fs, ident, _, _, records = candidate(settings, signal)
    await recover_one(store, settings, ident, History([records]), dict(requests=0), 1)
    row = fs.dataset(now_ms(), source="binance", schema_version=SCHEMA_VERSION)[0]
    validate_recovered(row)
    for bad in [
        row | dict(source="okx"),
        row | dict(label=row["label"] | dict(data_kind="original-venue-public-1m-OHLC")),
        row | dict(label=row["label"] | dict(net_r=100)),
        row | dict(label_available_ms=1000),
    ]:
        with pytest.raises(ValueError, match="certificate"):
            validate_recovered(bad)
    store.close()


@pytest.mark.parametrize("bad_checksum", [False, True])
async def test_old_history_uses_checked_archive_not_candles(settings, bad_checksum):
    store = Store(settings.data_dir)
    decision = 2 * 86400000 + 12 * 3600000
    asof = decision + 3 * 86400000
    day = str(datetime.fromtimestamp((decision - 60000) / 1000, UTC).date())
    name = f"TESTUSDT-aggTrades-{day}"
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        archive.writestr(
            name + ".csv",
            "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\\n"
            f"10,100,1000,100,101,{decision - 1000},false\\n"
            f"11,100,1000,102,103,{decision + 600},false\\n".replace("\\n", "\n"),
        )
    content = data.getvalue()
    checksum = "0" * 64 if bad_checksum else hashlib.sha256(content).hexdigest()

    def handler(request):
        assert request.url.host == "data.binance.vision"
        if request.url.path.endswith(".CHECKSUM"):
            return httpx.Response(200, text=checksum + "  " + name + ".zip")
        return httpx.Response(200, content=content)

    api = VenueAPI("binance", settings, transport=httpx.MockTransport(handler))
    provider = BinanceHistory(store, settings, api)
    row = dict(decision_ms=decision, signal=dict(symbol="TESTUSDT"))
    if bad_checksum:
        with pytest.raises(ValueError, match="checksum mismatch"):
            await provider.page(row, {}, dict(requests=0), asof)
        assert not list(provider.cache.glob("*.zip"))
    else:
        page = await provider.page(row, {}, dict(requests=0), asof)
        assert [r["a"] for r in page["trades"]] == [10, 11]
        assert page["trades"][0]["_archive"]["sha256"] == checksum
    await provider.close()
    store.close()


@pytest.mark.parametrize(
    "horizon,minutes,exit_price,classification",
    [
        ("CORE_INTRADAY", 240, 115, "win"),
        ("SWING", 1440, 94, "loss"),
    ],
)
async def test_longer_setup_survives_shutdown_and_uses_first_exit(
    settings, signal, horizon, minutes, exit_price, classification
):
    signal = signal.model_copy(update={"horizon_profile": horizon, "expected_hold_max": minutes})
    store, fs, ident, _, _, records = candidate(settings, signal)
    records = records[:2] + [
        trade(12, 1000 + minutes * 30000, exit_price),
        trade(13, 1000 + minutes * 30000 + 1000, 94 if classification == "win" else 115),
    ]
    assert await recover_one(store, settings, ident, History([records[:2]]), dict(requests=0), 1) == "PENDING"
    store.close()
    store = Store(settings.data_dir)
    changed_costs = settings.model_copy(
        update={"taker_fee_bps": 90, "slippage_bps": 50, "funding_reserve_bps": 90}
    )
    assert (
        await recover_one(store, changed_costs, ident, History([records[2:]]), dict(requests=0), 1)
        == "VERIFIED"
    )
    label = FeatureStore(store).dataset(now_ms(), source="binance", schema_version=SCHEMA_VERSION)[0]["label"]
    assert label["classification"] == classification
    assert label["exit_ms"] == records[2]["T"]
    assert label["horizon_profile"] == horizon
    assert label["fees_bps"] == settings.taker_fee_bps
    assert label["slippage_bps"] == settings.slippage_bps
    assert label["funding_assumption_bps"] == settings.funding_reserve_bps
    assert await recover_one(store, settings, ident, History([]), dict(requests=0), 1) == "VERIFIED"
    store.close()


async def test_recovery_yields_to_live_recorder_and_retries_automatically(settings, signal):
    store, fs, ident, _, _, records = candidate(settings, signal)
    settings = settings.model_copy(update={"ml_enabled": True})
    store.put("recorder_health", dict(at_ms=now_ms(), healthy=False))
    from bybit_flow.ml.cli import LiveRecordingPressure

    with pytest.raises(LiveRecordingPressure):
        await backfill(store, settings, provider=History([records]))
    assert not fs.dataset(now_ms(), source="binance", schema_version=SCHEMA_VERSION)
    store.put("recorder_health", dict(at_ms=now_ms(), healthy=True))
    result = await backfill(store, settings, provider=History([records]))
    assert result["verified"] == result["complete_recovered_labels"] == 1
    assert not pending(store, now_ms())
    store.close()


async def test_frozen_retention_inventory_does_not_count_new_recovery(settings, signal):
    store, fs, ident, _, _, records = candidate(settings, signal)
    limits = {
        table: store.db.execute(f"SELECT coalesce(max(rowid),0) FROM {table}").fetchone()[0]
        for table in ("ml_labels", "ml_snapshots")
    }
    before = trainability(store, row_limits=limits)["by_source"]["binance"]["trainable_current_schema"]
    await recover_one(store, settings, ident, History([records]), dict(requests=0), 1)
    assert (
        trainability(store, row_limits=limits)["by_source"]["binance"]["trainable_current_schema"]
        == before
        == 0
    )
    assert trainability(store)["by_source"]["binance"]["trainable_current_schema"] == 1
    row = fs.dataset(now_ms(), source="binance", schema_version=SCHEMA_VERSION)[0]
    row["label"]["certificate"]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="certificate"):
        read_dataset(fs.write_dataset([row]))
    store.close()


async def test_unfilled_setup_is_not_a_recovered_loss(settings, signal):
    store, fs, ident, _, _, _ = candidate(settings, signal)
    result = await recover_one(
        store, settings, ident, History([[trade(10, 900), trade(11, 62000, 110)]]), dict(requests=0), 1
    )
    assert result == "NO_FILL"
    assert not fs.dataset(now_ms(), source="binance", schema_version=SCHEMA_VERSION)
    store.close()
