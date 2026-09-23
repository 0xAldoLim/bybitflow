import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_ml_training import dataset

from bybit_flow.cross_venue import CrossVenue, compare
from bybit_flow.diagnostics import doctor
from bybit_flow.exchanges import probe_freshness
from bybit_flow.lifecycle import tick
from bybit_flow.ml.cli import cycle
from bybit_flow.ml.inference import select_compatible
from bybit_flow.ml.registry import Registry
from bybit_flow.ml.store import canonical, digest
from bybit_flow.models import Trade
from bybit_flow.native_streams import NativeStreams
from bybit_flow.orderflow import Book, Tape
from bybit_flow.storage import Store, now_ms
from bybit_flow.thesis_health import evaluate


async def test_original_bybit_lifecycle_with_binance_primary(settings, signal):
    store = Store(settings.data_dir)
    signal.source, signal.state = "bybit", "ALERTED"
    store.signal(signal)
    tape = Tape()
    tape.reset(1000)
    tape.add(Trade(signal.symbol, 2000, 2000, "own-venue", "Sell", Decimal(94), Decimal(1)))
    primary = SimpleNamespace(selected=(), tapes={}, books={}, select=AsyncMock())
    original = SimpleNamespace(
        selected=(signal.symbol,), tapes={signal.symbol: tape}, books={}, select=AsyncMock()
    )
    scanner = SimpleNamespace(
        store=store,
        settings=settings,
        exchange="binance",
        api=Mock(),
        streams=primary,
        recorder=Mock(),
        reconcile_pending=set(),
    )
    scanner.cross_venue = CrossVenue(scanner)
    scanner.cross_venue.peers["bybit"] = (Mock(), original)
    await tick(scanner, 2000)
    assert signal.symbol in original.required_symbols
    result = store.db.execute("SELECT state FROM signals WHERE id=?", (signal.id,)).fetchone()[0]
    assert result == "INVALIDATED"
    assert store.get("active_lifecycle")["feed_stale"] == 1
    store.close()


async def test_pending_pin_released_only_after_terminal(settings, signal):
    store = Store(settings.data_dir)
    settings.deep_symbols = 1
    signal.source = "binance"
    store.signal(signal)
    streams = NativeStreams(settings, store, Mock(), SimpleNamespace(name="binance"))

    async def idle(symbol):
        await asyncio.Event().wait()

    streams.run_symbol = idle
    manager = CrossVenue(SimpleNamespace(store=store))
    streams.required_symbols = manager.required("binance")
    await streams.select(["OTHER"])
    assert streams.selected == (signal.symbol,)
    await streams.select(["OTHER"])
    assert streams.selected == (signal.symbol,)
    signal.state = "EXPIRED"
    store.signal(signal)
    streams.required_symbols = manager.required("binance")
    await streams.select(["OTHER"])
    assert signal.symbol not in streams.selected
    await streams.stop()
    store.close()


def test_two_stage_immaturity_trains_real_baseline(settings, signal, monkeypatch):
    pytest.importorskip("lightgbm")
    store = Store(settings.data_dir)
    settings.ml_two_stage = True
    store.db.execute("INSERT INTO segments VALUES('test',0,'{}')")
    store.db.commit()
    store.put("active_exchange", {"current": "okx"})
    store.put("runtime_health", dict(at_ms=now_ms(), source=signal.source))
    store.put("ml_monitor", {"last_success_ms": now_ms()})
    path = settings.data_dir / "software-test.parquet"
    pq.write_table(pa.Table.from_pylist([{"payload": canonical(row)} for row in dataset(signal)]), path)
    monkeypatch.setattr("bybit_flow.ml.cli.FeatureStore.export", lambda *args, **kwargs: path)
    ident = cycle(settings, store)
    assert Registry(store).get(ident)["model"]["kind"] in {"logistic", "lightgbm"}
    assert store.get("ml_training_mode")["kinds"] == ["logistic", "lightgbm"]
    assert store.get("ml_training_mode")["sequences_ready"] == 0
    assert store.get("ml_training_mode")["source"] == signal.source
    store.close()


def test_newest_incompatible_skipped_for_compatible_challenger(settings, signal):
    store = Store(settings.data_dir)
    model = dict(
        id="compatible",
        created_ms=100,
        feature_schema_version=signal.feature_schema_version,
        source=signal.source,
        stage="decision",
        strategy_versions=[signal.version],
        periods={"holdout": {"end": 100}},
        contexts={},
        required_features=[],
    )
    for item in (model, model | dict(id="wrong-source", created_ms=200, source="okx")):
        store.db.execute(
            "INSERT INTO ml_models VALUES(?,?,?,?)",
            (item["id"], item["created_ms"], canonical(item), digest(item)),
        )
    store.db.commit()
    selected, champion, skipped = select_compatible(
        Registry(store), signal, {"values": {}, "data_coverage": 1}, 1000
    )
    assert selected["id"] == "compatible" and not champion
    assert skipped[0]["model_id"] == "wrong-source"
    store.close()


def test_secondary_requires_persistent_primary_failure(signal):
    signal.horizon_profile = "SHORT_INTRADAY"
    observation = dict(
        coverage_complete=True,
        cross_venue_health="DEGRADED",
        derivatives_health="DEGRADED",
        window_end_ms=200000,
        flow={},
    )
    previous = dict(checked_ms=180000, adverse_since_ms=1, observation={"window_end_ms": 180000})
    assert not evaluate(signal, observation, previous, 200000)["withdraw"]
    observation.update(structural_failure=True, flow=dict(delta_pct=-50, cvd_slope=-1, delta_persistence=1))
    assert evaluate(signal, observation, previous, 200000)["withdraw"]
    base = dict(
        event_ms=190000,
        mid=100,
        spread_bps=1,
        window_start=100000,
        window_end=180000,
        flow=dict(available=True, delta_pct=-30),
    )
    rows = [base | {"exchange": source} for source in ("bybit", "binance")]
    assert compare(rows, 200000)["consensus_delta_sign"] == "NEGATIVE"
    rows[1] = rows[1] | {"flow": dict(available=True, delta_pct=30)}
    assert compare(rows, 200000)["consensus_delta_sign"] == "MIXED"
    assert compare(rows, 200000)["predictive_weight"] == 0


async def test_failed_probe_cannot_override_live_persistent_feed(settings, monkeypatch):
    store = Store(settings.data_dir)
    store.put(
        "runtime_health",
        dict(
            at_ms=now_ms(),
            source_ready=True,
            source_feed_available=True,
            persistent_connected=True,
            streams_fresh=False,
            selected_streams_total=2,
            selected_streams_fresh=1,
        ),
    )

    async def failed(name, settings):
        return dict(exchange=name, status="UNAVAILABLE")

    monkeypatch.setattr("bybit_flow.diagnostics.market_probe", failed)
    result = await doctor(settings, store)
    assert result["checks"]["market_stream"]["connected"]
    assert result["checks"]["runtime"]["status"] == "DEGRADED"
    assert "Public connectivity probe degraded; persistent feed remains active." in result["warnings"]
    assert not any("no trade" in warning for warning in result["warnings"])
    store.close()


@pytest.mark.parametrize(
    "event_age,receipt_age,skew,expected",
    [
        (-1500, 0, 0, True),
        (-2000, 1, -1900, True),
        (-9000, 0, -9000, False),
        (20000, 0, 0, False),
        (0, 20000, 0, False),
    ],
)
def test_probe_accepts_bounded_skew_not_stale_transport(event_age, receipt_age, skew, expected):
    trade = SimpleNamespace(event_ms=100000 - event_age, receipt_ms=100000 - receipt_age)
    assert probe_freshness(trade, 100000, 10000, skew) is expected
    book = Book()
    book.valid = True
    book.event_ms, book.receipt_ms = trade.event_ms, trade.receipt_ms
    assert book.fresh(100000, 10000) is expected
