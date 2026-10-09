"""Current minute OI and current-policy production expectancy regressions."""

import asyncio
from collections import deque

import httpx
import pytest

from bybit_flow import context_ev, current_oi, liquidation, v8_gating
from bybit_flow.exchanges import VenueAPI
from bybit_flow.ml import SCHEMA_VERSION
from bybit_flow.ml.features import snapshot
from bybit_flow.ml.store import FeatureStore, canonical
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Store

AT = 1_800_000_000_000


@pytest.mark.parametrize("venue,notional", [("binance", None), ("bybit", 12000), ("okx", 12000)])
async def test_current_oi_adapter_normalizes_base_units(settings, monkeypatch, venue, notional):
    monkeypatch.setattr("bybit_flow.exchanges.now_ms", lambda: AT)
    requests = []

    def handle(request):
        requests.append(request)
        if venue == "binance":
            assert request.url.path == "/fapi/v1/openInterest"
            body = dict(symbol="BTCUSDT", openInterest="120", time=AT - 100)
        elif venue == "bybit":
            assert request.url.path == "/v5/market/tickers"
            body = dict(
                retCode=0,
                time=AT - 100,
                result=dict(list=[dict(symbol="BTCUSDT", openInterest="120", openInterestValue="12000")]),
            )
        else:
            assert request.url.path == "/api/v5/public/open-interest"
            body = dict(
                code="0",
                data=[dict(instId="BTC-USDT-SWAP", oi="12000", oiCcy="120", oiUsd="12000", ts=AT - 100)],
            )
        return httpx.Response(200, json=body)

    api = VenueAPI(venue, settings, transport=httpx.MockTransport(handle))
    try:
        row = await api.current_oi("BTCUSDT")
        assert row == dict(
            source=venue,
            symbol="BTCUSDT",
            open_interest=120,
            open_interest_notional=notional,
            source_ms=AT - 100,
            available_ms=AT,
        )
        assert len(requests) == 1 and requests[0].method == "GET"
    finally:
        await api.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("openInterest", "nan"),
        ("openInterest", "0"),
        ("symbol", "ETHUSDT"),
        ("time", AT - 100_000),
        ("time", AT + 3000),
    ],
)
async def test_current_oi_rejects_invalid_observations(settings, monkeypatch, field, value):
    monkeypatch.setattr("bybit_flow.exchanges.now_ms", lambda: AT)
    body = dict(symbol="BTCUSDT", openInterest="120", time=AT)
    body[field] = value
    api = VenueAPI(
        "binance", settings, transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body))
    )
    try:
        with pytest.raises(ValueError):
            await api.current_oi("BTCUSDT")
    finally:
        await api.close()


async def test_current_oi_tolerated_skew_keeps_real_source_timestamp(settings, monkeypatch):
    clock = [AT]
    monkeypatch.setattr("bybit_flow.exchanges.now_ms", lambda: clock[0])

    async def wait(seconds):
        clock[0] += round(seconds * 1000)

    monkeypatch.setattr("bybit_flow.exchanges.asyncio.sleep", wait)
    api = VenueAPI(
        "binance",
        settings,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json=dict(symbol="BTCUSDT", openInterest="120", time=AT + 500))
        ),
    )
    try:
        row = await api.current_oi("BTCUSDT")
        assert row["source_ms"] == AT + 500
        assert row["available_ms"] == AT + 500
    finally:
        await api.close()


async def test_minute_oi_collection_is_cached_bounded_and_failure_is_missing(settings, monkeypatch):
    settings.market_source = "binance"
    settings.rest_concurrency = 2
    clock = [AT]
    monkeypatch.setattr(current_oi, "now_ms", lambda: clock[0])
    store = Store(settings.data_dir)
    scanner = Scanner(settings, store, None)
    scanner.streams.selected = [f"COIN{i}USDT" for i in range(20)]
    scanner.pending_symbols = lambda: ["ACTIVEUSDT", "PENDINGUSDT"]
    calls, inflight, peak = [], 0, 0
    failed = set()

    async def observe(symbol):
        nonlocal inflight, peak
        calls.append(symbol)
        inflight += 1
        peak = max(peak, inflight)
        try:
            await asyncio.sleep(0)
            if symbol in failed:
                raise httpx.ConnectError("Synthetic unavailable endpoint")
            return dict(
                source="binance",
                symbol=symbol,
                open_interest=100 + len(calls),
                open_interest_notional=None,
                source_ms=clock[0],
                available_ms=clock[0],
            )
        finally:
            inflight -= 1

    scanner.api.current_oi = observe
    try:
        await scanner.current_oi_collector.refresh()
        assert calls[:4] == ["BTCUSDT", "ETHUSDT", "ACTIVEUSDT", "PENDINGUSDT"]
        assert len(calls) == 10 and peak == 2
        assert len(scanner.oi_series) == 10
        await scanner.current_oi_collector.refresh()
        assert len(calls) == 10
        # Refresh late in the same exchange minute: replace safely, not append.
        clock[0] += 56_000
        await scanner.current_oi_collector.refresh()
        assert len(calls) == 20
        assert len(scanner.oi_series[("binance", "BTCUSDT")]) == 1
        for _ in range(35):
            clock[0] += 60_000
            await scanner.current_oi_collector.refresh()
        assert all(len(series) == 30 for series in scanner.oi_series.values())
        assert store.get("v8_current_oi")["five_minute_ready"] == 10
        previous = list(scanner.oi_series[("binance", "BTCUSDT")])
        failed.add("BTCUSDT")
        clock[0] += 60_000
        await scanner.current_oi_collector.refresh()
        assert list(scanner.oi_series[("binance", "BTCUSDT")]) == previous
        series = scanner.current_oi_collector.series("binance", "BTCUSDT")
        assert not series
        assert all(value is None for value in liquidation.oi_changes(series, clock[0]).values())
        assert store.get("v8_current_oi")["errors"] == {"BTCUSDT": "ConnectError"}
        await scanner.current_oi_collector.refresh()
        assert calls.count("BTCUSDT") == 38
    finally:
        scanner.streams.selected = ()
        await scanner.stop()
        store.close()


def test_binance_liquidation_uses_minute_oi_never_historical_substitute():
    series = deque(maxlen=30)
    for offset, value in [(-300_000, 120), (-60_000, 110), (0, 100)]:
        liquidation.sample_oi(series, AT + offset, AT + offset, value)
    event = dict(
        event_ms=AT - 20_000, receipt_ms=AT - 19_000, liquidated_side="LONG", notional=10000, price=100
    )
    kwargs = dict(
        price=98,
        flow=dict(delta_pct=-30, flow_trust_score=1),
        baseline=[100] * 20,
        derivatives=dict(oi_change_pct=-20),
    )
    row = liquidation.assess([event], AT, "binance", oi_series=series, **kwargs)
    assert row["oi_change_1m_pct"] == pytest.approx(-100 / 11)
    assert row["oi_change_5m_pct"] == pytest.approx(-100 / 6)
    assert row["oi_change_since_first_liquidation_pct"] == row["oi_change_1m_pct"]
    assert row["production_ready"]
    missing = liquidation.assess([event], AT, "binance", **kwargs)
    assert not missing["production_ready"]
    assert missing["oi_change_1m_pct"] is None


def add_outcome(
    store,
    signal,
    ident,
    decision_ms,
    *,
    schema=SCHEMA_VERSION,
    source="binance",
    policy=v8_gating.POLICY,
    label_available_ms=None,
    label_policy="prints-v1",
):
    candidate = signal.model_copy(
        deep=True,
        update=dict(id=ident, source=source, created_ms=decision_ms, horizon_profile="CORE_INTRADAY"),
    )
    candidate.evidence["confirmation_policy"] = policy
    row = snapshot(candidate, decision_ms, "decision", None)
    row["schema_version"] = schema
    snapshot_id = "snapshot:" + ident
    with store.db:
        store.db.execute(
            "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
            (snapshot_id, ident, "decision", decision_ms, schema, canonical(row)),
        )
    FeatureStore(store).label(
        snapshot_id,
        dict(policy=label_policy, complete=True, net_r=-1, exit_reason="stop", exit_ms=decision_ms + 1000),
        label_available_ms or decision_ms + 1000,
    )


def test_production_context_filters_population_before_bounded_limit(tmp_path, signal):
    store = Store(tmp_path)
    try:
        for i in range(30):
            add_outcome(store, signal, f"old{i}", AT - 300_000 + i, schema="candidate-v9")
        for i in range(5):
            add_outcome(store, signal, f"current{i}", AT - 200_000 + i)
        for i in range(30):
            add_outcome(store, signal, f"venue{i}", AT - 100_000 + i, source="bybit")
            add_outcome(store, signal, f"policy{i}", AT - 100_000 + i, policy="flow-quality-v1")
        add_outcome(store, signal, "future", AT - 20_000, label_available_ms=AT)
        add_outcome(store, signal, "late-ohlc", AT - 10_000, label_policy="ohlc-v1")
        research = context_ev.refresh(store, AT, source="binance")
        assert research["primary_outcomes"] == 65
        assert research["population"] == "historical-research"
        production = context_ev.refresh(store, AT, max_rows=5, source="binance", production=True)
        assert production["primary_outcomes"] == 5
        assert production["schema_version"] == signal.feature_schema_version
        assert production["confirmation_policy"] == v8_gating.POLICY
        assert production["status"] == "INSUFFICIENT"
        assert store.get("v8_context_ev:binance") == research
        assert store.get("v8_context_ev_production:binance") == production
        rows = FeatureStore(store).dataset(
            AT - 1, source="binance", schema_version=SCHEMA_VERSION, confirmation_policy=v8_gating.POLICY
        )
        assert {r["signal_id"] for r in rows} == {f"current{i}" for i in range(5)}
    finally:
        store.close()


@pytest.mark.parametrize("weeks,expected", [(6, False), (40, True)])
def test_current_policy_context_maturity_and_research_cannot_gate(
    tmp_path, signal, settings, weeks, expected
):
    store = Store(tmp_path)
    signal.source = "binance"
    signal.horizon_profile = "CORE_INTRADAY"
    signal.evidence["confirmation_policy"] = v8_gating.POLICY
    try:
        for week in range(weeks):
            for i in range(5):
                add_outcome(store, signal, f"week{week}:{i}", AT - (weeks - week) * 604_800_000 + i * 2000)
        cache = context_ev.refresh(store, AT - 1, source="binance", production=True)
        signal.evidence["context_ev"] = context_ev.assess(cache, signal, AT, production=True)
        row = signal.evidence["context_ev"]
        assert row["effective_samples"] == weeks * 5
        assert row["lower_confidence_bound_r"] == -1
        assert v8_gating.evaluate(signal, AT, settings)["blocked"] is expected
        broad = context_ev.refresh(store, AT - 1, source="binance")
        signal.evidence["context_ev"] = context_ev.assess(broad, signal, AT)
        assert not v8_gating.evaluate(signal, AT, settings)["blocked"]
        assert context_ev.assess(broad, signal, AT, production=True)["status"] == "INSUFFICIENT"
        for field, wrong in (
            ("schema_version", "candidate-v9"),
            ("confirmation_policy", "old"),
            ("source", "okx"),
        ):
            assert (
                context_ev.assess(cache | {field: wrong}, signal, AT, production=True)["status"]
                == "INSUFFICIENT"
            )
    finally:
        store.close()
