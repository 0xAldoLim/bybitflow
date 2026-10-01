"""V8.1 A–T readiness, causality, continuity and publication invariants."""

from collections import deque
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from conftest import synthetic_bars

from bybit_flow import anchored, breadth, context_ev, liquidation, ofi, opportunity, spot_perp, v8_gating
from bybit_flow.ml import SCHEMA_VERSION
from bybit_flow.ml.features import CATALOG, snapshot
from bybit_flow.ml.store import FeatureStore
from bybit_flow.notifications import Notifier
from bybit_flow.orderflow import Book
from bybit_flow.storage import Store

AT = 10_000_000


def observed(**fields):

    return dict(source="bybit", source_ms=AT - 1000, available_ms=AT, **fields)


def candidate(signal):

    signal.horizon_profile = "CORE_INTRADAY"
    signal.state = "PENDING CONFIRMATION"

    signal.evidence["confirmation_policy"] = v8_gating.POLICY

    return signal


def gate(signal, settings, **kw):

    return v8_gating.evaluate(candidate(signal), AT, settings, **kw)


@pytest.mark.asyncio
@pytest.mark.parametrize("exhausted", [False, True])
async def test_a_b_spot_closed_prices_survive_bounded_trade_window(exhausted):

    calls = []

    end = 600_000

    def serve(request):

        calls.append(request)

        if request.url.path.endswith("klines"):
            bars = [[i * 60_000, 0, 0, 0, str(100 + i), 1, i * 60_000 + 59_999] for i in range(3, 11)]

            # The final bar closes in the future and must never enter a return.

            return httpx.Response(200, json=bars)

        assert request.url.path.endswith("aggTrades")

        assert int(request.url.params["endTime"]) == end

        page = sum(r.url.path.endswith("aggTrades") for r in calls)

        if exhausted and page == 2:
            return httpx.Response(200, json=[dict(a=1000, T=end - 1000, q="2", m=True)])

        return httpx.Response(
            200,
            json=[
                dict(a=(page - 1) * 1000 + i, T=end - 4000 + page * 1000, q="1", m=False) for i in range(1000)
            ],
        )

    client = spot_perp.Collector(transport=httpx.MockTransport(serve), clock=lambda: end)

    try:
        result = await client.collect("BTCUSDT", end)

        assert result["price"] == 109

        assert result["return_1m"] == pytest.approx(109 / 108 - 1)

        assert result["price_coverage_complete"]

        assert result["trade_window_complete"] is exhausted

        assert result["delta"] == (998 if exhausted else None)

        assert len(calls) <= 4

        assert calls[1].url.params["startTime"] == str(end - 60_000)

    finally:
        await client.close()


def book_seconds(seconds, *, events_per_second=1):

    book = Book()

    book.apply(
        dict(
            type="snapshot",
            ts=AT - seconds * 1000,
            data=dict(u=1, seq=1, b=[["100", "100"]], a=[["100.02", "100"]]),
        ),
        AT - seconds * 1000,
    )

    index = 2

    for sec in range(seconds):
        for event in range(events_per_second):
            ts = AT - (seconds - sec - 1) * 1000 + event // max(1, events_per_second)

            quantity = 100 - (sec + 1) / 2

            book.apply(
                dict(
                    type="delta",
                    ts=ts,
                    data=dict(
                        u=index, seq=index, b=[["100", str(quantity)]], a=[["100.02", str(200 - quantity)]]
                    ),
                ),
                ts,
            )

            index += 1

    return book


def test_c_d_e_ofi_burst_readiness_and_epoch_reset(signal, settings):

    short = ofi.assess(book_seconds(3, events_per_second=1000), AT, "bybit")

    assert short["ofi_coverage_seconds"] == 3 and not short["production_coverage_ready"]

    book = book_seconds(50)

    priors = [dict(ofi_normalized_l1=-0.001, ofi_normalized_5bps=-0.001, ofi_persistence=0.5)] * 20

    value = ofi.assess(book, AT, "bybit", baseline=priors)

    signal.evidence["ofi"] = value

    result = gate(signal, settings)

    assert "STRONG_ADVERSE_BOOK_ORDER_FLOW" in result["blocking_reasons"]

    old_epoch = book.ofi_epoch

    book.reset()

    assert book.ofi_epoch > old_epoch and not book.ofi_buckets

    assert not ofi.assess(book, AT, "bybit", baseline=priors)["production_coverage_ready"]


def breadth_rows(n, regimes="range"):

    bars = synthetic_bars(65, start=AT - 65 * 3_600_000)
    bars[-1] = replace(bars[-1], close=bars[-2].close + 0.1)

    return [
        dict(
            symbol=f"ALT{i}USDT",
            source="bybit",
            eligible=True,
            h1=bars,
            h4=bars,
            h1_features={"regime": regimes},
            h4_features={"regime": regimes},
        )
        for i in range(n)
    ]


def test_f_g_h_distinct_return_trend_and_history():

    rows = breadth_rows(20)

    at = rows[0]["h1"][-1].end

    history = [
        dict(source="bybit", available_ms=at - 3_700_000, pct_uptrend_1h=0.4),
        dict(source="bybit", available_ms=at - 300_000, pct_uptrend_1h=0.8),
    ]

    result = breadth.assess(rows, at, "bybit", history)

    assert result["pct_positive_return_1h"] == 1

    assert result["pct_uptrend_1h"] == 0 and result["state"] == "MIXED"

    assert result["breadth_change_1h"] == pytest.approx(-0.4)

    assert result["production_ready"]

    assert not breadth.assess(rows[:19], at, "bybit")["production_ready"]

    assert len(breadth.retain_history(history * 100, result, at)) <= 96


def test_i_valid_idiosyncratic_override_is_not_blanket_veto(signal, settings):

    signal.evidence.update(
        breadth=observed(state="BROAD_RISK_OFF", denominator=25, production_ready=True),
        market_factor=observed(available=True, beta_to_btc=1, correlation_to_btc=0.8, beta_stability_btc=0.1),
    )

    assert gate(signal, settings)["blocked"]

    signal.evidence["market_alignment"] = dict(
        alignment="IDIOSYNCRATIC_DIVERGENCE", contrarian_override_passed=True
    )

    assert not gate(signal, settings)["blocked"]


def test_j_value_boundaries_are_independent_of_poc():

    bars = synthetic_bars(5)

    bars = [replace(bar, close=100) for bar in bars]

    result = anchored.value_ratios(bars, dict(val=99, vah=101, poc=99.5))

    assert result["time_inside_value_ratio"] == 1

    assert result["time_above_vah_ratio"] == result["time_below_val_ratio"] == 0

    assert result["time_above_poc_ratio"] == 1


def test_k_weekly_uses_full_h1_history():

    day = 86_400_000

    h1 = synthetic_bars(144, 3_600_000, 4 * day)

    at = h1[-1].end

    m15 = synthetic_bars(120, 900_000, at - 120 * 900_000)

    value = anchored.assess(m15, at, "bybit", h1=h1)

    assert value["anchors"]["weekly_open"]["bars"] == 144

    assert anchored.assess(m15, at, "bybit")["anchors"]["weekly_open"]["state"] == "UNAVAILABLE"


def test_l_event_window_oi_is_not_multi_hour_change():

    series = deque(maxlen=30)

    for index in range(8):
        ts = AT - (7 - index) * 60_000

        liquidation.sample_oi(series, ts, ts, 100 - index)

    result = liquidation.oi_changes(series, AT, AT - 60_000)

    assert result["oi_change_1m_pct"] == pytest.approx((93 / 94 - 1) * 100)

    assert result["oi_change_5m_pct"] == pytest.approx((93 / 98 - 1) * 100)

    assert liquidation.oi_changes([], AT)["oi_change_1m_pct"] is None

    event = dict(
        event_ms=AT - 10_000, receipt_ms=AT - 9000, liquidated_side="LONG", notional=10000, price=100
    )

    value = liquidation.assess(
        [event],
        AT,
        "bybit",
        derivatives={"oi_change_pct": -20},
        baseline=[100] * 20,
        flow={"flow_trust_score": 1},
        price=98,
    )

    assert not value["production_ready"] and value["oi_change_since_first_liquidation_pct"] is None


def test_m_source_ev_refresh_uses_separate_venue_limits(monkeypatch, tmp_path):

    seen = []

    def dataset(self, asof, **kw):

        seen.append((asof, kw))

        return []

    monkeypatch.setattr(FeatureStore, "dataset", dataset)

    store = Store(tmp_path)

    try:
        for source in ("bybit", "binance", "okx"):
            context_ev.refresh(store, AT, max_rows=30, source=source)

            assert store.get("v8_context_ev:" + source)["source"] == source

        assert [k["source"] for _, k in seen] == ["bybit", "binance", "okx"]

        assert all(t < AT and k["limit"] == 30 for t, k in seen)

    finally:
        store.close()


@pytest.mark.parametrize(
    "confidence,n,expected", [("EARLY", 30, False), ("MATURE", 199, False), ("MATURE", 200, True)]
)
def test_n_o_context_ev_requires_mature_causal_lower_bound(signal, settings, confidence, n, expected):

    signal.evidence["context_ev"] = observed(
        status="AVAILABLE",
        confidence=confidence,
        source_specific=True,
        production_compatible=True,
        schema_version=SCHEMA_VERSION,
        confirmation_policy=v8_gating.POLICY,
        effective_samples=n,
        max_label_available_ms=AT - 1,
        lower_confidence_bound_r=-0.2,
        cache_age_ms=1000,
    )

    assert gate(signal, settings)["blocked"] is expected

    signal.evidence["context_ev"]["cache_age_ms"] = 1_800_001

    assert not gate(signal, settings)["blocked"]


@pytest.mark.parametrize(
    "section,key",
    [("execution", "spread_cost_bps"), ("book", "spread_bps"), ("session_metrics", "spread_relative")],
)
def test_p_opportunity_uses_observed_liquidity(signal, section, key):

    signal.evidence[section] = {key: 2}

    assert opportunity.rank([signal], AT)[signal.id]["liquidity_quality"] == 80


def test_q_r_s_new_gate_shadow_v7_failure_and_active_plan(tmp_path, signal, settings):

    store = Store(tmp_path)

    scanner = SimpleNamespace(store=store, settings=settings, v8_disabled={})

    try:
        s = candidate(signal)

        s.evidence["score_components"] = {"structure": {"earned": 10}}

        s.evidence["spot_perp"] = observed(
            symbol=s.symbol,
            state="SPOT_PERP_CONFIRMED",
            price_coverage_complete=True,
            spot_return_1m=0.01,
            perp_return_1m=0.01,
        )

        s.gates = ["bad RR"]

        assert v8_gating.apply_new_candidate(scanner, s, AT) is None

        assert s.gates == ["bad RR"]

        s.gates = []

        s.evidence["volatility"] = observed(state="JUMP_SHOCK")

        plan = (s.entry, s.stop, s.tp1, s.tp2)

        assert v8_gating.apply_new_candidate(scanner, s, AT)["blocked"]

        store.signal(s)

        snap = next(FeatureStore(store).snapshots())

        assert snap["signal"]["evidence"]["v8_shadow"]["v7_valid"]

        assert snap["schema_version"] == SCHEMA_VERSION

        assert store.db.execute("SELECT blocked FROM v8_gate_decisions").fetchone()[0] == 1

        s.state = "ALERTED"

        old = s.model_dump()

        assert v8_gating.apply_new_candidate(scanner, s, AT + 1) is None

        assert s.model_dump() == old and (s.entry, s.stop, s.tp1, s.tp2) == plan

        assert v8_gating.effectiveness(store, AT + 10)["status"] == "INSUFFICIENT_EVIDENCE"

    finally:
        store.close()


def test_t_only_invalid_or_disabled_feature_fails_open(signal, settings):

    signal.evidence.update(volatility=observed(state="JUMP_SHOCK"), ofi=observed(ofi_coverage_seconds=-1))

    disabled = {}

    result = gate(signal, settings, disabled=disabled)

    assert disabled == {"ofi": "NEGATIVE_COVERAGE"}

    assert result["feature_states"]["ofi"]["state"] == "UNAVAILABLE" and result["blocked"]

    settings.v8_gate_volatility = False

    assert not gate(signal, settings, disabled=disabled)["blocked"]

    settings.v8_gate_volatility = True

    settings.v8_production_gating = False

    result = gate(signal, settings, disabled=disabled)

    assert result["hard_adverse"] and not result["blocked"]


def test_repaired_v10_catalog_never_contains_priority_or_old_misnomers():

    assert "time_above_value_ratio" not in CATALOG

    assert "oi_change_during_liquidation" not in CATALOG

    assert not any("priority" in key for key in CATALOG)

    assert "spot_perp_basis_change_bps" in CATALOG

    assert "breadth_pct_uptrend_1h" in CATALOG


def test_v9_snapshots_stay_immutable_and_out_of_v10_sequences(tmp_path, signal):

    store = Store(tmp_path)

    try:
        fs = FeatureStore(store)

        old = snapshot(signal, 2000, "decision")

        old["schema_version"] = "candidate-v9"

        from bybit_flow.ml.store import canonical

        store.db.execute(
            "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
            ("v9", signal.id, "decision", 2000, "candidate-v9", canonical(old)),
        )

        store.db.commit()

        assert fs.capture(signal, 2500, "decision") == "v9"

        signal.id = "new-v10"

        fs.capture(signal, 3000, "decision")

        new = next(r for r in fs.snapshots() if r["signal_id"] == "new-v10")

        assert len(new["sequence"]) == 1

        assert store.db.execute("SELECT payload FROM ml_snapshots WHERE id='v9'").fetchone()[0] == canonical(
            old
        )

    finally:
        store.close()


@pytest.mark.asyncio
async def test_redundant_unseen_setup_cannot_send_orphan_lifecycle(tmp_path, signal, settings):

    store = Store(tmp_path)

    try:
        signal.evidence["delivery_policy"] = "REDUNDANT_OPPORTUNITY"

        notifier = Notifier(settings, store)

        assert await notifier.send_research(signal) == "blocked:REDUNDANT_OPPORTUNITY"

        signal.coverage["monitoring_event"] = "paused"

        assert await notifier.send_research(signal, update=True) == "blocked:no-visible-initial"

    finally:
        store.close()


def test_spot_closed_windows_must_match(signal, settings):
    spot = dict(
        symbol=signal.symbol,
        source_ms=AT - 60_000,
        available_ms=AT,
        price=100,
        return_1m=0.01,
        price_coverage_complete=True,
    )
    perp = dict(
        symbol=signal.symbol,
        source="bybit",
        source_ms=AT - 1,
        available_ms=AT,
        price=101,
        return_1m=-0.01,
        price_coverage_complete=True,
    )
    signal.evidence["spot_perp"] = spot_perp.compare(spot, perp, AT)
    assert not signal.evidence["spot_perp"]["price_coverage_complete"]
    assert not gate(signal, settings)["blocked"]


def test_opportunity_suppression_requires_actual_risk_capacity(signal, settings):
    s = candidate(signal)
    s.evidence["opportunity_priority"] = observed(
        cluster_id="BTC_HIGH_BETA_LONG_CLUSTER", priority_score_0_100=60, related_ids=["active"]
    )
    active = [
        dict(
            id="active",
            source=s.source,
            direction="LONG",
            state="ALERTED",
            evidence=dict(
                opportunity_priority=dict(cluster_id="BTC_HIGH_BETA_LONG_CLUSTER", priority_score_0_100=75)
            ),
        )
    ]
    assert not gate(s, settings, active=active)["delivery_suppressed"]
    settings.equity = 10_000
    settings.risk_fraction = 0.0005
    portfolio = dict(at_ms=AT, positions=[dict(risk_fraction=0.007)])
    result = gate(s, settings, active=active, portfolio=portfolio)
    assert result["delivery_suppressed"] and not result["blocked"]
    assert s.state == "PENDING CONFIRMATION" and not s.gates
    portfolio["positions"] = [dict(risk_fraction=0.002)]
    assert not gate(s, settings, active=active, portfolio=portfolio)["delivery_suppressed"]


@pytest.mark.parametrize(
    "bad,reason",
    [
        (dict(source_ms=AT + 1), "FUTURE_TIMESTAMP"),
        (dict(source="binance"), "SOURCE_MISMATCH"),
        (dict(rv_ratio=float("nan")), "NON_FINITE"),
        (dict(epoch_contradiction=True), "CONTINUITY_EPOCH_CONTRADICTION"),
    ],
)
def test_feature_health_degrades_only_corrupt_family(signal, settings, bad, reason):
    signal.evidence["volatility"] = observed(state="JUMP_SHOCK") | bad
    disabled = {}
    assert not gate(signal, settings, disabled=disabled)["blocked"]
    assert disabled == {"volatility": reason}
    signal.evidence["volatility"] = observed(state="JUMP_SHOCK")
    assert not gate(signal, settings, disabled=disabled)["blocked"]


def test_ofi_uses_latest_causal_bucket_when_exchange_clock_is_ahead(signal, settings):
    book = book_seconds(50)
    # The latest received delta is within normal clock tolerance but not yet causal.
    book.apply(
        dict(type="delta", ts=AT + 700, data=dict(u=100, seq=100, b=[["100", "74"]], a=[["100.02", "126"]])),
        AT,
    )
    value = ofi.assess(
        book,
        AT,
        "bybit",
        baseline=[dict(ofi_normalized_l1=-0.001, ofi_normalized_5bps=-0.001, ofi_persistence=1)] * 20,
    )
    assert value["production_coverage_ready"]
    assert value["ofi_coverage_seconds"] == 49
    assert value["source_ms"] <= AT
    signal.evidence["ofi"] = value
    disabled = {}
    assert gate(signal, settings, disabled=disabled)["blocked"]
    assert not disabled
