"""Research-only evidence-power measurements: causal, paired, bounded and never production."""

import json
import random
import sqlite3
from copy import deepcopy

import pytest
from test_setup_v22 import NOW, tagged
from test_v22_research import candle, registered, result

from bybit_flow import forward_evidence as forward
from bybit_flow import forward_power as power
from bybit_flow import forward_stats as stats
from bybit_flow.migrations import status
from bybit_flow.quality_v22 import apply
from bybit_flow.setup_research import advance, delayed_plan, entry_reached, trade_through_plan
from bybit_flow.storage import Store

FLOW_GATE = "executed order flow did not confirm family trigger"
SETTINGS = dict(bootstrap=400, seed=7, cluster_window_ms=3_600_000)


def job(store, ident):
    raw = store.db.execute("SELECT payload FROM setup_path_jobs WHERE signal_id=?", (ident,)).fetchone()
    return json.loads(raw[0]) if raw else None


def insert(store, ident, plan, cursor):
    with store.db:
        store.db.execute(
            "INSERT INTO setup_path_jobs VALUES(?,?,?,?,?,?,?)",
            (ident, plan["source"], plan["symbol"], 0, cursor, plan["deadline_ms"], json.dumps(plan)),
        )


def delivered(signal, settings, instrument, delivered_ms=NOW, bid=99.98, ask=100.02):
    """Decision plan of the standard fixture, re-expressed as a delivery-stage plan."""
    s, store = registered(signal, settings, instrument)
    plan = job(store, s.id)
    plan.update(stage="delivery", delivered_ms=delivered_ms, decision_ms=delivered_ms, visible_at_entry=True)
    return s, store, plan, dict(best_bid=bid, best_ask=ask)


# ------------------------------------------------------------- delay ladder


@pytest.mark.parametrize(
    "bar,expected",
    [
        (dict(low=97.5), "MISSED_INVALIDATED"),
        (dict(high=106.5), "MISSED_TARGET_PASSED"),
    ],
)
def test_crossing_before_delayed_entry_is_kept_as_zero_exposure(signal, settings, instrument, bar, expected):
    s, store, plan, quote = delivered(signal, settings, instrument)
    p = delayed_plan(plan, quote, 60000)
    insert(store, s.id + ":delay60", p, NOW)
    advance(store, s.id + ":delay60", [candle(NOW, **bar), candle(NOW + 60000)], NOW + 120000)
    label = result(store, s.id + ":delay60")
    assert label["state"] == expected and label["zero_exposure"] is True
    assert label["entered_ms"] is None
    assert all(r["net_r"] is None for r in label["policies"].values())
    assert power.per_opportunity(label, "execution_tp1") == 0.0
    store.close()


def test_expired_entry_window_is_missed_not_a_loss(signal, settings, instrument):
    s, store, plan, quote = delivered(signal, settings, instrument)
    plan["entry_deadline_ms"] = NOW + 60000
    insert(store, s.id + ":delay600", delayed_plan(plan, quote, 600000), NOW)
    advance(store, s.id + ":delay600", [candle(NOW), candle(NOW + 60000)], NOW + 120000)
    label = result(store, s.id + ":delay600")
    assert label["state"] == "MISSED_EXPIRED" and label["zero_exposure"]
    store.close()


def test_send_minute_crossing_is_unknown_not_missed(signal, settings, instrument):
    s, store, plan, quote = delivered(signal, settings, instrument, delivered_ms=NOW + 30000)
    insert(store, s.id + ":delay60", delayed_plan(plan, quote, 60000), NOW)
    advance(store, s.id + ":delay60", [candle(NOW, low=97.5), candle(NOW + 60000)], NOW + 120000)
    label = result(store, s.id + ":delay60")
    assert label["state"] == "UNKNOWN_PRE_ENTRY_ORDER" and not label["zero_exposure"]
    assert power.per_opportunity(label, "execution_tp1") is None
    store.close()


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_delayed_entry_pays_half_spread_against_the_trader(signal, settings, instrument, direction):
    s, store, plan, quote = delivered(signal, settings, instrument)
    plan["direction"] = direction
    if direction == "SHORT":
        plan["stop"] = 102
        for row in plan["policies"].values():
            row["target"] = 94
    p = delayed_plan(plan, quote, 60000)
    assert p["half_spread"] == pytest.approx(0.02)
    insert(store, s.id + ":delay60", p, NOW)
    entry_bar = candle(NOW + 60000, opening=100.5, high=100.6, low=100.4, close=100.5)
    advance(store, s.id + ":delay60", [candle(NOW), entry_bar], NOW + 120000)
    saved = job(store, s.id + ":delay60") or result(store, s.id + ":delay60")
    sign = 1 if direction == "LONG" else -1
    assert saved["entry"] == pytest.approx(100.5 + sign * 0.02)
    assert saved["entered_ms"] == NOW + 60000
    assert saved["risk"] == plan["risk"]  # Original stop distance stays the R denominator.
    assert saved["delayed_entry_risk"] == pytest.approx(abs(saved["entry"] - saved["stop"]))
    assert saved["cost_per_base"] == plan["cost_per_base"]
    store.close()


def test_unknown_delivery_spread_stays_unknown(signal, settings, instrument):
    s, store, plan, _ = delivered(signal, settings, instrument)
    insert(store, s.id + ":delay60", delayed_plan(plan, {}, 60000), NOW)
    advance(store, s.id + ":delay60", [candle(NOW), candle(NOW + 60000)], NOW + 120000)
    assert result(store, s.id + ":delay60")["state"] == "UNKNOWN_SPREAD"
    store.close()


def test_delay_ladder_enrolls_from_delivery_and_pairs_every_exit_variant(signal, settings, instrument):
    s, store, plan, quote = delivered(signal, settings, instrument)
    power.initialize(store, 0)
    s.evidence["delivery_execution_snapshot"] = quote
    s.state = "ALERTED"
    store.signal(s)
    insert(store, s.id + ":delivery", plan, NOW)
    counts = power.sweep(store, NOW + 1)
    assert counts["delay"] == 3
    assert power.sweep(store, NOW + 2)["delay"] == 0  # Each parent is decided once.
    stages = {r[0] for r in store.db.execute("SELECT path_id FROM evidence_power_paths WHERE kind='delay'")}
    assert stages == {s.id + ":delay60", s.id + ":delay180", s.id + ":delay600"}
    bars = [candle(NOW + i * 60000) for i in range(4)] + [
        candle(NOW + 240000, high=101.7, close=101.5),
        candle(NOW + 300000, opening=101.5, high=106.2, low=101.2, close=106),
    ]
    for ident in (s.id + ":delivery", *stages):
        advance(store, ident, bars, NOW + 360000)
    section = power.report_section(store, NOW + 360000)
    paired = section["delay_ladder"]["minus_delivery"]
    for stage in ("delay60", "delay180"):
        for variant in power.VARIANTS:
            row = paired[f"{stage}:{variant}:research_only:{s.source}"]
            assert row["raw_n"] == 1 and row["status"] == "INSUFFICIENT SAMPLE"
            assert row["p_value"] is None and row["comparison_id"]
    assert section["comparisons"]["total"] == len(section["comparisons"]["ids"]) > 0
    assert not store.db.execute("SELECT 1 FROM outbox WHERE signal_id LIKE '%:delay%'").fetchone()
    store.close()


def test_hourly_cap_marks_parent_not_enrolled_explicitly(signal, settings, instrument):
    s, store, plan, quote = delivered(signal, settings, instrument)
    power.initialize(store, 0)
    store.put(power.CONFIG, power.DEFAULTS | dict(max_per_hour=2))
    insert(store, s.id + ":delivery", plan, NOW)
    assert power.sweep(store, NOW + 1) == dict(delay=0, trade_through=0, not_enrolled=1)
    row = store.db.execute("SELECT payload FROM evidence_power_paths WHERE kind='not_enrolled'").fetchone()
    assert json.loads(row[0])["reason"] == "HOURLY_CAP"
    assert not job(store, s.id + ":delay60")
    store.close()


# ----------------------------------------------------- structure-only control


def flow_failed(signal, settings, instrument, ident="control-signal", event=None):
    s, b = tagged(signal, settings, instrument)
    s.id = ident
    if event is not None:
        s.evidence["frozen_structure"]["event_start_ms"] = event  # A distinct candidate identity.
    s.evidence["flow_confirmed"] = False
    s.gates = [FLOW_GATE]
    apply(s, 100.01, NOW, book=b, settings=settings)
    assert s.gates == [FLOW_GATE] and s.evidence["v22_counterfactual"]
    return s


def test_structure_only_control_enrolls_once_and_never_reaches_delivery_or_ml(signal, settings, instrument):
    store = Store(settings.data_dir)
    power.initialize(store, 0)
    labels_before = store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0]
    s = flow_failed(signal, settings, instrument)
    original = s.model_dump()
    store.signal(s)
    store.signal(s)
    rows = store.db.execute("SELECT payload FROM structure_controls").fetchall()
    assert len(rows) == 1
    c = json.loads(rows[0][0])
    assert c["policy"] == "structure_only_control" and c["flow_confirmed"] is False
    assert c["decision_ms"] == NOW and c["flow_gates"] == [FLOW_GATE]
    assert c["research_only"] and not c["production_ml"] and not c["production_authority"]
    assert job(store, s.id + ":control")["decision_ms"] == NOW
    assert s.model_dump() == original
    assert not store.db.execute("SELECT 1 FROM outbox").fetchone()
    assert not store.db.execute("SELECT 1 FROM ml_snapshots WHERE signal_id LIKE '%:control'").fetchone()
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == labels_before
    with pytest.raises(sqlite3.DatabaseError, match="immutable"):
        with store.db:
            store.db.execute("UPDATE structure_controls SET decision_ms=0")
    store.close()


def test_structure_control_requires_only_flow_failure(signal, settings, instrument):
    store = Store(settings.data_dir)
    power.initialize(store, 0)
    s = flow_failed(signal, settings, instrument)
    s.gates = [FLOW_GATE, "current spread exceeds gate"]
    store.signal(s)
    confirmed = flow_failed(signal, settings, instrument, ident="confirmed")
    confirmed.evidence["flow_confirmed"] = True
    store.signal(confirmed)
    assert not store.db.execute("SELECT 1 FROM structure_controls").fetchone()
    store.close()


def test_structure_control_respects_hourly_cap(signal, settings, instrument):
    store = Store(settings.data_dir)
    power.initialize(store, 0)
    store.put(power.CONFIG, power.DEFAULTS | dict(control_max_per_hour=2))
    store.signal(flow_failed(signal, settings, instrument, "first", event=1))
    store.signal(flow_failed(signal, settings, instrument, "same-identity", event=1))
    assert store.db.execute("SELECT count(*) FROM structure_controls").fetchone()[0] == 1
    assert "evidence_power_control_deferred" not in store.db.health.snapshot()
    for event, ident in enumerate(("second", "third", "fourth"), start=2):
        store.signal(flow_failed(signal, settings, instrument, ident, event=event))
    assert store.db.execute("SELECT count(*) FROM structure_controls").fetchone()[0] == 2
    assert store.db.health.snapshot()["evidence_power_control_deferred"] == 2
    store.close()


def test_shadow_failure_cannot_prevent_persistence(signal, settings, instrument, monkeypatch):
    store = Store(settings.data_dir)
    power.initialize(store, 0)
    monkeypatch.setattr(power, "on_save", lambda *a, **k: (_ for _ in ()).throw(ValueError("shadow")))
    store.signal(signal)
    assert store.signals()[0]["id"] == signal.id
    assert store.db.health.snapshot()["evidence_power_errors"] == 1
    store.close()


# --------------------------------------------------------------- statistics


def observations(n=60, days=6, seed=1):
    rng = random.Random(seed)
    return [
        dict(
            t=(i % days) * stats.DAY + i * 7_200_000 % stats.DAY,
            key=("bybit", "LONG", i % 3),
            value=rng.gauss(0.1, 1),
        )
        for i in range(n)
    ]


def test_day_resampling_is_deterministic_for_a_fixed_seed():
    data = observations()
    first, second = stats.summarize(data, SETTINGS, 30), stats.summarize(data, SETTINGS, 30)
    assert first == second and first["ci95_cluster_means"] is not None
    other = stats.summarize(data, SETTINGS | dict(seed=8), 30)
    assert other["ci95_cluster_means"] != first["ci95_cluster_means"]
    assert stats.bootstrap_mean(data, 50, 3) == stats.bootstrap_mean(data, 50, 3)


def test_single_day_cannot_produce_a_day_resampled_interval():
    data = [dict(t=1000 + i, key="k", value=float(i)) for i in range(40)]
    assert stats.summarize(data, SETTINGS, 30)["ci95_day_resampled"] is None


def test_clustering_merges_correlated_alerts_only_within_key_and_window():
    window = 3_600_000
    data = [
        dict(t=0, key=("bybit", "LONG", "majors"), value=1.0),
        dict(t=600_000, key=("bybit", "LONG", "majors"), value=-1.0),
        dict(t=3_000_000, key=("bybit", "LONG", "majors"), value=3.0),
        dict(t=3_700_000, key=("bybit", "LONG", "majors"), value=5.0),
        dict(t=600_000, key=("bybit", "SHORT", "majors"), value=7.0),
    ]
    groups = stats.clusters(data, window)
    assert len(groups) == 3
    first = next(g for g in groups if g["t"] == 0)
    assert first["size"] == 3 and first["value"] == 1.0
    assert stats.summarize(data, SETTINGS, 1)["cluster_n"] == 3


def test_holm_adjustment_matches_known_example():
    assert stats.holm([0.01, 0.04, 0.03, 0.005]) == pytest.approx([0.03, 0.06, 0.06, 0.02])
    assert stats.holm([0.2, None]) == pytest.approx([0.4, 1.0])


def test_minimum_detectable_difference_and_spearman():
    assert stats.minimum_detectable(1.0, 100) == pytest.approx(0.2802, abs=1e-3)
    assert stats.minimum_detectable(1.0, 1) is None
    assert stats.spearman([(1, 2), (2, 4), (3, 9), (4, 16)]) == pytest.approx(1)
    assert stats.spearman([(1, 3), (2, 2), (3, 1)]) == pytest.approx(-1)
    assert stats.ranks([5, 1, 5, 3]) == [3.5, 1, 3.5, 2]


def test_conservative_bound_is_never_above_optimistic():
    rng = random.Random(4)
    for _ in range(300):
        sign = rng.choice([1, -1])
        entry = 100
        label = dict(
            entered_ms=1,
            coverage_complete=True,
            direction="LONG" if sign > 0 else "SHORT",
            entry=entry,
            stop=entry - sign * rng.uniform(0.5, 3),
            risk=2.0,
            cost_per_base=rng.uniform(0, 0.2),
            policies=dict(
                execution_tp1=dict(
                    target=entry + sign * rng.uniform(-1, 6),
                    ambiguous=True,
                    complete=False,
                    partial_fraction=rng.choice([0, 0.5]),
                    partial_done=rng.random() < 0.5,
                    gross_r=rng.choice([None, rng.uniform(-2, 3)]),
                )
            ),
        )
        low, high = power.bounds(label, "execution_tp1")
        assert low <= high


def test_ambiguous_ordering_bounds_are_reported_side_by_side(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    advance(store, s.id, [candle(NOW), candle(NOW + 60000, high=107, low=97)], NOW + 120000)
    label = result(store, s.id)
    low, high = power.bounds(label, "r100")
    cost = label["cost_per_base"] / label["risk"]
    assert low == pytest.approx(-1 - cost) and high == pytest.approx(1 - cost)
    store.close()


def test_cost_stress_uses_stored_gross_only(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    bars = [candle(NOW), candle(NOW + 60000, high=101.7, close=101.5)]
    advance(store, s.id, bars + [candle(NOW + 120000, opening=101.5, high=106.2, low=101.2)], NOW + 180000)
    label = job(store, s.id) or result(store, s.id)
    row = label["policies"]["r050"]
    assert row["gross_r"] == pytest.approx(0.5)
    assert row["net_r"] == pytest.approx(row["gross_r"] - label["cost_per_base"] / label["risk"])
    store.close()


# --------------------------------------------------------- preregistration


def prereg_record(start=NOW):
    return dict(
        hypothesis="Delayed entry at 180 seconds loses at least 0.1R versus delivery",
        metric="per-opportunity net R",
        comparison="delay180 minus delivery, execution_tp1",
        acceptance_threshold=-0.1,
        evaluation_window=dict(start_ms=start, end_ms=start + 30 * stats.DAY),
        attempted_variants=["delay60", "delay180", "delay600"],
    )


def test_preregistration_is_immutable_and_hashed(settings):
    store = Store(settings.data_dir)
    first = power.preregister(store, prereg_record(), 5000)
    again = power.preregister(store, prereg_record(), 9000)
    assert first == again and first["created_ms"] == 5000 and len(first["digest"]) == 64
    for statement in (
        "UPDATE policy_preregistrations SET created_ms=0",
        "DELETE FROM policy_preregistrations",
    ):
        with pytest.raises(sqlite3.DatabaseError, match="immutable"):
            with store.db:
                store.db.execute(statement)
    with pytest.raises(ValueError):
        power.preregister(store, dict(hypothesis="incomplete"), 5000)
    store.close()


def review(prereg_id=None, start=10_000):
    record = dict(
        policy_id="v22_delay_review",
        parent_policy="v22_actual",
        development_period="none",
        forward_start=start,
        evaluation_end=20_000,
        sample_counts={},
        net_r={},
        mfe={},
        mae={},
        loss_avoidance=0,
        winner_rejection=0,
        subgroup_stability={},
        attempted_variants=["delay180"],
        code_commit="test",
    )
    return record | (dict(prereg_id=prereg_id) if prereg_id else {})


def test_reviews_without_valid_preregistration_are_flagged(settings):
    store = Store(settings.data_dir)
    ident = power.preregister(store, prereg_record(), 5000)["prereg_id"]
    assert power.preregistration_status(store, review())["status"] == "NOT_PREREGISTERED"
    early = power.preregistration_status(store, review(ident, start=4000))
    assert early["status"] == "NOT_PREREGISTERED"
    assert early["reason"] == "REVIEW_WINDOW_STARTED_BEFORE_PREREGISTRATION"
    valid = power.preregistration_status(store, review(ident))
    assert valid["status"] == "PREREGISTERED" and valid["production_authority"] is False
    saved = forward.record_promotion(store, review(), 30_000)
    payload = json.loads(
        store.db.execute("SELECT payload FROM policy_promotions WHERE id=?", (saved,)).fetchone()[0]
    )
    assert payload["preregistration"]["status"] == "NOT_PREREGISTERED"
    assert payload["production_authority"] is False
    store.close()


# ------------------------------------------------------- fill bound (task 4)


def test_trade_through_requires_more_than_one_tick_beyond_entry(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    plan = job(store, s.id)
    insert(store, s.id + ":tt", trade_through_plan(plan, 0.01), plan["decision_ms"] // 60000 * 60000)
    # The entry window spans two bars. Exactly one tick beyond is not a trade-through.
    one_tick = candle(NOW, opening=100.2, high=100.3, low=99.99, close=100.2)
    through = candle(NOW + 60000, opening=100.2, high=100.3, low=99.98, close=100.2)
    bars = [one_tick, through, candle(NOW + 120000)]
    advance(store, s.id, bars, NOW + 180000)
    advance(store, s.id + ":tt", bars, NOW + 180000)
    touch = job(store, s.id) or result(store, s.id)
    conservative = job(store, s.id + ":tt") or result(store, s.id + ":tt")
    assert touch["entered_ms"] == NOW + 60000
    assert conservative["entered_ms"] == NOW + 120000
    assert conservative["entry"] == touch["entry"] == plan["entry"]
    # A bar that only touches never fills the conservative bound.
    exact = trade_through_plan(plan, 0.01)
    assert not entry_reached(exact, candle(NOW, low=100.0), 1)
    assert entry_reached(exact, candle(NOW, low=99.98), 1)
    store.close()


def test_trade_through_enrolls_once_per_forward_opportunity(signal, settings, instrument):
    from test_reliability_evidence import enrolled

    store = Store(settings.data_dir)
    power.initialize(store, 0)
    store.close()
    s, store = enrolled(signal, settings, instrument)
    assert power.sweep(store, NOW + 1)["trade_through"] == 1
    assert power.sweep(store, NOW + 2)["trade_through"] == 0
    p = job(store, s.id + ":forward:tt")
    assert p["entry_rule"] == "trade_through" and p["tick"] == 0.01 and p["entered_ms"] is None
    o = json.loads(store.db.execute("SELECT payload FROM policy_opportunities").fetchone()[0])
    assert o["structural_planned_entry"] == s.entry and o["flow_confirmed"] is True
    assert o["structural_decision_ms"] == 60000
    assert o["stop_exit_depth"]["exit_side"] == "bid"
    section = forward.report(store, NOW + 3)["evidence_power"]
    assert "stop_exit_slippage" in section["fill_bound"]
    assert section["fill_bound"]["stop_exit_depth_diagnostic"]["status"] in {"RESEARCH_DIAGNOSTIC", "UNKNOWN"}
    store.close()


# ------------------------------------------------------- attempt numbering


def attempt_signal(signal, settings, instrument, ident, created, reference=99):
    s, _ = tagged(signal, settings, instrument)
    s.id, s.created_ms = ident, created
    s.evidence["frozen_structure"].update(frozen_reference=reference, trend_qualified_ms=15_000)
    return s


def test_attempt_ordinal_is_causal_and_frozen(signal, settings, instrument):
    store = Store(settings.data_dir)
    power.initialize(store, 0)
    store.put(power.CONFIG, power.DEFAULTS | dict(attempt_window_ms=10_000))
    first = attempt_signal(signal, settings, instrument, "a1", 20_000)
    second = attempt_signal(signal, settings, instrument, "a2", 21_000)
    elsewhere = attempt_signal(signal, settings, instrument, "b1", 21_500, reference=101)
    store.signal(first)
    with store.db:
        # Resolved after the second decision: must not be visible to it.
        store.db.execute(
            "INSERT INTO setup_path_labels VALUES('a1',21500,?)",
            (json.dumps(dict(state="OBSERVED", policies=dict(execution_tp1=dict(outcome="STOP")))),),
        )
    store.signal(second)
    store.signal(elsewhere)
    a1, a2, b1 = (power.attempt(store, i) for i in ("a1", "a2", "b1"))
    assert (a1["attempt_ordinal"], a2["attempt_ordinal"], b1["attempt_ordinal"]) == (1, 2, 1)
    assert a2["attempt_ordinal_status"] == "EXACT" and a2["prior_attempt_ids"] == ["a1"]
    assert a2["prior_attempt_outcomes_known"] == []
    assert (a1["pullback_ordinal"], a2["pullback_ordinal"], b1["pullback_ordinal"]) == (1, 2, 3)
    frozen = json.dumps(a1, sort_keys=True)
    third = attempt_signal(signal, settings, instrument, "a3", 22_000)
    store.signal(third)
    assert json.dumps(power.attempt(store, "a1"), sort_keys=True) == frozen
    a3 = power.attempt(store, "a3")
    assert a3["attempt_ordinal"] == 3
    assert [r["signal_id"] for r in a3["prior_attempt_outcomes_known"]] == ["a1"]
    with pytest.raises(sqlite3.DatabaseError, match="immutable"):
        with store.db:
            store.db.execute("UPDATE candidate_attempts SET decision_ms=0")
    assert "attempt_ordinal" not in first.evidence and "attempt_ordinal" not in json.dumps(
        store.signals()[0]["evidence"]
    )
    store.close()


def test_deferred_attempt_history_makes_later_ordinals_lower_bounds(signal, settings, instrument):
    from bybit_flow.storage import now_ms

    store = Store(settings.data_dir)
    power.initialize(store, 0)
    store.put(power.CONFIG, power.DEFAULTS | dict(attempt_window_ms=10_000))
    store.put("operational_health", dict(at_ms=now_ms(), defer_from=6))
    store.signal(attempt_signal(signal, settings, instrument, "a1", 20_000))
    store.put("operational_health", dict(at_ms=now_ms(), defer_from=9))
    store.signal(attempt_signal(signal, settings, instrument, "a2", 21_000))
    assert power.attempt(store, "a1") is None
    a2 = power.attempt(store, "a2")
    assert a2["attempt_ordinal"] == 1 and a2["attempt_ordinal_status"] == "LOWER_BOUND"
    store.close()


def test_first_attempt_only_variant(signal, settings, instrument):
    s, b = tagged(signal, settings, instrument)
    apply(s, 100.01, NOW, book=b, settings=settings)
    actual = forward.variants(s, NOW)["v22_actual"]
    first = forward.variants(s, NOW, dict(attempt_ordinal=1, attempt_ordinal_status="EXACT"))
    assert first["first_attempt_only"]["accepted"] == actual["accepted"]
    repeat = forward.variants(s, NOW, dict(attempt_ordinal=2, attempt_ordinal_status="LOWER_BOUND"))
    assert "NOT_FIRST_ATTEMPT" in repeat["first_attempt_only"]["gates"]
    assert repeat["first_attempt_only"]["accepted"] is False
    unknown = forward.variants(s, NOW, dict(attempt_ordinal=1, attempt_ordinal_status="LOWER_BOUND"))
    assert unknown["first_attempt_only"]["status"] == "NOT_COMPARABLE"
    assert unknown["first_attempt_only"]["accepted"] is None
    assert set(first) == set(forward.POLICIES)


# --------------------------------------------------------------- migration


def test_fresh_database_has_append_only_evidence_power_schema(settings):
    store = Store(settings.data_dir)
    receipts = status(store)
    assert receipts["level"] == "012"
    for name in power.IMMUTABLE:
        assert store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        for action in ("update", "delete"):
            trigger = f"{name}_no_{action}"
            assert store.db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (trigger,)).fetchone()
    store.close()
    store = Store(settings.data_dir)
    assert status(store) == receipts
    store.close()


def test_research_tables_do_not_change_existing_ledgers(signal, settings, instrument):
    s, store = registered(signal, settings, instrument)
    before = {
        table: [tuple(r) for r in store.db.execute(f"SELECT * FROM {table} ORDER BY 1")]
        for table in ("signals", "setup_path_jobs", "ml_snapshots", "outbox")
    }
    power.initialize(store, 0)
    power.sweep(store, NOW)
    power.report_section(store, NOW)
    after = {
        table: [tuple(r) for r in store.db.execute(f"SELECT * FROM {table} ORDER BY 1")] for table in before
    }
    assert after == before
    store.close()


# --------------------------------------------------- production byte identity


def test_production_outputs_are_byte_identical_to_pre_release_golden(tmp_path):
    from production_golden import GOLDEN, production_view

    # Golden rendered by unmodified 2.2.2 (1e45199) with the same helper.
    assert production_view(tmp_path) == GOLDEN.read_text(encoding="utf-8").strip()


def test_evidence_power_hooks_do_not_change_persisted_production_signal(signal, settings, instrument):
    def persisted(enable):
        store = Store(settings.data_dir / ("on" if enable else "off"))
        if enable:
            power.initialize(store, 0)
        s = flow_failed(signal, settings, instrument)
        s.created_ms = 1000
        store.signal(s)
        s2 = deepcopy(s)
        s2.evidence["flow_confirmed"] = True
        s2.gates = []
        store.signal(s2)
        payload = store.db.execute("SELECT payload FROM signals WHERE id=?", (s.id,)).fetchone()[0]
        store.close()
        return payload

    assert persisted(True) == persisted(False)
