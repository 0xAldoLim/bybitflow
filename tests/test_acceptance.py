import json
import random

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow import acceptance
from bybit_flow.acceptance import (
    DAY_MS,
    evaluate,
    first_touch,
    fit_grades,
    grade_from,
    outcome_features,
    outcome_grade,
    rule_evidence,
    summarize,
    verdict,
)
from bybit_flow.notifications import Notifier
from bybit_flow.storage import Store

COST = 15


def rows(n, win_rate, family="liquidity_sweep", horizon="SWING", seed=1, edge_feature=False):
    """Synthetic candidates: `win_rate` of them reach the 2R plan target, the rest stop."""
    rng = random.Random(seed)
    out = []
    for i in range(n):
        strength = rng.random()
        probability = min(0.95, win_rate + (0.6 * (strength - 0.5) if edge_feature else 0))
        win = rng.random() < probability
        features = dict.fromkeys(
            (
                "stop_atr",
                "planned_rr",
                "trend_alignment",
                "context_efficiency",
                "setup_efficiency",
                "context_slope",
                "setup_slope",
                "ma_gap",
                "realized_volatility",
                "long",
                "family_trend_pullback",
                "family_range_rejection",
                "family_breakout_retest",
            ),
            0.0,
        )
        features.update(stop_atr=1.0 + rng.random(), planned_rr=2.0, setup_efficiency=strength)
        result = dict(even=("UP", 1) if win else ("DN", -1), plan=("UP", 2) if win else ("DN", -1))
        mirror = dict(even=("DN", -1) if win else ("UP", 1), plan=("DN", -1) if win else ("UP", 2))
        out.append(
            dict(
                symbol="TESTUSDT",
                t=(i // 6) * DAY_MS + i * 1000,
                horizon=horizon,
                family=family,
                direction="LONG",
                stop_bps=150.0,
                features=features,
                as_published=result,
                mirrored=mirror,
            )
        )
    return out


def test_first_touch_counts_a_same_bar_tie_as_the_stop():
    bar = [0, 100, 102, 98, 100, 1, 1]
    assert first_touch([bar], 0, 100, 1, 1, 1, 2) == ("DN", -1)
    assert first_touch([[0, 100, 102, 99.5, 101, 1, 1]], 0, 100, 1, 1, 1, 2) == ("UP", 1)
    assert first_touch([[0, 100, 102, 99.5, 101, 1, 1]], 0, 100, -1, 1, 1, 2) == ("DN", -1)
    assert first_touch([[10, 101, 101.5, 100.5, 101, 1, 1]], 0, 100, 1, 3, 1, 2, limit=5) == ("TIME", 0.5)


def test_summary_charges_costs_and_clusters_by_day():
    summary = summarize(rows(600, 0.5), "as_published", COST)
    assert summary["n"] == 600 and summary["days"] == 100
    # Half win 2R, half lose 1R, minus 15 bps on a 150 bps stop.
    assert summary["plan_net_r"] == pytest.approx(0.5 * 2 - 0.5 - 0.1, abs=0.12)
    assert summary["plan_net_r_lower"] < summary["plan_net_r"]


def test_grade_bands_need_a_proven_lower_bound_for_the_top_grades():
    assert grade_from(0.6, 0.55) == "SSS"
    assert grade_from(0.6, 0.31) == "S"
    assert grade_from(0.6, 0.16) == "A"
    assert grade_from(0.6, 0.06) == "B"
    assert grade_from(0.6, -0.2) == "C"  # Looks good, not proven.
    assert grade_from(-0.1, -0.3) == "D"
    assert grade_from(-0.2, -0.3) == "E"
    assert grade_from(-0.4, -0.5) == "F"


def test_verdict_requires_test_sample_and_held_out_sample():
    good = dict(n=500, z=4.0, plan_net_r=0.3, plan_net_r_lower=0.1, hit_rate=0.6)
    assert verdict(good, dict(n=100, hit_rate=0.58, plan_net_r=0.2), 3.0) == "ACCEPTED"
    assert verdict(good, dict(n=100, hit_rate=0.48, plan_net_r=0.2), 3.0) == "REJECTED"
    assert verdict(good, dict(n=100, hit_rate=0.58, plan_net_r=-0.1), 3.0) == "REJECTED"
    assert verdict(good | dict(z=2.5), dict(n=100, hit_rate=0.58, plan_net_r=0.2), 3.0) == "REJECTED"
    assert verdict(good | dict(plan_net_r_lower=-0.01), dict(n=100, hit_rate=0.6, plan_net_r=0.2), 3.0) == (
        "REJECTED"
    )
    assert verdict(good | dict(n=150), dict(n=100, hit_rate=0.58, plan_net_r=0.2), 3.0) == (
        "INSUFFICIENT_SAMPLE"
    )


def test_evaluate_accepts_a_real_edge_and_rejects_a_coin_flip():
    sample = rows(1200, 0.62, family="liquidity_sweep") + rows(1200, 0.33, family="range_rejection", seed=2)
    report = evaluate(sample, COST)
    assert report["rules_tried"] == 4 and report["z_required"] > 2
    assert report["segments"]["SWING|liquidity_sweep|as_published"]["verdict"] == "ACCEPTED"
    # 33% at 2R is break-even before costs: no directional edge, so not accepted.
    assert report["segments"]["SWING|range_rejection|as_published"]["verdict"] == "REJECTED"
    # The mirror of a losing direction can win; the mirror of a winner cannot.
    assert report["segments"]["SWING|liquidity_sweep|mirrored"]["verdict"] == "REJECTED"
    assert report["accepted"] == [
        "SWING|liquidity_sweep|as_published",
        "SWING|range_rejection|mirrored",
    ]


def test_grades_fall_back_to_rule_expectancy_without_model_skill():
    model = fit_grades(rows(1800, 0.30), COST)
    assert model["skill"] is False
    rule = model["rules"]["SWING|liquidity_sweep"]
    assert rule["mean_r"] < 0 and rule["grade"] in {"D", "E", "F"}


def test_grades_use_the_model_when_it_ranks_out_of_sample():
    model = fit_grades(rows(3000, 0.40, edge_feature=True), COST)
    assert model["skill"] is True
    table = model["calibration"]
    assert table[-1]["mean_r"] > table[0]["mean_r"] + 0.3
    assert acceptance.TIERS.index(table[-1]["grade"]) < acceptance.TIERS.index(table[0]["grade"])


def test_runtime_lookups_fail_closed(signal):
    signal.horizon_profile, signal.family = "SWING", "liquidity_sweep"
    assert rule_evidence(signal, {})["verdict"] == "NOT_EVALUATED"
    report = evaluate(rows(1200, 0.62), COST)
    found = rule_evidence(signal, report)
    assert found["verdict"] == "ACCEPTED" and found["n"] == 1200 and found["hit_rate"] > 0.55
    assert outcome_grade(signal, {})["grade"] == "F"
    model = fit_grades(rows(1800, 0.30), COST)
    graded = outcome_grade(signal, model)
    assert graded["basis"] == "RULE_EXPECTANCY" and graded["expected_r"] < 0
    signal.family = "breakout_retest"
    assert outcome_grade(signal, model)["basis"] == "RULE_NOT_EVALUATED"


def test_features_are_direction_aware(signal):
    signal.evidence = dict(
        context_features=dict(regime="trending up", efficiency=0.4, slope=0.2),
        setup_features=dict(atr=2.0, ma20=98.0, slope=-0.1, efficiency=0.3, realized_volatility=0.01),
    )
    long = outcome_features(signal)
    assert long["trend_alignment"] == 1.0 and long["stop_atr"] == 2.5 and long["planned_rr"] == 3.0
    assert long["ma_gap"] == 1.0 and long["setup_slope"] == -0.1 and long["family_trend_pullback"] == 1.0
    short = outcome_features(signal.model_copy(update={"direction": "SHORT", "stop": 105, "tp1": 85}))
    assert short["trend_alignment"] == -1.0 and short["ma_gap"] == -1.0 and short["context_slope"] == -0.2


@pytest.mark.parametrize(
    "gate,verdict_value,expected",
    [
        ("enforce", "REJECTED", "blocked:RULE_NOT_ACCEPTED"),
        ("enforce", "NOT_EVALUATED", "blocked:RULE_NOT_ACCEPTED"),
        ("enforce", "ACCEPTED", "sent"),
        ("report", "REJECTED", "sent"),
        ("off", "REJECTED", "sent"),
    ],
)
async def test_gate_modes(settings, signal, notification_clock, monkeypatch, gate, verdict_value, expected):
    cfg = settings.model_copy(
        update={
            "research_alerts": True,
            "acceptance_gate": gate,
            "research_webhook": SecretStr("https://discord.com/api/webhooks/mock/test"),
        }
    )
    evidence = dict(verdict=verdict_value, hit_rate=0.49, n=6351, plan_net_r=-0.32)
    monkeypatch.setattr("bybit_flow.acceptance.rule_evidence", lambda s: evidence)
    monkeypatch.setattr(
        "bybit_flow.acceptance.outcome_grade", lambda s: dict(grade="E", expected_r=-0.32, lower_r=-0.4)
    )
    signal.state = "CONFIRMED"
    store = Store(settings.data_dir)
    store.signal(signal)
    sent = []

    def handle(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "mock-id"})

    notifier = Notifier(cfg, store, httpx.MockTransport(handle))
    assert await notifier.send_research(signal) == expected
    assert bool(sent) == (expected == "sent")
    if sent and gate != "off":
        field = sent[0]["embeds"][0]["fields"][-1]
        assert field["name"] == "Proven record of this rule"
        if verdict_value == "REJECTED":
            assert "NOT PROVEN: 49.0% reached +1R before -1R on 6,351" in field["value"]
            assert "Outcome grade E (expected -0.32R per trade)" in field["value"]
        assert signal.evidence["outcome_grade"]["grade"] == "E"
    if gate == "off":
        assert "rule_evidence" not in signal.evidence


@pytest.mark.parametrize("grade,expected", [("E", "blocked:BELOW_MIN_ALERT_TIER"), ("B", "sent")])
async def test_outcome_grade_can_decide_delivery(
    settings, signal, notification_clock, monkeypatch, grade, expected
):
    cfg = settings.model_copy(
        update={
            "research_alerts": True,
            "acceptance_gate": "off",
            "grade_source": "outcome",
            "min_alert_tier": "C",
            "research_webhook": SecretStr("https://discord.com/api/webhooks/mock/test"),
        }
    )
    monkeypatch.setattr(
        "bybit_flow.acceptance.rule_evidence",
        lambda s: dict(verdict="REJECTED", hit_rate=0.5, n=10, plan_net_r=0.0),
    )
    monkeypatch.setattr("bybit_flow.acceptance.outcome_grade", lambda s: dict(grade=grade, expected_r=0.1))
    signal.raw_tier, signal.state = "A", "CONFIRMED"  # The designed score alone would pass.
    store = Store(settings.data_dir)
    store.signal(signal)
    notifier = Notifier(cfg, store, httpx.MockTransport(lambda r: httpx.Response(200, json={"id": "1"})))
    assert await notifier.send_research(signal) == expected


def test_defaults_report_without_blocking():
    from bybit_flow.config import Settings

    cfg = Settings(_env_file=None)
    assert cfg.acceptance_gate == "report" and cfg.grade_source == "setup_score"
