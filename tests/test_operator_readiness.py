from copy import deepcopy

from bybit_flow.ml.operator_readiness import project


def test_saved_model_does_not_imply_compatibility_or_authority():
    summary = dict(
        status="COLLECTING", summary_at_ms=1000, models=[dict(promotion_reasons=["verified costs required"])]
    )
    before = deepcopy(summary)
    stages = {r["stage"]: r for r in project(summary, now=2000)["stages"]}
    assert stages["Advisory compatibility"]["state"] == "NOT_ESTABLISHED"
    assert stages["Authority"]["state"] == "DETERMINISTIC"
    assert "verified costs" in stages["Production validation"]["detail"]
    assert summary == before


def test_live_heartbeat_cannot_make_old_data_summary_current():
    result = project(dict(status="WORKING", summary_at_ms=1000), now=1_000_000)
    assert result["stale"]


def test_stale_worker_does_not_present_readiness_as_current():
    result = project(dict(status="STALE", summary_at_ms=1000, latest_compatible_challenger="old-model"))
    assert result["stale"]
    assert next(r for r in result["stages"] if r["stage"] == "Advisory compatibility")["state"] == "STALE"
