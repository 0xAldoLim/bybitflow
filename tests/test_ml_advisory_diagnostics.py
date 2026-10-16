import json

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.ml.features import snapshot
from bybit_flow.ml.inference import apply, select_compatible
from bybit_flow.ml.registry import Registry
from bybit_flow.ml.store import canonical, digest
from bybit_flow.notifications import Notifier
from bybit_flow.storage import Store


def manifest(signal, track="bootstrap"):
    return dict(
        id="b" * 32,
        created_ms=100,
        track=track,
        feature_schema_version="bootstrap-core-v1" if track == "bootstrap" else signal.feature_schema_version,
        label_policy="ohlc-path-v1" if track == "bootstrap" else "prints-v1",
        source=signal.source,
        stage="decision",
        strategy_versions=[signal.version],
        periods={"holdout": {"end": 100}},
        minimum_coverage=0,
        contexts={},
        required_features=[],
        # Fixed JSON fixture, not a fitted model: its existing inference is exactly 0.5.
        model=dict(
            kind="logistic",
            vocabulary={"net_rr": 0},
            excluded=[],
            medians=[0],
            indicators=[],
            mean=[0],
            scale=[1],
            coef=[0],
            intercept=0,
            transformed_names=["net_rr"],
            thresholds=dict(min_probability=0.5, min_quality=65),
        ),
    )


def save_model(store, model):
    with store.db:
        store.db.execute(
            "INSERT INTO ml_models VALUES(?,?,?,?)",
            (model["id"], model["created_ms"], canonical(model), digest(model)),
        )


def test_valid_bootstrap_with_unsupported_live_schema_is_not_corrupt(settings, signal):
    store = Store(settings.data_dir)
    model = manifest(signal)
    save_model(store, model)
    registry = Registry(store)
    assert registry.get(model["id"]) == model  # Real decoding and checksum validation.

    selected, champion, skipped = select_compatible(
        registry, signal, snapshot(signal, 2000, "decision"), 2000
    )

    assert selected is None and not champion
    assert skipped[0]["track"] == "bootstrap"
    assert skipped[0]["reason_codes"] == ["FEATURE_SCHEMA_UNSUPPORTED"]
    assert "corrupt" not in " ".join(skipped[0]["reasons"]).lower()
    store.close()


def test_checksum_mismatch_is_artifact_corruption(settings, signal):
    store = Store(settings.data_dir)
    model = manifest(signal)
    save_model(store, model)
    with store.db:
        store.db.execute("UPDATE ml_models SET sha256=? WHERE id=?", ("0" * 64, model["id"]))

    selected, champion, skipped = select_compatible(
        Registry(store), signal, snapshot(signal, 2000, "decision"), 2000
    )

    assert selected is None and not champion
    assert skipped[0]["reason_codes"] == ["ARTIFACT_CORRUPT"]
    assert skipped[0]["track"] == "bootstrap"
    store.close()


@pytest.mark.parametrize("missing", [False, True])
def test_unreadable_or_missing_registry_artifact(settings, signal, missing):
    store = Store(settings.data_dir)
    model = manifest(signal)
    save_model(store, model)
    store.put("ml_champion", model["id"])
    with store.db:
        if missing:
            store.db.execute("DELETE FROM ml_models WHERE id=?", (model["id"],))
        else:
            store.db.execute("UPDATE ml_models SET manifest='not JSON' WHERE id=?", (model["id"],))

    selected, champion, skipped = select_compatible(
        Registry(store), signal, snapshot(signal, 2000, "decision"), 2000
    )

    assert selected is None and not champion
    assert skipped[0]["reason_codes"] == ["ARTIFACT_UNREADABLE"]
    store.close()


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"feature_schema_version": "candidate-v19"}, "SCHEMA_INCOMPATIBLE"),
        ({"contexts": {"family": ["different-family"]}}, "MODEL_CONTEXT_INCOMPATIBLE"),
        ({"source": "okx"}, "MODEL_CONTEXT_INCOMPATIBLE"),
    ],
)
def test_readable_model_compatibility_reasons(settings, signal, changes, code):
    store = Store(settings.data_dir)
    save_model(store, manifest(signal, "primary") | changes)
    selected, champion, skipped = select_compatible(
        Registry(store), signal, snapshot(signal, 2000, "decision"), 2000
    )
    assert selected is None and not champion
    assert skipped[0]["reason_codes"] == [code]
    assert "artifact" not in " ".join(skipped[0]["reasons"]).lower()
    store.close()


@pytest.mark.parametrize("track,schema", [("bootstrap", "candidate-v10"), ("primary", "candidate-v20")])
def test_compatible_model_keeps_existing_inference(settings, signal, track, schema):
    store = Store(settings.data_dir)
    settings.ml_enabled = True
    signal.feature_schema_version = schema
    signal.quality = 80
    signal.risk = dict(accepted=True, net_rr=2.5)
    model = manifest(signal, track)
    save_model(store, model)
    original = signal.quality, signal.entry, signal.stop, signal.tp1, signal.final_tier, list(signal.gates)

    apply(signal, settings, store, 2000)

    assert signal.model_version == model["id"]
    assert signal.validation_status == (
        "bootstrap_challenger" if track == "bootstrap" else "research_challenger"
    )
    assert signal.evidence["ml"]["research_score"] == 50
    assert signal.evidence["ml"]["research_acceptance"] is True
    assert signal.calibrated_probability is None
    prediction = store.db.execute("SELECT probability,accepted FROM ml_predictions").fetchone()
    assert tuple(prediction) == (0.5, 1)
    assert store.get("ml_advisory_status")["skipped"] == []
    assert original == (
        signal.quality,
        signal.entry,
        signal.stop,
        signal.tp1,
        signal.final_tier,
        signal.gates,
    )
    store.close()


@pytest.mark.asyncio
async def test_incompatible_bootstrap_cannot_block_deterministic_delivery(
    settings, signal, notification_clock
):
    store = Store(settings.data_dir)
    settings.ml_enabled = settings.ml_filter_research = settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/offline-fixture")
    signal.state, signal.quality, signal.raw_tier = "CONFIRMED", 80, "A"
    signal.risk = dict(accepted=True, net_rr=2.5)
    save_model(store, manifest(signal))
    original = signal.model_dump()

    apply(signal, settings, store, 2000)

    for field in (
        "state",
        "quality",
        "raw_tier",
        "final_tier",
        "risk",
        "gates",
        "entry",
        "zone",
        "stop",
        "tp1",
        "tp2",
    ):
        assert signal.model_dump()[field] == original[field]
    assert signal.validation_status == "abstained" and signal.calibrated_probability is None
    assert not store.db.execute("SELECT 1 FROM ml_predictions").fetchone()
    assert store.get("ml_advisory_status")["skipped"][0]["reason_codes"] == ["FEATURE_SCHEMA_UNSUPPORTED"]

    requests = []

    def delivered(request):
        requests.append(request)
        return httpx.Response(200, json={"id": "offline-message"})

    store.signal(signal)
    notifier = Notifier(settings, store, httpx.MockTransport(delivered))
    assert await notifier.send_research(signal) == "sent"
    assert len(requests) == 1
    assert json.loads(requests[0].content)["embeds"][0]["title"].startswith("NEW SETUP")
    assert notifier.was_initially_delivered(signal.id, "research")
    store.close()
