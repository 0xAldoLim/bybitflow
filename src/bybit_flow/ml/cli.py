"""Training runs only in an explicit CLI/worker process, never on the ingestion event loop."""

import argparse
import asyncio
import json
import time
from pathlib import Path

from ..storage import now_ms
from .registry import Registry
from .store import FeatureStore


def cycle(settings, store):
    from . import SCHEMA_VERSION
    from .labels import label_recordings
    from .recordings import worker_rows
    from .training import train

    if not store.db.execute("SELECT 1 FROM segments LIMIT 1").fetchone():
        raise ValueError("No verified real recording segments; weekly training abstains")
    source = store.get("active_exchange", {}).get("current") or store.get("scanner", {}).get("exchange")
    if source not in {"bybit", "binance", "okx"}:
        raise ValueError("No recorded active venue for source-specific training")
    # Materialize causal outcomes before choosing a model family. Missing LSTM
    # sequences must not prevent tabular training on otherwise complete labels.
    recent = store.get("ml_monitor", {}).get("last_success_ms") or 0
    if now_ms() - recent > 900000:
        label_recordings(store, worker_rows(store), settings)
    ready = 0
    if settings.ml_two_stage:
        ready = store.db.execute(
            "SELECT COUNT(DISTINCT coalesce(c.candidate_identity,s.signal_id)) FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
            "WHERE s.stage='decision' AND l.policy='prints-v1' "
            "AND json_extract(l.payload,'$.complete')=1 AND json_array_length(s.payload,'$.sequence')=16 "
            "AND json_extract(s.payload,'$.source')=? AND s.schema_version=?",
            (source, SCHEMA_VERSION),
        ).fetchone()[0]
    from .stacking import KINDS

    two_stage = settings.ml_two_stage and ready >= 500
    store.put(
        "ml_training_mode",
        dict(
            at_ms=now_ms(),
            source=source,
            requested_two_stage=settings.ml_two_stage,
            sequences_ready=ready,
            mode="TWO_STAGE" if two_stage else "BASELINE",
            kinds=list(KINDS) if two_stage else ["logistic", "lightgbm"],
        ),
    )
    path = FeatureStore(store).export(now_ms(), source=source)
    return train(store, path, kinds=KINDS if two_stage else ("logistic", "lightgbm"))["id"]


def monitor(settings, store):
    """Resolve paper labels and check drift independently of the weekly fit cadence."""
    from .drift import check
    from .labels import label_recordings
    from .recordings import worker_rows

    previous = store.get("ml_monitor", {})
    store.put(
        "ml_monitor",
        dict(
            at_ms=now_ms(),
            status="materializing",
            previous_status=previous.get("status"),
            last_success_ms=previous.get(
                "last_success_ms", previous.get("at_ms") if previous.get("status") == "observed" else None
            ),
        ),
    )
    paths = store.db.execute("SELECT 1 FROM segments LIMIT 1").fetchone()
    result = dict(at_ms=now_ms(), status="no-recordings")
    if settings.ml_two_stage:
        store.put(
            "ml_pipeline",
            dict(
                stage1=["LightGBM", "Random Forest", "LSTM"],
                stage2=["Logistic Regression", "SVM", "Random Forest probability"],
                status="collecting outcomes and 16-observation sequences",
                filtering_research=settings.ml_filter_research,
            ),
        )
    if paths:
        pending = store.db.execute(
            "SELECT 1 FROM ml_snapshots s WHERE s.stage='decision' AND s.decision_ms>? AND NOT EXISTS "
            "(SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy='prints-v1') LIMIT 1",
            (store.get("recording_retention", {}).get("through_ms", 0),),
        ).fetchone()
        labels = (
            label_recordings(
                store,
                worker_rows(store, after_ms=store.get("primary_materialization", {}).get("cursor_ms")),
                settings,
                observed_until_ms=now_ms(),
                incremental=True,
            )
            if pending
            else {"complete": 0}
        )
        result.update(status="observed", complete=labels["complete"])
    ident = store.get("ml_champion")
    if ident:
        result["drift"] = check(store, ident)
        if result["drift"]["status"] == "degraded":
            from ..notifications import Notifier

            asyncio.run(Notifier(settings, store).send_operational("drift", result["drift"]))
    if result["status"] == "observed":
        result["last_success_ms"] = now_ms()
    store.put("ml_monitor", result)
    from ..retention import prune_recordings

    result["retention"] = prune_recordings(store, settings)
    from .horizon import fit as fit_horizon

    result["horizon_model"] = fit_horizon(store, settings, now_ms())
    from .policy_research import run as policy_research

    result["score_profile_research"] = policy_research(store, now_ms())
    from .registry import Registry

    store.put("ml_summary_cache", Registry(store).summary() | dict(summary_at_ms=now_ms()))
    return result


def run(arguments, settings, store):
    parser = argparse.ArgumentParser(description="BybitFlow offline ML research, never live execution")
    sub = parser.add_subparsers(dest="command", required=True)
    label = sub.add_parser("label")
    label.add_argument("paths", type=Path, nargs="+")
    label.add_argument("--stage", choices=("generation", "decision"), default="decision")
    export = sub.add_parser("export")
    export.add_argument("--stage", choices=("generation", "decision"), default="decision")
    export.add_argument("--source", choices=("binance", "bybit", "okx", "tradingview"))
    chart = sub.add_parser("chart-label")
    chart.add_argument("events", type=Path)
    chart.add_argument("candles", type=Path)
    chart.add_argument("--symbol", required=True)
    sub.add_parser("chart-export")
    train = sub.add_parser("train")
    train.add_argument("dataset", type=Path)
    train.add_argument(
        "--model",
        choices=("both", "logistic", "lightgbm", "random_forest", "svm", "two-stage"),
        default="both",
    )
    train.add_argument("--calibration", choices=("sigmoid", "isotonic"), default="sigmoid")
    infer = sub.add_parser("infer")
    infer.add_argument("model_id")
    infer.add_argument("dataset", type=Path)
    promote = sub.add_parser("promote")
    promote.add_argument("model_id")
    promote.add_argument("--reviewer", required=True)
    rollback = sub.add_parser("rollback")
    rollback.add_argument("--reviewer", required=True)
    sub.add_parser("status")
    sub.add_parser("cycle")
    sub.add_parser("worker")
    drift = sub.add_parser("drift")
    drift.add_argument("model_id")
    args = parser.parse_args(arguments)
    registry = Registry(store)
    if args.command == "label":
        from ..replay import segment_rows
        from .labels import label_recordings

        result = label_recordings(store, segment_rows(args.paths), settings, args.stage)
    elif args.command == "chart-label":
        from .labels import label_chart

        result = label_chart(store, args.events, args.candles, settings, args.symbol)
    elif args.command == "chart-export":
        result = str(FeatureStore(store).export(now_ms(), policy="chart-v1", stage="chart"))
    elif args.command == "export":
        result = str(FeatureStore(store).export(now_ms(), stage=args.stage, source=args.source))
    elif args.command == "train":
        from .stacking import KINDS
        from .training import train

        model = train(
            store,
            args.dataset,
            kinds=KINDS
            if args.model == "two-stage"
            else (("logistic", "lightgbm") if args.model == "both" else (args.model,)),
            calibration=args.calibration,
        )
        result = dict(
            model_id=model["id"],
            status="challenger",
            validated=False,
            report=model["report"],
            note="No automatic deployment; inspect the model card",
        )
    elif args.command == "infer":
        from .models import explain, predict
        from .training import read_dataset

        model, rows = registry.get(args.model_id), read_dataset(args.dataset)
        result = dict(
            status="OFFLINE RESEARCH, NOT VALIDATED PROBABILITY",
            model_id=model["id"],
            research_predictions=predict(model["model"], rows).tolist(),
            explanation=explain(model["model"], rows[-1]),
        )
    elif args.command == "drift":
        from .drift import check

        result = check(store, args.model_id)
        if result["status"] == "degraded":
            from ..notifications import Notifier

            asyncio.run(Notifier(settings, store).send_operational("drift", result))
    elif args.command == "promote":
        registry.promote(args.model_id, args.reviewer)
        result = dict(champion=args.model_id)
    elif args.command == "rollback":
        registry.rollback(args.reviewer)
        result = registry.summary()
    elif args.command in {"cycle", "worker"}:
        # Separate-process advisory lock prevents duplicate trainers and holdout races.
        import os
        from contextlib import ExitStack

        from .operations import heartbeat

        with (store.root / "ml-worker.lock").open("a+") as lock, ExitStack() as background:
            if os.name == "nt":
                import msvcrt

                lock.write("0")
                lock.flush()
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            background.enter_context(heartbeat(store.root))
            first_monitor = True
            while True:
                if first_monitor or now_ms() - store.get("ml_monitor", {}).get("at_ms", 0) >= 900_000:
                    first_monitor = False
                    try:
                        monitor(settings, store)
                    except Exception as exc:
                        store.put("ml_monitor", dict(at_ms=now_ms(), status="abstained", reason=str(exc)))
                previous_cycle = store.get("ml_cycle", {})
                previous = previous_cycle.get("at_ms", 0)
                # Recheck data readiness daily; successful fits remain weekly.
                cadence = (
                    7 * 86_400_000
                    if previous_cycle.get("status") == "challenger"
                    else (900_000 if settings.ml_two_stage else 86_400_000)
                )
                if args.command == "cycle" or now_ms() - previous >= cadence:
                    try:
                        result = dict(at_ms=now_ms(), status="challenger", model_id=cycle(settings, store))
                    except Exception as exc:
                        result = dict(at_ms=now_ms(), status="abstained", reason=str(exc))
                    store.put("ml_cycle", result)
                    if settings.ops_webhook.get_secret_value():
                        from ..notifications import Notifier

                        asyncio.run(Notifier(settings, store).send_operational("weekly-cycle", result))
                if args.command == "cycle":
                    break
                time.sleep(30)
    else:
        result = registry.summary()
    print(json.dumps(result, indent=2, allow_nan=False))
