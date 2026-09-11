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
    from .labels import label_recordings
    from .recordings import worker_rows
    from .training import train

    if not store.db.execute("SELECT 1 FROM segments LIMIT 1").fetchone():
        raise ValueError("No verified real recording segments; weekly training abstains")
    label_recordings(store, worker_rows(store), settings)
    source = store.get("runtime_health", {}).get("source")
    path = FeatureStore(store).export(now_ms(), source=source)
    return train(store, path)["id"]


def monitor(settings, store):
    """Resolve paper labels and check drift independently of the weekly fit cadence."""
    from .drift import check
    from .labels import label_recordings
    from .recordings import worker_rows

    paths = store.db.execute("SELECT 1 FROM segments LIMIT 1").fetchone()
    result = dict(at_ms=now_ms(), status="no-recordings")
    if paths:
        labels = label_recordings(store, worker_rows(store), settings)
        result.update(status="observed", complete=labels["complete"])
    ident = store.get("ml_champion")
    if ident:
        result["drift"] = check(store, ident)
        if result["drift"]["status"] == "degraded":
            from ..notifications import Notifier

            asyncio.run(Notifier(settings, store).send_operational("drift", result["drift"]))
    store.put("ml_monitor", result)
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
    train.add_argument("--model", choices=("both", "logistic", "lightgbm"), default="both")
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
        from .training import train

        model = train(
            store,
            args.dataset,
            kinds=("logistic", "lightgbm") if args.model == "both" else (args.model,),
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

        with (store.root / "ml-worker.lock").open("a+") as lock:
            if os.name == "nt":
                import msvcrt

                lock.write("0")
                lock.flush()
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            while True:
                if now_ms() - store.get("ml_monitor", {}).get("at_ms", 0) >= 900_000:
                    try:
                        monitor(settings, store)
                    except (ValueError, OSError) as exc:
                        store.put("ml_monitor", dict(at_ms=now_ms(), status="abstained", reason=str(exc)))
                previous_cycle = store.get("ml_cycle", {})
                previous = previous_cycle.get("at_ms", 0)
                # Recheck data readiness daily; successful fits remain weekly.
                cadence = (7 if previous_cycle.get("status") == "challenger" else 1) * 86_400_000
                if args.command == "cycle" or now_ms() - previous >= cadence:
                    try:
                        result = dict(at_ms=now_ms(), status="challenger", model_id=cycle(settings, store))
                    except (ValueError, OSError) as exc:
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
