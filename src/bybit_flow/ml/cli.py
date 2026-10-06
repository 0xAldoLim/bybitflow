"""Training runs only in an explicit CLI/worker process, never on the ingestion event loop."""

import argparse
import asyncio
import json
import time
from pathlib import Path

from ..storage import now_ms
from .registry import Registry
from .store import FeatureStore


def cycle(settings, store, source_override=None):
    from .labels import label_recordings
    from .recordings import worker_rows
    from .training import train

    if not store.db.execute("SELECT 1 FROM segments LIMIT 1").fetchone():
        raise ValueError("No verified real recording segments; weekly training abstains")
    runtime = store.get("runtime_health", {})
    source = source_override or (
        (runtime.get("source") if 0 <= now_ms() - runtime.get("at_ms", 0) <= 90000 else None)
        or store.get("active_exchange", {}).get("current")
        or store.get("scanner", {}).get("exchange")
    )
    if source not in {"bybit", "binance", "okx"}:
        raise ValueError("No recorded active venue for source-specific training")
    # Materialize causal outcomes before choosing a model family. Missing LSTM
    # sequences must not prevent tabular training on otherwise complete labels.
    recent = store.get("ml_monitor", {}).get("last_success_ms") or 0
    if now_ms() - recent > 900000:
        label_recordings(store, worker_rows(store), settings)
    from .operations import trainability

    readiness = trainability(store)["by_source"]
    ready_sources = [name for name, detail in readiness.items() if detail["baseline_ready"]]
    if ready_sources and source not in ready_sources and not source_override:
        source = max(ready_sources, key=lambda name: readiness[name]["baseline_trainable"])
    ready = readiness[source]["sequence_ready_16"] if settings.ml_two_stage else 0
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
    from .labels import close_pre_retention_decisions, label_recordings
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
    result["pre_retention_closed"] = close_pre_retention_decisions(store)
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

    # Reclaim consumed recordings gradually while under budget. Pressure cleanup
    # remains unbounded so it can restore the recorder before it drops more data.
    result["retention"] = prune_recordings(store, settings, force=True, max_reclaim_bytes=256_000_000)
    from ..storage import directory_bytes

    if directory_bytes(store.root) >= settings.max_storage_gb * 1e9 * 0.95:
        result.update(
            status="STORAGE_BACKPRESSURE",
            reason="Checkpoint/pruning completed; bootstrap and fitting deferred",
        )
        store.put("ml_monitor", result)
        return result
    from .bootstrap import backfill, readiness

    result["bootstrap"] = {
        source: asyncio.run(backfill(store, settings, source, limit=100, resume=True))
        for source in ("binance", "bybit", "okx")
    }
    store.put("ml_bootstrap_trainability", readiness(store))
    from .horizon import fit as fit_horizon

    result["horizon_model"] = fit_horizon(store, settings, now_ms())
    from .policy_research import run as policy_research

    result["score_profile_research"] = policy_research(store, now_ms())
    from .operations import trainability

    store.put("ml_trainability_cache", trainability(store))
    from .registry import Registry

    store.put("ml_summary_cache", Registry(store).summary() | dict(summary_at_ms=now_ms()))
    return result


def fit_due(previous, count, now, manual=False):
    """First ready fit is immediate; later fits need new information or age."""
    if count < 500:
        return False
    if manual:
        return True
    if previous.get("status") != "challenger" and not previous.get("model_id"):
        return count != previous.get("attempted_count") or now - previous.get("at_ms", 0) >= 900_000
    if (
        previous.get("status") == "abstained"
        and count == previous.get("attempted_count")
        and now - previous.get("at_ms", 0) < 900_000
    ):
        return False
    return (
        count - previous.get("trained_count", 0) >= 50
        or now - previous.get("last_success_ms", previous.get("at_ms", 0)) >= 7 * 86_400_000
    )


def train_ready(settings, store, manual=False):
    from ..storage import directory_bytes
    from .bootstrap import train as bootstrap_train

    if directory_bytes(store.root) >= settings.max_storage_gb * 1e9 * 0.95:
        return dict(at_ms=now_ms(), status="STORAGE_BACKPRESSURE")
    results = {}
    primary = store.get("ml_trainability_cache", {}).get("by_source", {})
    bootstrap = store.get("ml_bootstrap_trainability", {})
    for track, counts in (("primary", primary), ("bootstrap", bootstrap)):
        for source, detail in counts.items():
            count = detail.get("baseline_trainable" if track == "primary" else "trainable", 0)
            from . import SCHEMA_VERSION

            schema, policy = (
                (SCHEMA_VERSION, "prints-v1") if track == "primary" else ("bootstrap-core-v1", "ohlc-path-v1")
            )
            key = f"ml_fit:{track}:{source}:{schema}:{policy}"
            previous = store.get(key, {})
            if not fit_due(previous, count, now_ms(), manual):
                continue
            result = dict(
                previous,
                at_ms=now_ms(),
                track=track,
                source=source,
                schema=schema,
                policy=policy,
                attempted_count=count,
            )
            try:
                ident = (
                    cycle(settings, store, source)
                    if track == "primary"
                    else bootstrap_train(store, source)["id"]
                )
                result.update(
                    status="challenger", model_id=ident, trained_count=count, last_success_ms=now_ms()
                )
                result.pop("reason", None)
            except Exception as exc:
                result.update(status="abstained", reason=str(exc))
            plan = store.get(f"ml_partition_feasibility:{track}:{source}:{schema}:{policy}")
            if plan:
                result["partition_feasibility"] = plan
            store.put(key, result)
            results[track + ":" + source] = result
    state = (
        "challenger"
        if any(r["status"] == "challenger" for r in results.values())
        else "abstained"
        if results
        else "collecting"
    )
    result = dict(at_ms=now_ms(), status=state, tracks=results)
    reasons = [
        f"{track}: {detail['reason']}"
        for track, detail in results.items()
        if detail["status"] == "abstained" and detail.get("reason")
    ]
    if reasons:
        result["reason"] = "; ".join(reasons)
    return result


def worker_step(settings, store, first_monitor=False, manual=False):
    """Writer contention defers a worker iteration without a restart loop."""
    import sqlite3
    import sys

    try:
        store.put("ml_worker_state", dict(at_ms=now_ms(), status="RUNNING"))
        if first_monitor or now_ms() - store.get("ml_monitor", {}).get("at_ms", 0) >= 900_000:
            try:
                monitor(settings, store)
            except sqlite3.OperationalError:
                raise
            except Exception as exc:
                store.put("ml_monitor", dict(at_ms=now_ms(), status="abstained", reason=str(exc)))
        result = train_ready(settings, store, manual)
        if result.get("tracks") or manual or result["status"] == "STORAGE_BACKPRESSURE":
            store.put("ml_cycle", result)
        return result
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
            raise
        print("ML worker deferred: SQLite writer busy; retrying next loop", file=sys.stderr, flush=True)
        return dict(at_ms=now_ms(), status="DEFERRED_SQLITE_BUSY")


def run(arguments, settings, store):
    parser = argparse.ArgumentParser(description="BybitFlow offline ML research, never live execution")
    sub = parser.add_subparsers(dest="command", required=True)
    label = sub.add_parser("label")
    label.add_argument("paths", type=Path, nargs="+")
    label.add_argument("--stage", choices=("generation", "decision"), default="decision")
    export = sub.add_parser("export")
    export.add_argument("--stage", choices=("generation", "decision"), default="decision")
    export.add_argument("--source", choices=("binance", "bybit", "okx", "tradingview"))
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
    for name in ("bootstrap-labels", "bootstrap-export", "bootstrap-train"):
        command = sub.add_parser(name)
        command.add_argument("--source", choices=("binance", "bybit", "okx"), required=True)
        if name == "bootstrap-labels":
            command.add_argument("--limit", type=int, default=100)
            command.add_argument("--resume", action="store_true")
            command.add_argument("--dry-run", action="store_true")
    drift = sub.add_parser("drift")
    drift.add_argument("model_id")
    args = parser.parse_args(arguments)
    registry = Registry(store)
    if args.command.startswith("bootstrap-"):
        from . import bootstrap

        if args.command == "bootstrap-labels":
            result = asyncio.run(
                bootstrap.backfill(store, settings, args.source, args.limit, args.resume, args.dry_run)
            )
        elif args.command == "bootstrap-export":
            result = str(bootstrap.export(store, args.source))
        else:
            result = {k: v for k, v in bootstrap.train(store, args.source).items() if k != "model"}
    elif args.command == "label":
        from ..replay import segment_rows
        from .labels import label_recordings

        result = label_recordings(store, segment_rows(args.paths), settings, args.stage)
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

        model = registry.get(args.model_id)
        rows = read_dataset(args.dataset, track=model.get("track", "primary"))
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
                if not settings.ml_enabled:
                    result = dict(at_ms=now_ms(), status="DISABLED")
                    store.put("ml_worker_state", result)
                    if args.command == "cycle":
                        break
                    time.sleep(30)
                    continue
                result = worker_step(settings, store, first_monitor, args.command == "cycle")
                first_monitor = result["status"] == "DEFERRED_SQLITE_BUSY"
                if args.command == "cycle":
                    break
                time.sleep(30)
    else:
        result = registry.summary(enabled=settings.ml_enabled)
    print(json.dumps(result, indent=2, allow_nan=False))
